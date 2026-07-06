package chaos

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "kronos/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Scenario 1 — worker crash. SIGKILL the worker mid-job: its process and TCP
// stream die together, so the scheduler learns instantly via stream error.
// The job must be re-queued and complete on the surviving worker.
func TestWorkerCrashMidJob(t *testing.T) {
	t.Parallel()
	addr := freeAddr(t)
	startScheduler(t, addr)
	workers := map[string]*proc{
		"crash-a": startWorker(t, "crash-a", addr),
		"crash-b": startWorker(t, "crash-b", addr),
	}
	c := dial(t, addr)

	job := submitShell(t, c, "/bin/sleep 2; echo survived", nil)
	victim := waitFor(t, c, job, 5*time.Second, assigned, "assignment").WorkerId
	workers[victim].kill()
	t.Logf("killed %s mid-job", victim)

	st := waitFor(t, c, job, 15*time.Second, succeeded, "completion after worker crash")
	if st.WorkerId == victim {
		t.Fatalf("job reported complete on the dead worker %s", victim)
	}
	if st.Output != "survived" {
		t.Fatalf("output = %q, want %q", st.Output, "survived")
	}
}

// Scenario 2 — network partition. SIGSTOP freezes the worker: heartbeats stop
// but the TCP stream stays open, so only the reaper can detect it. The job
// must be re-queued after the TTL and complete elsewhere. Then the partition
// heals (SIGCONT): the zombie's stale result must not corrupt job state, the
// duplicate execution is visible (at-least-once semantics), and the healed
// worker must rejoin the pool and take new work.
func TestWorkerPartitionAndHeal(t *testing.T) {
	t.Parallel()
	addr := freeAddr(t)
	startScheduler(t, addr)
	workers := map[string]*proc{
		"part-a": startWorker(t, "part-a", addr),
		"part-b": startWorker(t, "part-b", addr),
	}
	c := dial(t, addr)

	runLog := filepath.Join(t.TempDir(), "runs")
	job := submitShell(t, c, `echo ran >> "$OUT"; /bin/sleep 2; echo done`,
		map[string]string{"OUT": runLog})
	victim := waitFor(t, c, job, 5*time.Second, assigned, "assignment").WorkerId
	workers[victim].pause()
	t.Logf("partitioned %s mid-job", victim)

	st := waitFor(t, c, job, 15*time.Second, succeeded, "completion on the surviving worker")
	if st.WorkerId == victim {
		t.Fatalf("job reported complete on the partitioned worker %s", victim)
	}
	survivor := st.WorkerId

	// Heal the partition. The zombie finishes its copy of the job and reports
	// a stale result; the job must stay SUCCEEDED and both runs must be visible.
	workers[victim].resume()
	waitRuns(t, runLog, 2)
	st = waitFor(t, c, job, 2*time.Second, succeeded, "state after stale result")
	if !succeeded(st) {
		t.Fatalf("stale result from healed worker corrupted job state: %v", st.State)
	}

	// The healed worker must be schedulable again: remove the survivor so it
	// is the only worker left, then submit new work.
	workers[survivor].kill()
	job2 := submitShell(t, c, "echo back", nil)
	st = waitFor(t, c, job2, 10*time.Second, succeeded, "new job on the healed worker")
	if st.WorkerId != victim {
		t.Fatalf("job2 ran on %q, want healed worker %q", st.WorkerId, victim)
	}
}

// Scenario 3 — scheduler crash. Workers must reconnect on their own and new
// jobs must schedule. Completed history is lost (state is in-memory by
// design); the old job ID must return NotFound, not a stale answer.
func TestSchedulerRestart(t *testing.T) {
	t.Parallel()
	addr := freeAddr(t)
	sched := startScheduler(t, addr)
	startWorker(t, "restart-a", addr)
	startWorker(t, "restart-b", addr)
	c := dial(t, addr)

	job1 := submitShell(t, c, "echo one", nil)
	waitFor(t, c, job1, 10*time.Second, succeeded, "baseline job before restart")

	sched.kill()
	startScheduler(t, addr)

	job2 := submitShell(t, c, "echo two", nil)
	st := waitFor(t, c, job2, 10*time.Second, succeeded, "job after scheduler restart")
	if st.Output != "two" {
		t.Fatalf("output = %q, want %q", st.Output, "two")
	}

	_, err := c.GetJobStatus(context.Background(), &pb.JobStatusRequest{JobId: job1})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("pre-restart job: got err %v, want NotFound (state is in-memory)", err)
	}
}

// Scenario 4 — burst baseline. 20 jobs across 3 workers: everything completes
// and the load actually spreads instead of serializing on one worker.
func TestBurstSpreadsAndCompletes(t *testing.T) {
	t.Parallel()
	addr := freeAddr(t)
	startScheduler(t, addr)
	for _, id := range []string{"burst-a", "burst-b", "burst-c"} {
		startWorker(t, id, addr)
	}
	c := dial(t, addr)

	const n = 20
	jobs := make([]string, n)
	for i := range jobs {
		jobs[i] = submitShell(t, c, fmt.Sprintf("/bin/sleep 0.2; echo %d", i), nil)
	}

	used := map[string]bool{}
	for i, id := range jobs {
		st := waitFor(t, c, id, 30*time.Second, succeeded, fmt.Sprintf("burst job %d", i))
		if st.Output != fmt.Sprintf("%d", i) {
			t.Fatalf("job %d: output = %q", i, st.Output)
		}
		used[st.WorkerId] = true
	}
	if len(used) < 2 {
		t.Fatalf("all %d jobs ran on a single worker %v; scheduler is not spreading load", n, used)
	}
}

// waitRuns polls the shared run log until it has n "ran" lines.
func waitRuns(t *testing.T, path string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	last := -1
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(path)
		last = strings.Count(string(data), "ran")
		if last == n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run log has %d executions, want %d (at-least-once should duplicate here)", last, n)
}
