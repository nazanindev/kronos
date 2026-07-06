// Package chaos is a black-box fault-injection suite for kronos. Every
// scenario builds and runs the real scheduler and worker binaries as separate
// OS processes, injects a failure with signals (SIGKILL = crash, SIGSTOP =
// partition), and asserts on observable job state over the real gRPC API.
package chaos

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "kronos/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Timing used by every scenario: heartbeats every 200ms, workers reaped after
// 1s of silence. Fast enough that a full partition round-trip fits in seconds.
const (
	workerTTL    = 1 * time.Second
	reapInterval = 250 * time.Millisecond
	heartbeat    = 200 * time.Millisecond
	retry        = 300 * time.Millisecond
)

var binDir string

// TestMain builds the real binaries once; scenarios exercise those, not fakes.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kronos-chaos-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, target := range []string{"scheduler", "worker"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(dir, target), "kronos/"+target)
		cmd.Dir = ".." // repo root, where go.work lives
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", target, err, out)
			os.Exit(1)
		}
	}
	binDir = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// proc is a spawned scheduler or worker: captured logs plus signal helpers.
type proc struct {
	name string
	cmd  *exec.Cmd
	logs *lockedBuffer
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func start(t *testing.T, name, bin string, args ...string) *proc {
	t.Helper()
	logs := &lockedBuffer{}
	cmd := exec.Command(filepath.Join(binDir, bin), args...)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	p := &proc{name: name, cmd: cmd, logs: logs}
	go cmd.Wait()
	t.Cleanup(func() {
		p.resume() // a SIGSTOPped process ignores SIGKILL until continued
		p.kill()
		if t.Failed() {
			t.Logf("--- %s ---\n%s", name, logs.String())
		}
	})
	return p
}

func (p *proc) signal(sig syscall.Signal) {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(sig)
	}
}

func (p *proc) kill()   { p.signal(syscall.SIGKILL) } // crash: socket dies with the process
func (p *proc) pause()  { p.signal(syscall.SIGSTOP) } // partition: silent, but TCP stays open
func (p *proc) resume() { p.signal(syscall.SIGCONT) }

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startScheduler(t *testing.T, addr string) *proc {
	t.Helper()
	p := start(t, "scheduler", "scheduler",
		"-addr", addr,
		"-worker-ttl", workerTTL.String(),
		"-reap-interval", reapInterval.String())
	waitListening(t, addr)
	return p
}

func startWorker(t *testing.T, id, addr string) *proc {
	t.Helper()
	return start(t, "worker/"+id, "worker",
		"-scheduler", addr, "-id", id,
		"-cpu", "4", "-mem", "4096",
		"-heartbeat", heartbeat.String(),
		"-retry", retry.String())
}

func waitListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("scheduler at %s never started listening", addr)
}

func dial(t *testing.T, addr string) pb.SchedulerClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewSchedulerClient(conn)
}

// submitShell submits `/bin/sh -c script`. Scripts must use absolute paths for
// external commands (/bin/sleep) because the executor passes only `env` to the
// job, so the child has no PATH.
func submitShell(t *testing.T, c pb.SchedulerClient, script string, env map[string]string) string {
	t.Helper()
	receipt, err := c.SubmitJob(context.Background(), &pb.JobSpec{
		Command:         "/bin/sh",
		Args:            []string{"-c", script},
		Env:             env,
		RequireCpuCores: 1,
		RequireMemoryMb: 64,
	})
	if err != nil {
		t.Fatalf("submit %q: %v", script, err)
	}
	return receipt.JobId
}

// waitFor polls job status until pred holds or the timeout expires.
func waitFor(t *testing.T, c pb.SchedulerClient, jobID string, timeout time.Duration,
	pred func(*pb.JobStatus) bool, desc string) *pb.JobStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *pb.JobStatus
	for time.Now().Before(deadline) {
		st, err := c.GetJobStatus(context.Background(), &pb.JobStatusRequest{JobId: jobID})
		if err == nil {
			last = st
			if pred(st) {
				return st
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job %s: timed out after %s waiting for %s; last status: %v", jobID, timeout, desc, last)
	return nil
}

func succeeded(s *pb.JobStatus) bool { return s.State == pb.JobState_SUCCEEDED }
func assigned(s *pb.JobStatus) bool  { return s.WorkerId != "" }
