package internal

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	pb "kronos/proto"
)

// WorkerEntry is the scheduler's view of a connected worker.
type WorkerEntry struct {
	ID       string
	CpuCores int32
	MemoryMb int64
	Labels   map[string]string
	State    pb.WorkerState
	LastSeen time.Time
	AssignCh chan *pb.JobAssignment // buffered(1); scheduler sends here, stream reads here
}

// Job is the scheduler's record of a submitted job.
type Job struct {
	ID       string
	Spec     *pb.JobSpec
	State    pb.JobState
	WorkerID string
	Output   string
	Error    string
}

// Scheduler is the core scheduling engine. A single mutex protects all state so
// that dispatch() can atomically claim both a worker and a job together.
type Scheduler struct {
	mu      sync.Mutex
	workers map[string]*WorkerEntry
	jobs    map[string]*Job
	pending []*Job // FIFO queue of unassigned jobs
}

func New() *Scheduler {
	return &Scheduler{
		workers: make(map[string]*WorkerEntry),
		jobs:    make(map[string]*Job),
	}
}

// RegisterWorker adds a newly connected worker and immediately tries to assign work.
func (s *Scheduler) RegisterWorker(hb *pb.WorkerHeartbeat, ch chan *pb.JobAssignment) {
	s.mu.Lock()
	s.workers[hb.WorkerId] = &WorkerEntry{
		ID:       hb.WorkerId,
		CpuCores: hb.CpuCores,
		MemoryMb: hb.MemoryMb,
		Labels:   copyMap(hb.Labels),
		State:    pb.WorkerState_IDLE,
		LastSeen: time.Now(),
		AssignCh: ch,
	}
	s.dispatch()
	s.mu.Unlock()
}

// Heartbeat refreshes a worker's last-seen time and capabilities. A heartbeat
// from an unknown worker means the reaper evicted it while its stream stayed
// alive (e.g. a healed partition): the live heartbeat is proof it is back, so
// it is re-registered — always as IDLE, even if it is still finishing a job
// that has since been re-queued. Registering BUSY would wedge it: the stale
// result credits the job's recorded (new) worker, so nothing would ever flip
// this one back. IDLE at worst buffers one assignment behind the stale job,
// and the agent executes sequentially. State is otherwise managed exclusively
// by the scheduler (dispatch/CompleteJob); heartbeat state is ignored to
// avoid races.
func (s *Scheduler) Heartbeat(hb *pb.WorkerHeartbeat, ch chan *pb.JobAssignment) {
	s.mu.Lock()
	w, ok := s.workers[hb.WorkerId]
	if !ok {
		s.workers[hb.WorkerId] = &WorkerEntry{
			ID:       hb.WorkerId,
			CpuCores: hb.CpuCores,
			MemoryMb: hb.MemoryMb,
			Labels:   copyMap(hb.Labels),
			State:    pb.WorkerState_IDLE,
			LastSeen: time.Now(),
			AssignCh: ch,
		}
		s.dispatch()
		s.mu.Unlock()
		return
	}
	w.CpuCores = hb.CpuCores
	w.MemoryMb = hb.MemoryMb
	w.Labels = copyMap(hb.Labels)
	w.LastSeen = time.Now()
	s.mu.Unlock()
}

// UnregisterWorker removes a worker whose stream has died and re-queues its
// in-flight jobs. Without the re-queue, a crashed worker's job would stay
// ASSIGNED forever: the reaper only scans workers still in the map, so it can
// recover from silence (partition) but never from a stream death (crash).
func (s *Scheduler) UnregisterWorker(workerID string) {
	s.mu.Lock()
	delete(s.workers, workerID)
	s.requeue(map[string]bool{workerID: true})
	s.dispatch()
	s.mu.Unlock()
}

// SubmitJob enqueues a job and tries to assign it immediately.
func (s *Scheduler) SubmitJob(spec *pb.JobSpec) string {
	s.mu.Lock()
	job := &Job{
		ID:    newID(),
		Spec:  spec,
		State: pb.JobState_PENDING,
	}
	s.jobs[job.ID] = job
	s.pending = append(s.pending, job)
	s.dispatch()
	s.mu.Unlock()
	return job.ID
}

// CompleteJob records the outcome of a finished job and marks the worker idle.
func (s *Scheduler) CompleteJob(result *pb.JobResult) {
	s.mu.Lock()
	job, ok := s.jobs[result.JobId]
	if !ok {
		s.mu.Unlock()
		return
	}
	if result.Success {
		job.State = pb.JobState_SUCCEEDED
	} else {
		job.State = pb.JobState_FAILED
	}
	job.Output = result.Output
	job.Error = result.Error
	if w, ok := s.workers[job.WorkerID]; ok {
		w.State = pb.WorkerState_IDLE
	}
	s.dispatch()
	s.mu.Unlock()
}

// JobStatus returns a snapshot of the job's current state, or nil if unknown.
func (s *Scheduler) JobStatus(jobID string) *pb.JobStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return nil
	}
	return &pb.JobStatus{
		JobId:    j.ID,
		State:    j.State,
		WorkerId: j.WorkerID,
		Output:   j.Output,
		Error:    j.Error,
	}
}

// Reap removes workers silent for longer than ttl and re-queues their jobs.
func (s *Scheduler) Reap(ttl time.Duration) {
	s.mu.Lock()
	now := time.Now()
	dead := make(map[string]bool)
	for id, w := range s.workers {
		if now.Sub(w.LastSeen) > ttl {
			dead[id] = true
			delete(s.workers, id)
		}
	}
	if len(dead) == 0 {
		s.mu.Unlock()
		return
	}
	s.requeue(dead)
	s.dispatch()
	s.mu.Unlock()
}

// requeue moves in-flight jobs owned by the given workers back to the pending
// queue. Must be called with s.mu held.
func (s *Scheduler) requeue(dead map[string]bool) {
	for _, j := range s.jobs {
		if dead[j.WorkerID] && (j.State == pb.JobState_ASSIGNED || j.State == pb.JobState_RUNNING) {
			j.State = pb.JobState_PENDING
			j.WorkerID = ""
			s.pending = append(s.pending, j)
		}
	}
}

// dispatch tries to pair every pending job with an idle matching worker.
// Must be called with s.mu held. Sends are non-blocking (AssignCh is buffered).
func (s *Scheduler) dispatch() {
	i := 0
	for i < len(s.pending) {
		job := s.pending[i]
		matched := false
		for _, w := range s.workers {
			if match(w, job.Spec) {
				w.State = pb.WorkerState_BUSY
				job.State = pb.JobState_ASSIGNED
				job.WorkerID = w.ID
				s.pending = append(s.pending[:i], s.pending[i+1:]...)
				w.AssignCh <- &pb.JobAssignment{
					JobId:   job.ID,
					Command: job.Spec.Command,
					Args:    job.Spec.Args,
					Env:     job.Spec.Env,
				}
				matched = true
				break
			}
		}
		if !matched {
			i++
		}
	}
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}
