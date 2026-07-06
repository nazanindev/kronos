package internal

import (
	"context"
	"io"
	"log"
	"sync/atomic"
	"time"

	pb "kronos/proto"

	"google.golang.org/grpc"
)

// Agent connects to the scheduler, sends heartbeats, and executes assigned jobs.
type Agent struct {
	id                string
	cpuCores          int32
	memoryMb          int64
	labels            map[string]string
	heartbeatInterval time.Duration
	client            pb.SchedulerClient
	exec              Executor
	busy              atomic.Bool
}

func New(id string, cpuCores int32, memoryMb int64, labels map[string]string, heartbeatInterval time.Duration, conn *grpc.ClientConn) *Agent {
	return &Agent{
		id:                id,
		cpuCores:          cpuCores,
		memoryMb:          memoryMb,
		labels:            labels,
		heartbeatInterval: heartbeatInterval,
		client:            pb.NewSchedulerClient(conn),
	}
}

// Run opens the Connect stream and blocks until the stream closes or ctx is cancelled.
// The caller should retry on error.
func (a *Agent) Run(ctx context.Context) error {
	stream, err := a.client.Connect(ctx)
	if err != nil {
		return err
	}

	// Send initial heartbeat to register with the scheduler.
	if err := stream.Send(a.heartbeat()); err != nil {
		return err
	}
	log.Printf("registered as %s (cpu=%d mem=%dMB)", a.id, a.cpuCores, a.memoryMb)

	// Heartbeat goroutine: one sender on the stream, ticker-driven.
	// gRPC allows concurrent Send + Recv on the same stream from different goroutines.
	go func() {
		t := time.NewTicker(a.heartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := stream.Send(a.heartbeat()); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Receive job assignments and execute them sequentially.
	for {
		assignment, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		log.Printf("job %s: executing %q %v", assignment.JobId, assignment.Command, assignment.Args)
		a.busy.Store(true)

		result := a.exec.Run(ctx, assignment)

		if result.Success {
			log.Printf("job %s: succeeded — %s", assignment.JobId, result.Output)
		} else {
			log.Printf("job %s: failed — %s", assignment.JobId, result.Error)
		}

		if _, err := a.client.ReportResult(ctx, result); err != nil {
			log.Printf("job %s: report result: %v", assignment.JobId, err)
		}

		a.busy.Store(false)
	}
}

func (a *Agent) heartbeat() *pb.WorkerHeartbeat {
	state := pb.WorkerState_IDLE
	if a.busy.Load() {
		state = pb.WorkerState_BUSY
	}
	return &pb.WorkerHeartbeat{
		WorkerId: a.id,
		CpuCores: a.cpuCores,
		MemoryMb: a.memoryMb,
		Labels:   a.labels,
		State:    state,
	}
}
