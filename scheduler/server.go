package main

import (
	"context"
	"io"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"kronos/scheduler/internal"

	pb "kronos/proto"
)

type server struct {
	pb.UnimplementedSchedulerServer
	sched *internal.Scheduler
}

// Connect is the bidi-stream RPC that workers use for the lifetime of their connection.
// The worker sends heartbeats; the scheduler sends job assignments back.
func (s *server) Connect(stream pb.Scheduler_ConnectServer) error {
	// First message must arrive to register the worker.
	hb, err := stream.Recv()
	if err != nil {
		return err
	}

	assignCh := make(chan *pb.JobAssignment, 1)
	workerID := hb.WorkerId
	s.sched.RegisterWorker(hb, assignCh)
	defer s.sched.UnregisterWorker(workerID)

	log.Printf("worker %s connected (cpu=%d mem=%dMB)", workerID, hb.CpuCores, hb.MemoryMb)

	recvErr := make(chan error, 1)

	// Goroutine: read heartbeats from the worker.
	go func() {
		for {
			hb, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			s.sched.Heartbeat(hb)
		}
	}()

	// Main loop: forward assignments to the worker as they arrive.
	for {
		select {
		case assignment := <-assignCh:
			if err := stream.Send(assignment); err != nil {
				return err
			}
			log.Printf("assigned job %s to worker %s", assignment.JobId, workerID)
		case err := <-recvErr:
			if err == io.EOF {
				log.Printf("worker %s disconnected", workerID)
				return nil
			}
			return err
		}
	}
}

func (s *server) SubmitJob(_ context.Context, spec *pb.JobSpec) (*pb.JobReceipt, error) {
	id := s.sched.SubmitJob(spec)
	log.Printf("job %s submitted (cmd=%q cpu=%d mem=%dMB)", id, spec.Command, spec.RequireCpuCores, spec.RequireMemoryMb)
	return &pb.JobReceipt{JobId: id}, nil
}

func (s *server) ReportResult(_ context.Context, result *pb.JobResult) (*pb.ResultAck, error) {
	s.sched.CompleteJob(result)
	if result.Success {
		log.Printf("job %s succeeded", result.JobId)
	} else {
		log.Printf("job %s failed: %s", result.JobId, result.Error)
	}
	return &pb.ResultAck{}, nil
}

func (s *server) GetJobStatus(_ context.Context, req *pb.JobStatusRequest) (*pb.JobStatus, error) {
	js := s.sched.JobStatus(req.JobId)
	if js == nil {
		return nil, status.Errorf(codes.NotFound, "job %s not found", req.JobId)
	}
	return js, nil
}
