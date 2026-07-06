package main

import (
	"flag"
	"log"
	"net"
	"time"

	pb "kronos/proto"
	"kronos/scheduler/internal"

	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50051", "listen address")
	workerTTL := flag.Duration("worker-ttl", 15*time.Second, "evict workers silent longer than this")
	reapInterval := flag.Duration("reap-interval", 5*time.Second, "how often to check for dead workers")
	flag.Parse()

	sched := internal.New()

	// Reaper: periodically evict workers that have gone silent.
	go func() {
		t := time.NewTicker(*reapInterval)
		defer t.Stop()
		for range t.C {
			sched.Reap(*workerTTL)
		}
	}()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterSchedulerServer(srv, &server{sched: sched})

	log.Printf("scheduler listening on %s", *addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
