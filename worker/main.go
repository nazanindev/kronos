package main

import (
	"flag"
	"log"

	pb "kronos/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	schedulerAddr := flag.String("scheduler", "localhost:50051", "scheduler address")
	flag.Parse()

	conn, err := grpc.NewClient(*schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial scheduler: %v", err)
	}
	defer conn.Close()

	_ = pb.NewSchedulerClient(conn)

	log.Printf("worker connected to scheduler at %s", *schedulerAddr)
	// agent loop coming in Phase 3
	select {}
}
