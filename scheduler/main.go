package main

import (
	"flag"
	"log"
	"net"

	pb "kronos/proto"

	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50051", "listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterSchedulerServer(srv, &server{})

	log.Printf("scheduler listening on %s", *addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
