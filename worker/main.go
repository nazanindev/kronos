package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"kronos/worker/internal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type labelFlags map[string]string

func (l labelFlags) String() string { return "" }
func (l labelFlags) Set(s string) error {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) == 2 {
		l[parts[0]] = parts[1]
	}
	return nil
}

func main() {
	schedulerAddr := flag.String("scheduler", "localhost:50051", "scheduler address")
	cpuCores := flag.Int("cpu", 1, "available CPU cores")
	memoryMb := flag.Int64("mem", 512, "available memory in MB")
	workerID := flag.String("id", "", "worker ID (default: hostname)")

	labels := make(labelFlags)
	flag.Var(labels, "label", "label as key=value (repeatable)")
	flag.Parse()

	id := *workerID
	if id == "" {
		host, _ := os.Hostname()
		id = host
	}

	conn, err := grpc.NewClient(*schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial scheduler: %v", err)
	}
	defer conn.Close()

	agent := internal.New(id, int32(*cpuCores), *memoryMb, labels, conn)

	ctx := context.Background()
	for {
		if err := agent.Run(ctx); err != nil {
			log.Printf("disconnected: %v — retrying in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}
