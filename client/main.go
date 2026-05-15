package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pb "kronos/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	schedulerAddr := flag.String("scheduler", "localhost:50051", "scheduler address")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: client [-scheduler addr] <submit|status> ...")
		os.Exit(1)
	}

	conn, err := grpc.NewClient(*schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	c := pb.NewSchedulerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch flag.Arg(0) {
	case "submit":
		runSubmit(ctx, c, flag.Args()[1:])
	case "status":
		runStatus(ctx, c, flag.Args()[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", flag.Arg(0))
		os.Exit(1)
	}
}

// runSubmit: client submit [-cpu N] [-mem N] [-label k=v] [-env k=v] <command> [args...]
func runSubmit(ctx context.Context, c pb.SchedulerClient, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	cpu := fs.Int("cpu", 0, "required CPU cores")
	mem := fs.Int64("mem", 0, "required memory in MB")
	labels := make(kvMap)
	envVars := make(kvMap)
	fs.Var(labels, "label", "label selector key=value (repeatable)")
	fs.Var(envVars, "env", "env var key=value (repeatable)")
	_ = fs.Parse(args)

	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: client submit [-cpu N] [-mem N] [-label k=v] [-env k=v] <command> [args...]")
		os.Exit(1)
	}

	receipt, err := c.SubmitJob(ctx, &pb.JobSpec{
		Command:         fs.Arg(0),
		Args:            fs.Args()[1:],
		Env:             envVars,
		RequireCpuCores: int32(*cpu),
		RequireMemoryMb: *mem,
		LabelSelectors:  labels,
	})
	if err != nil {
		log.Fatalf("submit: %v", err)
	}
	fmt.Println(receipt.JobId)
}

// runStatus: client status <job-id>
func runStatus(ctx context.Context, c pb.SchedulerClient, args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	_ = fs.Parse(args)

	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: client status <job-id>")
		os.Exit(1)
	}

	js, err := c.GetJobStatus(ctx, &pb.JobStatusRequest{JobId: fs.Arg(0)})
	if err != nil {
		log.Fatalf("status: %v", err)
	}

	fmt.Printf("id:     %s\n", js.JobId)
	fmt.Printf("state:  %s\n", strings.TrimPrefix(js.State.String(), "JobState_"))
	if js.WorkerId != "" {
		fmt.Printf("worker: %s\n", js.WorkerId)
	}
	if js.Output != "" {
		fmt.Printf("output: %s\n", js.Output)
	}
	if js.Error != "" {
		fmt.Printf("error:  %s\n", js.Error)
	}
}

// kvMap is a repeatable flag that parses key=value pairs.
type kvMap map[string]string

func (k kvMap) String() string { return "" }
func (k kvMap) Set(s string) error {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) == 2 {
		k[parts[0]] = parts[1]
	}
	return nil
}
