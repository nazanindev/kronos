package internal

import (
	"bytes"
	"context"
	"os/exec"
	"strings"

	pb "kronos/proto"
)

type Executor struct{}

func (e *Executor) Run(ctx context.Context, a *pb.JobAssignment) *pb.JobResult {
	cmd := exec.CommandContext(ctx, a.Command, a.Args...)
	for k, v := range a.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	result := &pb.JobResult{JobId: a.JobId}
	if err := cmd.Run(); err != nil {
		result.Success = false
		result.Error = strings.TrimSpace(stderr.String())
		if result.Error == "" {
			result.Error = err.Error()
		}
	} else {
		result.Success = true
		result.Output = strings.TrimSpace(stdout.String())
	}
	return result
}
