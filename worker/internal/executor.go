package internal

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"

	pb "kronos/proto"
)

type Executor struct{}

func (e *Executor) Run(ctx context.Context, a *pb.JobAssignment) *pb.JobResult {
	cmd := exec.CommandContext(ctx, a.Command, a.Args...)
	// Merge the job's env over the worker's own rather than replacing it: a
	// non-nil cmd.Env would otherwise strip PATH, HOME, etc. from the child.
	// Duplicate keys are fine — exec uses the last value, so job env wins.
	if len(a.Env) > 0 {
		cmd.Env = os.Environ()
		for k, v := range a.Env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
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
