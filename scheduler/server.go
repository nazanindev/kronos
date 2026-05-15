package main

import (
	pb "kronos/proto"
)

// server implements pb.SchedulerServer.
// Real logic lives in internal/; this will grow in upcoming phases.
type server struct {
	pb.UnimplementedSchedulerServer
}
