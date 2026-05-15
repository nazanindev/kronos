package internal

import pb "kronos/proto"

// match reports whether w satisfies all of spec's resource requirements.
func match(w *WorkerEntry, spec *pb.JobSpec) bool {
	if w.State != pb.WorkerState_IDLE {
		return false
	}
	if w.CpuCores < spec.RequireCpuCores {
		return false
	}
	if w.MemoryMb < spec.RequireMemoryMb {
		return false
	}
	for k, v := range spec.LabelSelectors {
		if w.Labels[k] != v {
			return false
		}
	}
	return true
}
