# kronos

A resource-constrained distributed job scheduler in Go + gRPC.

Workers register their capabilities. Jobs declare their requirements. Kronos matches them.

## Architecture

```
┌─────────┐   SubmitJob / GetJobStatus   ┌───────────┐
│ client  │ ──────────────────────────► │           │
└─────────┘                             │ scheduler │
                                        │           │
┌─────────┐   Connect (bidi stream)     │           │
│ worker  │ ◄────────────────────────── │           │
│         │ ──heartbeats──────────────► │           │
│         │ ◄──job assignments──────── │           │
│         │   ReportResult             │           │
│         │ ──────────────────────────► │           │
└─────────┘                             └───────────┘
```

**scheduler** — accepts jobs, tracks workers, matches resource constraints, detects failures via heartbeat TTL.

**worker** — registers capabilities, sends heartbeats every 5 s, executes jobs as shell commands, reports results.

**client** — submits jobs with resource requirements, queries job status.

Each service is an independent Go module linked by a `go.work` workspace. The only shared dependency is the `proto/` module which contains the gRPC service definition and generated stubs.

## Services

| Service | Module | Default port |
|---------|--------|-------------|
| scheduler | `kronos/scheduler` | `:50051` |
| worker | `kronos/worker` | — (dials scheduler) |
| client | `kronos/client` | — (dials scheduler) |

## Building

```sh
make build          # produces bin/scheduler, bin/worker, bin/client
make proto          # regenerate gRPC stubs (requires protoc)
make tidy           # go mod tidy for all modules
```

## Running

**1. Start the scheduler**

```sh
./bin/scheduler -addr :50051
```

**2. Start one or more workers**

```sh
./bin/worker \
  -scheduler localhost:50051 \
  -id worker-1 \
  -cpu 8 \
  -mem 4096 \
  -label region=us-west \
  -label arch=arm64
```

Workers reconnect automatically if the scheduler restarts.

**3. Submit jobs**

```sh
# submit returns a job ID
JOB=$(./bin/client submit -cpu 2 -mem 512 echo "hello world")

# optional resource requirements and label selectors
JOB=$(./bin/client submit \
  -cpu 4 \
  -mem 1024 \
  -label region=us-west \
  -env DEBUG=1 \
  python3 train.py --epochs 10)
```

**4. Check status**

```sh
./bin/client status "$JOB"
# id:     c7f272e04c2f7155
# state:  SUCCEEDED
# worker: worker-1
# output: hello world
```

## Scheduling

A job is matched to a worker when:

- `worker.cpu_cores ≥ job.require_cpu_cores`
- `worker.memory_mb ≥ job.require_memory_mb`
- all `job.label_selectors` key-value pairs exist in `worker.labels`
- the worker is idle (not currently running another job)

Matching uses first-fit across the idle worker pool. Unmatched jobs queue until a suitable worker becomes available.

## Failure semantics

Kronos provides **at-least-once execution**: a job assigned to a worker that crashes or partitions is re-queued and run again, so a job that straddles a failure can execute more than once. Exactly-once is not attempted.

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Worker crash (process dies) | Stream error, immediate | In-flight jobs re-queued, worker removed |
| Worker partition (silent but connected) | Heartbeat TTL (default 15 s, `-worker-ttl`) | In-flight jobs re-queued, worker evicted |
| Partition heals | Next heartbeat from the evicted worker | Worker re-registered idle; its stale result cannot corrupt completed job state |
| Scheduler crash | Workers retry their stream (`-retry`) | Workers reconnect and re-register; job history is in-memory and lost by design — old job IDs return `NotFound` |

## Chaos suite

`chaos/` is a black-box fault-injection suite: each scenario builds the real binaries, runs them as separate OS processes, injects failures with signals (`SIGKILL` = crash, `SIGSTOP`/`SIGCONT` = partition and heal), and asserts on job state over the real gRPC API.

```sh
make chaos
```

| Scenario | Injects | Asserts |
|----------|---------|---------|
| `TestWorkerCrashMidJob` | `SIGKILL` mid-job | Job completes on the surviving worker |
| `TestWorkerPartitionAndHeal` | `SIGSTOP` mid-job, later `SIGCONT` | Re-queue after TTL; duplicate run is visible (at-least-once); stale result ignored; healed worker takes new work |
| `TestSchedulerRestart` | `SIGKILL` the scheduler | Workers reconnect; new jobs schedule; old IDs return `NotFound` |
| `TestBurstSpreadsAndCompletes` | 20 jobs, 3 workers | All complete, load spreads |

The suite found (and now guards) three real bugs: crashed workers orphaned their in-flight jobs forever (only the TTL path re-queued), an evicted-but-alive worker could never rejoin the pool, and a rejoining worker could wedge itself permanently `BUSY`.

## gRPC API

```protobuf
service Scheduler {
  rpc Connect(stream WorkerHeartbeat) returns (stream JobAssignment);
  rpc SubmitJob(JobSpec)             returns (JobReceipt);
  rpc ReportResult(JobResult)        returns (ResultAck);
  rpc GetJobStatus(JobStatusRequest) returns (JobStatus);
}
```

See [`proto/kronos.proto`](proto/kronos.proto) for the full message definitions.

## Repo layout

```
kronos/
├── proto/       # shared gRPC contract (module: kronos/proto)
├── scheduler/   # scheduler service   (module: kronos/scheduler)
├── worker/      # worker service      (module: kronos/worker)
├── client/      # client CLI          (module: kronos/client)
├── chaos/       # fault-injection suite (module: kronos/chaos)
├── go.work      # workspace linking all modules
└── Makefile
```
