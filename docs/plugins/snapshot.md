# KAI Scheduler Snapshot Plugin and Tool

## Overview

The KAI Scheduler provides a snapshot plugin and tool that allows capturing and analyzing the state of the scheduler and cluster resources. This documentation covers both the snapshot plugin and the snapshot tool.

## Snapshot Plugin

The snapshot plugin is a framework plugin that provides an HTTP endpoint to capture the current state of the scheduler and cluster resources.

### Features

- Captures scheduler configuration and parameters
- Collects raw Kubernetes objects that the scheduler uses to perform its actions including:
  - Pods
  - Nodes
  - Queues
  - PodGroups
  - BindRequests
  - PriorityClasses
  - ConfigMaps
  - PersistentVolumeClaims
  - CSIStorageCapacities
  - StorageClasses
  - CSIDrivers
  - ResourceClaims
  - ResourceSlices
  - DeviceClasses

### Capturing a Snapshot

The plugin registers an HTTP endpoint `/get-snapshot` that returns a ZIP file containing a JSON snapshot of the cluster state.

To capture a snapshot, port-forward to the scheduler Deployment and call the endpoint:

```bash
kubectl port-forward -n kai-scheduler deployment/kai-scheduler-default 8081 &
sleep 2
curl -sSf "localhost:8081/get-snapshot" -o snapshot.zip
```

#### Capturing a Snapshot with Multiple Replicas

When the scheduler Deployment has more than one replica, resolve the leader from the scheduler
lease and port-forward directly to its pod:

```bash
LEADER=$(kubectl get lease -n kai-scheduler kai-scheduler \
           -o jsonpath='{.spec.holderIdentity}' | cut -d'_' -f1)

kubectl port-forward -n kai-scheduler "pod/$LEADER" 8081:8081 &
sleep 2
curl -sSf "localhost:8081/get-snapshot" -o snapshot.zip
```

The lease is named after the scheduler (`kai-scheduler` by default). A shard that sets
`partitionLabelValue` uses `<scheduler-name>-<partition-label-value>` instead — for example
`kai-scheduler-dra`.

### Analyzing a Snapshot

Build the snapshot tool:

```bash
make build-go SERVICE_NAME=snapshot-tool
```

See [Building from Source](../developer/building-from-source.md) for more details. There is no
published `snapshot-tool` release artifact.

Then use the tool to analyze a captured snapshot:

```bash
./bin/snapshot-tool-amd64 --filename snapshot.zip --verbosity 8
```

See the [Snapshot Tool](#snapshot-tool) section below for more details.

### Response Format

The snapshot is returned as a ZIP file containing a single JSON file (`snapshot.json`) with the following structure:

```json
{
  "config": {
    // Scheduler configuration
  },
  "schedulerParams": {
    // Scheduler parameters
  },
  "rawObjects": {
    // Raw Kubernetes objects
  }
}
```

## Snapshot Tool

The snapshot tool is a command-line utility that can load and analyze snapshots captured by the snapshot plugin.

### Features

- Loads snapshots from ZIP files
- Recreates the scheduler environment from a snapshot
- Supports running scheduler actions on the snapshot data
- Provides detailed logging of operations

### Usage

```bash
snapshot-tool --filename <snapshot-file> [--verbosity <log-level>]
```

#### Arguments

- `--filename`: Path to the snapshot ZIP file (required)
- `--verbosity`: Logging verbosity level (default: 4)
- `--cycles`: Scheduling cycles to replay (default: 1)
- `--dump-events`: Write the events the replay emitted to a sibling `<base>.events.zip`
- `--create-decision-snapshots`: Capture the state before and after every cycle as
  `<base>.<n>.snapshot.zip`, each paired with the `<base>.<n>.events.zip` of the decisions that
  produced it (implies `--dump-events`)

### Example

```bash
# Load and analyze a snapshot
snapshot-tool --filename snapshot.zip

# Load and analyze a snapshot with increased verbosity
snapshot-tool --filename snapshot.zip --verbosity 5
```

### Replaying More Than One Cycle

A single cycle cannot complete a preemption: one cycle evicts the victims and frees the node, and a
later one allocates the freed resources, exactly as the scheduler's own loop does. Pass `--cycles`
to see the whole sequence. The tool waits for the evictor's goroutine between cycles.

```bash
snapshot-tool --filename snapshot.zip --cycles 3
```

### Recovering the Events a Snapshot Does Not Carry

A snapshot holds objects but **no events**, and events are where the scheduler records what it
decided — `Pipelined`, `Evict`, `Scheduled`, `Unschedulable` and the rest. Since the replay runs the
real scheduler over the snapshot, the events it emits are the ones the cluster would have produced
from that state. `--dump-events` keeps them:

```bash
snapshot-tool --filename run1.snapshot.zip --cycles 3 --dump-events
# writes run1.events.zip
```

The output is a ZIP holding a single `events.json` with a `v1.EventList`, named after the snapshot
it came from: `<base>.snapshot.zip` is paired with `<base>.events.zip`. The pairing is by base name,
so keep the two files together.

A single snapshot plus its events is a mismatched pair, though: the objects are frozen at capture
time while the events come from a run that happened afterwards, so a preempted workload still reads
`Running` above a timeline that ends in `Evicted`. Capturing each cycle fixes that.

### Capturing the State Between Cycles

`--create-decision-snapshots` writes a numbered chain instead of a single pair — one snapshot per
replayed state, each with the events of the cycle that produced it:

```bash
snapshot-tool --filename a.snapshot.zip --cycles 3 --create-decision-snapshots
```

```
a.snapshot.zip     the input, untouched
a.1.snapshot.zip   the state the replay started from  (no events: nothing produced it)
a.2.snapshot.zip   a.2.events.zip   the decisions of cycle 1, and the state they produced
a.3.snapshot.zip   a.3.events.zip   cycle 2
a.4.snapshot.zip   a.4.events.zip   cycle 3
```

Three cycles produce four states, since the first is the starting point. Each `<n>.events.zip` holds
only the events of that one cycle: the recorder updates an existing event rather than creating a
second one when the same thing recurs, so the cycle boundary is drawn by a rising event count.

[`story`](../workload-story-util/README.md) reads the chain in order and tells the scheduling story of a workload
without a live cluster:

```bash
story --from-snapshots a.1.snapshot.zip,a.2.snapshot.zip,a.3.snapshot.zip,a.4.snapshot.zip \
      -n team-a high-prio
```

The files may also be given as bare arguments (`story -n team-a high-prio a.1.snapshot.zip
a.2.snapshot.zip`), and `--serve` works the same way.

Objects are taken from the newest snapshot that holds them. One that disappears along the way — a
preempted pod, say — is kept so the story can still name it and the node it was taken from, but is
marked deleted so it no longer counts towards the workload's state. A snapshot with no events file
is read for its objects, and the story carries a note saying its decisions are missing; the same
note appears in the `--serve` page.

Timestamps throughout are replay time, not cluster time: intervals between decisions are meaningful,
absolute times are not.

## Implementation Details

### Snapshot Plugin

The snapshot plugin (`pkg/scheduler/plugins/snapshot/snapshot.go`) implements the following key components:

1. `RawKubernetesObjects`: Structure containing all captured Kubernetes objects
2. `Snapshot`: Main structure containing configuration, parameters, and raw objects
3. `snapshotPlugin`: Plugin implementation with HTTP endpoint handler

### Snapshot Tool

The snapshot tool (`cmd/snapshot-tool/main.go`) implements:

1. Snapshot loading and parsing
2. Fake client creation with snapshot data
3. Scheduler cache initialization
4. Session management
5. Action execution

## Limitations

- The snapshot tool runs in a simulated environment
- Some real-time cluster features may not be available
- Resource constraints may differ from the original cluster 
