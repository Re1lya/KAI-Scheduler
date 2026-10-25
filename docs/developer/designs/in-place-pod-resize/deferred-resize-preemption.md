<!--
Copyright 2026 NVIDIA CORPORATION
SPDX-License-Identifier: Apache-2.0
-->

# Deferred Resize Preemption

*Status: Implemented (opt-in, scheduler flag `deferred-resize-preemption`, off by default)*

Related issue: [#1872](https://github.com/kai-scheduler/KAI-Scheduler/issues/1872).
Parent design: [In-Place Pod Resize Accounting and Queue Admission](README.md).

## Problem

When a node has no room for an in-place upsize, the kubelet sets
`PodResizePending` with reason `Deferred` and retries later. KAI already
charges the pod at `max(spec, allocated, actual)`, so the node's idle CPU or
memory goes negative by the shortfall and nothing else is placed there. But
nothing frees room either, so the pod can stay at its old size forever.

## Mechanism

This follows kube-scheduler's
[KEP-5836](https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/5836-scheduler-preemption-for-ippr).
The resizing pod is never evicted or re-placed; only victims are.

1. Jobs with a deferred upsize enter the preempt and reclaim job queues even
   without pending pods. When a job is popped, its pending pods are tried
   first, as before. Then, if its gang is secured, each deferred pod is tried
   in name order.
2. If the pod's node has `idle + releasing >= 0` for CPU and memory, nothing is
   done: the resize fits, or victims of an earlier round are still
   terminating.
3. Otherwise the action solves a copy of the job that holds its allocated
   pods and, instead of the resizing pod, a pending **stand-in**
   (`pod_info.NewInPlaceResizeRequest`). The stand-in requests nothing,
   because the node already charges the pod at its target size. This is
   KEP-5836's `max(0, request - cached)`, which is zero for a pod already in
   the cache. Pending pods are left out of the copy.
4. The regular `JobSolver` runs with the action's validator and a victim
   queue limited to jobs with a pod on that node. "Allocating" the stand-in
   (`AllocateJob`) is only a fit check: `node.IsInPlaceResizeAllocatable`,
   which passes when the node is no longer over-committed for CPU and memory.
   Nothing is bound or pipelined for it. Victims may be placed back or moved
   to any node, as in the regular path.
5. On success the solver's statement holds only victim evictions and moves,
   and it is committed as is.

The stand-in requests zero, so the increase is never counted twice and
nothing that sums requests needs a special case: GPU scenario filters, the
sub-scenario emitter, reclaim's fair-share checks and the validators all see
no extra demand.

## Comparison with KEP-5836

| KEP-5836 (kube-scheduler) | KAI |
| --- | --- |
| Deferred pod enters the scheduling queue | Deferred job enters the preempt/reclaim job queues |
| Fit for a pod on its own node uses `max(0, request - cached)` | The stand-in requests zero; the fit check is "node not over-committed" |
| Standard preemption dry run removes victims on that node | Regular `JobSolver` with victims from that node |
| Start-time checks skipped per plugin (`IsPodResizeDeferred`) | Skipped by returning before predicates, node subsetting and NUMA in `AllocateJob`, plus the node-affinity scenario filter |
| No binding; Permit waits for kubelet actuation | Nothing is bound or pipelined; only victims are committed |
| No queue quota in kube-scheduler | Reclaim keeps fair-share admission (`CanReclaimResources`, reclaim validator). Queue limits and non-preemptible quota for the resize are enforced when the resize is admitted (pod resize webhook) |
| Node opt-out `disableResizePreemption` | Not supported yet |

## Design choices

- Pending pods first, then resizes, in the same pop, so the job-queue order
  is kept. Resize-only jobs skip the "easier to schedule" signature cache,
  which is keyed on pending tasks.
- A job whose gang is not secured gets no evictions for its resizes.
- Resource fit only, as in KEP-5836: node health, affinity, taints, ports,
  volumes and CSI capacity, ConfigMaps, topology, NUMA zones and resource
  claims are not checked for the resizing pod. A resize does not change its
  claims.
- Preempt victims come from the pod's own queue, so the queue's charged usage
  only drops. A zero request makes KAI's queue limit and quota checks skip
  the resize, like kube-scheduler's `podRequest > 0` guards. Those bounds are
  checked by the pod resize webhook when the resize is admitted
  (`validateQuota`, on by default).

## Known limitations

- Other deferred pods on the same node stay charged at their target size, so
  evictions happen only when every deferred resize on the node can be met.
- GPUs are never part of a resize.
- NUMA zones are ignored: only node-level room is freed, so the kubelet may
  still defer the resize if the room is in another zone.
- A pod bound to the node directly (static pod, or `spec.nodeName` set by a
  controller) can take the freed room before the kubelet retries.
