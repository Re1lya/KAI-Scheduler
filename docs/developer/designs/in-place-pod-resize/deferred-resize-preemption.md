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
charges the deferred target (`max(spec, allocated, actual)`), so the node
shows the shortfall and freed room is not backfilled. But nothing frees room,
so an important workload can stay at its old size forever.

## Upstream alignment (KEP-5836)

[KEP-5836](https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/5836-scheduler-preemption-for-ippr)
(kube-scheduler, alpha in v1.37) solves the same gap:

| KEP-5836 | KAI |
| --- | --- |
| Deferred pod enters the normal scheduling queue | Deferred job enters the normal preempt/reclaim job queues |
| Restricted cycle: own node, resource fit only | Solver pinned to the pod's node; full predicates for the pod at its desired size |
| Standard preemption picks victims | Standard preempt/reclaim victim selection, validators and budgets |
| Reservation via `max(spec, allocated, actual)` | Same effective-request accounting (parent design) |
| Kubelet retries once victims are gone | Same; KAI never touches the resizing pod |
| Node opt-out: `PodResizePreemptionDisabled=True` | Pod with that condition is skipped |

## Mechanism: virtual self-eviction

For each job with deferred upsizes and no pending pods, per node:

0. **Re-check the pods.** A pod evicted earlier in the same action, or no
   longer deferred, is dropped from the attempt.
1. **Skip if the node already has room.** If `idle + releasing ≥ 0` for cpu and
   memory, the kubelet will enact the resize on its own (for example, victims
   of an earlier round are still terminating). This prevents repeat evictions.
2. **Virtually evict the resizing pods** inside a statement. They become
   `Releasing` with a virtual status, so `ShouldAllocate` treats them as
   pending at their charged (desired) size, and the queue is credited back.
3. **Run the action's eligibility checks** on the job in that state:
   victim-invariant pre-predicates, plus `IsNonPreemptibleJobOverQueueQuota`
   (preempt) or `CanReclaimResources` (reclaim).
4. **Run the action's `JobSolver`** for exactly those tasks
   (`SolveTasksWithResult`). It is pinned to the pod's node
   (`PinFeasibleNodes`) and uses victims from that node only. The pod fits
   back iff `used − releasing − victims ≤ allocatable`, which is exactly the
   resize shortfall. Pipelining it back consumes the releasing room, so the
   solver's reprieve step cannot give that room back to a victim.
5. **On success**, read the victims from the solver statement
   (`Statement.EvictedTasks`), discard both statements, and evict only the
   victims in a fresh statement. The resizing pod is never evicted.

Two small framework changes support this:

- `Statement.Pipeline` unevicts only when this statement evicted the task. A
  task evicted by an enclosing statement is pipelined in place.
- The solver pipelines clones of the job's tasks, so success is checked on the
  job's current task objects.

## Ordering

Both preempt and reclaim run the deferred attempt after the regular attempt
for the same popped job, each with its own statement and commit. A job with
pending pods places them first; its resize is attempted only once nothing is
pending, so the two are never bundled. Resize-only jobs skip the
"easier to schedule" signature cache, which is keyed on pending tasks.

## Known limitations

- Node-local only. KAI never migrates the resizing pod.
- Full predicates, not resource fit only (unlike KEP-5836). A pod whose node
  no longer passes its predicates (not ready, node affinity changed after
  start, queue limit lowered) triggers no evictions. This is conservative.
- Other deferred pods on the same node stay charged at their desired size
  during the search (conservative: may fail to find a solution that exists).
- A job with both pending pods and a deferred resize places the pending pods
  first.
- GPUs are never part of a resize delta.
- Freed room is protected only from KAI: the resizing pod is charged at its
  desired size, so KAI does not backfill it, also across scheduler restarts.
  Pods placed by other schedulers can still take it before the kubelet
  retries, and KAI then evicts again (same exposure as upstream alpha).
