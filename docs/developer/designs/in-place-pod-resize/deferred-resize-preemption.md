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

For each job with deferred upsizes and no pending pods, one deferred pod at a
time:

0. **Re-check the pod.** A pod evicted earlier in the same action, or no
   longer deferred, is skipped.
1. **Skip if the node already has room.** If `idle + releasing ≥ 0` for cpu and
   memory, the kubelet will enact the resize on its own (for example, victims
   of an earlier round are still terminating). This prevents repeat evictions.
2. **Virtually evict the resizing pod** inside a statement. It becomes
   `Releasing` with a virtual status, so `ShouldAllocate` treats it as
   pending at its charged (desired) size, and the queue is credited back.
3. **Run the action's eligibility checks** on the job in that state:
   victim-invariant pre-predicates, plus `IsNonPreemptibleJobOverQueueQuota`
   (preempt) or `CanReclaimResources` (reclaim).
4. **Run the action's `JobSolver`** for exactly that pod
   (`SolveTasksWithResult`). It is pinned to the pod's node
   (`PinFeasibleNodes`) and uses victims from that node only. The pod fits
   back iff `used − releasing − victims ≤ allocatable`, which is exactly the
   resize shortfall. Pipelining it back consumes the releasing room, so the
   solver's reprieve step cannot give that room back to a victim.
5. **On success**, read the victims from the solver statement
   (`Statement.EvictedTasks`), discard both statements, and evict only the
   victims in a fresh statement. The resizing pod is never evicted.

One pod per attempt is enough: other deferred pods on the node are already
charged at their desired size, so the pod fits back only once the whole node
shortfall is covered, and the next pod on that node then skips at step 1. It
also avoids the solver's elastic task selection, which adds pods above a
gang's `minAvailable` one at a time.

Framework changes that support this:

- `Statement.Pipeline` used to treat a task already on the target node as one
  it had evicted itself, and undo that eviction. It now does so only when the
  eviction is in the same statement. The resizing pod is evicted in an outer
  statement, because the solver opens and discards its own statements per
  scenario and would otherwise undo it. So the solver's statement pipelines it
  on its node normally; this case previously always failed.
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
