// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

// Package deferredresize lets preempt and reclaim free room for kubelet-Deferred in-place resizes.
// The resizing pod is only virtually evicted, never for real.
package deferredresize

import (
	"sort"

	v1 "k8s.io/api/core/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common/solvers"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/eviction_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/metrics"
)

// Solve must pin the task to node and take victims from node only. A nil statement means no solution.
type Solve func(
	job *podgroup_info.PodGroupInfo, node *node_info.NodeInfo, task *pod_info.PodInfo,
) (*framework.Statement, *solvers.SearchResult)

func hasDeferredResize(job *podgroup_info.PodGroupInfo) bool {
	for _, task := range job.GetAllPodsMap() {
		if pod_info.IsDeferredResizeTask(task) {
			return true
		}
	}
	return false
}

// NonPendingJobFilter is nil when the feature is disabled.
func NonPendingJobFilter(ssn *framework.Session) func(*podgroup_info.PodGroupInfo) bool {
	if !ssn.DeferredResizePreemptionEnabled() {
		return nil
	}
	return hasDeferredResize
}

func HasActiveAllocatedTaskOnNode(job *podgroup_info.PodGroupInfo, nodeName string) bool {
	for _, task := range job.GetAllPodsMap() {
		if task.NodeName == nodeName && pod_status.IsActiveAllocatedStatus(task.Status) {
			return true
		}
	}
	return false
}

// AttemptForJob scans the job when popped, so earlier evictions and moves are seen. It returns
// whether the action should stop.
func AttemptForJob(
	ssn *framework.Session, job *podgroup_info.PodGroupInfo,
	actionType framework.ActionType, solve Solve, shouldStopForResult func(*solvers.SearchResult) bool,
) (shouldStop bool) {
	if !ssn.DeferredResizePreemptionEnabled() || !isGangSecured(job) {
		return false
	}
	for _, task := range deferredTasks(ssn, job) {
		metrics.IncPodgroupsConsideredByAction()
		succeeded, result := attempt(ssn, job, ssn.ClusterInfo.Nodes[task.NodeName], task, actionType, solve)
		if succeeded {
			continue
		}
		if shouldStopForResult(result) {
			return true
		}
		log.InfraLogger.V(3).Infof("Didn't find a %s strategy for deferred resize of <%s/%s> on node <%s>",
			actionType, task.Namespace, task.Name, task.NodeName)
	}
	return false
}

func deferredTasks(ssn *framework.Session, job *podgroup_info.PodGroupInfo) []*pod_info.PodInfo {
	var tasks []*pod_info.PodInfo
	for _, task := range job.GetAllPodsMap() {
		if _, found := ssn.ClusterInfo.Nodes[task.NodeName]; found && pod_info.IsDeferredResizeTask(task) {
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Name < tasks[j].Name })
	return tasks
}

// attempt handles one task: other deferred tasks on the node are already charged at their target
// size, and with several tasks the solver may place only some.
func attempt(
	ssn *framework.Session, job *podgroup_info.PodGroupInfo, node *node_info.NodeInfo,
	task *pod_info.PodInfo, actionType framework.ActionType, solve Solve,
) (succeeded bool, result *solvers.SearchResult) {
	if nodeFitsAfterReleasing(node) {
		return true, nil
	}

	self := ssn.Statement()
	defer self.Discard()
	if err := self.Evict(task, "", eviction_info.EvictionMetadata{}); err != nil {
		log.InfraLogger.Errorf("Failed to virtually evict deferred resize task <%s/%s>: %v",
			task.Namespace, task.Name, err)
		return false, nil
	}

	task.IsInPlaceResize = true
	defer func() { task.IsInPlaceResize = false }()
	stmt, result := solve(job, node, task)
	if stmt == nil {
		return false, result
	}
	defer stmt.Discard()
	// The solved flag is always false here: a Releasing task already counts as active.
	victims := stmt.EvictedTasks()
	if result.Reason() != solvers.SearchResultSolved || len(victims) == 0 {
		return false, result
	}
	// Keep the victims' evictions and moves, not the resizing pod's placement.
	if err := stmt.Unpipeline(task); err != nil {
		log.InfraLogger.Errorf("Failed to undo the placement of deferred resize task <%s/%s>: %v",
			task.Namespace, task.Name, err)
		return false, result
	}
	if err := stmt.Commit(); err != nil {
		log.InfraLogger.Errorf("Failed to commit %s statement for deferred resize: %v", actionType, err)
	}
	metrics.IncPodgroupScheduledByAction()
	log.InfraLogger.V(3).Infof("Evicted <%v> for deferred resize of <%s/%s> on node <%s>",
		taskNames(victims), task.Namespace, task.Name, node.Name)
	return true, result
}

// A job below its gang minimum must not evict others so one of its pods can grow.
func isGangSecured(job *podgroup_info.PodGroupInfo) bool {
	return job.RootSubGroupSet != nil && job.RootSubGroupSet.IsMinRequirementSatisfied()
}

// ResizeRepresentative copies the job without its pending pods, so their demand is not counted.
func ResizeRepresentative(job *podgroup_info.PodGroupInfo, tasks []*pod_info.PodInfo) *podgroup_info.PodGroupInfo {
	return job.CloneWithTasks(append(job.GetAllAllocatedPods(), tasks...))
}

func nodeFitsAfterReleasing(node *node_info.NodeInfo) bool {
	for _, resource := range []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory} {
		if node.NonAllocatedResource(resource) < 0 {
			return false
		}
	}
	return true
}

func taskNames(tasks []*pod_info.PodInfo) []string {
	names := make([]string, 0, len(tasks))
	for _, task := range tasks {
		names = append(names, task.Namespace+"/"+task.Name)
	}
	return names
}
