// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

// Package deferredresize lets preempt and reclaim free room on a node for in-place resizes
// that the kubelet marked as Deferred.
//
// The resizing pods are virtually evicted inside a statement so the job solver sees them as
// pending at their desired size, pinned to their own node. On success only the victim
// evictions are committed; the resizing pods are never evicted.
package deferredresize

import (
	"sort"

	v1 "k8s.io/api/core/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common/solvers"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/eviction_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/metrics"
)

// NodeTasks groups the deferred resize tasks of one job by node name.
type NodeTasks map[string][]*pod_info.PodInfo

// SolverFactory builds a job solver restricted to a single node.
type SolverFactory func(node *node_info.NodeInfo) *solvers.JobSolver

// Eligibility runs the action's own admission check on the job while its resizing tasks are
// virtually evicted.
type Eligibility func(job *podgroup_info.PodGroupInfo, tasks []*pod_info.PodInfo) bool

// Jobs returns the deferred resize tasks of every job, grouped by node. Empty when the
// feature is disabled.
func Jobs(ssn *framework.Session) map[common_info.PodGroupID]NodeTasks {
	result := map[common_info.PodGroupID]NodeTasks{}
	if !ssn.DeferredResizePreemptionEnabled() {
		return result
	}
	for _, job := range ssn.ClusterInfo.PodGroupInfos {
		for _, task := range job.GetAllPodsMap() {
			if !pod_info.IsDeferredResizeTask(task) {
				continue
			}
			if _, found := ssn.ClusterInfo.Nodes[task.NodeName]; !found {
				continue
			}
			if result[job.UID] == nil {
				result[job.UID] = NodeTasks{}
			}
			result[job.UID][task.NodeName] = append(result[job.UID][task.NodeName], task)
		}
	}
	return result
}

// HasActiveAllocatedTaskOnNode reports whether a candidate victim job can free room on the node.
func HasActiveAllocatedTaskOnNode(job *podgroup_info.PodGroupInfo, nodeName string) bool {
	for _, task := range job.GetAllPodsMap() {
		if task.NodeName == nodeName && pod_status.IsActiveAllocatedStatus(task.Status) {
			return true
		}
	}
	return false
}

// AttemptForJob tries every node group of the job and reports whether the action should stop.
func AttemptForJob(
	ssn *framework.Session, job *podgroup_info.PodGroupInfo, nodeTasks NodeTasks,
	actionType framework.ActionType, eligible Eligibility, newSolver SolverFactory,
	shouldStop func(*solvers.SearchResult) bool,
) bool {
	nodeNames := make([]string, 0, len(nodeTasks))
	for nodeName := range nodeTasks {
		nodeNames = append(nodeNames, nodeName)
	}
	sort.Strings(nodeNames)

	for _, nodeName := range nodeNames {
		node := ssn.ClusterInfo.Nodes[nodeName]
		metrics.IncPodgroupsConsideredByAction()
		succeeded, victims, result := Attempt(ssn, job, node, nodeTasks[nodeName], actionType, eligible, newSolver)
		if succeeded {
			if len(victims) > 0 {
				metrics.IncPodgroupScheduledByAction()
				log.InfraLogger.V(3).Infof("Evicted <%v> for deferred resize of job <%s/%s> on node <%s>",
					taskNames(victims), job.Namespace, job.Name, nodeName)
			}
			continue
		}
		if shouldStop(result) {
			return true
		}
		log.InfraLogger.V(3).Infof("Didn't find a %s strategy for deferred resize of job <%s/%s> on node <%s>",
			actionType, job.Namespace, job.Name, nodeName)
	}
	return false
}

// Attempt frees room on node for all the given deferred resize tasks of job, or for none.
// succeeded is true with no victims when the node already has room.
func Attempt(
	ssn *framework.Session, job *podgroup_info.PodGroupInfo, node *node_info.NodeInfo,
	tasks []*pod_info.PodInfo, actionType framework.ActionType, eligible Eligibility, newSolver SolverFactory,
) (succeeded bool, victims []*pod_info.PodInfo, result *solvers.SearchResult) {
	tasks = currentDeferredTasks(job, tasks, node.Name)
	if len(tasks) == 0 {
		return false, nil, nil
	}
	if nodeFitsAfterReleasing(node) {
		return true, nil, nil
	}

	self := ssn.Statement()
	defer self.Discard()
	for _, task := range tasks {
		if err := self.Evict(task, "", eviction_info.EvictionMetadata{}); err != nil {
			log.InfraLogger.Errorf("Failed to virtually evict deferred resize task <%s/%s>: %v",
				task.Namespace, task.Name, err)
			return false, nil, nil
		}
	}

	if task, failure := common.VictimInvariantPrePredicateFailureForTasks(ssn, tasks); failure != nil {
		log.InfraLogger.V(3).Infof("Deferred resize task <%s/%s> can't trigger %s: %v",
			task.Namespace, task.Name, actionType, failure.Err)
		return false, nil, nil
	}
	if !eligible(job, tasks) {
		return false, nil, nil
	}

	_, stmt, _, result := newSolver(node).SolveTasksWithResult(ssn, job, tasks)
	if stmt == nil {
		return false, nil, result
	}
	// JobSolver reports unsolved here since a Releasing task already counts as active.
	solved := result.Reason() == solvers.SearchResultSolved && allPipelinedOnNode(job, tasks, node.Name)
	victims = stmt.EvictedTasks()
	stmt.Discard()
	self.Discard()
	if !solved || len(victims) == 0 {
		return false, nil, result
	}

	final := ssn.Statement()
	if err := common.EvictAllPreemptees(ssn, victims, job, final, actionType); err != nil {
		log.InfraLogger.Errorf("Failed to evict victims for deferred resize of job <%s/%s>: %v",
			job.Namespace, job.Name, err)
		final.Discard()
		return false, nil, result
	}
	if err := final.Commit(); err != nil {
		log.InfraLogger.Errorf("Failed to commit %s statement for deferred resize: %v", actionType, err)
	}
	return true, victims, result
}

// currentDeferredTasks re-resolves tasks against the job: earlier evictions in the same action
// may have changed them since Jobs was computed, and statements may have swapped in clones.
func currentDeferredTasks(job *podgroup_info.PodGroupInfo, tasks []*pod_info.PodInfo, nodeName string) []*pod_info.PodInfo {
	jobTasks := job.GetAllPodsMap()
	current := make([]*pod_info.PodInfo, 0, len(tasks))
	for _, task := range tasks {
		jobTask, found := jobTasks[task.UID]
		if !found || jobTask.IsVirtualStatus || jobTask.NodeName != nodeName || !pod_info.IsDeferredResizeTask(jobTask) {
			continue
		}
		current = append(current, jobTask)
	}
	return current
}

func nodeFitsAfterReleasing(node *node_info.NodeInfo) bool {
	for _, resource := range []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory} {
		if node.NonAllocatedResource(resource) < 0 {
			return false
		}
	}
	return true
}

// allPipelinedOnNode checks the job's current task objects; the solver pipelines clones.
func allPipelinedOnNode(job *podgroup_info.PodGroupInfo, tasks []*pod_info.PodInfo, nodeName string) bool {
	jobTasks := job.GetAllPodsMap()
	for _, task := range tasks {
		current, found := jobTasks[task.UID]
		if !found || current.Status != pod_status.Pipelined || current.NodeName != nodeName {
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
