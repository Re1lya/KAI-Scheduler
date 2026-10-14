// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package reclaim

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
)

func TestOrderedVictimsQueueOnNodeKeepsOnlyJobsOnTheNode(t *testing.T) {
	ssn, reclaimer, onNode, otherNode := newVictimQueueTestSession()
	for _, task := range otherNode.GetAllPodsMap() {
		task.NodeName = "node-1"
	}
	ssn.AddReclaimVictimFilterFn(func(_, _ *podgroup_info.PodGroupInfo) bool { return true })

	queue := getOrderedVictimsQueueOnNode(ssn, reclaimer, "node-0")()
	require.Same(t, onNode, queue.PopNextJob())
	require.True(t, queue.IsEmpty(), "same-queue and other-node jobs are not victims")

	queue = getOrderedVictimsQueueOnNode(ssn, reclaimer, "node-1")()
	require.Same(t, otherNode, queue.PopNextJob())
	require.True(t, queue.IsEmpty())
}

func TestIsEligibleForDeferredResizeUsesCanReclaimResources(t *testing.T) {
	ssn, reclaimer, _, _ := newVictimQueueTestSession()
	canReclaim := false
	var gotJob *podgroup_info.PodGroupInfo
	ssn.AddCanReclaimResourcesFn(func(job *podgroup_info.PodGroupInfo) bool {
		gotJob = job
		return canReclaim
	})

	require.False(t, isEligibleForDeferredResize(ssn)(reclaimer, nil))
	require.Same(t, reclaimer, gotJob)
	canReclaim = true
	require.True(t, isEligibleForDeferredResize(ssn)(reclaimer, nil))
}

func TestDeferredResizeSolverStartsJobSolution(t *testing.T) {
	ssn, reclaimer, _, _ := newVictimQueueTestSession()
	calls := 0
	ssn.AddOnJobSolutionStartFn(func() { calls++ })

	solver := newDeferredResizeSolver(ssn, reclaimer, nil)(&node_info.NodeInfo{Name: "node-0"})
	require.NotNil(t, solver)
	require.Equal(t, 1, calls)
}
