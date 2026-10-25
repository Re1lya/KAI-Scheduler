// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package preempt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/common"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
)

func TestOrderedVictimsQueueOnNodeKeepsOnlyJobsOnTheNode(t *testing.T) {
	ssn, preemptor, onNode, otherNode := newPreemptVictimQueueTestSession()
	for _, task := range otherNode.GetAllPodsMap() {
		task.NodeName = "node-1"
	}
	ssn.ClusterInfo.Nodes["node-1"] = nil
	ssn.AddPreemptVictimFilterFn(func(_, _ *podgroup_info.PodGroupInfo) bool { return true })

	queue := getOrderedVictimsQueueOnNode(ssn, preemptor, "node-0")()
	require.Same(t, onNode, queue.PopNextJob())
	require.True(t, queue.IsEmpty())

	queue = getOrderedVictimsQueueOnNode(ssn, preemptor, "node-1")()
	require.Same(t, otherNode, queue.PopNextJob())
	require.True(t, queue.IsEmpty())
}

func TestOrderedVictimsQueueOnNodeKeepsPreemptRules(t *testing.T) {
	ssn, preemptor, onNode, filtered := newPreemptVictimQueueTestSession()
	filtered.Priority = preemptor.Priority
	ssn.AddPreemptVictimFilterFn(func(_, _ *podgroup_info.PodGroupInfo) bool { return true })

	queue := getOrderedVictimsQueueOnNode(ssn, preemptor, "node-0")()
	require.Same(t, onNode, queue.PopNextJob())
	require.True(t, queue.IsEmpty(), "equal priority job is not a victim")
}

// A failed resize-only job has no demand and would make every later job in its queue look harder.
func TestPreemptForPendingTasksSkipsJobWithoutPendingTasks(t *testing.T) {
	ssn, _, running, _ := newPreemptVictimQueueTestSession()
	require.Zero(t, running.GetNumPendingTasks())

	smallestFailedJobsByQueue := map[common_info.QueueID]*common.MinimalJobRepresentatives{}
	require.False(t, preemptForPendingTasks(ssn, running, nil, smallestFailedJobsByQueue))
	require.Empty(t, smallestFailedJobsByQueue)
}
