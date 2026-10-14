// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package preempt_test

import (
	"testing"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/integration_tests/integration_tests_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

func TestPreemptForDeferredResizeIntegrationTest(t *testing.T) {
	integration_tests_utils.RunTests(t, getDeferredResizePreemptTestsMetadata())
}

func getDeferredResizePreemptTestsMetadata() []integration_tests_utils.TestTopologyMetadata {
	return []integration_tests_utils.TestTopologyMetadata{
		{
			TestTopologyBasic: test_utils.TestTopologyBasic{
				Name: "deferred resize evicts once while the victim terminates and the kubelet has not enacted",
				Jobs: []*jobs_fake.TestJobBasic{
					{
						Name:                "resizer",
						RequiredCPUsPerTask: 3,
						Priority:            constants.PriorityInteractivePreemptibleNumber,
						QueueName:           "queue0",
						Tasks: []*tasks_fake.TestTaskBasic{{
							NodeName:     "node0",
							State:        pod_status.Running,
							ResizeStatus: &tasks_fake.TestResizeStatus{ActualCPUs: 1, Deferred: true},
						}},
					},
					{
						Name:                "victim",
						RequiredCPUsPerTask: 2,
						Priority:            constants.PriorityTrainNumber - 10,
						QueueName:           "queue0",
						DeleteJobInTest:     true,
						Tasks:               []*tasks_fake.TestTaskBasic{{NodeName: "node0", State: pod_status.Running}},
					},
					{
						Name:                "bystander",
						RequiredCPUsPerTask: 2,
						Priority:            constants.PriorityTrainNumber,
						QueueName:           "queue0",
						DeleteJobInTest:     true,
						Tasks:               []*tasks_fake.TestTaskBasic{{NodeName: "node0", State: pod_status.Running}},
					},
				},
				Nodes: map[string]nodes_fake.TestNodeBasic{
					"node0": {CPUMillis: 6},
				},
				Queues: []test_utils.TestQueueBasic{{Name: "queue0"}},
				JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
					"resizer":   {NodeName: "node0", Status: pod_status.Running},
					"victim":    {NodeName: "node0", Status: pod_status.Releasing},
					"bystander": {NodeName: "node0", Status: pod_status.Running},
				},
				Mocks: &test_utils.TestMock{
					CacheRequirements: &test_utils.CacheMocking{NumberOfCacheEvictions: 1},
				},
				DeferredResizePreemption: true,
			},
			RoundsUntilMatch: 2,
		},
	}
}
