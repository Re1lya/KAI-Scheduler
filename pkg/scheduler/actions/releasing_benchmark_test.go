// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package actions_test

import (
	"fmt"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/allocate"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/reclaim"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

const benchmarkHostnameKey = "kubernetes.io/hostname"

var benchmarkReleasingLabels = map[string]string{"tier": "preprocess"}

// BenchmarkAllocateAction_ReleasingAntiAffinity_MediumCluster benchmarks allocation on 50 nodes where
// every fifth node hosts a terminating pod that the pending pods are anti-affine to.
func BenchmarkAllocateAction_ReleasingAntiAffinity_MediumCluster(b *testing.B) {
	benchmarkReleasing(b, allocate.New(), createBenchmarkTopologyWithReleasingPods(50, 200, false))
}

// BenchmarkReclaimAction_ReleasingAntiAffinity_MediumCluster benchmarks reclaim on the same cluster,
// fully used by an over-quota queue.
func BenchmarkReclaimAction_ReleasingAntiAffinity_MediumCluster(b *testing.B) {
	benchmarkReleasing(b, reclaim.New(), createBenchmarkTopologyWithReleasingPods(50, 200, true))
}

func benchmarkReleasing(b *testing.B, action framework.Action, topology test_utils.TestTopologyBasic) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ssn := test_utils.BuildSession(topology, ctrl)
		action.Execute(ssn)
	}
}

func createBenchmarkTopologyWithReleasingPods(numNodes, numJobs int, fullyUsed bool) test_utils.TestTopologyBasic {
	nodes := make(map[string]nodes_fake.TestNodeBasic)
	var jobs []*jobs_fake.TestJobBasic
	for i := 0; i < numNodes; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		nodes[nodeName] = nodes_fake.TestNodeBasic{GPUs: 8, Labels: map[string]string{benchmarkHostnameKey: nodeName}}

		usedGPUs := 6
		if fullyUsed {
			usedGPUs = 8
		}
		if i%5 == 0 {
			usedGPUs -= 2
			jobs = append(jobs, &jobs_fake.TestJobBasic{
				Name:                fmt.Sprintf("releasing-job-%d", i),
				RequiredGPUsPerTask: 2,
				Priority:            constants.PriorityTrainNumber,
				QueueName:           "queue-0",
				Tasks: []*tasks_fake.TestTaskBasic{{
					State:             pod_status.Releasing,
					NodeName:          nodeName,
					PodAffinityLabels: benchmarkReleasingLabels,
				}},
			})
		}
		jobs = append(jobs, &jobs_fake.TestJobBasic{
			Name:                fmt.Sprintf("running-job-%d", i),
			RequiredGPUsPerTask: float64(usedGPUs),
			Priority:            constants.PriorityTrainNumber,
			QueueName:           "queue-0",
			Tasks:               []*tasks_fake.TestTaskBasic{{State: pod_status.Running, NodeName: nodeName}},
		})
	}

	for i := 0; i < numJobs; i++ {
		jobs = append(jobs, &jobs_fake.TestJobBasic{
			Name:                fmt.Sprintf("pending-job-%d", i),
			RequiredGPUsPerTask: 1,
			Priority:            constants.PriorityTrainNumber,
			QueueName:           "queue-1",
			Tasks: []*tasks_fake.TestTaskBasic{{
				State:                      pod_status.Pending,
				PodAntiAffinitySelector:    benchmarkReleasingLabels,
				PodAntiAffinityTopologyKey: benchmarkHostnameKey,
			}},
		})
	}

	totalGPUs := float64(numNodes * 8)
	return test_utils.TestTopologyBasic{
		Name:  "benchmark-topology-releasing",
		Nodes: nodes,
		Jobs:  jobs,
		Queues: []test_utils.TestQueueBasic{
			{Name: "queue-0", DeservedGPUs: 0, GPUOverQuotaWeight: 0},
			{Name: "queue-1", DeservedGPUs: totalGPUs, GPUOverQuotaWeight: 1},
		},
		Mocks: &test_utils.TestMock{
			CacheRequirements: &test_utils.CacheMocking{
				NumberOfCacheBinds:      numJobs * 2,
				NumberOfCacheEvictions:  numNodes * 2,
				NumberOfPipelineActions: numJobs * 2,
			},
		},
	}
}
