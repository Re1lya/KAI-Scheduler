// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package preempt_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	. "go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/preempt"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
	enginev2alpha2 "github.com/kai-scheduler/api/scheduling/v2alpha2"
)

const resizerPriority = constants.PriorityInteractivePreemptibleNumber

func deferredResizeTask(nodeName string, actualCPUs float64) *tasks_fake.TestTaskBasic {
	return &tasks_fake.TestTaskBasic{
		State:        pod_status.Running,
		NodeName:     nodeName,
		ResizeStatus: &tasks_fake.TestResizeStatus{ActualCPUs: actualCPUs, Deferred: true},
	}
}

func deferredResizeJob(name, nodeName string, desiredCPUs, actualCPUs float64) *jobs_fake.TestJobBasic {
	return &jobs_fake.TestJobBasic{
		Name:                name,
		QueueName:           "queue0",
		Priority:            resizerPriority,
		RequiredCPUsPerTask: desiredCPUs,
		Tasks:               []*tasks_fake.TestTaskBasic{deferredResizeTask(nodeName, actualCPUs)},
	}
}

func runningCPUJob(name, queueName, nodeName string, priority int32, cpus float64) *jobs_fake.TestJobBasic {
	return &jobs_fake.TestJobBasic{
		Name:                name,
		QueueName:           queueName,
		Priority:            priority,
		RequiredCPUsPerTask: cpus,
		Tasks:               []*tasks_fake.TestTaskBasic{{State: pod_status.Running, NodeName: nodeName}},
	}
}

func cpuNodes(names ...string) map[string]nodes_fake.TestNodeBasic {
	nodes := map[string]nodes_fake.TestNodeBasic{}
	for _, name := range names {
		nodes[name] = nodes_fake.TestNodeBasic{CPUMillis: 4}
	}
	return nodes
}

func expectStatuses(nodeName string, statuses map[string]pod_status.PodStatus) map[string]test_utils.TestExpectedResultBasic {
	results := map[string]test_utils.TestExpectedResultBasic{}
	for jobName, status := range statuses {
		results[jobName] = test_utils.TestExpectedResultBasic{NodeName: nodeName, Status: status}
	}
	return results
}

func noEvictionMocks() *test_utils.TestMock {
	return &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{}}
}

func evictionMocks(evictions int) *test_utils.TestMock {
	return &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{NumberOfCacheEvictions: evictions}}
}

func TestPreemptForDeferredResize(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	for testNumber, topology := range deferredResizePreemptTopologies() {
		t.Logf("Running Test %d: %s", testNumber, topology.Name)
		ssn := test_utils.BuildSession(topology, controller)
		preempt.New().Execute(ssn)
		test_utils.MatchExpectedAndRealTasks(t, testNumber, topology, ssn)
	}
}

func deferredResizePreemptTopologies() []test_utils.TestTopologyBasic {
	queues := []test_utils.TestQueueBasic{{Name: "queue0"}}
	nonPreemptibleVictim := runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2)
	nonPreemptibleVictim.Preemptibility = enginev2alpha2.NonPreemptible
	preemptionDisabled := deferredResizeJob("resizer", "node0", 3, 1)
	preemptionDisabled.Tasks[0].ResizeStatus.PreemptionDisabled = true

	return []test_utils.TestTopologyBasic{
		{
			Name: "evicts a lower priority victim on the resizing pod's node",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 3, 1),
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Releasing,
			}),
			Mocks:                    evictionMocks(1),
			DeferredResizePreemption: true,
		},
		{
			Name: "victim only on another node",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 3, 1),
				runningCPUJob("peer", "queue0", "node0", resizerPriority, 2),
				runningCPUJob("victim", "queue0", "node1", constants.PriorityTrainNumber, 2),
			},
			Nodes:  cpuNodes("node0", "node1"),
			Queues: queues,
			JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
				"resizer": {NodeName: "node0", Status: pod_status.Running},
				"peer":    {NodeName: "node0", Status: pod_status.Running},
				"victim":  {NodeName: "node1", Status: pod_status.Running},
			},
			Mocks:                    noEvictionMocks(),
			DeferredResizePreemption: true,
		},
		{
			Name: "no room even after evicting all eligible victims",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 4, 1),
				runningCPUJob("peer", "queue0", "node0", resizerPriority, 1),
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 1),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"peer":    pod_status.Running,
				"victim":  pod_status.Running,
			}),
			Mocks:                    noEvictionMocks(),
			DeferredResizePreemption: true,
		},
		{
			Name: "node already has room for the resize",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 3, 1),
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 1),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Running,
			}),
			Mocks:                    noEvictionMocks(),
			DeferredResizePreemption: true,
		},
		{
			Name: "node disabled preemption for the resize",
			Jobs: []*jobs_fake.TestJobBasic{
				preemptionDisabled,
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Running,
			}),
			Mocks:                    noEvictionMocks(),
			DeferredResizePreemption: true,
		},
		{
			Name: "feature disabled",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 3, 1),
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Running,
			}),
			Mocks: noEvictionMocks(),
		},
		{
			Name: "equal priority victim is not preempted",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 3, 1),
				runningCPUJob("victim", "queue0", "node0", resizerPriority, 2),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Running,
			}),
			Mocks:                    noEvictionMocks(),
			DeferredResizePreemption: true,
		},
		{
			Name: "higher priority victim is not preempted",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 3, 1),
				runningCPUJob("victim", "queue0", "node0", constants.PriorityBuildNumber, 2),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Running,
			}),
			Mocks:                    noEvictionMocks(),
			DeferredResizePreemption: true,
		},
		{
			Name: "non-preemptible victim is not preempted",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 3, 1),
				nonPreemptibleVictim,
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Running,
			}),
			Mocks:                    noEvictionMocks(),
			DeferredResizePreemption: true,
		},
		{
			Name: "gang victim spanning nodes is evicted whole",
			Jobs: []*jobs_fake.TestJobBasic{
				deferredResizeJob("resizer", "node0", 3, 1),
				{
					Name:                "victim",
					QueueName:           "queue0",
					Priority:            constants.PriorityTrainNumber,
					RequiredCPUsPerTask: 2,
					RootSubGroupSet:     jobs_fake.DefaultSubGroup(2),
					Tasks: []*tasks_fake.TestTaskBasic{
						{State: pod_status.Running, NodeName: "node0"},
						{State: pod_status.Running, NodeName: "node1"},
					},
				},
			},
			Nodes:  cpuNodes("node0", "node1"),
			Queues: queues,
			JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
				"resizer": {NodeName: "node0", Status: pod_status.Running},
				"victim":  {Status: pod_status.Releasing},
			},
			Mocks:                    evictionMocks(2),
			DeferredResizePreemption: true,
		},
		{
			Name: "elastic resizing job",
			Jobs: []*jobs_fake.TestJobBasic{
				{
					Name:                "resizer",
					QueueName:           "queue0",
					Priority:            resizerPriority,
					RequiredCPUsPerTask: 3,
					RootSubGroupSet:     jobs_fake.DefaultSubGroup(1),
					Tasks: []*tasks_fake.TestTaskBasic{
						deferredResizeTask("node0", 1),
						{State: pod_status.Running, NodeName: "node1"},
					},
				},
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
			},
			Nodes:  cpuNodes("node0", "node1"),
			Queues: queues,
			JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
				"resizer": {Status: pod_status.Running},
				"victim":  {NodeName: "node0", Status: pod_status.Releasing},
			},
			Mocks:                    evictionMocks(1),
			DeferredResizePreemption: true,
		},
		{
			Name: "job with unschedulable pending tasks does not preempt for its resize",
			Jobs: []*jobs_fake.TestJobBasic{
				{
					Name:                "resizer",
					QueueName:           "queue0",
					Priority:            resizerPriority,
					RequiredCPUsPerTask: 3,
					RootSubGroupSet:     jobs_fake.DefaultSubGroup(1),
					Tasks: []*tasks_fake.TestTaskBasic{
						deferredResizeTask("node0", 1),
						{State: pod_status.Pending, NodeAffinityNames: []string{"missing"}},
					},
				},
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
				"victim": {NodeName: "node0", Status: pod_status.Running},
			},
			TaskExpectedResults: map[string]test_utils.TestExpectedResultBasic{
				"resizer-0": {NodeName: "node0", Status: pod_status.Running},
				"resizer-1": {Status: pod_status.Pending},
			},
			Mocks:                    noEvictionMocks(),
			DeferredResizePreemption: true,
		},
		{
			Name: "two deferred pods of a job on one node resize together",
			Jobs: []*jobs_fake.TestJobBasic{
				{
					Name:                "resizer",
					QueueName:           "queue0",
					Priority:            resizerPriority,
					RequiredCPUsPerTask: 2,
					RootSubGroupSet:     jobs_fake.DefaultSubGroup(2),
					Tasks: []*tasks_fake.TestTaskBasic{
						deferredResizeTask("node0", 1),
						deferredResizeTask("node0", 1),
					},
				},
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Releasing,
			}),
			Mocks:                    evictionMocks(1),
			DeferredResizePreemption: true,
		},
		{
			Name: "GPU-sharing resizing pod fits back on its own node",
			Jobs: []*jobs_fake.TestJobBasic{
				{
					Name:                "resizer",
					QueueName:           "queue0",
					Priority:            resizerPriority,
					RequiredCPUsPerTask: 3,
					RequiredGPUsPerTask: 0.5,
					Tasks: []*tasks_fake.TestTaskBasic{{
						State:        pod_status.Running,
						NodeName:     "node0",
						GPUGroups:    []string{"0"},
						ResizeStatus: &tasks_fake.TestResizeStatus{ActualCPUs: 1, Deferred: true},
					}},
				},
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
			},
			Nodes:  map[string]nodes_fake.TestNodeBasic{"node0": {CPUMillis: 4, GPUs: 1}},
			Queues: []test_utils.TestQueueBasic{{Name: "queue0", DeservedGPUs: 1}},
			JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
				"resizer": {NodeName: "node0", Status: pod_status.Running, GPUsRequired: 0.5, GPUGroups: []string{"0"}},
				"victim":  {NodeName: "node0", Status: pod_status.Releasing},
			},
			Mocks:                    evictionMocks(1),
			DeferredResizePreemption: true,
		},
		{
			Name: "host-port resizing pod fits back on its own node",
			Jobs: []*jobs_fake.TestJobBasic{
				{
					Name:                "resizer",
					QueueName:           "queue0",
					Priority:            resizerPriority,
					RequiredCPUsPerTask: 3,
					Tasks: []*tasks_fake.TestTaskBasic{{
						State:        pod_status.Running,
						NodeName:     "node0",
						HostPorts:    []int32{8080},
						ResizeStatus: &tasks_fake.TestResizeStatus{ActualCPUs: 1, Deferred: true},
					}},
				},
				runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
			},
			Nodes:  cpuNodes("node0"),
			Queues: queues,
			JobExpectedResults: expectStatuses("node0", map[string]pod_status.PodStatus{
				"resizer": pod_status.Running,
				"victim":  pod_status.Releasing,
			}),
			Mocks:                    evictionMocks(1),
			DeferredResizePreemption: true,
		},
	}
}

func TestPreemptForDeferredResize_RestoresResizingPodState(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	topology := deferredResizePreemptTopologies()[0]
	ssn := test_utils.BuildSession(topology, controller)
	queue := ssn.ClusterInfo.Queues["queue0"]
	queueCPUBefore := ssn.QueueAllocatedResources(queue).Get(v1.ResourceCPU)

	preempt.New().Execute(ssn)

	node := ssn.ClusterInfo.Nodes["node0"]
	resizer := ssn.ClusterInfo.PodGroupInfos["resizer"].GetAllPodsMap()["resizer-0"]
	assert.Equal(t, pod_status.Running, resizer.Status)
	assert.False(t, resizer.IsVirtualStatus)
	assert.Equal(t, pod_status.Running, node.PodInfos[pod_info.PodKey(resizer.Pod)].Status)
	assert.Equal(t, 5000.0, node.UsedVector.Get(resource_info.CPUIndex))
	assert.Equal(t, 2000.0, node.ReleasingVector.Get(resource_info.CPUIndex))
	assert.Equal(t, -1000.0, node.IdleVector.Get(resource_info.CPUIndex))
	// The victim is released and the resizing pod keeps its desired-size charge.
	assert.Equal(t, queueCPUBefore-2000, ssn.QueueAllocatedResources(queue).Get(v1.ResourceCPU))
}
