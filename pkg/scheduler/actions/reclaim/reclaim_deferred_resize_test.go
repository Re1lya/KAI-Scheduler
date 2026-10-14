// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package reclaim_test

import (
	"testing"
	"time"

	. "go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/reclaim"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

func reclaimResizeJob(priority int32, desiredCPUs, actualCPUs float64) *jobs_fake.TestJobBasic {
	return &jobs_fake.TestJobBasic{
		Name:                "resizer",
		QueueName:           "queue0",
		Priority:            priority,
		RequiredCPUsPerTask: desiredCPUs,
		Tasks: []*tasks_fake.TestTaskBasic{{
			State:        pod_status.Running,
			NodeName:     "node0",
			ResizeStatus: &tasks_fake.TestResizeStatus{ActualCPUs: actualCPUs, Deferred: true},
		}},
	}
}

func reclaimVictimJob(cpus float64) *jobs_fake.TestJobBasic {
	return &jobs_fake.TestJobBasic{
		Name:                "victim",
		QueueName:           "queue1",
		Priority:            constants.PriorityTrainNumber,
		RequiredCPUsPerTask: cpus,
		Tasks:               []*tasks_fake.TestTaskBasic{{State: pod_status.Running, NodeName: "node0"}},
	}
}

// cpuQueue sets cpu quota in millicores and a large memory quota so only cpu matters.
func cpuQueue(name string, deservedMilliCPUs float64) test_utils.TestQueueBasic {
	return test_utils.TestQueueBasic{
		Name:           name,
		DeservedCPUs:   test_utils.CreateFloat64Pointer(deservedMilliCPUs),
		DeservedMemory: test_utils.CreateFloat64Pointer(100000),
	}
}

func TestReclaimForDeferredResize(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	for testNumber, topology := range deferredResizeReclaimTopologies() {
		t.Logf("Running Test %d: %s", testNumber, topology.Name)
		ssn := test_utils.BuildSession(topology, controller)
		reclaim.New().Execute(ssn)
		test_utils.MatchExpectedAndRealTasks(t, testNumber, topology, ssn)
	}
}

func deferredResizeReclaimTopologies() []test_utils.TestTopologyBasic {
	minRuntimeProtectedQueue := cpuQueue("queue1", 1000)
	minRuntimeProtectedQueue.ReclaimMinRuntime = &metav1.Duration{Duration: time.Hour}

	expect := func(resizer, victim pod_status.PodStatus) map[string]test_utils.TestExpectedResultBasic {
		return map[string]test_utils.TestExpectedResultBasic{
			"resizer": {NodeName: "node0", Status: resizer},
			"victim":  {NodeName: "node0", Status: victim},
		}
	}
	mocks := func(evictions int) *test_utils.TestMock {
		return &test_utils.TestMock{CacheRequirements: &test_utils.CacheMocking{NumberOfCacheEvictions: evictions}}
	}

	return []test_utils.TestTopologyBasic{
		{
			Name:                     "resizing queue within fair share reclaims from a queue over fair share",
			Jobs:                     []*jobs_fake.TestJobBasic{reclaimResizeJob(constants.PriorityTrainNumber, 3, 1), reclaimVictimJob(2)},
			Nodes:                    map[string]nodes_fake.TestNodeBasic{"node0": {CPUMillis: 4}},
			Queues:                   []test_utils.TestQueueBasic{cpuQueue("queue0", 3000), cpuQueue("queue1", 1000)},
			JobExpectedResults:       expect(pod_status.Running, pod_status.Releasing),
			Mocks:                    mocks(1),
			DeferredResizePreemption: true,
		},
		{
			Name:                     "resize would put the queue over fair share",
			Jobs:                     []*jobs_fake.TestJobBasic{reclaimResizeJob(constants.PriorityTrainNumber, 3, 1), reclaimVictimJob(2)},
			Nodes:                    map[string]nodes_fake.TestNodeBasic{"node0": {CPUMillis: 4}},
			Queues:                   []test_utils.TestQueueBasic{cpuQueue("queue0", 1000), cpuQueue("queue1", 3000)},
			JobExpectedResults:       expect(pod_status.Running, pod_status.Running),
			Mocks:                    mocks(0),
			DeferredResizePreemption: true,
		},
		{
			Name:                     "feature disabled",
			Jobs:                     []*jobs_fake.TestJobBasic{reclaimResizeJob(constants.PriorityTrainNumber, 3, 1), reclaimVictimJob(2)},
			Nodes:                    map[string]nodes_fake.TestNodeBasic{"node0": {CPUMillis: 4}},
			Queues:                   []test_utils.TestQueueBasic{cpuQueue("queue0", 3000), cpuQueue("queue1", 1000)},
			JobExpectedResults:       expect(pod_status.Running, pod_status.Running),
			Mocks:                    mocks(0),
			DeferredResizePreemption: false,
		},
		{
			Name:                     "non-preemptible resizer within deserved quota",
			Jobs:                     []*jobs_fake.TestJobBasic{reclaimResizeJob(constants.PriorityBuildNumber, 3, 1), reclaimVictimJob(2)},
			Nodes:                    map[string]nodes_fake.TestNodeBasic{"node0": {CPUMillis: 4}},
			Queues:                   []test_utils.TestQueueBasic{cpuQueue("queue0", 3000), cpuQueue("queue1", 1000)},
			JobExpectedResults:       expect(pod_status.Running, pod_status.Releasing),
			Mocks:                    mocks(1),
			DeferredResizePreemption: true,
		},
		{
			Name:                     "non-preemptible resizer beyond deserved quota",
			Jobs:                     []*jobs_fake.TestJobBasic{reclaimResizeJob(constants.PriorityBuildNumber, 4, 1), reclaimVictimJob(2)},
			Nodes:                    map[string]nodes_fake.TestNodeBasic{"node0": {CPUMillis: 5}},
			Queues:                   []test_utils.TestQueueBasic{cpuQueue("queue0", 3000), cpuQueue("queue1", 0)},
			JobExpectedResults:       expect(pod_status.Running, pod_status.Running),
			Mocks:                    mocks(0),
			DeferredResizePreemption: true,
		},
		{
			Name:                     "victim protected by reclaim min-runtime",
			Jobs:                     []*jobs_fake.TestJobBasic{reclaimResizeJob(constants.PriorityTrainNumber, 3, 1), reclaimVictimJob(2)},
			Nodes:                    map[string]nodes_fake.TestNodeBasic{"node0": {CPUMillis: 4}},
			Queues:                   []test_utils.TestQueueBasic{cpuQueue("queue0", 3000), minRuntimeProtectedQueue},
			JobExpectedResults:       expect(pod_status.Running, pod_status.Running),
			Mocks:                    mocks(0),
			DeferredResizePreemption: true,
		},
	}
}
