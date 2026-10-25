// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package preempt_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	. "go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/preempt"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/topology_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
	kaiv1alpha1 "github.com/kai-scheduler/api/kai/v1alpha1"
)

// The running GPU sibling is already placed, so it is no GPU demand for a CPU-only victim to cover.
func TestPreemptForDeferredResize_TopologyGangWithRunningGPUSibling(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()

	resizer := deferredResizeJob("resizer", "node0", 3, 1)
	resizer.RequiredGPUsPerTask = 1
	resizer.RootSubGroupSet = subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName,
		&topology_info.TopologyConstraintInfo{Topology: "cluster-topology", RequiredLevel: "rack"})
	resizer.RootSubGroupSet.AddPodSet(subgroup_info.NewPodSet("default", 2, nil))
	resizer.Tasks = append(resizer.Tasks, &tasks_fake.TestTaskBasic{State: pod_status.Running, NodeName: "node0"})

	topology := test_utils.TestTopologyBasic{
		Name: "topology gang with a running GPU sibling",
		Jobs: []*jobs_fake.TestJobBasic{
			resizer,
			runningCPUJob("victim", "queue0", "node0", constants.PriorityTrainNumber, 2),
		},
		Nodes: map[string]nodes_fake.TestNodeBasic{
			"node0": {CPUMillis: 7, GPUs: 2, Labels: map[string]string{"rack": "rack0"}},
		},
		Queues: []test_utils.TestQueueBasic{{Name: "queue0", DeservedGPUs: 2}},
		Topologies: []*kaiv1alpha1.Topology{{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-topology"},
			Spec:       kaiv1alpha1.TopologySpec{Levels: []kaiv1alpha1.TopologyLevel{{NodeLabel: "rack"}}},
		}},
		Mocks:                    evictionMocks(1),
		DeferredResizePreemption: true,
	}
	ssn := test_utils.BuildSession(topology, controller)
	preempt.New().Execute(ssn)

	assert.Equal(t, pod_status.Releasing, ssn.ClusterInfo.PodGroupInfos["victim"].GetAllPodsMap()["victim-0"].Status)
	for _, task := range ssn.ClusterInfo.PodGroupInfos["resizer"].GetAllPodsMap() {
		assert.Equal(t, pod_status.Running, task.Status, task.Name)
	}
}
