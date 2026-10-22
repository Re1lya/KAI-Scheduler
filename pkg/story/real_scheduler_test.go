// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package story_test

// This test runs the real scheduler against fake clients, the way cmd/snapshot-tool replays a
// cluster, so that the story is built from events the production code actually emitted rather than
// from hand-written ones. The scenario mirrors the e2e spec
// "High preemptible PREEMPTING low preemptible" in test/e2e/suites/preempt/preempt_specs.go.

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	kaiclient "github.com/kai-scheduler/api/client/clientset/versioned"
	kaifake "github.com/kai-scheduler/api/client/clientset/versioned/fake"
	"github.com/kai-scheduler/api/constants"
	enginev2 "github.com/kai-scheduler/api/scheduling/v2"
	enginev2alpha2 "github.com/kai-scheduler/api/scheduling/v2alpha2"

	"github.com/kai-scheduler/KAI-scheduler/pkg/podgrouper/topowner"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/cache"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/conf_util"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins"
	"github.com/kai-scheduler/KAI-scheduler/pkg/story"
)

const (
	testNamespace  = "team-a"
	testQueueName  = "research"
	testSchedName  = "kai-scheduler"
	lowPriorityPC  = "low-priority"
	highPriorityPC = "high-priority"

	lowOwnerUID  = types.UID("3b8d2f61-9c4a-4e07-b1d8-5f2a6c90e374")
	highOwnerUID = types.UID("d47e5a90-1f6b-4c82-a3e9-8b0d2c71f5a6")
)

// workload is one Job plus the PodGroup and pod the podgrouper would have created for it.
type workload struct {
	ownerName     string
	ownerUID      types.UID
	priorityClass string
	node          string
	created       time.Time
}

func (w workload) podGroupName() string {
	return "pg-" + w.ownerName + "-" + string(w.ownerUID)
}

// podGroup builds the PodGroup with the same top-owner annotation the default grouper writes.
func (w workload) podGroup(t *testing.T) *enginev2alpha2.PodGroup {
	t.Helper()
	owner := &unstructured.Unstructured{}
	owner.SetAPIVersion("batch/v1")
	owner.SetKind("Job")
	owner.SetNamespace(testNamespace)
	owner.SetName(w.ownerName)
	owner.SetUID(w.ownerUID)

	metadata := topowner.GetTopOwnerMetadata(owner)
	marshalled, err := metadata.MarshalYAML()
	require.NoError(t, err)

	return &enginev2alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              w.podGroupName(),
			Namespace:         testNamespace,
			UID:               types.UID("pg-" + string(w.ownerUID)),
			CreationTimestamp: metav1.NewTime(w.created),
			Annotations:       map[string]string{constants.TopOwnerMetadataKey: marshalled},
		},
		Spec: enginev2alpha2.PodGroupSpec{
			MinMember:         ptr.To(int32(1)),
			Queue:             testQueueName,
			PriorityClassName: w.priorityClass,
		},
	}
}

func (w workload) pod() *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              w.ownerName + "-0",
			Namespace:         testNamespace,
			UID:               types.UID("pod-" + string(w.ownerUID)),
			CreationTimestamp: metav1.NewTime(w.created),
			Annotations:       map[string]string{constants.PodGroupAnnotationForPod: w.podGroupName()},
			Labels:            map[string]string{constants.DefaultQueueLabel: testQueueName},
		},
		Spec: v1.PodSpec{
			SchedulerName:     testSchedName,
			PriorityClassName: w.priorityClass,
			NodeName:          w.node,
			Containers: []v1.Container{{
				Name:  "main",
				Image: "ubuntu",
				Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("500m"),
				}},
			}},
		},
		Status: v1.PodStatus{Phase: v1.PodPending},
	}
	if w.node != "" {
		pod.Status.Phase = v1.PodRunning
	}
	return pod
}

// runScheduler replays scheduling cycles of the real scheduler over the given objects. More than
// one cycle is needed to see a preemption through: the first frees the resources, a later one
// allocates them, exactly as the scheduler's own loop does.
func runScheduler(t *testing.T, ctx context.Context, kube kubernetes.Interface, kai kaiclient.Interface, cycles int) {
	t.Helper()
	require.NoError(t, log.InitLoggers(0, false))
	actions.InitDefaultActions()
	plugins.InitDefaultPlugins()

	schedulerParams := &conf.SchedulerParams{
		SchedulerName:   testSchedName,
		PartitionParams: &conf.SchedulingNodePoolParams{},
	}
	schedulerCache, err := cache.New(&cache.SchedulerCacheParams{
		KubeClient:                  kube,
		KAISchedulerClient:          kai,
		SchedulerName:               schedulerParams.SchedulerName,
		NodePoolParams:              schedulerParams.PartitionParams,
		NumOfStatusRecordingWorkers: 1,
		DetailedFitErrors:           true,
		DiscoveryClient:             kube.Discovery(),
	})
	require.NoError(t, err)

	schedulerCache.Run(ctx.Done())
	schedulerCache.WaitForCacheSync(ctx.Done())

	config, err := conf_util.GetDefaultSchedulerConf()
	require.NoError(t, err)
	configuredActions, err := conf_util.GetActionsFromConfig(config)
	require.NoError(t, err)

	for cycle := range cycles {
		session, err := framework.OpenSession(schedulerCache, config, schedulerParams,
			fmt.Sprintf("story-test-%d", cycle), &http.ServeMux{})
		require.NoError(t, err)
		for _, action := range configuredActions {
			action.Execute(session)
		}
		framework.CloseSession(session)

		// The evictor deletes pods on its own goroutine; the next cycle must see the freed node.
		time.Sleep(500 * time.Millisecond)
		schedulerCache.WaitForCacheSync(ctx.Done())
	}
}

// collectWhenSettled polls until the asynchronous event broadcaster and status updater have
// written their events, since both run on their own goroutines.
func collectWhenSettled(t *testing.T, ctx context.Context, kube kubernetes.Interface, kai kaiclient.Interface) *story.Snapshot {
	t.Helper()
	var snapshot *story.Snapshot
	require.Eventually(t, func() bool {
		var err error
		snapshot, err = story.Collect(ctx, kube, kai, testNamespace)
		return err == nil && len(snapshot.Events) > 0
	}, 30*time.Second, 200*time.Millisecond, "the scheduler emitted no events")
	return snapshot
}

func TestStoryFromRealSchedulerRun(t *testing.T) {
	submitted := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	low := workload{ownerName: "low-prio", ownerUID: lowOwnerUID, priorityClass: lowPriorityPC,
		node: "gpu-node-1", created: submitted}
	high := workload{ownerName: "high-prio", ownerUID: highOwnerUID, priorityClass: highPriorityPC,
		created: submitted.Add(5 * time.Minute)}

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-node-1"},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("500m"),
				v1.ResourceMemory: resource.MustParse("4Gi"),
				v1.ResourcePods:   resource.MustParse("10"),
			},
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
		},
	}
	priorityClasses := []*schedulingv1.PriorityClass{
		{ObjectMeta: metav1.ObjectMeta{Name: lowPriorityPC}, Value: 50},
		{ObjectMeta: metav1.ObjectMeta{Name: highPriorityPC}, Value: 51},
	}
	queues := []*enginev2.Queue{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "default-parent"},
			Spec: enginev2.QueueSpec{Resources: &enginev2.QueueResources{
				CPU:    enginev2.QueueResource{Quota: -1, Limit: -1, OverQuotaWeight: 1},
				GPU:    enginev2.QueueResource{Quota: -1, Limit: -1, OverQuotaWeight: 1},
				Memory: enginev2.QueueResource{Quota: -1, Limit: -1, OverQuotaWeight: 1},
			}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: testQueueName},
			Spec: enginev2.QueueSpec{ParentQueue: "default-parent", Resources: &enginev2.QueueResources{
				CPU:    enginev2.QueueResource{Quota: 500, Limit: 500, OverQuotaWeight: 1},
				GPU:    enginev2.QueueResource{Quota: -1, Limit: -1, OverQuotaWeight: 1},
				Memory: enginev2.QueueResource{Quota: -1, Limit: -1, OverQuotaWeight: 1},
			}},
		},
	}

	kubeObjects := []runtime.Object{node, low.pod(), high.pod()}
	for _, priorityClass := range priorityClasses {
		kubeObjects = append(kubeObjects, priorityClass)
	}
	kaiObjects := []runtime.Object{low.podGroup(t), high.podGroup(t)}
	for _, queue := range queues {
		kaiObjects = append(kaiObjects, queue)
	}

	kube := kubefake.NewSimpleClientset(kubeObjects...)
	kai := kaifake.NewSimpleClientset(kaiObjects...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runScheduler(t, ctx, kube, kai, 3)
	snapshot := collectWhenSettled(t, ctx, kube, kai)

	t.Run("the victim's story names the preemptor", func(t *testing.T) {
		candidates := story.Resolve(snapshot, low.ownerName, testNamespace)
		require.Len(t, candidates, 1)
		assert.Equal(t, low.ownerUID, candidates[0].Root.UID)
		assert.Equal(t, "Job", candidates[0].Root.Kind)

		result := story.Build(snapshot, candidates[0].Root)
		var evicted *story.Entry
		for i, entry := range result.Entries {
			if entry.Decision == story.Evicted {
				evicted = &result.Entries[i]
			}
		}
		require.NotNil(t, evicted, "expected an Evict entry, got %+v", result.Entries)
		assert.Equal(t, constants.ActionPreempt, evicted.Action)
		require.NotNil(t, evicted.By)
		assert.Equal(t, high.ownerUID, evicted.By.UID)
	})

	t.Run("the preemptor's story shows the allocation it won", func(t *testing.T) {
		candidates := story.Resolve(snapshot, high.ownerName, testNamespace)
		require.Len(t, candidates, 1)

		result := story.Build(snapshot, candidates[0].Root)
		var allocated bool
		for _, entry := range result.Entries {
			if entry.Decision == story.Allocated {
				allocated = true
				assert.Equal(t, "gpu-node-1", entry.Node)
			}
		}
		assert.True(t, allocated, "expected an Allocated entry, got %+v", result.Entries)
	})
}
