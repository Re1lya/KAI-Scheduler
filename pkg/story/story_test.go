// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package story

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"

	kaifake "github.com/kai-scheduler/api/client/clientset/versioned/fake"
	"github.com/kai-scheduler/api/constants"
	"github.com/kai-scheduler/api/scheduling/v2alpha2"
)

const (
	liveTrainLLM    = types.UID("c91b4e2a-8f17-4c3d-b5a6-1d9e7a02f4c8")
	deletedTrainLLM = types.UID("7f3a19c2-5b0e-4d6a-9a41-2c8e0f7d1b33")
	highPrioJob     = types.UID("d47e5a90-1f6b-4c82-a3e9-8b0d2c71f5a6")
	lowPrioJob      = types.UID("3b8d2f61-9c4a-4e07-b1d8-5f2a6c90e374")
)

func loadFixture(t *testing.T, name string) *Snapshot {
	t.Helper()
	snapshot, err := LoadSnapshot("testdata/" + name + ".json")
	require.NoError(t, err)
	return snapshot
}

func entriesOf(story *Story, decision Decision) []Entry {
	var matching []Entry
	for _, entry := range story.Entries {
		if entry.Decision == decision {
			matching = append(matching, entry)
		}
	}
	return matching
}

func TestResolve(t *testing.T) {
	train := loadFixture(t, "train-llm")

	t.Run("same name twice gives both incarnations, live first", func(t *testing.T) {
		candidates := Resolve(train, "train-llm", "")
		require.Len(t, candidates, 2)
		assert.Equal(t, liveTrainLLM, candidates[0].Root.UID)
		assert.True(t, candidates[0].Live)
		assert.Equal(t, "PyTorchJob", candidates[0].Root.Kind)
		assert.Equal(t, deletedTrainLLM, candidates[1].Root.UID)
		assert.False(t, candidates[1].Live)
	})

	t.Run("pod, podgroup and root names collapse into one candidate", func(t *testing.T) {
		preempted := loadFixture(t, "preempted")
		assert.Len(t, Resolve(preempted, "low-prio", ""), 1)
		assert.Len(t, Resolve(preempted, "low-prio-r7k2d", ""), 1)
		assert.Len(t, Resolve(preempted, "pg-low-prio-"+string(lowPrioJob), ""), 1)
	})

	t.Run("namespace filter", func(t *testing.T) {
		assert.Empty(t, Resolve(train, "train-llm", "team-b"))
		assert.Len(t, Resolve(train, "train-llm", "team-a"), 2)
	})

	t.Run("unknown name", func(t *testing.T) {
		assert.Empty(t, Resolve(train, "nope", ""))
	})
}

func TestSelect(t *testing.T) {
	candidates := Resolve(loadFixture(t, "train-llm"), "train-llm", "")

	t.Run("single candidate is chosen silently", func(t *testing.T) {
		selected, err := Select(candidates[:1], "", nil, nil)
		require.NoError(t, err)
		assert.Equal(t, liveTrainLLM, selected.Root.UID)
	})

	t.Run("uid prefix narrows to one", func(t *testing.T) {
		selected, err := Select(candidates, "7f3a", nil, nil)
		require.NoError(t, err)
		assert.Equal(t, deletedTrainLLM, selected.Root.UID)
	})

	t.Run("ambiguous without a way to ask", func(t *testing.T) {
		_, err := Select(candidates, "", nil, nil)
		assert.ErrorIs(t, err, ErrAmbiguous)
	})

	t.Run("asks and takes the answer", func(t *testing.T) {
		var out strings.Builder
		selected, err := Select(candidates, "", strings.NewReader("2\n"), &out)
		require.NoError(t, err)
		assert.Equal(t, deletedTrainLLM, selected.Root.UID)
		assert.Contains(t, out.String(), "Found 2 workloads")
	})

	t.Run("rejects an invalid answer", func(t *testing.T) {
		_, err := Select(candidates, "", strings.NewReader("7\n"), &strings.Builder{})
		assert.Error(t, err)
	})

	t.Run("nothing matches", func(t *testing.T) {
		_, err := Select(nil, "", nil, nil)
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestBuildLiveWorkload(t *testing.T) {
	snapshot := loadFixture(t, "train-llm")
	story := Build(snapshot, Resolve(snapshot, "train-llm", "")[0].Root)

	assert.Equal(t, "Running", story.Verdict.State)
	assert.Equal(t, "4 pods: 4 Running", story.Verdict.Pods)
	assert.Empty(t, story.Notes)

	t.Run("keeps the other incarnation's history out", func(t *testing.T) {
		for _, entry := range story.Entries {
			assert.NotEqual(t, 40, entry.Count)
		}
	})

	t.Run("pending run is merged and not echoed per pod", func(t *testing.T) {
		pending := entriesOf(story, Pending)
		require.Len(t, pending, 1)
		assert.Equal(t, 212, pending[0].Count)
		assert.Equal(t, "PodGroup", pending[0].Subjects[0].Kind)
	})

	t.Run("bind failure precedes the successful bind of the same pod", func(t *testing.T) {
		failed := entriesOf(story, BindFailed)
		require.Len(t, failed, 1)
		assert.Equal(t, "train-llm-worker-3", failed[0].Subjects[0].Name)

		var bound Entry
		for _, entry := range entriesOf(story, Bound) {
			if entry.Subjects[0].Name == "train-llm-worker-3" {
				bound = entry
			}
		}
		assert.True(t, failed[0].Time.Before(bound.Time))
	})

	t.Run("pods bound to different nodes are not collapsed together", func(t *testing.T) {
		assert.Len(t, entriesOf(story, Allocated), 4)
	})

	t.Run("entries are in time order", func(t *testing.T) {
		for i := 1; i < len(story.Entries); i++ {
			assert.False(t, story.Entries[i].Time.Before(story.Entries[i-1].Time))
		}
	})
}

func TestBuildDeletedIncarnation(t *testing.T) {
	snapshot := loadFixture(t, "train-llm")
	story := Build(snapshot, Resolve(snapshot, "train-llm", "")[1].Root)

	assert.Equal(t, "Deleted", story.Verdict.State)
	require.Len(t, story.Entries, 1)
	assert.Equal(t, Pending, story.Entries[0].Decision)
	assert.Equal(t, 40, story.Entries[0].Count)
	assert.NotEmpty(t, story.Notes)
}

func TestBuildPreemptedWorkload(t *testing.T) {
	snapshot := loadFixture(t, "preempted")
	story := Build(snapshot, Resolve(snapshot, "low-prio", "")[0].Root)

	evicted := entriesOf(story, Evicted)
	require.Len(t, evicted, 1)
	assert.Equal(t, "preempt", evicted[0].Action)
	require.NotNil(t, evicted[0].By)
	assert.Equal(t, highPrioJob, evicted[0].By.UID, "the preemptor is linked by its root workload UID")
	assert.Equal(t, "Job", evicted[0].By.Kind)

	assert.Equal(t, "Pending", story.Verdict.State)
	assert.Contains(t, story.Verdict.Why, "Insufficient nvidia.com/gpu")
}

func TestEntriesSharingATimestampFollowTheLifecycle(t *testing.T) {
	second := time.Date(2026, 10, 20, 22, 43, 49, 0, time.UTC)
	pod := Ref{Kind: "Pod", Name: "p0", UID: "p0"}
	// Deliberately reversed: the allocation is listed before the pipelining that produced it.
	entries := []Entry{
		{Time: second, Count: 1, Subjects: []Ref{pod}, Decision: Allocated},
		{Time: second, Count: 1, Subjects: []Ref{pod}, Decision: Pipelined},
		{Time: second, Count: 1, Subjects: []Ref{pod}, Decision: Bound},
	}

	var ordered []Decision
	for _, entry := range mergeRepeats(entries) {
		ordered = append(ordered, entry.Decision)
	}
	assert.Equal(t, []Decision{Pipelined, Allocated, Bound}, ordered)
}

func TestEventStartPrefersTheMostPreciseTimestamp(t *testing.T) {
	precise := time.Date(2026, 10, 20, 22, 43, 49, 123456000, time.UTC)
	event := &corev1.Event{
		EventTime:      metav1.NewMicroTime(precise),
		FirstTimestamp: metav1.NewTime(precise.Truncate(time.Second)),
	}
	assert.Equal(t, precise, eventStart(event).UTC())

	onlyLegacy := &corev1.Event{FirstTimestamp: metav1.NewTime(precise.Truncate(time.Second))}
	assert.Equal(t, precise.Truncate(time.Second), eventStart(onlyLegacy).UTC())
}

func TestReclaimIsADecisionInTheReclaimersOwnStory(t *testing.T) {
	snapshot := loadFixture(t, "reclaim-repipeline")
	reclaimer := Build(snapshot, Resolve(snapshot, "batch-infer", "team-b")[0].Root)

	evicting := entriesOf(reclaimer, Evicting)
	require.Len(t, evicting, 1, "the two Evict events of one reclaim belong in one entry")
	assert.Equal(t, "reclaim", evicting[0].Action)
	assert.Equal(t, "PodGroup", evicting[0].Subjects[0].Kind)

	require.Len(t, evicting[0].Victims, 2)
	assert.Equal(t, "long-run-0", evicting[0].Victims[0].Name)
	assert.Equal(t, "gpu-01", evicting[0].Victims[0].Node)
	assert.Equal(t, "gpu-02", evicting[0].Victims[1].Node)
	assert.Contains(t, evicting[0].Message, "evicted 2 pods of Job.batch team-b/long-run")
	assert.Contains(t, evicting[0].Message, "long-run-0 from gpu-01")

	t.Run("it precedes the pipelining it made room for", func(t *testing.T) {
		pipelined := entriesOf(reclaimer, Pipelined)
		require.NotEmpty(t, pipelined)
		assert.True(t, evicting[0].Time.Before(pipelined[0].Time))
	})

	t.Run("the victim still reports being evicted", func(t *testing.T) {
		victim := Build(snapshot, Resolve(snapshot, "long-run", "team-b")[0].Root)
		assert.Len(t, entriesOf(victim, Evicted), 2)
		assert.Empty(t, entriesOf(victim, Evicting))
	})
}

func TestVictimNodeFallsBackToTheNextPipeline(t *testing.T) {
	snapshot := loadFixture(t, "reclaim-repipeline")

	t.Run("a terminating victim still carries its node", func(t *testing.T) {
		for i := range snapshot.Pods {
			if snapshot.Pods[i].Name == "long-run-0" {
				deleting := metav1.NewTime(time.Now())
				snapshot.Pods[i].DeletionTimestamp = &deleting
			}
		}
		evicting := entriesOf(Build(snapshot, Resolve(snapshot, "batch-infer", "team-b")[0].Root), Evicting)
		require.Len(t, evicting, 1)
		assert.Equal(t, "gpu-01", evicting[0].Victims[0].Node)
		assert.NotContains(t, evicting[0].Message, "pipelined onto")
	})

	t.Run("a deleted victim says so and points at the next pipeline", func(t *testing.T) {
		snapshot.Pods = slices.DeleteFunc(snapshot.Pods, func(pod corev1.Pod) bool {
			return strings.HasPrefix(pod.Name, "long-run-")
		})
		evicting := entriesOf(Build(snapshot, Resolve(snapshot, "batch-infer", "team-b")[0].Root), Evicting)
		require.Len(t, evicting, 1)
		assert.Empty(t, evicting[0].Victims[0].Node)
		assert.Contains(t, evicting[0].Message, "node unrecorded, pod already gone")
		// The first pipeline after the eviction went to gpu-01 and gpu-02, not the later gpu-03.
		assert.Contains(t, evicting[0].Message, "pipelined onto gpu-01, gpu-02 next")
		assert.NotContains(t, evicting[0].Message, "gpu-03")
	})
}

func TestGroupingWarningsAreDroppedForOwnerlessPods(t *testing.T) {
	snapshot := loadFixture(t, "train-llm")
	root := Resolve(snapshot, "train-llm", "")[0].Root

	owned, bare := &snapshot.Pods[0], &snapshot.Pods[1]
	owned.OwnerReferences = []metav1.OwnerReference{{Kind: root.Kind, Name: root.Name, UID: root.UID}}
	warn := func(pod *corev1.Pod, at time.Time) corev1.Event {
		return corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name + ".warn"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Reason:         constants.PodGrouperWarning,
			Message:        "failed to resolve the pod owner",
			EventTime:      metav1.NewMicroTime(at),
			Count:          1,
		}
	}
	at := owned.CreationTimestamp.Time
	snapshot.Events = append(snapshot.Events, warn(owned, at), warn(bare, at.Add(time.Second)))

	grouping := entriesOf(Build(snapshot, root), GroupingFailed)
	require.Len(t, grouping, 1)
	assert.Equal(t, owned.Name, grouping[0].Subjects[0].Name)
}

func TestVictimOf(t *testing.T) {
	// The four messages pkg/scheduler/api/unschedule_info.go builds for an eviction.
	cases := map[string]string{
		"Pod ns/p0 was preempted by higher priority workload ns/pg-other":                               "p0",
		"Pod ns/p1 was preempted by workload ns/pg-other.":                                              "p1",
		"Pod ns/p2 was preempted and rescheduled due to bin packing (resource consolidation) procedure": "p2",
		"Workload doesn't meet the minimum required number of pods (3), evicting remaining pod: ns/p3":  "p3",
	}
	for message, want := range cases {
		namespace, name, found := victimOf(message)
		require.True(t, found, message)
		assert.Equal(t, "ns", namespace)
		assert.Equal(t, want, name)
	}

	_, _, found := victimOf("something the scheduler does not say")
	assert.False(t, found)
}

func TestRootOfFallsBackToPodGroup(t *testing.T) {
	podGroup := &v2alpha2.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pg", UID: "pg-uid"}}
	root := rootOf(podGroup)
	assert.Equal(t, Ref{Kind: "PodGroup", Namespace: "ns", Name: "pg", UID: "pg-uid"}, root.Ref)
}

func TestCollect(t *testing.T) {
	podGroup := &v2alpha2.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "pg"}}
	snapshot, err := Collect(context.Background(), kubefake.NewSimpleClientset(), kaifake.NewSimpleClientset(podGroup), "team-a")
	require.NoError(t, err)
	assert.Len(t, snapshot.PodGroups, 1)
}
