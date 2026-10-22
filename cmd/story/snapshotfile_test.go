// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins/snapshot"
)

func writeZippedJSON(t *testing.T, path, entryName string, content any) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	zipWriter := zip.NewWriter(file)
	entry, err := zipWriter.Create(entryName)
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(entry).Encode(content))
	require.NoError(t, zipWriter.Close())
}

func pod(name string, uid types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name, UID: uid},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// writeCycleFiles lays out one state of a replay chain the way snapshot-tool does.
func writeCycleFiles(t *testing.T, dir string, cycle int, pods []*corev1.Pod, events []corev1.Event) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("a.%d%s", cycle, snapshotSuffix))
	writeZippedJSON(t, path, snapshot.SnapshotFileName, &snapshot.Snapshot{
		RawObjects: &snapshot.RawKubernetesObjects{Pods: pods},
	})
	if events != nil {
		writeZippedJSON(t, eventsPathFor(path), eventsFileName, &corev1.EventList{Items: events})
	}
	return path
}

func TestEventsPathFor(t *testing.T) {
	assert.Equal(t, "a.1.events.zip", eventsPathFor("a.1.snapshot.zip"))
	assert.Equal(t, "/tmp/a.1.events.zip", eventsPathFor("/tmp/a.1.snapshot.zip"))
	// A snapshot that does not follow the convention still pairs predictably.
	assert.Equal(t, "snapshot.events.zip", eventsPathFor("snapshot.zip"))
}

func TestSplitSnapshotArguments(t *testing.T) {
	name, snapshots := splitSnapshotArguments([]string{"a.1.snapshot.zip", "high-prio", "a.2.snapshot.zip"})
	assert.Equal(t, "high-prio", name)
	assert.Equal(t, []string{"a.1.snapshot.zip", "a.2.snapshot.zip"}, snapshots)

	name, _ = splitSnapshotArguments([]string{"high-prio", "low-prio"})
	assert.Empty(t, name, "two names is a usage error")
}

func TestLoadReplaysFollowsTheChain(t *testing.T) {
	dir := t.TempDir()
	evicted := time.Now().Truncate(time.Second)
	first := writeCycleFiles(t, dir, 1, []*corev1.Pod{pod("victim-0", "u1"), pod("winner-0", "u2")}, nil)
	second := writeCycleFiles(t, dir, 2, []*corev1.Pod{pod("winner-0", "u2")}, []corev1.Event{{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "pg.evict"},
		Reason:     "Evict",
		Message:    "Pod team-a/victim-0 was preempted by higher priority workload team-a/pg-winner",
		EventTime:  metav1.NewMicroTime(evicted),
		Count:      1,
	}})

	loaded, err := loadReplays([]string{first, second})
	require.NoError(t, err)
	require.Len(t, loaded.Events, 1)

	t.Run("a pod that vanished along the way is kept and marked deleted", func(t *testing.T) {
		require.Len(t, loaded.Pods, 2)
		assert.Equal(t, "victim-0", loaded.Pods[0].Name)
		require.NotNil(t, loaded.Pods[0].DeletionTimestamp, "the victim should be marked deleted")
		assert.Equal(t, evicted.UTC(), loaded.Pods[0].DeletionTimestamp.Time.UTC())
		assert.Nil(t, loaded.Pods[1].DeletionTimestamp, "the survivor should not be")
	})

	t.Run("the state it ends at is named", func(t *testing.T) {
		require.Len(t, loaded.Notes, 1)
		assert.Contains(t, loaded.Notes[0], "a.2.snapshot.zip")
	})
}

func TestLoadReplaysWarnsAboutAMissingEventsFile(t *testing.T) {
	dir := t.TempDir()
	first := writeCycleFiles(t, dir, 1, []*corev1.Pod{pod("winner-0", "u2")}, nil)
	second := writeCycleFiles(t, dir, 2, []*corev1.Pod{pod("winner-0", "u2")}, nil)

	loaded, err := loadReplays([]string{first, second})
	require.NoError(t, err)
	require.NotEmpty(t, loaded.Notes)
	// The first state is where the replay began, so only the second is missing its decisions.
	assert.Contains(t, loaded.Notes[0], "a.2.events.zip")
	assert.NotContains(t, loaded.Notes[0], "a.1.events.zip")
}

func TestLoadReplaySaysHowToMakeTheEventsFile(t *testing.T) {
	dir := t.TempDir()
	only := writeCycleFiles(t, dir, 1, []*corev1.Pod{pod("winner-0", "u2")}, nil)

	_, err := loadReplay(only)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--create-decision-snapshots")
}
