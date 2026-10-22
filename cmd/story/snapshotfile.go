// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kai-scheduler/api/scheduling/v1alpha2"
	"github.com/kai-scheduler/api/scheduling/v2alpha2"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins/snapshot"
	"github.com/kai-scheduler/KAI-scheduler/pkg/story"
)

// A scheduler snapshot holds objects but no events, so the two travel as a pair sharing a base
// name: foo.snapshot.zip next to foo.events.zip. `snapshot-tool --create-decision-snapshots`
// writes one such pair per replayed cycle, numbered foo.1, foo.2, ... Reading the chain in order
// gives both halves of the story: the events of each cycle, and the state they produced.
const (
	snapshotSuffix = ".snapshot.zip"
	eventsSuffix   = ".events.zip"
	eventsFileName = "events.json"
)

// loadReplays reads a chain of snapshots and the events recorded between them. The snapshots are
// taken in the order given, each one superseding the last.
func loadReplays(paths []string) (*story.Snapshot, error) {
	merged := &story.Snapshot{}
	pods := newLatest[corev1.Pod, *corev1.Pod]()
	podGroups := newLatest[v2alpha2.PodGroup, *v2alpha2.PodGroup]()
	bindRequests := newLatest[v1alpha2.BindRequest, *v1alpha2.BindRequest]()

	for index, path := range paths {
		var raw snapshot.Snapshot
		if err := readZippedJSON(path, snapshot.SnapshotFileName, &raw); err != nil {
			return nil, err
		}
		if raw.RawObjects == nil {
			return nil, fmt.Errorf("%s holds no objects", path)
		}
		pods.add(index, raw.RawObjects.Pods)
		podGroups.add(index, raw.RawObjects.PodGroups)
		bindRequests.add(index, raw.RawObjects.BindRequests)

		events, err := readEvents(path)
		switch {
		case err != nil:
			return nil, err
		case events == nil && index == 0:
			// The first state is where the replay started; nothing produced it.
		case events == nil:
			merged.Notes = append(merged.Notes, fmt.Sprintf(
				"%s has no %s: the decisions that led to that state are missing from this story",
				path, eventsPathFor(path)))
		default:
			merged.Events = append(merged.Events, events...)
		}
	}

	last := len(paths) - 1
	gone := lastEventTime(merged.Events)
	merged.Pods = pods.resolve(last, gone)
	merged.PodGroups = podGroups.resolve(last, gone)
	merged.BindRequests = bindRequests.resolve(last, gone)
	if len(paths) > 1 {
		merged.Notes = append(merged.Notes, fmt.Sprintf(
			"state is as of %s; objects missing from it are shown as deleted", paths[last]))
	} else if len(merged.Events) > 0 {
		merged.Notes = append(merged.Notes, fmt.Sprintf(
			"%s was captured before the decisions below were replayed, so the state above is where "+
				"the workload started, not where it ended; replay with --create-decision-snapshots "+
				"to capture the state after each cycle", paths[last]))
	}
	return merged, nil
}

// loadReplay reads a single snapshot and its events.
func loadReplay(path string) (*story.Snapshot, error) {
	loaded, err := loadReplays([]string{path})
	if err != nil {
		return nil, err
	}
	if len(loaded.Events) == 0 {
		return nil, fmt.Errorf("%s not found; generate it with:\n"+
			"  snapshot-tool --filename %s --cycles 3 --create-decision-snapshots\n"+
			"then pass the whole chain to --from-snapshots", eventsPathFor(path), path)
	}
	return loaded, nil
}

// object is the pointer to a snapshot object, which is always a Kubernetes object.
type object[T any] interface {
	*T
	metav1.Object
}

// latest keeps the newest version of every object, remembering the last snapshot it appeared in.
type latest[T any, P object[T]] struct {
	order   []types.UID
	objects map[types.UID]P
	seenAt  map[types.UID]int
}

func newLatest[T any, P object[T]]() *latest[T, P] {
	return &latest[T, P]{objects: map[types.UID]P{}, seenAt: map[types.UID]int{}}
}

func (l *latest[T, P]) add(index int, objects []P) {
	for _, object := range objects {
		if object == nil {
			continue
		}
		uid := object.GetUID()
		if _, known := l.objects[uid]; !known {
			l.order = append(l.order, uid)
		}
		l.objects[uid] = object
		l.seenAt[uid] = index
	}
}

// resolve returns the objects in the order they were first seen. An object that is absent from the
// final snapshot was deleted along the way; it is kept, since the story still refers to it, but
// marked as deleted so it is not counted as part of the workload any more.
func (l *latest[T, P]) resolve(lastIndex int, deletedAt time.Time) []T {
	values := make([]T, 0, len(l.order))
	for _, uid := range l.order {
		kept := l.objects[uid]
		if l.seenAt[uid] != lastIndex && kept.GetDeletionTimestamp() == nil {
			stamp := metav1.NewTime(deletedAt)
			kept.SetDeletionTimestamp(&stamp)
		}
		values = append(values, *kept)
	}
	return values
}

func lastEventTime(events []corev1.Event) time.Time {
	latest := time.Time{}
	for i := range events {
		at := events[i].EventTime.Time
		if at.IsZero() {
			at = events[i].FirstTimestamp.Time
		}
		if at.After(latest) {
			latest = at
		}
	}
	if latest.IsZero() {
		return time.Now()
	}
	return latest
}

// readEvents reads the events file paired with a snapshot, returning nil when there is none.
func readEvents(snapshotPath string) ([]corev1.Event, error) {
	var events corev1.EventList
	err := readZippedJSON(eventsPathFor(snapshotPath), eventsFileName, &events)
	switch {
	case os.IsNotExist(err):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return events.Items, nil
}

// eventsPathFor is the events file belonging to a snapshot file. It must agree with the name
// snapshot-tool writes.
func eventsPathFor(snapshotPath string) string {
	base := strings.TrimSuffix(snapshotPath, snapshotSuffix)
	if base == snapshotPath {
		base = strings.TrimSuffix(snapshotPath, ".zip")
	}
	return base + eventsSuffix
}

// readZippedJSON decodes one named JSON entry out of a zip file.
func readZippedJSON(path, entryName string, into any) error {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, file := range reader.File {
		if file.Name != entryName {
			continue
		}
		entry, err := file.Open()
		if err != nil {
			return err
		}
		defer entry.Close()
		if err := json.NewDecoder(entry).Decode(into); err != nil {
			return fmt.Errorf("decoding %s from %s: %w", entryName, path, err)
		}
		return nil
	}
	return fmt.Errorf("%s holds no %s: %w", path, entryName, io.ErrUnexpectedEOF)
}
