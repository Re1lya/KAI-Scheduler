// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/plugins/snapshot"
)

const (
	// EventsFileName is the JSON entry inside an events zip.
	EventsFileName = "events.json"
	// A snapshot is paired with its events by base name: foo.snapshot.zip and foo.events.zip.
	snapshotSuffix = ".snapshot.zip"
	eventsSuffix   = ".events.zip"

	cycleSettleDelay = 500 * time.Millisecond
	// The event broadcaster writes asynchronously, so events are taken once the count holds still.
	eventPollInterval = 200 * time.Millisecond
	eventStablePolls  = 3
	eventTimeout      = 30 * time.Second
)

// snapshotBase strips the snapshot naming off a file name, leaving the stem the cycle files are
// built from.
func snapshotBase(snapshotFile string) string {
	base := strings.TrimSuffix(snapshotFile, snapshotSuffix)
	if base == snapshotFile {
		base = strings.TrimSuffix(snapshotFile, ".zip")
	}
	return base
}

// eventsFileName is the events file belonging to a snapshot file.
func eventsFileName(snapshotFile string) string {
	return snapshotBase(snapshotFile) + eventsSuffix
}

// cycleFileNames are the snapshot and events files of the nth replayed state.
func cycleFileNames(snapshotFile string, cycle int) (string, string) {
	base := fmt.Sprintf("%s.%d", snapshotBase(snapshotFile), cycle)
	return base + snapshotSuffix, base + eventsSuffix
}

// writeCycle records one replayed state: the objects as the session sees them, and the events that
// produced them. The first state is the one the replay started from, so it has no events of its own
// and gets no events file.
func writeCycle(snapshotFile string, cycle int, session *framework.Session, events *eventTracker) error {
	snapshotName, eventsName := cycleFileNames(snapshotFile, cycle)
	if cycle > 1 {
		emitted, err := events.sinceLastCall(context.TODO())
		if err != nil {
			return err
		}
		if err := writeEvents(eventsName, emitted); err != nil {
			return err
		}
		log.InfraLogger.V(1).Infof("Wrote %d events of cycle %d to %s", len(emitted), cycle-1, eventsName)
	}

	file, err := os.Create(snapshotName)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := snapshot.WriteZippedSnapshot(context.TODO(), file, session); err != nil {
		return err
	}
	log.InfraLogger.V(1).Infof("Wrote state %d to %s", cycle, snapshotName)
	return nil
}

// writeEvents saves events in the same zipped-JSON shape as a snapshot.
func writeEvents(filename string, events []corev1.Event) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	zipWriter := zip.NewWriter(file)
	entry, err := zipWriter.Create(EventsFileName)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(entry).Encode(&corev1.EventList{Items: events}); err != nil {
		return err
	}
	return zipWriter.Close()
}

// eventTracker hands out the events emitted since it was last asked, so each replayed cycle keeps
// only its own decisions.
type eventTracker struct {
	kubeClient kubernetes.Interface
	// seen counts by event name: the recorder updates an existing event rather than creating a
	// second one when the same thing happens again, so a rising count is new activity.
	seen map[string]int32
}

func newEventTracker(kubeClient kubernetes.Interface) *eventTracker {
	return &eventTracker{kubeClient: kubeClient, seen: map[string]int32{}}
}

func (t *eventTracker) sinceLastCall(ctx context.Context) ([]corev1.Event, error) {
	all, err := t.settled(ctx)
	if err != nil {
		return nil, err
	}
	emitted := make([]corev1.Event, 0, len(all))
	for _, event := range all {
		key := event.Namespace + "/" + event.Name
		if event.Count > t.seen[key] {
			t.seen[key] = event.Count
			emitted = append(emitted, event)
		}
	}
	sort.SliceStable(emitted, func(i, j int) bool {
		return eventTime(&emitted[i]).Before(eventTime(&emitted[j]))
	})
	return emitted, nil
}

// settled polls until the broadcaster stops adding events. A cycle that emits nothing is a
// legitimate result: a settled cluster has no news.
func (t *eventTracker) settled(ctx context.Context) ([]corev1.Event, error) {
	deadline := time.Now().Add(eventTimeout)
	previous, stable := -1, 0
	var items []corev1.Event
	for stable < eventStablePolls && time.Now().Before(deadline) {
		time.Sleep(eventPollInterval)
		list, err := t.kubeClient.CoreV1().Events("").List(ctx, v1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing the replayed events: %w", err)
		}
		items = list.Items
		if len(items) == previous {
			stable++
			continue
		}
		previous, stable = len(items), 0
	}
	return items, nil
}

func eventTime(event *corev1.Event) time.Time {
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	return event.FirstTimestamp.Time
}
