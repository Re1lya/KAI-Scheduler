// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package story

import (
	"cmp"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/kai-scheduler/api/scheduling/v2alpha2"
)

// Candidate is one workload incarnation a user-supplied name may refer to.
type Candidate struct {
	Root    Root      `json:"root"`
	Live    bool      `json:"live"`
	Created time.Time `json:"created,omitzero"`
}

// Resolve finds every workload incarnation matching name, deduplicated by root UID, live ones
// first and newest first. An empty namespace matches all namespaces.
func Resolve(snapshot *Snapshot, name, namespace string) []Candidate {
	found := map[types.UID]*Candidate{}
	add := func(root Root, live bool, created time.Time) {
		candidate, exists := found[root.UID]
		if !exists {
			candidate = &Candidate{Root: root}
			found[root.UID] = candidate
		}
		if candidate.Root.Kind == "" {
			candidate.Root = root
		}
		candidate.Live = candidate.Live || live
		if !created.IsZero() && (candidate.Created.IsZero() || created.Before(candidate.Created)) {
			candidate.Created = created
		}
	}
	inScope := func(ns string) bool { return namespace == "" || ns == namespace }

	livePodGroups := map[types.UID]bool{}
	for i := range snapshot.PodGroups {
		podGroup := &snapshot.PodGroups[i]
		livePodGroups[podGroup.UID] = true
		root := rootOf(podGroup)
		if inScope(podGroup.Namespace) && (podGroup.Name == name || root.Name == name) {
			add(root, true, podGroup.CreationTimestamp.Time)
		}
	}

	for i := range snapshot.Pods {
		pod := &snapshot.Pods[i]
		if pod.Name != name || !inScope(pod.Namespace) {
			continue
		}
		if podGroup := findPodGroup(snapshot, pod.Namespace, podGroupNameOf(pod)); podGroup != nil {
			add(rootOf(podGroup), true, podGroup.CreationTimestamp.Time)
		}
	}

	for i := range snapshot.Events {
		event := &snapshot.Events[i]
		involved := event.InvolvedObject
		if involved.Kind != podGroupKind || livePodGroups[involved.UID] || !inScope(involved.Namespace) {
			continue
		}
		root := rootOfDeletedPodGroup(involved.Namespace, involved.Name, involved.UID)
		if involved.Name == name || root.Name == name {
			add(root, false, eventStart(event))
		}
	}

	candidates := make([]Candidate, 0, len(found))
	for _, candidate := range found {
		candidates = append(candidates, *candidate)
	}
	slices.SortFunc(candidates, func(a, b Candidate) int {
		if a.Live != b.Live {
			if a.Live {
				return -1
			}
			return 1
		}
		return cmp.Or(b.Created.Compare(a.Created), cmp.Compare(a.Root.UID, b.Root.UID))
	})
	return candidates
}

func findPodGroup(snapshot *Snapshot, namespace, name string) *v2alpha2.PodGroup {
	for i := range snapshot.PodGroups {
		if snapshot.PodGroups[i].Namespace == namespace && snapshot.PodGroups[i].Name == name {
			return &snapshot.PodGroups[i]
		}
	}
	return nil
}
