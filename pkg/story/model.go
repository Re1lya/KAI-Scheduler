// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

// Package story reconstructs the scheduling decisions KAI made for a workload
// from the objects and events it left in the cluster.
package story

import (
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Ref identifies an object by UID so that same-named incarnations stay apart.
type Ref struct {
	Kind      string    `json:"kind"`
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
}

// Root is the top owner of a workload, as recorded by the podgrouper.
type Root struct {
	Ref
	Group string `json:"group,omitempty"`
}

type Decision string

const (
	PodGroupCreated Decision = "PodGroupCreated"
	GroupingFailed  Decision = "GroupingFailed"
	NotReady        Decision = "NotReady"
	Pending         Decision = "Pending"
	Stale           Decision = "Stale"
	Evicting        Decision = "Evicting"
	Pipelined       Decision = "Pipelined"
	Allocated       Decision = "Allocated"
	Bound           Decision = "Bound"
	BindFailed      Decision = "BindFailed"
	Evicted         Decision = "Evicted"
)

// Entry is one decision, or a run of identical repeated decisions.
type Entry struct {
	Time     time.Time `json:"time"`
	Until    time.Time `json:"until,omitzero"`
	Count    int       `json:"count"`
	Subjects []Ref     `json:"subjects"`
	Decision Decision  `json:"decision"`
	Message  string    `json:"message,omitempty"`
	Node     string    `json:"node,omitempty"`
	Action   string    `json:"action,omitempty"`
	// By names the workload that evicted this one; Victims names the pods this one evicted.
	By      *Ref     `json:"by,omitempty"`
	Victims []Victim `json:"victims,omitempty"`
}

// Victim is a pod this workload displaced, and the node it was taken from.
type Victim struct {
	Ref
	Node string `json:"node,omitempty"`
}

// Verdict summarizes where the workload stands now.
type Verdict struct {
	State string    `json:"state"`
	Pods  string    `json:"pods"`
	Why   string    `json:"why,omitempty"`
	Since time.Time `json:"since,omitzero"`
}

type Story struct {
	Root      Root     `json:"root"`
	PodGroups []Ref    `json:"podGroups"`
	Verdict   Verdict  `json:"verdict"`
	Entries   []Entry  `json:"entries"`
	Notes     []string `json:"notes,omitempty"`
}
