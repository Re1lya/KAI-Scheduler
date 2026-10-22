// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package story

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	kaiclient "github.com/kai-scheduler/api/client/clientset/versioned"
	"github.com/kai-scheduler/api/scheduling/v1alpha2"
	"github.com/kai-scheduler/api/scheduling/v2alpha2"
)

// Snapshot is everything a story is built from. It is also the fixture format.
type Snapshot struct {
	Pods         []corev1.Pod           `json:"pods"`
	PodGroups    []v2alpha2.PodGroup    `json:"podGroups"`
	BindRequests []v1alpha2.BindRequest `json:"bindRequests"`
	Events       []corev1.Event         `json:"events"`

	// Notes say where the snapshot came from, and what it cannot tell, when that changes how the
	// story should be read. They are carried onto every story built from it.
	Notes []string `json:"notes,omitempty"`
}

func LoadSnapshot(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	snapshot := &Snapshot{}
	if err := json.Unmarshal(data, snapshot); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return snapshot, nil
}

// Collect reads the snapshot from the cluster. An empty namespace reads all namespaces.
func Collect(ctx context.Context, kube kubernetes.Interface, kai kaiclient.Interface, namespace string) (*Snapshot, error) {
	pods, err := kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing pods: %w", err)
	}
	events, err := kube.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing events: %w", err)
	}
	podGroups, err := kai.SchedulingV2alpha2().PodGroups(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing podgroups: %w", err)
	}
	bindRequests, err := kai.SchedulingV1alpha2().BindRequests(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing bindrequests: %w", err)
	}
	return &Snapshot{
		Pods:         pods.Items,
		PodGroups:    podGroups.Items,
		BindRequests: bindRequests.Items,
		Events:       events.Items,
	}, nil
}
