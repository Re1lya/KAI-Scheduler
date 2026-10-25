// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package podgroup_info

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
)

func TestIsInPlaceResizeRequest(t *testing.T) {
	standIn := &pod_info.PodInfo{UID: "stand-in", Status: pod_status.Pending, IsInPlaceResize: true}
	pending := &pod_info.PodInfo{UID: "pending", Status: pod_status.Pending}
	running := &pod_info.PodInfo{UID: "running", Status: pod_status.Running}
	tests := []struct {
		name  string
		tasks []*pod_info.PodInfo
		want  bool
	}{
		{name: "stand-in with running siblings", tasks: []*pod_info.PodInfo{standIn, running}, want: true},
		{name: "regular pending pod", tasks: []*pod_info.PodInfo{pending, running}, want: false},
		{name: "stand-in with a regular pending pod", tasks: []*pod_info.PodInfo{standIn, pending}, want: false},
		{name: "no pending pods", tasks: []*pod_info.PodInfo{running}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &PodGroupInfo{PodStatusIndex: map[pod_status.PodStatus]pod_info.PodsMap{}}
			for _, task := range tt.tasks {
				if job.PodStatusIndex[task.Status] == nil {
					job.PodStatusIndex[task.Status] = pod_info.PodsMap{}
				}
				job.PodStatusIndex[task.Status][task.UID] = task
			}
			assert.Equal(t, tt.want, job.IsInPlaceResizeRequest())
		})
	}
}
