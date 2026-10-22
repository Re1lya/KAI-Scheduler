// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package framework

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
)

// ReleasingPods are the pods that were already terminating when the session opened. Pipeline
// predicates evaluate nodes as if these pods were gone, matching how their resources are counted
// as releasing. Victims evicted by this session are not included: they are released through the
// session's own eviction path.
type ReleasingPods struct {
	byNode                  map[string][]*pod_info.PodInfo
	uids                    sets.Set[types.UID]
	hasRequiredAntiAffinity bool
}

// ReleasingPods returns the terminating pods of the session, computed once on first use.
func (ssn *Session) ReleasingPods() *ReleasingPods {
	ssn.releasingPodsOnce.Do(func() {
		ssn.releasingPods = newReleasingPods(ssn)
	})
	return ssn.releasingPods
}

func newReleasingPods(ssn *Session) *ReleasingPods {
	releasing := &ReleasingPods{
		byNode: map[string][]*pod_info.PodInfo{},
		uids:   sets.New[types.UID](),
	}
	if ssn.ClusterInfo == nil {
		return releasing
	}
	for nodeName, node := range ssn.ClusterInfo.Nodes {
		for _, task := range node.PodInfos {
			if task.Status != pod_status.Releasing || task.IsVirtualStatus || task.Pod == nil {
				continue
			}
			releasing.byNode[nodeName] = append(releasing.byNode[nodeName], task)
			releasing.uids.Insert(task.Pod.UID)
			if hasRequiredAntiAffinity(task.Pod) {
				releasing.hasRequiredAntiAffinity = true
			}
		}
	}
	return releasing
}

func (r *ReleasingPods) IsEmpty() bool {
	return r == nil || r.uids.Len() == 0
}

func (r *ReleasingPods) Has(uid types.UID) bool {
	return r != nil && r.uids.Has(uid)
}

func (r *ReleasingPods) OnNode(nodeName string) []*pod_info.PodInfo {
	if r == nil {
		return nil
	}
	return r.byNode[nodeName]
}

func (r *ReleasingPods) ByNode() map[string][]*pod_info.PodInfo {
	if r == nil {
		return nil
	}
	return r.byNode
}

// MayChangeFit reports whether removing the releasing pods can change a predicate result for the
// task on the node: the node hosts releasing pods, or required inter-pod (anti-)affinity makes
// releasing pods on other nodes of the same topology domain relevant.
func (r *ReleasingPods) MayChangeFit(task *pod_info.PodInfo, nodeName string) bool {
	if r.IsEmpty() {
		return false
	}
	if len(r.byNode[nodeName]) > 0 || r.hasRequiredAntiAffinity {
		return true
	}
	return hasRequiredPodAffinityTerms(task.Pod)
}

func hasRequiredAntiAffinity(pod *v1.Pod) bool {
	return pod.Spec.Affinity != nil && pod.Spec.Affinity.PodAntiAffinity != nil &&
		len(pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0
}

func hasRequiredPodAffinityTerms(pod *v1.Pod) bool {
	if pod == nil || pod.Spec.Affinity == nil {
		return false
	}
	if hasRequiredAntiAffinity(pod) {
		return true
	}
	return pod.Spec.Affinity.PodAffinity != nil &&
		len(pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0
}
