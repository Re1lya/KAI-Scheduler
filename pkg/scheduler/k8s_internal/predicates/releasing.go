// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package predicates

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	ksf "k8s.io/kube-scheduler/framework"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/cache/cluster_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/k8s_internal"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
)

var releasingLogger = klog.NewKlogr()

// releasingNodeView serves copies of nodes without their releasing pods, for filters that only
// read the candidate node (e.g. host ports). A copy is reused until the node changes.
type releasingNodeView struct {
	ssn   *framework.Session
	nodes map[string]releasingNodeCopy
}

type releasingNodeCopy struct {
	generation int64
	nodeInfo   *k8sframework.NodeInfo
}

func newReleasingNodeView(ssn *framework.Session) *releasingNodeView {
	return &releasingNodeView{ssn: ssn, nodes: map[string]releasingNodeCopy{}}
}

func (v *releasingNodeView) withoutReleasingPods(nodeInfo *k8sframework.NodeInfo) *k8sframework.NodeInfo {
	nodeName := nodeInfo.Node().Name
	releasing := v.ssn.ReleasingPods().OnNode(nodeName)
	if len(releasing) == 0 {
		return nodeInfo
	}
	if cached, found := v.nodes[nodeName]; found && cached.generation == nodeInfo.Generation {
		return cached.nodeInfo
	}

	nodeCopy := nodeInfo.SnapshotConcrete()
	for _, task := range releasing {
		if err := nodeCopy.RemovePod(releasingLogger, task.Pod); err != nil {
			log.InfraLogger.V(4).Infof("Releasing pod %s/%s is not indexed on node %s: %v",
				task.Namespace, task.Name, nodeName, err)
		}
	}
	v.nodes[nodeName] = releasingNodeCopy{generation: nodeInfo.Generation, nodeInfo: nodeCopy}
	return nodeCopy
}

func (v *releasingNodeView) pipelineFilter(filter k8s_internal.FitPredicateFilter) k8s_internal.FitPredicateFilter {
	return func(pod *v1.Pod, nodeInfo *k8sframework.NodeInfo) (bool, []string, error) {
		return filter(pod, v.withoutReleasingPods(nodeInfo))
	}
}

// releasingPodAffinity evaluates inter-pod affinity without the releasing pods of the whole
// cluster. Affinity counts are aggregated per topology domain, so a releasing pod on another node
// of the candidate's rack or zone matters as much as one on the candidate itself.
// The adjusted state is derived from the pod's prefilter state once per prefilter run.
type releasingPodAffinity struct {
	ssn         *framework.Session
	plugin      *interpodaffinity.InterPodAffinity
	generations map[types.UID]uint64
	states      map[types.UID]releasingAffinityState
}

type releasingAffinityState struct {
	generation uint64
	state      ksf.CycleState
}

func newReleasingPodAffinity(ssn *framework.Session, plugin *interpodaffinity.InterPodAffinity) *releasingPodAffinity {
	return &releasingPodAffinity{
		ssn:         ssn,
		plugin:      plugin,
		generations: map[types.UID]uint64{},
		states:      map[types.UID]releasingAffinityState{},
	}
}

func (a *releasingPodAffinity) preFilter(preFilter k8s_internal.FitPredicatePreFilter) k8s_internal.FitPredicatePreFilter {
	return func(pod *v1.Pod) (sets.Set[string], *ksf.Status) {
		a.generations[pod.UID]++
		return preFilter(pod)
	}
}

func (a *releasingPodAffinity) pipelineFilter(filter k8s_internal.FitPredicateFilter) k8s_internal.FitPredicateFilter {
	return func(pod *v1.Pod, nodeInfo *k8sframework.NodeInfo) (bool, []string, error) {
		if a.ssn.ReleasingPods().IsEmpty() {
			return filter(pod, nodeInfo)
		}
		state, err := a.stateWithoutReleasingPods(pod)
		if err != nil {
			return false, nil, err
		}
		result := a.plugin.Filter(context.Background(), state, pod, nodeInfo)
		if result == nil {
			return true, nil, nil
		}
		return result.IsSuccess(), result.Reasons(), result.AsError()
	}
}

func (a *releasingPodAffinity) stateWithoutReleasingPods(pod *v1.Pod) (ksf.CycleState, error) {
	generation := a.generations[pod.UID]
	if cached, found := a.states[pod.UID]; found && cached.generation == generation {
		return cached.state, nil
	}

	state := a.ssn.GetSessionStateForResource(pod.UID).Clone()
	releasingPods := a.ssn.ReleasingPods()
	for nodeName := range releasingPods.ByNode() {
		nodeInfo := k8sNodeInfo(a.ssn, nodeName)
		if nodeInfo == nil {
			continue
		}
		for _, podInfo := range nodeInfo.GetPods() {
			if !releasingPods.Has(podInfo.GetPod().UID) {
				continue
			}
			status := a.plugin.RemovePod(context.Background(), state, pod, podInfo, nodeInfo)
			if !status.IsSuccess() {
				return nil, fmt.Errorf("failed to remove releasing pod %s/%s from affinity state: %w",
					podInfo.GetPod().Namespace, podInfo.GetPod().Name, status.AsError())
			}
		}
	}

	a.states[pod.UID] = releasingAffinityState{generation: generation, state: state}
	return state, nil
}

func k8sNodeInfo(ssn *framework.Session, nodeName string) *k8sframework.NodeInfo {
	node, found := ssn.ClusterInfo.Nodes[nodeName]
	if !found {
		return nil
	}
	podAffinityInfo, ok := node.PodAffinityInfo.(*cluster_info.K8sNodePodAffinityInfo)
	if !ok || podAffinityInfo == nil {
		return nil
	}
	return podAffinityInfo.NodeInfo
}
