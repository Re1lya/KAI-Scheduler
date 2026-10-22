// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package story

import (
	"regexp"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kai-scheduler/api/constants"
	"github.com/kai-scheduler/api/scheduling/v2alpha2"

	"github.com/kai-scheduler/KAI-scheduler/pkg/podgrouper/topowner"
)

const podGroupKind = "PodGroup"

// defaultGrouperName matches the default grouper's pg-<owner name>-<owner UID> naming.
var defaultGrouperName = regexp.MustCompile(`^pg-(.+)-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$`)

// rootOf returns the top owner recorded by the podgrouper, or the PodGroup itself when it is missing.
func rootOf(podGroup *v2alpha2.PodGroup) Root {
	var owner topowner.Metadata
	annotation := podGroup.Annotations[constants.TopOwnerMetadataKey]
	if annotation != "" && yaml.Unmarshal([]byte(annotation), &owner) == nil && owner.UID != "" {
		return Root{
			Ref:   Ref{Kind: owner.Kind, Namespace: podGroup.Namespace, Name: owner.Name, UID: owner.UID},
			Group: owner.Group,
		}
	}
	return Root{Ref: podGroupRef(podGroup)}
}

// rootOfDeletedPodGroup infers the root of a PodGroup that only survives in events. The default
// grouper embeds the owner name and UID in the PodGroup name; the kind is not recoverable.
func rootOfDeletedPodGroup(namespace, name string, uid types.UID) Root {
	if match := defaultGrouperName.FindStringSubmatch(name); match != nil {
		return Root{Ref: Ref{Namespace: namespace, Name: match[1], UID: types.UID(match[2])}}
	}
	return Root{Ref: Ref{Kind: podGroupKind, Namespace: namespace, Name: name, UID: uid}}
}

func podGroupRef(podGroup *v2alpha2.PodGroup) Ref {
	return Ref{Kind: podGroupKind, Namespace: podGroup.Namespace, Name: podGroup.Name, UID: podGroup.UID}
}

func podRef(pod *corev1.Pod) Ref {
	return Ref{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}
}

// podGroupNameOf returns the PodGroup a pod belongs to. The annotation carries a name only, no UID.
func podGroupNameOf(pod *corev1.Pod) string {
	return pod.Annotations[constants.PodGroupAnnotationForPod]
}
