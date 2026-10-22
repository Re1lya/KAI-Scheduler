// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package story

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kai-scheduler/api/constants"
	"github.com/kai-scheduler/api/scheduling/v1alpha2"
	"github.com/kai-scheduler/api/scheduling/v2alpha2"
)

// Event reasons emitted by the scheduler, the binder and the podgrouper, mapped to the decision
// they record. Reasons not listed here are not scheduling decisions and are left out of the story.
// The literals are the ones the emitters pass inline; see the comment on each group.
var eventDecisions = map[string]Decision{
	// pkg/scheduler/cache/status_updater/default_status_updater.go. The pod-level and the
	// PodGroup-level unschedulable reasons are both the string "Unschedulable".
	corev1.PodReasonUnschedulable: Pending,
	"NotReady":                    NotReady,
	"StaleJob":                    Stale,
	pipelinedReason:               Pipelined,
	"Scheduled":                   Allocated,
	"FailedBinding":               BindFailed,
	evictReason:                   Evicted,
	// pkg/binder/controllers/bindrequest_controller.go
	"Bound":        Bound,
	"BindingError": BindFailed,
	// pkg/podgrouper/pod_controller.go
	constants.PodGrouperWarning: GroupingFailed,
}

const (
	bindRequestSucceeded = "Succeeded"
	evictReason          = "Evict"
	pipelinedReason      = "Pipelined"
)

// Annotations the scheduler puts on Evict events.
const (
	evictorPodGroupName      = "evictor-pod-group-name"
	evictorPodGroupNamespace = "evictor-pod-group-namespace"
	evictorActionType        = "evictor-action-type"
)

// Build tells the story of one workload incarnation.
func Build(snapshot *Snapshot, root Root) *Story {
	story := &Story{Root: root}

	podGroups := podGroupsOf(snapshot, root)
	pods := podsOf(snapshot, podGroups)
	deleted := deletedPodGroups(snapshot, root, podGroups)
	subjects := map[types.UID]Ref{}
	for _, podGroup := range podGroups {
		story.PodGroups = append(story.PodGroups, podGroupRef(podGroup))
		subjects[podGroup.UID] = podGroupRef(podGroup)
	}
	for _, ref := range deleted {
		story.PodGroups = append(story.PodGroups, ref)
		subjects[ref.UID] = ref
	}
	for _, pod := range pods {
		subjects[pod.UID] = podRef(pod)
	}

	var entries []Entry
	for _, podGroup := range podGroups {
		entries = append(entries, Entry{
			Time:     podGroup.CreationTimestamp.Time,
			Count:    1,
			Subjects: []Ref{podGroupRef(podGroup)},
			Decision: PodGroupCreated,
		})
	}
	entries = append(entries, bindRequestEntries(snapshot, subjects)...)
	entries = append(entries, eventEntries(snapshot, subjects, ownerlessPods(pods))...)
	entries = append(entries, evictingEntries(snapshot, podGroups, subjects)...)
	for i := range entries {
		if entries[i].By != nil {
			entries[i].By = resolveRoot(snapshot, *entries[i].By)
		}
	}

	story.Entries = collapseGangs(dropPodEchoes(mergeRepeats(mergeAllocations(entries))))
	story.Verdict = verdictOf(podGroups, pods, story.Entries)
	if len(deleted) > 0 {
		story.Notes = append(story.Notes, "some PodGroups were deleted; their history comes from events only")
	}
	story.Notes = append(story.Notes, snapshot.Notes...)
	return story
}

// podGroupsOf returns the live PodGroups whose top owner is root.
func podGroupsOf(snapshot *Snapshot, root Root) []*v2alpha2.PodGroup {
	var podGroups []*v2alpha2.PodGroup
	for i := range snapshot.PodGroups {
		if rootOf(&snapshot.PodGroups[i]).UID == root.UID {
			podGroups = append(podGroups, &snapshot.PodGroups[i])
		}
	}
	return podGroups
}

// podsOf returns the pods of the given PodGroups. The pod annotation carries a PodGroup name only.
func podsOf(snapshot *Snapshot, podGroups []*v2alpha2.PodGroup) []*corev1.Pod {
	var pods []*corev1.Pod
	for i := range snapshot.Pods {
		pod := &snapshot.Pods[i]
		if slices.ContainsFunc(podGroups, func(podGroup *v2alpha2.PodGroup) bool {
			return podGroup.Namespace == pod.Namespace && podGroup.Name == podGroupNameOf(pod)
		}) {
			pods = append(pods, pod)
		}
	}
	return pods
}

// deletedPodGroups lists PodGroups of root that exist only in events.
func deletedPodGroups(snapshot *Snapshot, root Root, live []*v2alpha2.PodGroup) []Ref {
	seen := map[types.UID]bool{}
	for _, podGroup := range live {
		seen[podGroup.UID] = true
	}
	var refs []Ref
	for i := range snapshot.Events {
		involved := snapshot.Events[i].InvolvedObject
		if involved.Kind != podGroupKind || seen[involved.UID] {
			continue
		}
		if rootOfDeletedPodGroup(involved.Namespace, involved.Name, involved.UID).UID == root.UID {
			seen[involved.UID] = true
			refs = append(refs, Ref{Kind: podGroupKind, Namespace: involved.Namespace, Name: involved.Name, UID: involved.UID})
		}
	}
	return refs
}

func bindRequestEntries(snapshot *Snapshot, subjects map[types.UID]Ref) []Entry {
	var entries []Entry
	for i := range snapshot.BindRequests {
		bindRequest := &snapshot.BindRequests[i]
		for _, owner := range bindRequest.OwnerReferences {
			subject, ours := subjects[owner.UID]
			if !ours {
				continue
			}
			message := bindRequestDetail(bindRequest)
			entries = append(entries, Entry{
				Time:     bindRequest.CreationTimestamp.Time,
				Count:    1,
				Subjects: []Ref{subject},
				Decision: Allocated,
				Node:     bindRequest.Spec.SelectedNode,
				Message:  message,
			})
		}
	}
	return entries
}

// bindRequestDetail reports only what the scheduler's Scheduled event does not already say.
func bindRequestDetail(bindRequest *v1alpha2.BindRequest) string {
	var details []string
	if phase := bindRequest.Status.Phase; phase != "" && phase != bindRequestSucceeded {
		details = append(details, fmt.Sprintf("bind request %s", strings.ToLower(phase)))
	}
	if reason := bindRequest.Status.Reason; reason != "" {
		details = append(details, reason)
	}
	if attempts := bindRequest.Status.FailedAttempts; attempts > 0 {
		details = append(details, fmt.Sprintf("%d failed attempts", attempts))
	}
	return strings.Join(details, ", ")
}

// ownerlessPods collects the pods that are owned by nothing. The podgrouper warns about every pod
// it cannot resolve an owner for, which for a bare pod is how things are and not a decision.
func ownerlessPods(pods []*corev1.Pod) map[types.UID]bool {
	ownerless := map[types.UID]bool{}
	for _, pod := range pods {
		if len(pod.OwnerReferences) == 0 {
			ownerless[pod.UID] = true
		}
	}
	return ownerless
}

func eventEntries(snapshot *Snapshot, subjects map[types.UID]Ref, ownerless map[types.UID]bool) []Entry {
	var entries []Entry
	for i := range snapshot.Events {
		event := &snapshot.Events[i]
		subject, ours := subjects[event.InvolvedObject.UID]
		decision, isDecision := eventDecisions[event.Reason]
		if !ours || !isDecision {
			continue
		}
		if decision == GroupingFailed && ownerless[event.InvolvedObject.UID] {
			continue
		}
		entry := Entry{
			Time:     eventStart(event),
			Until:    eventEnd(event),
			Count:    max(int(event.Count), 1),
			Subjects: []Ref{subject},
			Decision: decision,
			Message:  event.Message,
		}
		if decision == Evicted {
			entry.Action = event.Annotations[evictorActionType]
			if name := event.Annotations[evictorPodGroupName]; name != "" {
				entry.By = &Ref{Kind: podGroupKind, Namespace: event.Annotations[evictorPodGroupNamespace], Name: name}
			}
		}
		entries = append(entries, entry)
	}
	return entries
}

// victimPodPattern pulls the evicted pod out of an Evict message. The four messages the scheduler
// builds (GetPreemptMessage, GetReclaimMessage, GetConsolidateMessage and GetGangEvictionMessage in
// pkg/scheduler/api/unschedule_info.go) name the pod either at the start or after "pod:". The
// message is the only place the victim appears: a single eviction can have many victims, so they
// are not worth carrying in the event annotations the way the single evictor is.
var victimPodPattern = regexp.MustCompile(`^Pod (\S+)/(\S+?) was |evicting remaining pod: (\S+)/(\S+)$`)

// pipelinedNodePattern reads the node back out of the message built in
// pkg/scheduler/framework/statement.go:319.
var pipelinedNodePattern = regexp.MustCompile(`was pipelined to node (\S+)$`)

func victimOf(message string) (namespace, name string, found bool) {
	match := victimPodPattern.FindStringSubmatch(message)
	switch {
	case match == nil:
		return "", "", false
	case match[1] != "":
		return match[1], match[2], true
	default:
		return match[3], match[4], true
	}
}

// evictingEntries records the evictions this workload caused. The Evict event lives on the victim's
// PodGroup and names the evictor in its annotations, so the actor's own story only shows a reclaim
// or preemption if the whole snapshot is scanned for events pointing back at it.
func evictingEntries(snapshot *Snapshot, podGroups []*v2alpha2.PodGroup, subjects map[types.UID]Ref) []Entry {
	type key struct {
		action string
		victim types.UID
		moment time.Time
	}
	grouped := map[key]*Entry{}
	var order []key

	for i := range snapshot.Events {
		event := &snapshot.Events[i]
		if event.Reason != evictReason {
			continue
		}
		evictor := slices.IndexFunc(podGroups, func(podGroup *v2alpha2.PodGroup) bool {
			return podGroup.Name == event.Annotations[evictorPodGroupName] &&
				podGroup.Namespace == event.Annotations[evictorPodGroupNamespace]
		})
		if evictor < 0 {
			continue
		}

		k := key{event.Annotations[evictorActionType], event.InvolvedObject.UID, eventStart(event)}
		entry, seen := grouped[k]
		if !seen {
			entry = &Entry{
				Time:     eventStart(event),
				Count:    1,
				Subjects: []Ref{podGroupRef(podGroups[evictor])},
				Decision: Evicting,
				Action:   k.action,
			}
			grouped[k] = entry
			order = append(order, k)
		}
		entry.Victims = append(entry.Victims, victimFromEvent(snapshot, event))
	}

	entries := make([]Entry, 0, len(order))
	for _, k := range order {
		entry := grouped[k]
		entry.Message = describeVictims(entry.Victims, k.victim, snapshot)
		if slices.ContainsFunc(entry.Victims, func(victim Victim) bool { return victim.Node == "" }) {
			entry.Message += nextPipelineHint(snapshot, subjects, entry.Time)
		}
		entries = append(entries, *entry)
	}
	return entries
}

// nextPipelineHint names the nodes this workload was pipelined onto right after the eviction. The
// evicted pod carries the authoritative node, but only until it is deleted, so when it is already
// gone the node the scheduler moved onto next is the best available indication of what was freed.
func nextPipelineHint(snapshot *Snapshot, subjects map[types.UID]Ref, evictedAt time.Time) string {
	var moment time.Time
	var nodes []string
	for i := range snapshot.Events {
		event := &snapshot.Events[i]
		match := pipelinedNodePattern.FindStringSubmatch(event.Message)
		if event.Reason != pipelinedReason || match == nil {
			continue
		}
		if _, ours := subjects[event.InvolvedObject.UID]; !ours {
			continue
		}
		at := eventStart(event)
		switch {
		case at.Before(evictedAt):
			continue
		case moment.IsZero() || at.Before(moment):
			moment, nodes = at, []string{match[1]}
		case at.Equal(moment) && !slices.Contains(nodes, match[1]):
			nodes = append(nodes, match[1])
		}
	}
	if len(nodes) == 0 {
		return ""
	}
	slices.Sort(nodes)
	return fmt.Sprintf("; pipelined onto %s next, likely what was freed", strings.Join(nodes, ", "))
}

func pluralPods(count int) string {
	if count == 1 {
		return "1 pod"
	}
	return fmt.Sprintf("%d pods", count)
}

func victimFromEvent(snapshot *Snapshot, event *corev1.Event) Victim {
	namespace, name, found := victimOf(event.Message)
	if !found {
		return Victim{Ref: Ref{Kind: "Pod", Namespace: event.InvolvedObject.Namespace}}
	}
	victim := Victim{Ref: Ref{Kind: "Pod", Namespace: namespace, Name: name}}
	// The evictor deletes the pod, so the node is only recoverable while the pod still exists.
	for i := range snapshot.Pods {
		if pod := &snapshot.Pods[i]; pod.Namespace == namespace && pod.Name == name {
			victim.UID = pod.UID
			victim.Node = pod.Spec.NodeName
			break
		}
	}
	return victim
}

func describeVictims(victims []Victim, victimPodGroup types.UID, snapshot *Snapshot) string {
	placed := make([]string, 0, len(victims))
	for _, victim := range victims {
		switch {
		case victim.Name == "":
			placed = append(placed, "an unnamed pod")
		case victim.Node == "":
			placed = append(placed, victim.Name+" (node unrecorded, pod already gone)")
		default:
			placed = append(placed, fmt.Sprintf("%s from %s", victim.Name, victim.Node))
		}
	}
	owner := "another workload"
	for i := range snapshot.PodGroups {
		if snapshot.PodGroups[i].UID == victimPodGroup {
			root := rootOf(&snapshot.PodGroups[i])
			owner = fmt.Sprintf("%s %s/%s", kindOf(root), root.Namespace, root.Name)
			break
		}
	}
	return fmt.Sprintf("evicted %s of %s: %s", pluralPods(len(victims)), owner, strings.Join(placed, ", "))
}

// resolveRoot replaces a PodGroup reference by the root workload that owns it, when the PodGroup
// is still live. The Evict event carries no UID, so an unresolved reference stays name-only.
func resolveRoot(snapshot *Snapshot, ref Ref) *Ref {
	if podGroup := findPodGroup(snapshot, ref.Namespace, ref.Name); podGroup != nil {
		root := rootOf(podGroup).Ref
		return &root
	}
	return &ref
}

// eventStart takes the most precise timestamp the event carries. EventTime comes first because the
// events.k8s.io recorder stamps it to the microsecond, while the legacy FirstTimestamp that the
// scheduler's own recorder writes is truncated to the second.
func eventStart(event *corev1.Event) time.Time {
	return firstNonZero(event.EventTime.Time, event.FirstTimestamp.Time, event.CreationTimestamp.Time)
}

func eventEnd(event *corev1.Event) time.Time {
	if event.LastTimestamp.IsZero() || event.LastTimestamp.Equal(&event.FirstTimestamp) {
		return time.Time{}
	}
	return event.LastTimestamp.Time
}

func firstNonZero(times ...time.Time) time.Time {
	for _, t := range times {
		if !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

// mergeAllocations folds the scheduler's Scheduled event and the BindRequest it created into a
// single entry: both record the same allocation, from two sources that each know part of it.
func mergeAllocations(entries []Entry) []Entry {
	position := map[types.UID]int{}
	var result []Entry
	for _, entry := range entries {
		index, seen := position[entry.Subjects[0].UID]
		if entry.Decision != Allocated || !seen {
			if entry.Decision == Allocated {
				position[entry.Subjects[0].UID] = len(result)
			}
			result = append(result, entry)
			continue
		}
		result[index] = foldAllocation(result[index], entry)
	}
	return result
}

func foldAllocation(first, second Entry) Entry {
	folded := first
	if folded.Time.IsZero() || (!second.Time.IsZero() && second.Time.Before(folded.Time)) {
		folded.Time = second.Time
	}
	folded.Node = cmp.Or(folded.Node, second.Node)
	folded.Message = strings.TrimSuffix(strings.Join(
		slices.DeleteFunc([]string{folded.Message, second.Message}, func(s string) bool { return s == "" }),
		", "), ", ")
	return folded
}

// mergeRepeats folds entries that say the same thing about the same object into one run.
func mergeRepeats(entries []Entry) []Entry {
	type key struct {
		uid      types.UID
		decision Decision
		message  string
	}
	merged := map[key]*Entry{}
	var order []key
	for _, entry := range entries {
		k := key{entry.Subjects[0].UID, entry.Decision, entry.Message}
		existing, seen := merged[k]
		if !seen {
			copied := entry
			merged[k] = &copied
			order = append(order, k)
			continue
		}
		existing.Count += entry.Count
		first, last := existing.Time, lastOf(*existing)
		existing.Time = minTime(first, entry.Time)
		existing.Until = maxTime(last, lastOf(entry))
	}
	result := make([]Entry, 0, len(order))
	for _, k := range order {
		result = append(result, *merged[k])
	}
	slices.SortStableFunc(result, byTime)
	return result
}

// lifecycleOrder ranks decisions by the order in which they can happen to a workload. The
// scheduler's recorder stamps events to the second, so a whole scheduling cycle usually shares one
// timestamp; ordering those by lifecycle keeps the story causal instead of leaving it to the order
// the sources happened to be read in, which showed an allocation before the pipelining that caused it.
var lifecycleOrder = map[Decision]int{
	PodGroupCreated: 0,
	GroupingFailed:  1,
	NotReady:        2,
	Pending:         3,
	Stale:           4,
	Evicting:        5,
	Pipelined:       6,
	Allocated:       7,
	BindFailed:      8,
	Bound:           9,
	Evicted:         10,
}

// byTime orders entries chronologically, putting entries whose source carried no timestamp last
// rather than letting a zero time sort before everything that is known. Entries sharing a timestamp
// fall back to lifecycleOrder.
func byTime(a, b Entry) int {
	switch {
	case a.Time.IsZero() && b.Time.IsZero():
		return 0
	case a.Time.IsZero():
		return 1
	case b.Time.IsZero():
		return -1
	default:
		return cmp.Or(
			a.Time.Compare(b.Time),
			cmp.Compare(lifecycleOrder[a.Decision], lifecycleOrder[b.Decision]),
		)
	}
}

// dropPodEchoes removes pod entries that repeat what the PodGroup entry already says. Pods that
// were rejected for their own reasons keep their entries.
func dropPodEchoes(entries []Entry) []Entry {
	type key struct {
		decision Decision
		message  string
		time     time.Time
		until    time.Time
	}
	groupLevel := map[key]bool{}
	for _, entry := range entries {
		if entry.Subjects[0].Kind == podGroupKind {
			groupLevel[key{entry.Decision, entry.Message, entry.Time, entry.Until}] = true
		}
	}
	return slices.DeleteFunc(slices.Clone(entries), func(entry Entry) bool {
		return entry.Subjects[0].Kind == "Pod" && entry.Decision == Pending &&
			groupLevel[key{entry.Decision, entry.Message, entry.Time, entry.Until}]
	})
}

// collapseGangs folds the identical entries of sibling pods, such as every pod of a gang being
// unschedulable for the same reason over the same period, into one entry listing all of them.
func collapseGangs(entries []Entry) []Entry {
	type key struct {
		kind     string
		decision Decision
		message  string
		node     string
		count    int
		time     time.Time
		until    time.Time
	}
	index := map[key]int{}
	var result []Entry
	for _, entry := range entries {
		k := key{entry.Subjects[0].Kind, entry.Decision, entry.Message, entry.Node, entry.Count, entry.Time, entry.Until}
		if i, seen := index[k]; seen && entry.Subjects[0].Kind == "Pod" {
			result[i].Subjects = append(result[i].Subjects, entry.Subjects...)
			continue
		}
		index[k] = len(result)
		result = append(result, entry)
	}
	return result
}

func lastOf(entry Entry) time.Time {
	return firstNonZero(entry.Until, entry.Time)
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func verdictOf(podGroups []*v2alpha2.PodGroup, pods []*corev1.Pod, entries []Entry) Verdict {
	// A pod on its way out is no longer part of the workload's state: counting it would report a
	// preempted workload as still running.
	pods = slices.DeleteFunc(slices.Clone(pods), func(pod *corev1.Pod) bool {
		return pod.DeletionTimestamp != nil
	})
	phases := map[corev1.PodPhase]int{}
	for _, pod := range pods {
		phases[pod.Status.Phase]++
	}
	verdict := Verdict{Pods: summarizePhases(phases, len(pods))}

	lastPodGroupDecision := func() (Entry, bool) {
		for i := len(entries) - 1; i >= 0; i-- {
			if entries[i].Subjects[0].Kind == podGroupKind && entries[i].Decision != PodGroupCreated {
				return entries[i], true
			}
		}
		return Entry{}, false
	}

	switch last, found := lastPodGroupDecision(); {
	case len(podGroups) == 0:
		verdict.State = "Deleted"
	case phases[corev1.PodPending] > 0:
		verdict.State = "Pending"
		verdict.Why = blockingReason(podGroups)
		if found && last.Decision == Pending {
			verdict.Since = last.Time
			verdict.Why = cmp.Or(verdict.Why, last.Message)
		}
	case len(pods) > 0 && phases[corev1.PodRunning] == len(pods):
		verdict.State = "Running"
	case found && last.Decision == Evicted:
		verdict.State = "Evicted"
		verdict.Why = last.Message
		verdict.Since = last.Time
	default:
		verdict.State = "Unknown"
	}
	return verdict
}

func blockingReason(podGroups []*v2alpha2.PodGroup) string {
	var reasons []string
	for _, podGroup := range podGroups {
		for _, condition := range podGroup.Status.SchedulingConditions {
			for _, reason := range condition.Reasons {
				reasons = append(reasons, reason.Message)
			}
		}
	}
	return strings.Join(reasons, "; ")
}

func summarizePhases(phases map[corev1.PodPhase]int, total int) string {
	if total == 0 {
		return "no pods"
	}
	parts := make([]string, 0, len(phases))
	for phase, count := range phases {
		parts = append(parts, fmt.Sprintf("%d %s", count, phase))
	}
	slices.Sort(parts)
	return fmt.Sprintf("%d pods: %s", total, strings.Join(parts, ", "))
}
