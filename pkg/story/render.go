// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package story

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

const shortUIDLength = 8

// RenderCandidates prints the numbered list a user picks a workload from.
func RenderCandidates(out io.Writer, candidates []Candidate) {
	fmt.Fprintf(out, "Found %d workloads matching the name:\n", len(candidates))
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "  #\tNAMESPACE\tKIND\tNAME\tUID\tCREATED\tSTATE")
	for i, candidate := range candidates {
		state := "live"
		if !candidate.Live {
			state = "deleted (from events)"
		}
		fmt.Fprintf(table, "  %d\t%s\t%s\t%s\t%s\t%s\t%s\n", i+1, candidate.Root.Namespace,
			kindOf(candidate.Root), candidate.Root.Name, shortUID(candidate.Root.UID),
			formatTime(candidate.Created, time.Time{}), state)
	}
	table.Flush()
}

// RenderText prints the story as a header, a verdict and a timeline.
func RenderText(out io.Writer, story *Story) {
	root := story.Root
	fmt.Fprintf(out, "%s %s/%s (uid %s)\n", kindOf(root), root.Namespace, root.Name, shortUID(root.UID))
	for _, podGroup := range story.PodGroups {
		fmt.Fprintf(out, "  PodGroup %s (uid %s)\n", podGroup.Name, shortUID(podGroup.UID))
	}

	verdict := story.Verdict
	fmt.Fprintf(out, "\nSTATE  %s", verdict.State)
	if !verdict.Since.IsZero() {
		fmt.Fprintf(out, " since %s", formatTime(verdict.Since, time.Time{}))
	}
	fmt.Fprintf(out, " (%s)\n", verdict.Pods)
	if verdict.Why != "" {
		fmt.Fprintf(out, "WHY    %s\n", verdict.Why)
	}

	fmt.Fprintln(out)
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "TIME\tSUBJECT\tDECISION\tDETAIL")
	day := firstEntryDay(story.Entries)
	for _, entry := range story.Entries {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", formatTime(entry.Time, day), subjectOf(entry),
			entry.Decision, detailOf(entry, day))
	}
	table.Flush()

	for _, note := range story.Notes {
		fmt.Fprintf(out, "\nnote: %s\n", note)
	}
}

// firstEntryDay picks the day the timeline is relative to, skipping entries whose source object
// carried no timestamp.
func firstEntryDay(entries []Entry) time.Time {
	for _, entry := range entries {
		if !entry.Time.IsZero() {
			return entry.Time
		}
	}
	return time.Time{}
}

func detailOf(entry Entry, day time.Time) string {
	var parts []string
	if entry.Node != "" {
		parts = append(parts, "node "+entry.Node)
	}
	if entry.Message != "" {
		parts = append(parts, entry.Message)
	}
	if entry.By != nil {
		by := fmt.Sprintf("by %s %s/%s", kindOf(Root{Ref: *entry.By}), entry.By.Namespace, entry.By.Name)
		if entry.By.UID != "" {
			by += fmt.Sprintf(" (uid %s)", shortUID(entry.By.UID))
		}
		parts = append(parts, by)
	}
	if entry.Action != "" && entry.Decision != Evicting {
		parts = append(parts, "action "+entry.Action)
	}
	detail := strings.Join(parts, ", ")
	// For an eviction this workload caused, the action names what it did, so it leads the line.
	if entry.Decision == Evicting && entry.Action != "" {
		detail = entry.Action + ": " + detail
	}
	if entry.Count > 1 {
		detail += fmt.Sprintf(" [x%d until %s]", entry.Count, formatTime(entry.Until, day))
	}
	return detail
}

func subjectOf(entry Entry) string {
	first := entry.Subjects[0]
	subject := fmt.Sprintf("%s/%s", strings.ToLower(first.Kind), first.Name)
	if len(entry.Subjects) > 1 {
		subject += fmt.Sprintf(" +%d more", len(entry.Subjects)-1)
	}
	return subject
}

func kindOf(root Root) string {
	switch {
	case root.Kind == "":
		return "unknown-kind"
	case root.Group == "":
		return root.Kind
	default:
		return fmt.Sprintf("%s.%s", root.Kind, root.Group)
	}
}

func shortUID(uid types.UID) string {
	return string(uid[:min(len(uid), shortUIDLength)])
}

// formatTime omits the date when t falls on the same day as reference.
func formatTime(t, reference time.Time) string {
	if t.IsZero() {
		return "-"
	}
	t = t.UTC()
	if !reference.IsZero() && t.YearDay() == reference.UTC().YearDay() && t.Year() == reference.UTC().Year() {
		return t.Format("15:04:05")
	}
	return t.Format("2006-01-02 15:04:05")
}
