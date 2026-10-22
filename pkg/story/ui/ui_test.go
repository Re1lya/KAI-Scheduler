// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kai-scheduler/KAI-scheduler/pkg/story"
)

var dataBlock = regexp.MustCompile(`(?s)<script id="story-data" type="application/json">(.*?)</script>`)

func TestPageInlinesTheStoriesInPickerOrder(t *testing.T) {
	first := &story.Story{Root: story.Root{Ref: story.Ref{Kind: "PyTorchJob", Namespace: "team-a", Name: "train", UID: "c91b4e2a-live"}}}
	second := &story.Story{Root: story.Root{Ref: story.Ref{Kind: "PyTorchJob", Namespace: "team-a", Name: "train", UID: "7f3a19c2-gone"}}}

	page, err := Page([]*story.Story{first, second}, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(page), dataPlaceholder) {
		t.Fatal("placeholder was not replaced")
	}
	match := dataBlock.FindSubmatch(page)
	if match == nil {
		t.Fatal("no story data block in the page")
	}
	data := string(match[1])
	if live, deleted := strings.Index(data, "c91b4e2a"), strings.Index(data, "7f3a19c2"); live > deleted {
		t.Errorf("stories were reordered: live at %d, deleted at %d", live, deleted)
	}
	if !strings.Contains(data, `"label":"team-a/train · c91b4e2a"`) {
		t.Errorf("missing picker label in %s", data)
	}
}

func TestPageOnlyPollsWhenLive(t *testing.T) {
	only := &story.Story{Root: story.Root{Ref: story.Ref{Name: "x", UID: "u"}}}

	fromFiles, err := Page([]*story.Story{only}, false)
	require.NoError(t, err)
	assert.Contains(t, string(fromFiles), "POLL_MS = 0")

	fromCluster, err := Page([]*story.Story{only}, true)
	require.NoError(t, err)
	assert.Contains(t, string(fromCluster), "POLL_MS = 2000")
	assert.NotContains(t, string(fromCluster), pollPlaceholder)
}

func TestPageEscapesAClosingScriptTag(t *testing.T) {
	hostile := &story.Story{
		Root:  story.Root{Ref: story.Ref{Name: "x", UID: "u"}},
		Notes: []string{"</script><script>alert(1)</script>"},
	}
	page, err := Page([]*story.Story{hostile}, false)
	if err != nil {
		t.Fatal(err)
	}
	// One closing tag ends the data block; a second would mean the note broke out of it.
	if count := strings.Count(string(dataBlock.Find(page)), "</script>"); count != 1 {
		t.Errorf("story data escaped its script block: %d closing tags", count)
	}
}
