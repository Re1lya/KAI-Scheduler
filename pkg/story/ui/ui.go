// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

// Package ui serves the scheduling story as a single self-contained page.
package ui

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kai-scheduler/KAI-scheduler/pkg/story"
)

//go:embed index.html
var page string

const (
	dataPlaceholder = "__STORY_DATA__"
	pollPlaceholder = "__STORY_POLL_MS__"
	shortUIDLength  = 8

	// LivePollInterval is how often a page reads a live cluster again. Scheduling decisions land in
	// bursts of a cycle, so anything slower makes the page lag behind the thing it describes.
	LivePollInterval = 2 * time.Second
)

// labeled is a story plus the name the picker shows for it.
type labeled struct {
	*story.Story
	Label string `json:"label"`
}

// Page renders the stories into the page, in picker order. A live page re-reads the cluster by
// itself; one built from files does not, since the files cannot change.
func Page(stories []*story.Story, live bool) ([]byte, error) {
	data, err := Data(stories)
	if err != nil {
		return nil, err
	}
	// The data sits in a <script type="application/json"> block, so the only sequence that can
	// break out of it is a literal "</script"; JSON escapes survive the parse unchanged.
	data = bytes.ReplaceAll(data, []byte("</"), []byte(`<\/`))
	poll := 0
	if live {
		poll = int(LivePollInterval.Milliseconds())
	}
	rendered := strings.Replace(page, dataPlaceholder, string(data), 1)
	rendered = strings.Replace(rendered, pollPlaceholder, strconv.Itoa(poll), 1)
	return []byte(rendered), nil
}

// Data is the stories as the page reads them, keyed by root UID. A live page re-reads this from
// the server, so it is also what the /story.json endpoint serves.
func Data(stories []*story.Story) ([]byte, error) {
	keyed := make(map[string]labeled, len(stories))
	order := make([]string, 0, len(stories))
	for _, s := range stories {
		key := string(s.Root.UID)
		keyed[key] = labeled{Story: s, Label: label(s)}
		order = append(order, key)
	}
	return marshalInOrder(keyed, order)
}

// marshalInOrder writes the map with its keys in the given order rather than sorted, so the
// picker lists live workloads before deleted ones the way Resolve ranked them.
func marshalInOrder(keyed map[string]labeled, order []string) ([]byte, error) {
	var out bytes.Buffer
	out.WriteByte('{')
	for i, key := range order {
		if i > 0 {
			out.WriteByte(',')
		}
		name, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		value, err := json.Marshal(keyed[key])
		if err != nil {
			return nil, err
		}
		out.Write(name)
		out.WriteByte(':')
		out.Write(value)
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}

func label(s *story.Story) string {
	uid := string(s.Root.UID)
	if len(uid) > shortUIDLength {
		uid = uid[:shortUIDLength]
	}
	return fmt.Sprintf("%s/%s · %s", s.Root.Namespace, s.Root.Name, uid)
}
