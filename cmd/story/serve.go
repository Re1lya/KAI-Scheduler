// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/kai-scheduler/KAI-scheduler/pkg/story"
	"github.com/kai-scheduler/KAI-scheduler/pkg/story/ui"
)

// loader reads a snapshot, either from the cluster or from a file.
type loader struct {
	read func(ctx context.Context) (*story.Snapshot, error)
	// live says the source is a cluster, which keeps moving while the page is open.
	live bool
}

func snapshotLoader(from string, chain fileList, kubeconfig, namespace string) loader {
	return loader{
		read: func(ctx context.Context) (*story.Snapshot, error) {
			return loadSnapshot(ctx, from, chain, kubeconfig, namespace)
		},
		live: from == "" && len(chain) == 0,
	}
}

// serveStory serves the page until interrupted. The snapshot is read per request, so reloading
// the browser shows the current state of the cluster.
func serveStory(addr string, load loader, name, namespace, uid string, out io.Writer) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	serve := func(contentType string, body func(ctx context.Context, stories []*story.Story) ([]byte, error)) http.HandlerFunc {
		return func(writer http.ResponseWriter, request *http.Request) {
			stories, err := tellStories(request.Context(), load, name, namespace, uid)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
			rendered, err := body(request.Context(), stories)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
			writer.Header().Set("Content-Type", contentType)
			writer.Header().Set("Cache-Control", "no-store")
			_, _ = writer.Write(rendered)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", serve("text/html; charset=utf-8",
		func(_ context.Context, stories []*story.Story) ([]byte, error) {
			return ui.Page(stories, load.live)
		}))
	mux.HandleFunc("/story.json", serve("application/json",
		func(_ context.Context, stories []*story.Story) ([]byte, error) {
			return ui.Data(stories)
		}))

	fmt.Fprintf(out, "story for %q on http://%s\n", name, listener.Addr())
	if load.live {
		fmt.Fprintf(out, "reading the cluster every %s while the page is open\n", ui.LivePollInterval)
	}
	return http.Serve(listener, mux)
}

// tellStories tells the story of every incarnation of the name: the page lets the user pick
// between them, so an ambiguous name is shown rather than refused.
func tellStories(ctx context.Context, load loader, name, namespace, uid string) ([]*story.Story, error) {
	snapshot, err := load.read(ctx)
	if err != nil {
		return nil, err
	}
	candidates := story.Resolve(snapshot, name, namespace)
	if uid != "" {
		candidates = story.FilterByUID(candidates, uid)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no workload named %q found; deleted workloads are only visible while their events remain", name)
	}
	stories := make([]*story.Story, 0, len(candidates))
	for _, candidate := range candidates {
		stories = append(stories, story.Build(snapshot, candidate.Root))
	}
	return stories, nil
}
