// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	kaiclient "github.com/kai-scheduler/api/client/clientset/versioned"

	"github.com/kai-scheduler/KAI-scheduler/pkg/story"
)

const (
	exitError     = 1
	exitAmbiguous = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin *os.File, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("story", flag.ContinueOnError)
	flags.SetOutput(stderr)
	namespace := flags.String("namespace", "", "namespace of the workload; all namespaces when empty")
	flags.StringVar(namespace, "n", "", "shorthand for --namespace")
	uid := flags.String("uid", "", "UID (or prefix) of the workload, to pick between same-named ones")
	output := flags.String("output", "text", "output format: text or json")
	from := flags.String("from-file", "", "read a story snapshot JSON, or a <base>.snapshot.zip paired with <base>.events.zip, instead of the cluster")
	var chain fileList
	flags.Var(&chain, "from-snapshots",
		"comma-separated chain of <base>.<n>.snapshot.zip files, newest last, each read with its "+
			"<base>.<n>.events.zip; repeat the flag or list the files as arguments")
	kubeconfig := flags.String("kubeconfig", "", "path to the kubeconfig; defaults to the standard lookup")
	serve := flags.Bool("serve", false, "serve the story as a web page instead of printing it")
	addr := flags.String("addr", "localhost:8080", "address --serve listens on; localhost only by default")
	flags.Usage = func() {
		fmt.Fprintf(stderr, "Usage: story [flags] <workload-name>\n\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return exitError
	}
	// Snapshot files are accepted as bare arguments too, so a list typed with spaces after the
	// commas still works.
	name, extra := splitSnapshotArguments(flags.Args())
	chain = append(chain, extra...)
	if name == "" {
		flags.Usage()
		return exitError
	}

	if *serve {
		if err := serveStory(*addr, snapshotLoader(*from, chain, *kubeconfig, *namespace), name, *namespace, *uid, stdout); err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return exitError
		}
		return 0
	}

	snapshot, err := loadSnapshot(context.Background(), *from, chain, *kubeconfig, *namespace)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return exitError
	}

	candidates := story.Resolve(snapshot, name, *namespace)
	var in io.Reader
	if term.IsTerminal(int(stdin.Fd())) {
		in = stdin
	}
	selected, err := story.Select(candidates, *uid, in, stderr)
	switch {
	case errors.Is(err, story.ErrAmbiguous):
		story.RenderCandidates(stderr, candidates)
		fmt.Fprintln(stderr, "\nre-run with --namespace or --uid to pick one")
		return exitAmbiguous
	case errors.Is(err, story.ErrNotFound):
		fmt.Fprintf(stderr, "error: no workload named %q found; deleted workloads are only visible while their events remain\n", name)
		return exitError
	case err != nil:
		fmt.Fprintln(stderr, "error:", err)
		return exitError
	}

	result := story.Build(snapshot, selected.Root)
	if *output == "json" {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return exitError
		}
		return 0
	}
	story.RenderText(stdout, result)
	return 0
}

func loadSnapshot(ctx context.Context, from string, chain fileList, kubeconfig, namespace string) (*story.Snapshot, error) {
	if len(chain) > 0 {
		return loadReplays(chain)
	}
	if strings.HasSuffix(from, ".zip") {
		return loadReplay(from)
	}
	if from != "" {
		return story.LoadSnapshot(from)
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	kai, err := kaiclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return story.Collect(ctx, kube, kai, namespace)
}
