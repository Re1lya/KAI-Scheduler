// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package main

import "strings"

// fileList is a repeatable, comma-separated flag of file paths.
type fileList []string

func (l *fileList) String() string { return strings.Join(*l, ",") }

func (l *fileList) Set(value string) error {
	for _, path := range strings.Split(value, ",") {
		if path = strings.TrimSpace(path); path != "" {
			*l = append(*l, path)
		}
	}
	return nil
}

// splitSnapshotArguments separates the workload name from snapshot files given as bare arguments,
// which is what a chain typed with spaces after the commas arrives as. The name is the only
// argument that is not a snapshot file; more than one of those is a mistake worth reporting.
func splitSnapshotArguments(args []string) (string, []string) {
	var name string
	var snapshots []string
	for _, arg := range args {
		switch {
		case strings.HasSuffix(arg, snapshotSuffix):
			snapshots = append(snapshots, arg)
		case name != "":
			return "", nil
		default:
			name = arg
		}
	}
	return name, snapshots
}
