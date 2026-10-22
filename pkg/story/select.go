// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package story

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	ErrNotFound = errors.New("no workload found")
	// ErrAmbiguous means more than one workload matched and the user could not be asked.
	ErrAmbiguous = errors.New("more than one workload matches")
)

// Select picks the candidate to tell the story of. A single candidate, or a UID prefix that
// narrows to one, is chosen silently; otherwise the user is asked, when in is non-nil.
func Select(candidates []Candidate, uidPrefix string, in io.Reader, out io.Writer) (Candidate, error) {
	if uidPrefix != "" {
		candidates = FilterByUID(candidates, uidPrefix)
	}
	switch len(candidates) {
	case 0:
		return Candidate{}, ErrNotFound
	case 1:
		return candidates[0], nil
	}
	if in == nil {
		return Candidate{}, ErrAmbiguous
	}

	RenderCandidates(out, candidates)
	fmt.Fprintf(out, "Select [1-%d]: ", len(candidates))
	answer, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && answer == "" {
		return Candidate{}, fmt.Errorf("reading selection: %w", err)
	}
	index, err := strconv.Atoi(strings.TrimSpace(answer))
	if err != nil || index < 1 || index > len(candidates) {
		return Candidate{}, fmt.Errorf("invalid selection %q", strings.TrimSpace(answer))
	}
	return candidates[index-1], nil
}

// FilterByUID keeps the candidates whose UID starts with prefix.
func FilterByUID(candidates []Candidate, prefix string) []Candidate {
	var matching []Candidate
	for _, candidate := range candidates {
		if strings.HasPrefix(string(candidate.Root.UID), prefix) {
			matching = append(matching, candidate)
		}
	}
	return matching
}
