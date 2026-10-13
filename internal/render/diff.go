/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package render

import (
	"fmt"
	"slices"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/textdiff"
)

// DiffOp says how a document differs.
type DiffOp string

const (
	DiffAdded   DiffOp = "added"
	DiffRemoved DiffOp = "removed"
	DiffChanged DiffOp = "changed"
)

// DocDiff is one differing document. Text is a unified diff of the redacted documents, safe for
// status, Events and logs.
type DocDiff struct {
	Key  DocKey
	Op   DiffOp
	Text string
}

// Diff compares two normalized configs document by document. Documents are matched by key, so
// order never produces a difference; an empty result means the configs are equal.
func Diff(from, to *Config) ([]DocDiff, error) {
	keys := make([]DocKey, 0, len(from.Docs)+len(to.Docs))
	for k := range from.Docs {
		keys = append(keys, k)
	}

	for k := range to.Docs {
		if _, ok := from.Docs[k]; !ok {
			keys = append(keys, k)
		}
	}

	slices.Sort(keys)

	var diffs []DocDiff

	for _, k := range keys {
		a, inFrom := from.Docs[k]
		b, inTo := to.Docs[k]

		var op DiffOp

		switch {
		case !inFrom:
			op = DiffAdded
		case !inTo:
			op = DiffRemoved
		case string(a) == string(b):
			continue
		default:
			op = DiffChanged
		}

		var ra, rb string
		if inFrom {
			ra = string(Redact(a))
		}

		if inTo {
			rb = string(Redact(b))
		}

		text, err := textdiff.DiffWithCustomPaths(ra, rb, "a/"+string(k), "b/"+string(k))
		if err != nil {
			return nil, fmt.Errorf("diff %s: %w", k, err)
		}

		if text == "" {
			// Only redacted values differ: say so without revealing them.
			text = fmt.Sprintf("%s: a redacted value changed\n", k)
		}

		diffs = append(diffs, DocDiff{Key: k, Op: op, Text: text})
	}

	return diffs, nil
}

// Reproduces requires a rendered config to be the config a node runs. what names, in the plural,
// what it was rendered from. The error shows the difference as Diff does: redacted.
func Reproduces(what string, rendered *Config, live config.Provider) error {
	want, err := Normalize(live)
	if err != nil {
		return fmt.Errorf("normalize live config: %w", err)
	}

	diffs, err := Diff(rendered, want)
	if err != nil {
		return err
	}

	if len(diffs) == 0 {
		return nil
	}

	var text strings.Builder
	for _, d := range diffs {
		text.WriteString(d.Text)
	}

	return fmt.Errorf("%s do not reproduce the live config:\n%s", what, text.String())
}
