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

package bump

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf8"

	yamlv4 "go.yaml.in/yaml/v4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
)

// A version is changed where the file writes it and nowhere else: the file is a human's, with
// its comments, its order and its quotes, and a bump that wrote it out again from the object
// would make every one of them part of the change.

// separator begins the line between two documents of a file. It is where the validate check
// splits a file too (utilyaml.NewYAMLReader), so both see the same documents.
var separator = []byte("---")

// found is one TalosCluster in a file.
type found struct {
	// offset is where its document begins in the file, and raw the document.
	offset  int
	raw     []byte
	cluster *talosv1alpha1.TalosCluster
}

// clusters returns the TalosClusters a file holds, in the order it holds them. A document that
// is something else, or nothing that can be read, is none of this command's business.
func clusters(content []byte) []found {
	var (
		out    []found
		offset int
	)

	document := func(end int) {
		raw := content[offset:end]

		var meta metav1.TypeMeta
		if err := yaml.Unmarshal(raw, &meta); err != nil || meta.Kind != "TalosCluster" {
			return
		}

		if gv, err := schema.ParseGroupVersion(meta.APIVersion); err != nil || gv.Group != talosv1alpha1.GroupVersion.Group {
			return
		}

		tc := &talosv1alpha1.TalosCluster{}
		if err := yaml.Unmarshal(raw, tc); err != nil {
			return
		}

		out = append(out, found{offset: offset, raw: raw, cluster: tc})
	}

	for at := 0; at < len(content); {
		end := len(content)
		if i := bytes.IndexByte(content[at:], '\n'); i >= 0 {
			end = at + i + 1
		}

		if bytes.HasPrefix(content[at:end], separator) {
			document(at)

			offset = end
		}

		at = end
	}

	document(len(content))

	return out
}

// write returns the file with the cluster's Talos or Kubernetes version replaced by to, and
// every other byte as it was.
func (f found) write(content []byte, kind, to string) ([]byte, error) {
	key, from := "talos", f.cluster.Spec.Versions.Talos
	if kind == plan.StepKubernetes {
		key, from = "kubernetes", f.cluster.Spec.Versions.Kubernetes
	}

	at, err := scalar(f.raw, from, "spec", "versions", key)
	if err != nil {
		return nil, fmt.Errorf("versions.%s: %w", key, err)
	}

	at += f.offset

	out := make([]byte, 0, len(content)+len(to))
	out = append(out, content[:at]...)
	out = append(out, to...)
	out = append(out, content[at+len(from):]...)

	// What was written is read back: the cluster must now ask for exactly this, and for nothing
	// else that it did not ask for before.
	after := clusters(out)
	before := clusters(content)

	for i := range before {
		want := before[i].cluster.DeepCopy()
		if before[i].offset == f.offset {
			if kind == plan.StepKubernetes {
				want.Spec.Versions.Kubernetes = to
			} else {
				want.Spec.Versions.Talos = to
			}
		}

		if len(after) != len(before) || !equal(after[i].cluster, want) {
			return nil, fmt.Errorf("versions.%s: writing %s where the file says %s would change more than that", key, to, from)
		}
	}

	return out, nil
}

func equal(a, b *talosv1alpha1.TalosCluster) bool {
	left, errA := yaml.Marshal(a)
	right, errB := yaml.Marshal(b)

	return errA == nil && errB == nil && bytes.Equal(left, right)
}

// scalar finds where a document writes the value at a path of keys, and returns the offset of
// the value in the document. It refuses a value it does not find written there as it is: one
// that comes through an alias or a merge key is shared with something else, and changing it
// would change that too.
func scalar(doc []byte, value string, path ...string) (int, error) {
	var root yamlv4.Node
	if err := yamlv4.Unmarshal(doc, &root); err != nil {
		return 0, errors.New("the document cannot be read")
	}

	node := &root
	if node.Kind == yamlv4.DocumentNode && len(node.Content) == 1 {
		node = node.Content[0]
	}

	for _, key := range path {
		var next *yamlv4.Node

		if node.Kind == yamlv4.MappingNode {
			for i := 0; i+1 < len(node.Content); i += 2 {
				if node.Content[i].Value == key {
					next = node.Content[i+1]
				}
			}
		}

		if next == nil {
			return 0, errors.New("it is not written in the document itself (an alias or a merge key?), so it is not changed here")
		}

		node = next
	}

	if node.Kind != yamlv4.ScalarNode || node.Value != value {
		return 0, errors.New("it is not written in the document itself (an alias?), so it is not changed here")
	}

	at, err := position(doc, node.Line, node.Column)
	if err != nil {
		return 0, err
	}

	// A quoted value begins with its quote.
	if node.Style&(yamlv4.DoubleQuotedStyle|yamlv4.SingleQuotedStyle) != 0 {
		at++
	}

	if !bytes.HasPrefix(doc[min(at, len(doc)):], []byte(value)) {
		return 0, fmt.Errorf("%s is not written as it is read (line %d), so it is not changed here", value, node.Line)
	}

	return at, nil
}

// position is the offset in a document of a line and a column, as the YAML parser counts them:
// from one, and a column in characters.
func position(doc []byte, line, column int) (int, error) {
	at := 0

	for range line - 1 {
		i := bytes.IndexByte(doc[at:], '\n')
		if i < 0 {
			return 0, fmt.Errorf("there is no line %d", line)
		}

		at += i + 1
	}

	for range column - 1 {
		_, size := utf8.DecodeRune(doc[at:])
		if size == 0 {
			return 0, fmt.Errorf("line %d has no column %d", line, column)
		}

		at += size
	}

	return at, nil
}
