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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	yaml "go.yaml.in/yaml/v4"
)

// Redaction is rule-based rather than a list of known secret paths: a list silently leaks when
// Talos adds a secret-bearing field. Any mapping key that names a credential has its whole value
// (scalar or subtree) replaced by a short hash, so diffs still show that it changed.
var (
	sensitiveExact = map[string]bool{
		"key": true, "keys": true, "crt": true, "auth": true, "ca": true, "cas": true,
		"acceptedcas": true, "issuingca": true, "token": true, "secret": true,
	}
	sensitiveSuffixes = []string{"key", "token", "secret", "password", "auth"}
)

func sensitive(name string) bool {
	n := strings.ToLower(name)
	if sensitiveExact[n] {
		return true
	}

	for _, s := range sensitiveSuffixes {
		if strings.HasSuffix(n, s) {
			return true
		}
	}

	return false
}

// Redact returns a YAML document with every credential replaced by "sha256:<prefix>". It fails
// closed: anything that does not parse is replaced entirely.
func Redact(doc []byte) []byte {
	var root yaml.Node
	if err := yaml.Unmarshal(doc, &root); err != nil {
		return []byte(redactedValue(doc) + "\n")
	}

	redactNode(&root)

	out, err := yaml.Marshal(&root)
	if err != nil {
		return []byte(redactedValue(doc) + "\n")
	}

	return out
}

func redactNode(n *yaml.Node) {
	switch n.Kind {
	case yaml.StreamNode, yaml.DocumentNode, yaml.SequenceNode:
		for _, c := range n.Content {
			redactNode(c)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, value := n.Content[i], n.Content[i+1]

			if sensitive(key.Value) {
				replaceWithHash(value)

				continue
			}

			redactNode(value)
		}
	case yaml.AliasNode:
		// Aliases point at nodes redacted where they are defined.
	case yaml.ScalarNode:
	}
}

func replaceWithHash(n *yaml.Node) {
	raw, err := yaml.Marshal(n)
	if err != nil {
		raw = []byte(fmt.Sprint(n.Value))
	}

	*n = yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: redactedValue(raw)}
}

func redactedValue(raw []byte) string {
	sum := sha256.Sum256(raw)

	return "sha256:" + hex.EncodeToString(sum[:6])
}
