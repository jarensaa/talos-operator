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
	"slices"

	"github.com/siderolabs/talos/pkg/machinery/config"
	configdocs "github.com/siderolabs/talos/pkg/machinery/config/config"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/types/v1alpha1"
)

// DocKey identifies one document in a multi-document config: "v1alpha1" for the legacy root
// document, otherwise "<apiVersion>/<kind>" plus "/<name>" for named documents. Talos rejects
// duplicate keys, so a key is unique within a config.
type DocKey string

// v1alpha1Key is the key of the legacy v1alpha1 root document.
const v1alpha1Key DocKey = "v1alpha1"

// Config is a machine config in normalized form: one canonical encoding per document, without
// comments, so rendered and live configs compare independently of comments and document order.
type Config struct {
	// Docs maps each document to its canonical YAML.
	Docs map[DocKey][]byte
	// Keys lists document keys in the config's own order (v1alpha1 first).
	Keys []DocKey
	// Bytes is the full config to apply: the documents in order, comments disabled.
	Bytes []byte
	// Hash fingerprints the documents independently of their order.
	Hash string
}

// Normalize canonicalizes a parsed config. Live configs (via talos.ParseMachineConfig) and
// rendered configs go through the same encoder, so equal content yields equal bytes.
func Normalize(cfg config.Provider) (*Config, error) {
	out := &Config{Docs: map[DocKey][]byte{}}

	add := func(key DocKey, doc any) error {
		b, err := encoder.NewEncoder(doc, encoder.WithComments(encoder.CommentsDisabled)).Encode()
		if err != nil {
			return fmt.Errorf("encode %s: %w", key, err)
		}

		if _, dup := out.Docs[key]; dup {
			return fmt.Errorf("duplicate document %s", key)
		}

		out.Docs[key] = b
		out.Keys = append(out.Keys, key)

		if len(out.Bytes) > 0 {
			out.Bytes = append(out.Bytes, "---\n"...)
		}

		out.Bytes = append(out.Bytes, b...)

		return nil
	}

	for _, doc := range cfg.Documents() {
		if err := add(docKey(doc), doc); err != nil {
			return nil, err
		}
	}

	out.Hash = hashDocs(out.Docs)

	return out, nil
}

// BytesOrderedLike encodes c with its documents in the order they appear in other, followed by
// any documents other lacks. Document order carries no meaning, but Talos's own dry-run diff is a
// text diff of the whole config, so a config sent for its verdict is ordered like the config it
// is compared against.
func (c *Config) BytesOrderedLike(other *Config) []byte {
	order := make([]DocKey, 0, len(c.Keys))
	seen := make(map[DocKey]bool, len(c.Keys))

	for _, k := range other.Keys {
		if _, ok := c.Docs[k]; ok && !seen[k] {
			order = append(order, k)
			seen[k] = true
		}
	}

	for _, k := range c.Keys {
		if !seen[k] {
			order = append(order, k)
		}
	}

	var out []byte

	for i, k := range order {
		if i > 0 {
			out = append(out, "---\n"...)
		}

		out = append(out, c.Docs[k]...)
	}

	return out
}

func docKey(doc configdocs.Document) DocKey {
	if _, ok := doc.(*v1alpha1.Config); ok {
		return v1alpha1Key
	}

	key := doc.APIVersion() + "/" + doc.Kind()

	if named, ok := doc.(configdocs.NamedDocument); ok && named.Name() != "" {
		key += "/" + named.Name()
	}

	return DocKey(key)
}

// hashDocs fingerprints documents in key order, so document order never changes the hash.
func hashDocs(docs map[DocKey][]byte) string {
	keys := make([]DocKey, 0, len(docs))
	for k := range docs {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	h := sha256.New()

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write(docs[k])
		h.Write([]byte{0})
	}

	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}
