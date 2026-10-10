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

	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
)

// PatchDocKeys returns the keys of the documents a strategic merge patch touches.
func PatchDocKeys(patch []byte) ([]DocKey, error) {
	p, err := configpatcher.LoadPatch(patch)
	if err != nil {
		return nil, err
	}

	smp, ok := p.(configpatcher.StrategicMergePatch)
	if !ok {
		return nil, ErrJSONPatch
	}

	docs := smp.Documents()
	keys := make([]DocKey, 0, len(docs))

	for _, doc := range docs {
		keys = append(keys, docKey(doc))
	}

	return keys, nil
}

// Withhold replaces the text of every diff of the given documents with a notice. Redaction is
// rule-based and only knows credential-named fields; values from a sensitive source, such as a
// patch read from a Secret, can sit anywhere, so those documents are named but never shown.
func Withhold(diffs []DocDiff, keys map[DocKey]bool) []DocDiff {
	out := make([]DocDiff, 0, len(diffs))

	for _, d := range diffs {
		if keys[d.Key] {
			d.Text = fmt.Sprintf("%s: %s; the diff is withheld because a patch from a Secret sets values in this document\n", d.Key, d.Op)
		}

		out = append(out, d)
	}

	return out
}
