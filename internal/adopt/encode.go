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

package adopt

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	yaml "go.yaml.in/yaml/v4"

	"github.com/jarensaa/talos-operator/internal/adopt/bundle"
)

// EncodeBundle encodes a secrets bundle in the secrets.yaml format of `talosctl gen secrets`, so
// the Secret and an off-cluster copy are interchangeable.
func EncodeBundle(b *secrets.Bundle) ([]byte, error) {
	return yaml.Marshal(b)
}

// DecodeBundle decodes a secrets.yaml.
func DecodeBundle(data []byte) (*secrets.Bundle, error) {
	b := &secrets.Bundle{Clock: secrets.NewClock()}

	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(b); err != nil {
		return nil, fmt.Errorf("decode secrets bundle: %w", err)
	}

	return b, nil
}

// Validity is the time a certificate is valid for.
type Validity struct {
	NotBefore, NotAfter time.Time
}

// AuthorityValidity returns the validity of each certificate authority of the bundle, by the name
// status.certificates lists it under (bundle.AuthorityCertificates). One the bundle does not
// have, or that is not a certificate, is left out.
func AuthorityValidity(b *secrets.Bundle) map[string]Validity {
	out := map[string]Validity{}

	for name, cert := range bundle.AuthorityCertificates(b) {
		out[name] = Validity{NotBefore: cert.NotBefore, NotAfter: cert.NotAfter}
	}

	return out
}

// BundleHash fingerprints a bundle; the user confirms an off-cluster backup by setting the
// backed-up annotation to this value.
func BundleHash(b *secrets.Bundle) (string, error) {
	data, err := EncodeBundle(b)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
