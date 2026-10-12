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
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	yaml "go.yaml.in/yaml/v4"
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

// Names of a bundle's certificate authorities, as status.certificates lists them.
const (
	AuthorityOS                   = "os"
	AuthorityKubernetes           = "kubernetes"
	AuthorityKubernetesAggregator = "kubernetes-aggregator"
	AuthorityEtcd                 = "etcd"
)

// Validity is the time a certificate is valid for.
type Validity struct {
	NotBefore, NotAfter time.Time
}

// AuthorityValidity returns the validity of each certificate authority of the bundle. One the
// bundle does not have, or that is not a certificate, is left out. Only the certificates are
// read, never their keys.
func AuthorityValidity(b *secrets.Bundle) map[string]Validity {
	out := map[string]Validity{}
	if b == nil || b.Certs == nil {
		return out
	}

	crt := func(name string, pemBytes []byte) {
		block, _ := pem.Decode(pemBytes)
		if block == nil {
			return
		}

		if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
			out[name] = Validity{NotBefore: cert.NotBefore, NotAfter: cert.NotAfter}
		}
	}

	if ca := b.Certs.OS; ca != nil {
		crt(AuthorityOS, ca.Crt)
	}

	if ca := b.Certs.K8s; ca != nil {
		crt(AuthorityKubernetes, ca.Crt)
	}

	if ca := b.Certs.K8sAggregator; ca != nil {
		crt(AuthorityKubernetesAggregator, ca.Crt)
	}

	if ca := b.Certs.Etcd; ca != nil {
		crt(AuthorityEtcd, ca.Crt)
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
