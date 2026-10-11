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
	"k8s.io/apimachinery/pkg/runtime"
	sigsyaml "sigs.k8s.io/yaml"
)

// Renovate annotations on the version fields of a proposed TalosCluster, so a committed proposal
// is picked up by Renovate as is (docs/scope.md §14.5).
var renovateAnnotations = map[string]string{
	"talos":      "renovate: datasource=github-releases depName=siderolabs/talos",
	"kubernetes": "renovate: datasource=github-releases depName=kubernetes/kubernetes",
}

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

// ClusterYAML renders the proposed TalosCluster as commit-ready YAML with Renovate annotations.
func (p *Proposal) ClusterYAML() ([]byte, error) {
	return manifestYAML(p.Cluster, true)
}

// PoolsYAML renders the proposed TalosMachinePools as one multi-document YAML.
func (p *Proposal) PoolsYAML() ([]byte, error) {
	objs := make([]runtime.Object, 0, len(p.Pools))
	for _, pool := range p.Pools {
		objs = append(objs, pool)
	}

	return manifestsYAML(objs)
}

// MachinesYAML renders the proposed TalosMachines as one multi-document YAML.
func (p *Proposal) MachinesYAML() ([]byte, error) {
	objs := make([]runtime.Object, 0, len(p.Machines))
	for _, tm := range p.Machines {
		objs = append(objs, tm)
	}

	return manifestsYAML(objs)
}

func manifestsYAML(objs []runtime.Object) ([]byte, error) {
	var out bytes.Buffer

	for i, obj := range objs {
		doc, err := manifestYAML(obj, false)
		if err != nil {
			return nil, err
		}

		if i > 0 {
			out.WriteString("---\n")
		}

		out.Write(doc)
	}

	return out.Bytes(), nil
}

// manifestYAML encodes an object without status or server-set metadata.
func manifestYAML(obj runtime.Object, annotate bool) ([]byte, error) {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}

	delete(u, "status")

	if md, ok := u["metadata"].(map[string]any); ok {
		delete(md, "creationTimestamp")
	}

	plain, err := sigsyaml.Marshal(u)
	if err != nil {
		return nil, err
	}

	if !annotate {
		return plain, nil
	}

	var root yaml.Node
	if err := yaml.Unmarshal(plain, &root); err != nil {
		return nil, err
	}

	if versions := find(&root, "spec", "versions"); versions != nil {
		for i := 0; i+1 < len(versions.Content); i += 2 {
			if c, ok := renovateAnnotations[versions.Content[i].Value]; ok {
				versions.Content[i].HeadComment = c
			}
		}
	}

	var out bytes.Buffer

	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)

	if err := enc.Encode(&root); err != nil {
		return nil, err
	}

	return out.Bytes(), enc.Close()
}

// find walks mapping keys from a document root.
func find(n *yaml.Node, path ...string) *yaml.Node {
	if n.Kind == yaml.DocumentNode && len(n.Content) == 1 {
		n = n.Content[0]
	}

	for _, key := range path {
		if n.Kind != yaml.MappingNode {
			return nil
		}

		var next *yaml.Node

		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == key {
				next = n.Content[i+1]
			}
		}

		if next == nil {
			return nil
		}

		n = next
	}

	return n
}
