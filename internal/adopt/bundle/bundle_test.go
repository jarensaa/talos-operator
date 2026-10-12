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

package bundle_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/adopt/bundle"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

func generateConfig(t *testing.T, b *secrets.Bundle, contract *config.VersionContract, role machine.Type) config.Provider {
	t.Helper()

	in := talostest.Input(t)
	in.Bundle, in.Contract, in.Role = b, contract, role

	return talostest.Generate(t, in)
}

func TestRecoverRoundTripsTheBundle(t *testing.T) {
	original := talostest.Bundle(t)
	cfg := generateConfig(t, original, config.TalosVersion1_14, machine.TypeControlPlane)

	got, err := bundle.Recover(cfg, config.TalosVersion1_14)
	if err != nil {
		t.Fatal(err)
	}

	for name, pair := range map[string][2]any{
		"certs":   {original.Certs, got.Certs},
		"cluster": {original.Cluster, got.Cluster},
		"secrets": {original.Secrets, got.Secrets},
		"trustd":  {original.TrustdInfo, got.TrustdInfo},
	} {
		if !reflect.DeepEqual(pair[0], pair[1]) {
			t.Errorf("%s differs after recovery", name)
		}
	}
}

func TestRecoverRefusesWorkerConfigs(t *testing.T) {
	cfg := generateConfig(t, talostest.Bundle(t), config.TalosVersion1_14, machine.TypeWorker)

	if _, err := bundle.Recover(cfg, config.TalosVersion1_14); !errors.Is(err, bundle.ErrNotControlPlane) {
		t.Fatalf("expected ErrNotControlPlane, got %v", err)
	}
}

func TestRecoverRefusesAESCBC(t *testing.T) {
	// Clusters created before Talos 1.3 carry an AESCBC secret in the v1alpha1 cluster section.
	old, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_2)
	if err != nil {
		t.Fatal(err)
	}

	if old.Secrets.AESCBCEncryptionSecret == "" {
		t.Skip("bundle for the 1.2 contract has no AESCBC secret")
	}

	cfg := generateConfig(t, old, config.TalosVersion1_2, machine.TypeControlPlane)

	if _, err := bundle.Recover(cfg, config.TalosVersion1_2); !errors.Is(err, bundle.ErrAESCBC) {
		t.Fatalf("expected ErrAESCBC, got %v", err)
	}
}

func TestVerifyAcceptsEveryRoleFromTheSameBundle(t *testing.T) {
	b := talostest.Bundle(t)

	err := bundle.Verify(b, []bundle.Node{
		{Name: "cp-1", Config: generateConfig(t, b, config.TalosVersion1_14, machine.TypeControlPlane)},
		{Name: "cp-2", Config: generateConfig(t, b, config.TalosVersion1_14, machine.TypeControlPlane)},
		{Name: "worker-1", Config: generateConfig(t, b, config.TalosVersion1_14, machine.TypeWorker)},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestVerifyNamesTheMismatchingNodeAndFields(t *testing.T) {
	b := talostest.Bundle(t)

	other, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	if err != nil {
		t.Fatal(err)
	}

	err = bundle.Verify(b, []bundle.Node{
		{Name: "cp-1", Config: generateConfig(t, b, config.TalosVersion1_14, machine.TypeControlPlane)},
		{Name: "cp-2", Config: generateConfig(t, other, config.TalosVersion1_14, machine.TypeControlPlane)},
		{Name: "worker-1", Config: generateConfig(t, other, config.TalosVersion1_14, machine.TypeWorker)},
	})

	var mismatch *bundle.MismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected MismatchError, got %v", err)
	}

	msg := err.Error()
	for _, want := range []string{
		"cp-2: Talos API CA", "cp-2: etcd CA", "cp-2: service account key", "worker-1: Kubernetes CA", "worker-1: trustd token",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("missing %q in %s", want, msg)
		}
	}

	if strings.Contains(msg, "cp-1") {
		t.Errorf("the matching node must not be reported: %s", msg)
	}

	// Values never appear: neither bundle's tokens nor key material.
	for _, secret := range []string{b.TrustdInfo.Token, other.TrustdInfo.Token, b.Cluster.Secret, "BEGIN"} {
		if strings.Contains(msg, secret) {
			t.Fatal("mismatch message leaks a secret")
		}
	}
}

func TestAuthorityCertificates(t *testing.T) {
	b := talostest.Bundle(t)

	got := bundle.AuthorityCertificates(b)
	if len(got) != 4 {
		t.Fatalf("a bundle has four authorities, got %d", len(got))
	}

	for _, name := range []string{"os", "kubernetes", "kubernetes-aggregator", "etcd"} {
		if cert := got[name]; cert == nil || !cert.IsCA {
			t.Errorf("%s: not listed as a certificate authority", name)
		}
	}

	// One the bundle does not have, and one that is not a certificate, are left out; the others
	// are still listed.
	certs := *b.Certs
	certs.Etcd = nil

	notACertificate := *certs.K8s
	notACertificate.Crt = []byte("not a certificate")
	certs.K8s = &notACertificate

	lacking := *b
	lacking.Certs = &certs

	got = bundle.AuthorityCertificates(&lacking)
	if len(got) != 2 || got["os"] == nil || got["kubernetes-aggregator"] == nil {
		t.Fatalf("got %d authorities of a bundle that has two", len(got))
	}

	if len(bundle.AuthorityCertificates(nil)) != 0 || len(bundle.AuthorityCertificates(&secrets.Bundle{})) != 0 {
		t.Fatal("no bundle, and a bundle without certificates, have no authorities")
	}
}

// A bundle read from a Secret is whatever someone put there. One that lacks a part is refused by
// name, and never read through a section it does not have.
func TestVerifyRefusesABundleThatLacksAPart(t *testing.T) {
	b := talostest.Bundle(t)
	nodes := []bundle.Node{{Name: "cp-1", Config: generateConfig(t, b, config.TalosVersion1_14, machine.TypeControlPlane)}}

	noCerts := *b
	noCerts.Certs = nil

	noEtcd := *b
	certs := *b.Certs
	certs.Etcd = nil
	noEtcd.Certs = &certs

	noCluster := *b
	noCluster.Cluster = nil

	noTokens := *b
	noTokens.Secrets, noTokens.TrustdInfo = nil, nil

	for name, tt := range map[string]struct {
		bundle *secrets.Bundle
		want   []string
	}{
		"nothing":         {&secrets.Bundle{}, []string{"Talos API CA key", "service account key", "cluster ID", "trustd token"}},
		"no bundle":       {nil, []string{"Talos API CA key", "secretbox encryption secret"}},
		"no certs":        {&noCerts, []string{"Talos API CA key, etcd CA key, Kubernetes CA key, aggregator CA key, service account key"}},
		"no etcd CA":      {&noEtcd, []string{"missing etcd CA key"}},
		"no cluster":      {&noCluster, []string{"missing cluster ID, cluster secret"}},
		"no tokens":       {&noTokens, []string{"missing trustd token, bootstrap token, secretbox encryption secret"}},
		"the whole of it": {b, nil},
	} {
		err := bundle.Verify(tt.bundle, nodes)

		if tt.want == nil {
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}

			continue
		}

		if err == nil || !strings.HasPrefix(err.Error(), "secrets bundle is incomplete: missing ") {
			t.Errorf("%s: got %v", name, err)

			continue
		}

		for _, part := range tt.want {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("%s: %q is not named in: %v", name, part, err)
			}
		}

		if strings.Contains(err.Error(), b.TrustdInfo.Token) || strings.Contains(err.Error(), "BEGIN") {
			t.Fatalf("%s: the message leaks a secret", name)
		}
	}
}
