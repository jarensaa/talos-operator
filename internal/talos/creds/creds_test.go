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

package creds_test

import (
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/role"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
)

var now = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// Context names used across tests.
const (
	adminContext  = "admin"
	readerContext = "reader"
)

// operatorNamespace is where credentials Secrets live in these tests.
const operatorNamespace = "talos-operator"

// talosconfigTTL is the client certificate lifetime in generated talosconfigs.
const talosconfigTTL = time.Hour

var sharedBundleOnce = sync.OnceValues(func() (*secrets.Bundle, error) {
	return secrets.NewBundle(secrets.NewFixedClock(now), config.TalosVersion1_14)
})

// sharedBundle returns one bundle for the whole package. Its clock is fixed at now, so every
// certificate it issues is valid from now.
func sharedBundle(t *testing.T) *secrets.Bundle {
	t.Helper()

	bundle, err := sharedBundleOnce()
	if err != nil {
		t.Fatal(err)
	}

	return bundle
}

func endpoints(t *testing.T, raw ...string) []creds.Endpoint {
	t.Helper()

	out := make([]creds.Endpoint, 0, len(raw))

	for _, r := range raw {
		ep, err := creds.ParseEndpoint(r)
		if err != nil {
			t.Fatal(err)
		}

		out = append(out, ep)
	}

	return out
}

// talosconfig builds a talosconfig with one context per entry, all signed by bundle.
func talosconfig(t *testing.T, bundle *secrets.Bundle, current string, contexts map[string]role.Set) []byte {
	t.Helper()

	cfg := &clientconfig.Config{Context: current, Contexts: map[string]*clientconfig.Context{}}

	for name, roles := range contexts {
		cfg.Contexts[name] = issueContext(t, bundle, roles, "10.0.0.11", "10.0.0.12:50000")
	}

	data, err := cfg.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	return data
}

// issueContext returns a talosconfig context with a client certificate issued by bundle.
func issueContext(t *testing.T, bundle *secrets.Bundle, roles role.Set, endpoints ...string) *clientconfig.Context {
	t.Helper()

	cert, err := bundle.GenerateTalosAPIClientCertificateWithTTL(roles, talosconfigTTL)
	if err != nil {
		t.Fatal(err)
	}

	return clientconfig.NewConfig("c", endpoints, bundle.Certs.OS.Crt, cert).Contexts["c"]
}

func requireReason(t *testing.T, err error, want string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected error with reason %s, got nil", want)
	}

	if got := creds.Reason(err); got != want {
		t.Fatalf("reason: got %s, want %s (error: %v)", got, want, err)
	}
}

func TestLoadValidAdminConfig(t *testing.T) {
	bundle := sharedBundle(t)
	data := talosconfig(t, bundle, adminContext, map[string]role.Set{adminContext: role.MakeSet(role.Admin)})

	got, err := creds.Load(data, "", now)
	if err != nil {
		t.Fatal(err)
	}

	if !got.Roles.IncludesAny(role.MakeSet(role.Admin)) {
		t.Fatalf("roles: %v", got.Roles.Strings())
	}

	wantEndpoints := []creds.Endpoint{
		{Addr: netip.MustParseAddr("10.0.0.11")},
		{Addr: netip.MustParseAddr("10.0.0.12"), Port: 50000},
	}
	if !slices.Equal(got.Endpoints, wantEndpoints) {
		t.Fatalf("endpoints: got %v, want %v", got.Endpoints, wantEndpoints)
	}

	if !got.NotAfter.Equal(now.Add(talosconfigTTL)) {
		t.Fatalf("notAfter: got %s", got.NotAfter)
	}
}

func TestLoadExpiredConfig(t *testing.T) {
	bundle := sharedBundle(t)
	data := talosconfig(t, bundle, adminContext, map[string]role.Set{adminContext: role.MakeSet(role.Admin)})

	_, err := creds.Load(data, "", now.Add(2*time.Hour))
	requireReason(t, err, talosv1alpha1.ReasonExpired)
}

func TestLoadReaderOnlyConfigIsInsufficient(t *testing.T) {
	bundle := sharedBundle(t)
	data := talosconfig(t, bundle, readerContext, map[string]role.Set{readerContext: role.MakeSet(role.Reader)})

	_, err := creds.Load(data, "", now)
	requireReason(t, err, talosv1alpha1.ReasonInsufficientRole)
}

func TestLoadFromSecretMissingKey(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: operatorNamespace},
		Data:       map[string][]byte{"other": []byte("x")},
	}

	_, err := creds.LoadFromSecret(secret, "", now)
	requireReason(t, err, talosv1alpha1.ReasonKeyMissing)
}

func TestLoadFromSecretExplicitContext(t *testing.T) {
	bundle := sharedBundle(t)
	data := talosconfig(t, bundle, readerContext, map[string]role.Set{
		readerContext: role.MakeSet(role.Reader),
		adminContext:  role.MakeSet(role.Admin),
	})

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: operatorNamespace},
		Data:       map[string][]byte{"talosconfig": data},
	}

	// The current context is reader-only, so the default selection is rejected...
	_, err := creds.LoadFromSecret(secret, "", now)
	requireReason(t, err, talosv1alpha1.ReasonInsufficientRole)

	// ...and an explicit "context" key selects the admin context.
	secret.Data["context"] = []byte(adminContext)
	if _, err := creds.LoadFromSecret(secret, "", now); err != nil {
		t.Fatalf("explicit admin context: %v", err)
	}
}

func TestLoadRejectsHostnameEndpoint(t *testing.T) {
	ctx := issueContext(t, sharedBundle(t), role.MakeSet(role.Admin), "cluster.example.com")

	data, err := (&clientconfig.Config{Context: "c", Contexts: map[string]*clientconfig.Context{"c": ctx}}).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	_, err = creds.Load(data, "", now)
	requireReason(t, err, talosv1alpha1.ReasonParseError)

	if !strings.Contains(err.Error(), "not an IP address") {
		t.Fatalf("unexpected message: %v", err)
	}
}

func TestErrorsNeverContainCredentialMaterial(t *testing.T) {
	bundle := sharedBundle(t)
	data := talosconfig(t, bundle, readerContext, map[string]role.Set{readerContext: role.MakeSet(role.Reader)})

	cfg, err := clientconfig.FromBytes(data)
	if err != nil {
		t.Fatal(err)
	}

	secretBits := []string{"BEGIN", cfg.Contexts[readerContext].Crt[:24], cfg.Contexts[readerContext].Key[:24]}

	errs := make([]error, 0, 4)

	_, err = creds.Load(data, "", now)
	errs = append(errs, err)
	_, err = creds.Load(data, "", now.Add(48*time.Hour))
	errs = append(errs, err)
	_, err = creds.Load([]byte("contexts: [this is: not valid"), "", now)
	errs = append(errs, err)

	corrupt := strings.Replace(string(data), cfg.Contexts[readerContext].Crt[10:30], "!!!!", 1)
	_, err = creds.Load([]byte(corrupt), "", now)
	errs = append(errs, err)

	for _, e := range errs {
		if e == nil {
			t.Fatal("expected an error")
		}

		for _, bit := range secretBits {
			if strings.Contains(e.Error(), bit) {
				t.Fatalf("error leaks credential material: %v", e)
			}
		}
	}
}

// issuedSecret is the Secret Talos creates for a talos.dev ServiceAccount: its talosconfig names
// the in-cluster Service, not a node.
func issuedSecret(t *testing.T, roles role.Set) *corev1.Secret {
	t.Helper()

	data, err := (&clientconfig.Config{Context: "default", Contexts: map[string]*clientconfig.Context{
		"default": issueContext(t, sharedBundle(t), roles, "talos.default"),
	}}).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "c-talos-operator", Namespace: operatorNamespace},
		Data:       map[string][]byte{talosv1alpha1.TalosServiceAccountKey: data},
	}
}

func TestLoadIssuedUsesSuppliedNodeEndpoints(t *testing.T) {
	nodes := endpoints(t, "10.0.0.11", "10.0.0.12")

	c, err := creds.LoadIssuedFromSecret(issuedSecret(t, role.MakeSet(role.Admin)), nodes, now)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(c.Endpoints, nodes) {
		t.Fatalf("endpoints: got %v, want the supplied node IPs %v", c.Endpoints, nodes)
	}

	// The same talosconfig is refused where its own endpoints would be dialled.
	_, err = creds.LoadFromSecret(issuedSecret(t, role.MakeSet(role.Admin)), talosv1alpha1.TalosServiceAccountKey, now)
	requireReason(t, err, talosv1alpha1.ReasonParseError)
}

func TestLoadIssuedChecksRoleExpiryAndKey(t *testing.T) {
	nodes := endpoints(t, "10.0.0.11")

	_, err := creds.LoadIssuedFromSecret(issuedSecret(t, role.MakeSet(role.Reader)), nodes, now)
	requireReason(t, err, talosv1alpha1.ReasonInsufficientRole)

	_, err = creds.LoadIssuedFromSecret(issuedSecret(t, role.MakeSet(role.Admin)), nodes, now.Add(talosconfigTTL))
	requireReason(t, err, talosv1alpha1.ReasonExpired)

	empty := issuedSecret(t, role.MakeSet(role.Admin))
	empty.Data = nil
	_, err = creds.LoadIssuedFromSecret(empty, nodes, now)
	requireReason(t, err, talosv1alpha1.ReasonKeyMissing)
}
