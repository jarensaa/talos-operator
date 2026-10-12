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

// Package talostest provides Talos fixtures for tests: a shared throwaway secrets bundle, the
// talosconfigs signed by it, and the machine configs of a made-up cluster generated from it.
package talostest

import (
	"sync"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/role"
)

// T is the subset of testing.TB the fixtures need; both *testing.T and GinkgoT() satisfy it.
type T interface {
	Helper()
	Fatal(args ...any)
}

// Generating a bundle's CA keys takes about a second, so one bundle serves every test in a binary.
var bundle = sync.OnceValues(func() (*secrets.Bundle, error) {
	return secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
})

// Bundle returns the shared throwaway secrets bundle.
func Bundle(t T) *secrets.Bundle {
	t.Helper()

	b, err := bundle()
	if err != nil {
		t.Fatal(err)
	}

	return b
}

// Talosconfig returns a single-context talosconfig signed by the shared bundle, with the given
// roles, endpoints and certificate lifetime. Endpoints are written as given, so tests can also
// build invalid ones.
func Talosconfig(t T, roles role.Set, ttl time.Duration, endpoints ...string) []byte {
	t.Helper()

	return talosconfig(t, "test", roles, ttl, endpoints)
}

// IssuedTalosconfig returns a talosconfig shaped like the one Talos issues for a talos.dev
// ServiceAccount: context "default", the in-cluster Service as its only endpoint, os:admin and a
// six-hour certificate (kubeaccess/serviceaccount/crd_controller.go:49,70-71,95 in Talos v1.14).
func IssuedTalosconfig(t T) []byte {
	t.Helper()

	return talosconfig(t, "default", role.MakeSet(role.Admin), 6*time.Hour, []string{"talos.default"})
}

func talosconfig(t T, contextName string, roles role.Set, ttl time.Duration, endpoints []string) []byte {
	t.Helper()

	b := Bundle(t)

	cert, err := b.GenerateTalosAPIClientCertificateWithTTL(roles, ttl)
	if err != nil {
		t.Fatal(err)
	}

	data, err := clientconfig.NewConfig(contextName, endpoints, b.Certs.OS.Crt, cert).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	return data
}

// AdminTalosconfig is Talosconfig with the os:admin role and a one-hour lifetime.
func AdminTalosconfig(t T, endpoints ...string) []byte {
	t.Helper()

	return Talosconfig(t, role.MakeSet(role.Admin), time.Hour, endpoints...)
}
