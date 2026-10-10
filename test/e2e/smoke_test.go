//go:build e2e

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

package e2e

import (
	"context"
	"net/netip"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
)

var _ = Describe("Observe smoke test", func() {
	It("reads every control plane through the access layer using the stored credentials", func(ctx context.Context) {
		secret := createCredentialsSecret(ctx, "smoke-talosconfig")

		c, err := creds.LoadFromSecret(secret, "", time.Now())
		Expect(err).NotTo(HaveOccurred(), "the harness talosconfig must satisfy the operator's credential checks")

		f := talos.NewFactory()
		DeferCleanup(f.Close)

		r, err := f.Reader(ctx, talos.Target{Key: "smoke", Credentials: c, Endpoints: c.Endpoints})
		Expect(err).NotTo(HaveOccurred())

		for _, ep := range c.Endpoints {
			v, err := r.Version(ctx, ep.Addr)
			Expect(err).NotTo(HaveOccurred(), "version of %s", ep.Addr)
			Expect(v.Tag).To(HavePrefix("v1.14."))
		}
	}, NodeTimeout(2*time.Minute))

	It("admits a minimal TalosCluster as observe-only with Talos-issued credentials", func(ctx context.Context) {
		// A dry run: the API server defaults and validates it, and stores nothing. A TalosCluster
		// that exists is adopted, and the cluster has one TalosCluster (adoption_test.go).
		tc := &talosv1alpha1.TalosCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "observe", Namespace: testNamespace},
		}
		Expect(k8sClient.Create(ctx, tc, client.DryRunAll)).To(Succeed())

		Expect(tc.Spec.Management.Config).To(Equal(talosv1alpha1.ConfigManagementObserve))
		Expect(tc.Spec.Credentials.Source).To(Equal(talosv1alpha1.CredentialsSourceTalosServiceAccount))
	}, NodeTimeout(time.Minute))
})

// harnessReader reads the e2e cluster with the harness's own admin talosconfig, independently of
// the operator, so specs can check what the operator did and did not do.
func harnessReader(ctx context.Context) talos.Reader {
	f, target := harnessTarget()

	r, err := f.Reader(ctx, target)
	Expect(err).NotTo(HaveOccurred())

	return r
}

// harnessWriter is the cluster admin changing a node behind the operator's back. Specs use it
// for what the operator is then expected to notice or undo, never to help the operator along.
func harnessWriter(ctx context.Context) talos.ConfigWriter {
	f, target := harnessTarget()

	w, err := f.ConfigWriter(ctx, target)
	Expect(err).NotTo(HaveOccurred())

	return w
}

// harnessTarget is the harness cluster as its admin talosconfig reaches it.
func harnessTarget() (*talos.Factory, talos.Target) {
	data, err := os.ReadFile(talosconfigPath)
	Expect(err).NotTo(HaveOccurred())

	c, err := creds.Load(data, "", time.Now())
	Expect(err).NotTo(HaveOccurred())

	f := talos.NewFactory()
	DeferCleanup(f.Close)

	return f, talos.Target{Key: "harness", Credentials: c, Endpoints: c.Endpoints}
}

// harnessNodes returns the addresses of every configured harness node.
func harnessNodes() []netip.Addr {
	var nodes []netip.Addr

	for _, env := range []string{"E2E_CONTROLPLANES", "E2E_WORKERS"} {
		for _, addr := range strings.Fields(os.Getenv(env)) {
			nodes = append(nodes, netip.MustParseAddr(addr))
		}
	}

	Expect(nodes).NotTo(BeEmpty(), "E2E_CONTROLPLANES / E2E_WORKERS not set")

	return nodes
}
