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
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
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

		endpoints := controlPlanesNow(ctx, c.Endpoints)

		r, err := f.Reader(ctx, talos.Target{Key: "smoke", Credentials: c, Endpoints: endpoints})
		Expect(err).NotTo(HaveOccurred())

		for _, ep := range endpoints {
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
	f, target := harnessTarget(ctx)

	r, err := f.Reader(ctx, target)
	Expect(err).NotTo(HaveOccurred())

	return r
}

// harnessWriter is the cluster admin changing a node behind the operator's back. Specs use it
// for what the operator is then expected to notice or undo, never to help the operator along.
func harnessWriter(ctx context.Context) talos.ConfigWriter {
	f, target := harnessTarget(ctx)

	w, err := f.ConfigWriter(ctx, target)
	Expect(err).NotTo(HaveOccurred())

	return w
}

// controlPlaneAddrsNow is the addresses of the control planes of the cluster as it is now. The
// harness's own list (E2E_CONTROLPLANES, and the endpoints of its talosconfig) was written when the
// cluster was built, and the suite replaces a control plane since (adoption_test.go): one address
// on it is then a machine in maintenance mode, or a worker, and the control plane that took its
// place is not on it.
func controlPlaneAddrsNow(ctx context.Context) []netip.Addr {
	GinkgoHelper()

	var nodes corev1.NodeList
	Expect(k8sClient.List(ctx, &nodes, client.HasLabels{"node-role.kubernetes.io/control-plane"})).To(Succeed())

	var now []netip.Addr

	for _, node := range nodes.Items {
		for _, addr := range node.Status.Addresses {
			if parsed, err := netip.ParseAddr(addr.Address); err == nil && addr.Type == corev1.NodeInternalIP {
				now = append(now, parsed)
			}
		}
	}

	Expect(now).NotTo(BeEmpty(), "the cluster has no control plane with an address")

	return now
}

// controlPlanesNow keeps the endpoints that are control planes of the cluster as it is now: what
// is no longer one forwards no request.
func controlPlanesNow(ctx context.Context, endpoints []creds.Endpoint) []creds.Endpoint {
	GinkgoHelper()

	now := controlPlaneAddrsNow(ctx)
	out := make([]creds.Endpoint, 0, len(endpoints))

	for _, ep := range endpoints {
		if slices.Contains(now, ep.Addr) {
			out = append(out, ep)
		}
	}

	Expect(out).NotTo(BeEmpty(), "none of the harness's endpoints is a control plane of the cluster")

	return out
}

// harnessTarget is the harness cluster as its admin talosconfig reaches it.
func harnessTarget(ctx context.Context) (*talos.Factory, talos.Target) {
	data, err := os.ReadFile(talosconfigPath)
	Expect(err).NotTo(HaveOccurred())

	c, err := creds.Load(data, "", time.Now())
	Expect(err).NotTo(HaveOccurred())

	f := talos.NewFactory()
	DeferCleanup(f.Close)

	return f, talos.Target{Key: "harness", Credentials: c, Endpoints: controlPlanesNow(ctx, c.Endpoints)}
}

// addrsOf returns the addresses one of the harness's variables lists: E2E_CONTROLPLANES,
// E2E_WORKERS, or E2E_MAINTENANCE_NODES, the machines it left in maintenance mode.
func addrsOf(env string) []netip.Addr {
	fields := strings.Fields(os.Getenv(env))
	out := make([]netip.Addr, 0, len(fields))

	for _, a := range fields {
		out = append(out, netip.MustParseAddr(a))
	}

	return out
}

// harnessNodes returns the addresses of every configured harness node.
func harnessNodes() []netip.Addr {
	nodes := slices.Concat(addrsOf("E2E_CONTROLPLANES"), addrsOf("E2E_WORKERS"))

	Expect(nodes).NotTo(BeEmpty(), "E2E_CONTROLPLANES / E2E_WORKERS not set")

	return nodes
}
