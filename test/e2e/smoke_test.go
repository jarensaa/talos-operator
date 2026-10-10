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
	"fmt"
	"net/netip"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
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
		tc := &talosv1alpha1.TalosCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "observe", Namespace: testNamespace},
		}
		Expect(k8sClient.Create(ctx, tc)).To(Succeed())
		DeferCleanup(func(ctx context.Context) {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, tc))).To(Succeed())
		})

		got := &talosv1alpha1.TalosCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got)).To(Succeed())
		Expect(got.Spec.Management.Config).To(Equal(talosv1alpha1.ConfigManagementObserve))
		Expect(got.Spec.Credentials.Source).To(Equal(talosv1alpha1.CredentialsSourceTalosServiceAccount))
	}, NodeTimeout(time.Minute))
})

var _ = Describe("Observe", func() {
	// No credentials are supplied: the operator requests its certificate from Talos, which the
	// harness allows for this namespace (hack/e2e/patches/controlplane.patch.yaml).
	It("reports the live cluster's state in TalosCluster status", func(ctx context.Context) {
		tc := &talosv1alpha1.TalosCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "observe-live", Namespace: testNamespace},
		}
		Expect(k8sClient.Create(ctx, tc)).To(Succeed())
		DeferCleanup(func(ctx context.Context) {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, tc))).To(Succeed())
		})

		conditionTrue := func(condType string) func(Gomega) {
			return func(g Gomega) {
				got := &talosv1alpha1.TalosCluster{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got)).To(Succeed())

				cond := meta.FindStatusCondition(got.Status.Conditions, condType)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionTrue), cond.Message)
			}
		}

		Eventually(conditionTrue(talosv1alpha1.ConditionCredentialsValid)).WithContext(ctx).Should(Succeed())
		Eventually(conditionTrue(talosv1alpha1.ConditionAdoptionPreconditionsMet)).WithContext(ctx).Should(Succeed())
		Eventually(conditionTrue(talosv1alpha1.ConditionTalosAPIReachable)).WithContext(ctx).Should(Succeed())
		Eventually(conditionTrue(talosv1alpha1.ConditionHealthy)).
			WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		got := &talosv1alpha1.TalosCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseObserving))
		Expect(got.Status.Observed.EtcdMembers).NotTo(BeEmpty())
		Expect(got.Status.Observed.KubernetesVersion).To(HavePrefix("1."))

		var nodes int32
		for tag, n := range got.Status.Observed.TalosVersions {
			Expect(tag).To(HavePrefix("v1.14."))
			nodes += n
		}

		Expect(nodes).To(BeNumerically(">=", 2), "every node's Talos version is observed")
		Expect(got.Status.Adoption.UnmatchedMembers).NotTo(BeEmpty(), "no TalosMachines exist yet")

		By("binding a TalosMachine to every live node by systemUUID")
		machines := machinesForLiveNodes(ctx, "observe-live")
		for _, tm := range machines {
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				deleteMachine(ctx, tm)
			})
		}

		for _, tm := range machines {
			Eventually(func(g Gomega) {
				bound := &talosv1alpha1.TalosMachine{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), bound)).To(Succeed())

				cond := meta.FindStatusCondition(bound.Status.Conditions, talosv1alpha1.ConditionMatched)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionTrue), cond.Message)
				g.Expect(bound.Status.Phase).To(Equal(talosv1alpha1.MachinePhase("Ready")))
				g.Expect(bound.Status.Config.LiveHash).To(HavePrefix("sha256:"))
			}).WithContext(ctx).Should(Succeed())
		}

		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got)).To(Succeed())
			g.Expect(got.Status.Adoption.UnmatchedMembers).To(BeEmpty())
		}).WithContext(ctx).Should(Succeed())
	}, NodeTimeout(5*time.Minute))
})

// machinesForLiveNodes builds one TalosMachine per harness node, identified the way a user
// would: by reading each node's systemUUID. It creates the cluster's pools, one per role.
func machinesForLiveNodes(ctx context.Context, cluster string) []*talosv1alpha1.TalosMachine {
	r := harnessReader(ctx)

	roles := []talosv1alpha1.MachineRole{talosv1alpha1.MachineRoleControlPlane, talosv1alpha1.MachineRoleWorker}

	for _, role := range roles {
		pool := &talosv1alpha1.TalosMachinePool{
			ObjectMeta: metav1.ObjectMeta{Name: adopt.PoolName(cluster, role), Namespace: testNamespace},
			Spec: talosv1alpha1.TalosMachinePoolSpec{
				ClusterRef: corev1.LocalObjectReference{Name: cluster},
				Role:       role,
			},
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		DeferCleanup(func(ctx context.Context) {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pool))).To(Succeed())
		})
	}

	var machines []*talosv1alpha1.TalosMachine

	for role, env := range map[talosv1alpha1.MachineRole]string{
		talosv1alpha1.MachineRoleControlPlane: "E2E_CONTROLPLANES",
		talosv1alpha1.MachineRoleWorker:       "E2E_WORKERS",
	} {
		for i, addr := range strings.Fields(os.Getenv(env)) {
			si, err := r.SystemInformation(ctx, netip.MustParseAddr(addr))
			Expect(err).NotTo(HaveOccurred())

			machines = append(machines, &talosv1alpha1.TalosMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s-%d", cluster, strings.ToLower(string(role)), i+1),
					Namespace: testNamespace,
				},
				Spec: talosv1alpha1.TalosMachineSpec{
					PoolRef:  corev1.LocalObjectReference{Name: adopt.PoolName(cluster, role)},
					Address:  addr,
					Identity: talosv1alpha1.MachineIdentity{SystemUUID: si.UUID},
				},
			})
		}
	}

	Expect(machines).NotTo(BeEmpty(), "E2E_CONTROLPLANES / E2E_WORKERS not set")

	return machines
}

// harnessReader reads the e2e cluster with the harness's own admin talosconfig, independently of
// the operator, so specs can check what the operator did and did not do.
func harnessReader(ctx context.Context) talos.Reader {
	data, err := os.ReadFile(talosconfigPath)
	Expect(err).NotTo(HaveOccurred())

	c, err := creds.Load(data, "", time.Now())
	Expect(err).NotTo(HaveOccurred())

	f := talos.NewFactory()
	DeferCleanup(f.Close)

	r, err := f.Reader(ctx, talos.Target{Key: "harness", Credentials: c, Endpoints: c.Endpoints})
	Expect(err).NotTo(HaveOccurred())

	return r
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
