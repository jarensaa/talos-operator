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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	sigsyaml "sigs.k8s.io/yaml"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// nodeState is what must not change on a node the operator is not meant to change.
type nodeState struct {
	BootID     string
	ConfigHash string
}

func snapshot(ctx context.Context, r talos.Reader, nodes []netip.Addr) map[netip.Addr]nodeState {
	GinkgoHelper()

	out := make(map[netip.Addr]nodeState, len(nodes))

	for _, n := range nodes {
		boot, err := r.BootID(ctx, n)
		Expect(err).NotTo(HaveOccurred())

		cfgs, err := r.MachineConfig(ctx, n)
		Expect(err).NotTo(HaveOccurred())

		out[n] = nodeState{BootID: boot, ConfigHash: talos.HashConfig(cfgs.Active)}
	}

	return out
}

// rejectedRPCs sums talos_operator_rejected_rpcs_total from the in-process manager's registry.
func rejectedRPCs() float64 {
	GinkgoHelper()

	families, err := metrics.Registry.Gather()
	Expect(err).NotTo(HaveOccurred())

	var total float64

	for _, f := range families {
		if f.GetName() != "talos_operator_rejected_rpcs_total" {
			continue
		}

		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}

	return total
}

// The spec cluster's rollout timings. The soak is long enough for a real kubelet to prove itself
// and short enough for a spec. The try window leaves room for a soak that starts over once or
// twice on a small CI runner; the spec that cuts a node off has to wait it out.
const (
	e2eSoak       = 30 * time.Second
	e2eTryTimeout = 4 * time.Minute
)

// Config documents the specs roll out.
const (
	rolloutLabel = "e2e.talos-operator/rollout"
	tokenPatch   = "machine:\n  token: aaaaaa.bbbbbbbbbbbbbbbb\n"
)

func labelDocument(value string) string {
	return "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  " + rolloutLabel + ": " + value + "\n"
}

// blackholeDocuments makes a node drop everything it would send to the given addresses. On a
// worker, with every control plane listed, that cuts it off: its Talos API cannot answer through
// any endpoint and its kubelet cannot reach the API server.
func blackholeDocuments(addrs []netip.Addr) string {
	docs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		docs = append(docs, "apiVersion: v1alpha1\nkind: BlackholeRouteConfig\nname: "+a.String()+"/32\n")
	}

	return strings.Join(docs, "---\n")
}

// The user's whole path on an existing cluster (docs/scope.md §7.1, §6.3): adopt
// it without changing a node, then own its config. The specs run in order on one TalosCluster and
// the operator holds only Talos-issued credentials throughout.
var _ = Describe("Adoption and config management", Ordered, func() {
	var (
		tc       *talosv1alpha1.TalosCluster
		pools    map[talosv1alpha1.MachineRole]*talosv1alpha1.TalosMachinePool
		machines []*talosv1alpha1.TalosMachine
		harness  talos.Reader
		nodes    []netip.Addr
	)

	conditionTrue := func(ctx context.Context, obj client.Object, conditions func() []metav1.Condition, condType string,
	) func(Gomega) {
		return func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())

			cond := meta.FindStatusCondition(conditions(), condType)
			g.Expect(cond).NotTo(BeNil(), "%s not reported yet", condType)
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue), "%s: %s", cond.Reason, cond.Message)
		}
	}

	clusterConditions := func() []metav1.Condition { return tc.Status.Conditions }

	// refresh re-reads the TalosCluster.
	refresh := func(ctx context.Context, g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
	}

	// updateCluster changes the TalosCluster, retrying when the operator wrote its status in between.
	updateCluster := func(ctx context.Context, mutate func(*talosv1alpha1.TalosCluster)) {
		GinkgoHelper()

		Eventually(func(g Gomega) {
			refresh(ctx, g)
			mutate(tc)
			g.Expect(k8sClient.Update(ctx, tc)).To(Succeed())
		}).WithContext(ctx).WithTimeout(30 * time.Second).Should(Succeed())
	}

	// poolConfig returns the config layer of a role's pool.
	poolConfig := func(ctx context.Context, role talosv1alpha1.MachineRole) []talosv1alpha1.ConfigSource {
		GinkgoHelper()

		pool := pools[role]
		Expect(pool).NotTo(BeNil(), "the proposal has no pool for role %s", role)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())

		return slices.Clone(pool.Spec.Config)
	}

	// setPoolConfig replaces the config layer of a role's pool, retrying when the operator wrote the
	// pool's status in between.
	setPoolConfig := func(ctx context.Context, role talosv1alpha1.MachineRole, config []talosv1alpha1.ConfigSource) {
		GinkgoHelper()

		pool := pools[role]

		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())

			pool.Spec.Config = config
			g.Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		}).WithContext(ctx).WithTimeout(30 * time.Second).Should(Succeed())
	}

	// rollouts returns how many operations have ended so far.
	rollouts := func(ctx context.Context) int {
		GinkgoHelper()

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())

		return len(tc.Status.History)
	}

	with := func(config []talosv1alpha1.ConfigSource, documents string) []talosv1alpha1.ConfigSource {
		return append(slices.Clone(config), talosv1alpha1.ConfigSource{Inline: documents})
	}

	// inSync waits until every node runs its rendered config and nothing is in flight or blocked.
	inSync := func(ctx context.Context, timeout time.Duration) {
		GinkgoHelper()

		Eventually(func(g Gomega) {
			conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionConfigInSync)(g)
			g.Expect(tc.Status.Operation).To(BeNil())
			g.Expect(meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionRolloutBlocked)).To(BeNil())
		}).WithContext(ctx).WithTimeout(timeout).Should(Succeed())
	}

	addrsOf := func(env string) []netip.Addr {
		fields := strings.Fields(os.Getenv(env))
		out := make([]netip.Addr, 0, len(fields))

		for _, a := range fields {
			out = append(out, netip.MustParseAddr(a))
		}

		return out
	}

	BeforeAll(func(ctx context.Context) {
		harness = harnessReader(ctx)
		nodes = harnessNodes()
		pools = map[talosv1alpha1.MachineRole]*talosv1alpha1.TalosMachinePool{}

		tc = &talosv1alpha1.TalosCluster{ObjectMeta: metav1.ObjectMeta{Name: "adopt", Namespace: testNamespace}}
		Expect(k8sClient.Create(ctx, tc)).To(Succeed())
	})

	AfterAll(func(ctx context.Context) {
		// Machines before their pools: a pool with members waits.
		for _, tm := range machines {
			deleteMachine(ctx, tm)
		}

		for _, pool := range pools {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pool))).To(Succeed())
		}

		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, tc))).To(Succeed())

		// Kept on purpose when a TalosCluster goes (no ownerRef), so the suite removes them.
		for _, name := range []string{"adopt-talos-secrets", "adopt-adoption-backup"} {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace}}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, secret))).To(Succeed())
		}
	})

	// Observe, review the proposal, commit it, and let Talos prove that it changes nothing.
	It("adopts the cluster from its own proposal without changing a node", func(ctx context.Context) {
		before := snapshot(ctx, harness, nodes)

		By("waiting for the operator's proposal")
		Eventually(conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionAdoptionProposed)).
			WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())

		proposal := &corev1.ConfigMap{}
		proposalKey := client.ObjectKey{Namespace: testNamespace, Name: tc.Status.Adoption.ProposalRef.Name}
		Expect(k8sClient.Get(ctx, proposalKey, proposal)).To(Succeed())

		By("committing the proposal as a user would")
		proposed := &talosv1alpha1.TalosCluster{}
		Expect(sigsyaml.Unmarshal([]byte(proposal.Data["taloscluster.yaml"]), proposed)).To(Succeed())

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec = proposed.Spec
			tc.Spec.Rollout.Soak = &metav1.Duration{Duration: e2eSoak}
			tc.Spec.Rollout.TryTimeout = &metav1.Duration{Duration: e2eTryTimeout}
		})

		By("checking that the API server refuses config: Manage before adoption is proven")
		gate := ContainSubstring("may become Manage only once status shows Adopted=True")

		// The operator writes status all the time, so an update may lose to it: only the admission
		// policy's own refusal counts, on the object as it is now.
		Eventually(func(g Gomega) {
			refresh(ctx, g)

			early := tc.DeepCopy()
			early.Spec.Management.Config = talosv1alpha1.ConfigManagementManage
			g.Expect(k8sClient.Update(ctx, early)).To(MatchError(gate))
		}).WithContext(ctx).WithTimeout(30 * time.Second).Should(Succeed())

		refresh(ctx, Default)
		Expect(tc.Spec.Management.Config).To(Equal(talosv1alpha1.ConfigManagementObserve), "a refused update changes nothing")

		for doc := range strings.SplitSeq(proposal.Data["talosmachinepools.yaml"], "\n---\n") {
			pool := &talosv1alpha1.TalosMachinePool{}
			Expect(sigsyaml.Unmarshal([]byte(doc), pool)).To(Succeed())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			pools[pool.Spec.Role] = pool
		}

		for doc := range strings.SplitSeq(proposal.Data["talosmachines.yaml"], "\n---\n") {
			tm := &talosv1alpha1.TalosMachine{}
			Expect(sigsyaml.Unmarshal([]byte(doc), tm)).To(Succeed())
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			machines = append(machines, tm)
		}

		Expect(machines).To(HaveLen(len(nodes)), "one proposed TalosMachine per node")

		By("waiting for every machine to match its live config")
		for _, tm := range machines {
			machineConditions := func() []metav1.Condition { return tm.Status.Conditions }
			// Evaluated on failure, so it shows the latest diff.
			lastDiff := func() string { return "TalosMachine " + tm.Name + " diff:\n" + diffOf(tm) }

			Eventually(conditionTrue(ctx, tm, machineConditions, talosv1alpha1.ConditionConfigMatchesLive)).
				WithContext(ctx).WithTimeout(2*time.Minute).Should(Succeed(), lastDiff)
		}

		By("waiting for Talos's own dry run to prove adoption")
		Eventually(conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionAdopted)).
			WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())
		Expect(tc.Status.Adoption.LiveConfigHashes).To(HaveLen(len(machines)))
		Expect(tc.Status.Adoption.ProvenAt).NotTo(BeNil())

		By("entering config: Manage, which must change nothing")
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage
		})

		Eventually(conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionConfigHandover)).
			WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())
		inSync(ctx, time.Minute)
		Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseManaged))

		By("checking that no node changed and nothing mutating was attempted")
		Expect(snapshot(ctx, harness, nodes)).To(Equal(before))
		Expect(rejectedRPCs()).To(BeZero())
	}, NodeTimeout(10*time.Minute))

	// A change of the spec reaches the nodes: applied in try mode, verified from outside, and
	// confirmed only after the node stayed healthy (docs/scope.md §8.5.1).
	It("rolls a label out to the workers without a reboot", func(ctx context.Context) {
		workers := addrsOf("E2E_WORKERS")
		if len(workers) == 0 {
			Skip("the harness cluster has no worker")
		}

		before := snapshot(ctx, harness, nodes)
		ended := rollouts(ctx)

		workerConfig := poolConfig(ctx, talosv1alpha1.MachineRoleWorker)
		setPoolConfig(ctx, talosv1alpha1.MachineRoleWorker, with(workerConfig, labelDocument("one")))

		By("waiting for the rollout to finish")
		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Operation).To(BeNil(), "still rolling out: %+v", tc.Status.Operation)
			g.Expect(tc.Status.History).To(HaveLen(ended + 1))
		}).WithContext(ctx).WithTimeout(time.Duration(len(workers)) * 5 * time.Minute).Should(Succeed())

		record := tc.Status.History[0]
		Expect(record.Outcome).To(Equal(talosv1alpha1.OperationSucceeded), record.Message)
		Expect(record.Completed).To(HaveLen(len(workers)))
		inSync(ctx, time.Minute)

		By("checking that the label reached Kubernetes, so the config really took effect")
		Eventually(func(g Gomega) {
			var labelled corev1.NodeList
			g.Expect(k8sClient.List(ctx, &labelled, client.MatchingLabels{rolloutLabel: "one"})).To(Succeed())
			g.Expect(labelled.Items).To(HaveLen(len(workers)))
		}).WithContext(ctx).WithTimeout(time.Minute).Should(Succeed())

		By("checking that only the workers changed, persistently, and nothing rebooted")
		after := snapshot(ctx, harness, nodes)

		for _, n := range nodes {
			Expect(after[n].BootID).To(Equal(before[n].BootID), "%s must not reboot", n)

			if slices.Contains(workers, n) {
				Expect(after[n].ConfigHash).NotTo(Equal(before[n].ConfigHash), "%s must run the new config", n)

				cfgs, err := harness.MachineConfig(ctx, n)
				Expect(err).NotTo(HaveOccurred())
				Expect(cfgs.Persistent).NotTo(BeEmpty(), "a confirmed config is persistent")
				Expect(cfgs.PendingConfigChange()).To(BeFalse(), "nothing is left tried or staged on %s", n)
			} else {
				Expect(after[n].ConfigHash).To(Equal(before[n].ConfigHash), "%s is not a worker and must not change", n)
			}
		}

		Expect(rejectedRPCs()).To(BeZero())
	}, NodeTimeout(15*time.Minute))

	It("refuses to change a protected field", func(ctx context.Context) {
		before := snapshot(ctx, harness, nodes)

		var previous []talosv1alpha1.ConfigSource

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			previous = slices.Clone(tc.Spec.Config)
			tc.Spec.Config = with(previous, tokenPatch)
		})

		Eventually(func(g Gomega) {
			refresh(ctx, g)

			blocked := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionRolloutBlocked)
			g.Expect(blocked).NotTo(BeNil())
			g.Expect(blocked.Reason).To(Equal(talosv1alpha1.ReasonProtectedChange), blocked.Message)
			g.Expect(blocked.Message).To(ContainSubstring("machine.token"))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		Consistently(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Operation).To(BeNil())
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(5 * time.Second).Should(Succeed())

		Expect(snapshot(ctx, harness, nodes)).To(Equal(before))

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Config = previous })
		inSync(ctx, 2*time.Minute)
		Expect(snapshot(ctx, harness, nodes)).To(Equal(before))
	}, NodeTimeout(6*time.Minute))

	// The M3 acceptance: a network change that cuts a node off is reverted by Talos without the
	// operator confirming it, the rollout halts, and no second node is touched.
	It("lets Talos revert a config that cuts a node off, and halts before the next node", func(ctx context.Context) {
		workers, controlPlanes := addrsOf("E2E_WORKERS"), addrsOf("E2E_CONTROLPLANES")
		if len(workers) == 0 {
			Skip("the harness cluster has no worker")
		}

		before := snapshot(ctx, harness, nodes)
		ended := rollouts(ctx)

		// One rollout, two changes: the workers lose their way to every control plane, and the
		// control planes get a harmless label. Workers go first, so the label must never arrive.
		// The pause makes the two edits one change, as a single commit would.
		workerConfig := poolConfig(ctx, talosv1alpha1.MachineRoleWorker)
		controlPlaneConfig := poolConfig(ctx, talosv1alpha1.MachineRoleControlPlane)

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		setPoolConfig(ctx, talosv1alpha1.MachineRoleWorker, with(workerConfig, blackholeDocuments(controlPlanes)))
		setPoolConfig(ctx, talosv1alpha1.MachineRoleControlPlane, with(controlPlaneConfig, labelDocument("never")))

		Eventually(func(g Gomega) {
			refresh(ctx, g)

			blocked := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionRolloutBlocked)
			g.Expect(blocked).NotTo(BeNil())
			g.Expect(blocked.Reason).To(Equal(talosv1alpha1.ReasonPaused), blocked.Message)

			for _, tm := range machines {
				g.Expect(blocked.Message).To(ContainSubstring(tm.Name), "every machine has a change waiting")
			}
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })

		By("waiting for the rollout to halt")
		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Operation).NotTo(BeNil(), "no rollout in flight")
			g.Expect(tc.Status.Operation.Failure).NotTo(BeNil(), "not halted yet: %+v", tc.Status.Operation.Current)
		}).WithContext(ctx).WithTimeout(e2eTryTimeout + 10*time.Minute).WithPolling(10 * time.Second).Should(Succeed())

		op := tc.Status.Operation
		Expect(op.Plan).To(HaveLen(len(nodes)), "workers and control planes are planned in one rollout")
		Expect(op.Completed).To(BeEmpty())
		Expect(op.Failure.Reason).To(Equal(plan.FailRolledBack), op.Failure.Message)
		Expect(op.Current.Class).To(Equal("Connectivity"))

		blocked := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionRolloutBlocked)
		Expect(blocked).NotTo(BeNil())
		Expect(blocked.Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))

		By("checking that every node runs what it ran before: the worker reverted, the rest untouched")
		Expect(snapshot(ctx, harness, nodes)).To(Equal(before))

		var labelled corev1.NodeList
		Expect(k8sClient.List(ctx, &labelled, client.MatchingLabels{rolloutLabel: "never"})).To(Succeed())
		Expect(labelled.Items).To(BeEmpty(), "no control plane was touched")

		By("aborting, and taking the change back")
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			if tc.Annotations == nil {
				tc.Annotations = map[string]string{}
			}

			tc.Annotations[talosv1alpha1.AnnotationAbort] = op.Failure.Token
		})

		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Operation).To(BeNil())
			g.Expect(tc.Status.History).To(HaveLen(ended + 1))
			g.Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		setPoolConfig(ctx, talosv1alpha1.MachineRoleWorker, workerConfig)
		setPoolConfig(ctx, talosv1alpha1.MachineRoleControlPlane, controlPlaneConfig)

		// The cluster is whole again once the worker's kubelet is back.
		inSync(ctx, 5*time.Minute)
		Expect(snapshot(ctx, harness, nodes)).To(Equal(before))
		Expect(rejectedRPCs()).To(BeZero())
	}, NodeTimeout(e2eTryTimeout+25*time.Minute))
})

// diffOf returns a machine's last reported config diff, for failure messages.
func diffOf(tm *talosv1alpha1.TalosMachine) string {
	if tm.Status.Config == nil {
		return "(none reported)"
	}

	return tm.Status.Config.Diff
}
