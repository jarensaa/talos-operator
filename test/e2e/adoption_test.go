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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/version"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/validate"
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

// rejectedRPCs is how many Talos calls the operator's own allowlist refused: mutations that a
// connection was not allowed to make. Nothing the operator does should ever get that far.
func rejectedRPCs(ctx context.Context) float64 {
	GinkgoHelper()

	return operatorCounter(ctx, "talos_operator_rejected_rpcs_total")
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
	// A flag no kubelet knows: it exits at once, every time Talos starts it.
	unknownKubeletFlag = "no-such-flag"
	brokenKubeletPatch = "apiVersion: v1alpha1\nkind: KubeletConfig\nextraArgs:\n  " + unknownKubeletFlag + ": \"true\"\n"
)

// A flag that kubelet 1.37 no longer has (go-kubernetes lists it among those removed with the
// deprecated cAdvisor flags), with its default value: 1.36 takes it, and it changes nothing.
const (
	removedKubeletFlag  = "global-housekeeping-interval"
	removedKubeletPatch = "apiVersion: v1alpha1\nkind: KubeletConfig\nextraArgs:\n  " + removedKubeletFlag + ": 1m0s\n"
)

// halted ends a wait for an operation that has halted: it stays halted until a human acts, so
// waiting for the timeout would only take the time, and the time of every scenario after it.
func halted(op *talosv1alpha1.Operation, what string) {
	if op != nil && op.Failure != nil {
		StopTrying(fmt.Sprintf("the %s halted: %+v", what, *op.Failure)).Now()
	}
}

// expectSucceeded fails the spec unless the operation that ended succeeded, with what the
// operation said of itself.
func expectSucceeded(record talosv1alpha1.OperationRecord) {
	GinkgoHelper()

	Expect(record.Outcome).To(Equal(talosv1alpha1.OperationSucceeded), record.Message)
}

func labelDocument(value string) string {
	return "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  " + rolloutLabel + ": " + value + "\n"
}

// withPatch returns a machine config with a strategic merge patch applied. The config is never
// printed: it holds the cluster's secrets.
func withPatch(raw []byte, patch string) []byte {
	GinkgoHelper()

	loaded, err := configpatcher.LoadPatch([]byte(patch))
	Expect(err).NotTo(HaveOccurred())

	out, err := configpatcher.Apply(configpatcher.WithBytes(raw), []configpatcher.Patch{loaded})
	Expect(err).NotTo(HaveOccurred())

	patched, err := out.Bytes()
	Expect(err).NotTo(HaveOccurred())

	return patched
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
		// untouched is every node's state before the TalosCluster existed.
		untouched map[netip.Addr]nodeState
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

	// lastEnded is the operation that ended last, or nothing. The history keeps only the most
	// recent ones, so its length does not say whether another has ended.
	lastEnded := func(ctx context.Context) string {
		GinkgoHelper()

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())

		if len(tc.Status.History) == 0 {
			return ""
		}

		return tc.Status.History[0].ID
	}

	// awaitEnd waits for an operation to end after the one named, and returns its record. An
	// operation that halts stays halted until a human acts, so a halt ends the wait: unless told
	// says that the spec is that human, and has just said how the operation is to end.
	awaitEnd := func(ctx context.Context, previous, what string, timeout time.Duration, told bool) talosv1alpha1.OperationRecord {
		GinkgoHelper()

		Eventually(func(g Gomega) {
			refresh(ctx, g)

			if !told {
				halted(tc.Status.Operation, what)
			}

			g.Expect(tc.Status.Operation).To(BeNil(), "still in flight: %+v", tc.Status.Operation)
			g.Expect(tc.Status.History).NotTo(BeEmpty())
			g.Expect(tc.Status.History[0].ID).NotTo(Equal(previous), "the %s has not run", what)
		}).WithContext(ctx).WithTimeout(timeout).WithPolling(5 * time.Second).Should(Succeed())

		return tc.Status.History[0]
	}

	// endedSince waits for an operation to end after the one named, and returns its record. One
	// that halts instead ends the wait.
	endedSince := func(ctx context.Context, previous, what string, timeout time.Duration) talosv1alpha1.OperationRecord {
		GinkgoHelper()

		return awaitEnd(ctx, previous, what, timeout, false)
	}

	// endedAsTold is endedSince for an operation that has halted, and that the spec has just told
	// by annotation how to end: that it is halted is what was to be expected, and ends no wait.
	endedAsTold := func(ctx context.Context, previous, what string, timeout time.Duration) talosv1alpha1.OperationRecord {
		GinkgoHelper()

		return awaitEnd(ctx, previous, what, timeout, true)
	}

	isControlPlane := func(tm *talosv1alpha1.TalosMachine) bool {
		return tm.Spec.PoolRef.Name == pools[talosv1alpha1.MachineRoleControlPlane].Name
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

	// workersOrSkip returns the harness cluster's workers, and skips the spec on a cluster that has none.
	workersOrSkip := func() []netip.Addr {
		workers := addrsOf("E2E_WORKERS")
		if len(workers) == 0 {
			Skip("the harness cluster has no worker")
		}

		return workers
	}

	// labelled lists the Kubernetes Nodes that carry a label with that value.
	labelled := func(ctx context.Context, g Gomega, label, value string) []corev1.Node {
		var list corev1.NodeList
		g.Expect(k8sClient.List(ctx, &list, client.MatchingLabels{label: value})).To(Succeed())

		return list.Items
	}

	// expectLabelled waits until that many Nodes carry the rollout's label with that value: the
	// label reached Kubernetes, so the config that sets it really took effect.
	expectLabelled := func(ctx context.Context, value string, count int) {
		GinkgoHelper()

		Eventually(func(g Gomega) {
			g.Expect(labelled(ctx, g, rolloutLabel, value)).To(HaveLen(count))
		}).WithContext(ctx).WithTimeout(time.Minute).Should(Succeed())
	}

	// expectNoReboot compares two snapshots of the nodes: none may have booted in between.
	expectNoReboot := func(before, after map[netip.Addr]nodeState) {
		GinkgoHelper()

		for _, n := range nodes {
			Expect(after[n].BootID).To(Equal(before[n].BootID), "%s must not reboot", n)
		}
	}

	BeforeAll(func(ctx context.Context) {
		harness = harnessReader(ctx)
		nodes = harnessNodes()
		pools = map[talosv1alpha1.MachineRole]*talosv1alpha1.TalosMachinePool{}
		untouched = snapshot(ctx, harness, nodes)

		// All a user does to have a cluster adopted: a TalosCluster with nothing in it.
		tc = &talosv1alpha1.TalosCluster{ObjectMeta: metav1.ObjectMeta{Name: "adopt", Namespace: testNamespace}}
		Expect(k8sClient.Create(ctx, tc)).To(Succeed())
	})

	AfterAll(func(ctx context.Context) {
		// The TalosCluster first, so that nothing is written again; then machines before their
		// pools, because a pool with members waits. None of them is owned by the TalosCluster.
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, tc))).To(Succeed())

		ofCluster := []client.ListOption{client.InNamespace(testNamespace), client.MatchingLabels{talosv1alpha1.LabelCluster: tc.Name}}

		var written talosv1alpha1.TalosMachineList
		Expect(k8sClient.List(ctx, &written, ofCluster...)).To(Succeed())

		for i := range written.Items {
			deleteMachine(ctx, &written.Items[i])
		}

		var writtenPools talosv1alpha1.TalosMachinePoolList
		Expect(k8sClient.List(ctx, &writtenPools, ofCluster...)).To(Succeed())

		for i := range writtenPools.Items {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &writtenPools.Items[i]))).To(Succeed())
		}

		// Kept on purpose when a TalosCluster goes (no ownerRef), so the suite removes them.
		for _, name := range []string{"adopt-talos-secrets", "adopt-adoption-backup"} {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace}}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, secret))).To(Succeed())
		}
	})

	// No credentials are supplied: the operator requests its certificate from Talos, which the
	// harness allows for this namespace (hack/e2e/patches/controlplane.patch.yaml).
	It("reports the live cluster's state in TalosCluster status", func(ctx context.Context) {
		for _, c := range []string{
			talosv1alpha1.ConditionCredentialsValid, talosv1alpha1.ConditionTalosAPIReachable,
			talosv1alpha1.ConditionAdoptionPreconditionsMet, talosv1alpha1.ConditionHealthy,
		} {
			Eventually(conditionTrue(ctx, tc, clusterConditions, c)).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())
		}

		Expect(tc.Status.Observed.EtcdMembers).NotTo(BeEmpty())
		Expect(tc.Status.Observed.KubernetesVersion).To(HavePrefix("1."))

		var observed int32
		for tag, n := range tc.Status.Observed.TalosVersions {
			Expect(tag).To(HavePrefix("v1.14."))
			observed += n
		}

		Expect(int(observed)).To(Equal(len(nodes)), "every node's Talos version is observed")
	}, NodeTimeout(5*time.Minute))

	// The whole of adoption, with nothing applied by hand (docs/scope.md §7.1, §12.14): the
	// operator writes the resources that describe the cluster, Talos proves that they change
	// nothing, and from then on they are the user's.
	It("adopts the cluster by writing the resources that describe it, without changing a node", func(ctx context.Context) {
		By("waiting for the operator to write the resources and for Talos's dry run to prove them")
		Eventually(func(g Gomega) {
			conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionConfigProven)(g)
			g.Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopted))
		}).WithContext(ctx).WithTimeout(5 * time.Minute).Should(Succeed())

		Expect(tc.Status.Adoption.AdoptedAt).NotTo(BeNil())
		Expect(tc.Status.Adoption.LiveConfigHashes).To(HaveLen(len(nodes)))
		Expect(tc.Spec.ClusterName).NotTo(BeEmpty(), "what the spec left empty is filled in from the cluster")
		Expect(tc.Spec.Versions).NotTo(BeNil())
		Expect(tc.Spec.ConfigGeneration).NotTo(BeNil())
		Expect(tc.Spec.Management.Config).To(Equal(talosv1alpha1.ConfigManagementObserve), "adoption never switches management on")

		By("collecting what it wrote: a pool per role and a machine per node")
		ofCluster := []client.ListOption{client.InNamespace(testNamespace), client.MatchingLabels{talosv1alpha1.LabelCluster: tc.Name}}

		var writtenPools talosv1alpha1.TalosMachinePoolList
		Expect(k8sClient.List(ctx, &writtenPools, ofCluster...)).To(Succeed())

		for i := range writtenPools.Items {
			pools[writtenPools.Items[i].Spec.Role] = &writtenPools.Items[i]
		}

		Expect(pools).To(HaveKey(talosv1alpha1.MachineRoleControlPlane))

		var written talosv1alpha1.TalosMachineList
		Expect(k8sClient.List(ctx, &written, ofCluster...)).To(Succeed())
		Expect(written.Items).To(HaveLen(len(nodes)), "one TalosMachine per node")

		for i := range written.Items {
			tm := &written.Items[i]
			machines = append(machines, tm)

			By("checking that " + tm.Name + " is pinned to the hardware at its address, and matches it")
			si, err := harness.SystemInformation(ctx, netip.MustParseAddr(tm.Spec.Address))
			Expect(err).NotTo(HaveOccurred())
			Expect(tm.Spec.Identity.SystemUUID).To(Equal(si.UUID))

			machineConditions := func() []metav1.Condition { return tm.Status.Conditions }
			// Evaluated on failure, so it shows the latest diff.
			lastDiff := func() string { return "TalosMachine " + tm.Name + " diff:\n" + diffOf(tm) }

			Eventually(conditionTrue(ctx, tm, machineConditions, talosv1alpha1.ConditionMatched)).WithContext(ctx).Should(Succeed())
			Eventually(conditionTrue(ctx, tm, machineConditions, talosv1alpha1.ConditionConfigMatchesLive)).
				WithContext(ctx).WithTimeout(2*time.Minute).Should(Succeed(), lastDiff)
			Expect(tm.Status.Phase).To(Equal(talosv1alpha1.MachinePhase("Ready")))
			Expect(tm.Status.Config.LiveHash).To(HavePrefix("sha256:"))
		}

		By("checking that the operator let go of them: it owns nothing of their specs any more")
		ownsSpec := func(obj client.Object) bool {
			return slices.ContainsFunc(obj.GetManagedFields(), func(e metav1.ManagedFieldsEntry) bool {
				return e.Manager == "talos-operator-adoption" && e.FieldsV1 != nil && strings.Contains(e.FieldsV1.GetRawString(), `"f:spec"`)
			})
		}

		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(ownsSpec(tc)).To(BeFalse(), "TalosCluster")

			for _, tm := range machines {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), tm)).To(Succeed())
				g.Expect(ownsSpec(tm)).To(BeFalse(), "TalosMachine %s", tm.Name)
			}

			for _, pool := range pools {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())
				g.Expect(ownsSpec(pool)).To(BeFalse(), "TalosMachinePool %s", pool.Name)
			}
		}).WithContext(ctx).WithTimeout(time.Minute).Should(Succeed())

		By("editing a pool before management is on: the edit is caught, and the API server refuses config: Manage")
		controlPlaneConfig := poolConfig(ctx, talosv1alpha1.MachineRoleControlPlane)
		setPoolConfig(ctx, talosv1alpha1.MachineRoleControlPlane, with(controlPlaneConfig, labelDocument("early")))

		Eventually(func(g Gomega) {
			refresh(ctx, g)

			proven := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionConfigProven)
			g.Expect(proven).NotTo(BeNil())
			g.Expect(proven.Status).To(Equal(metav1.ConditionFalse), proven.Message)
			g.Expect(proven.Reason).To(Equal(talosv1alpha1.ReasonProofInvalidated))
			g.Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopted), "an adopted cluster is never adopting again")
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		gate := ContainSubstring("may become Manage only once status shows ConfigProven=True")

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

		setPoolConfig(ctx, talosv1alpha1.MachineRoleControlPlane, controlPlaneConfig)
		Eventually(conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionConfigProven)).
			WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		By("entering config: Manage, which must change nothing")
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Rollout.Soak = &metav1.Duration{Duration: e2eSoak}
			tc.Spec.Rollout.TryTimeout = &metav1.Duration{Duration: e2eTryTimeout}
			tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage
		})

		Eventually(conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionConfigHandover)).
			WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())
		inSync(ctx, time.Minute)
		Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseManaged))

		By("checking that no node changed and nothing mutating was attempted")
		Expect(snapshot(ctx, harness, nodes)).To(Equal(untouched))
		Expect(rejectedRPCs(ctx)).To(BeZero())
	}, NodeTimeout(15*time.Minute))

	// What only the Talos API says about a node (docs/scope.md §14.2), read from real nodes: the
	// alerts on it assume that a node in order has every service running and healthy, its clock
	// synchronised and its volumes ready, and this is where that is seen to be so.
	It("reports what Talos says about every node, and how long the cluster's certificates last", func(ctx context.Context) {
		Eventually(func(g Gomega) {
			leaders := 0

			for _, tm := range machines {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), tm)).To(Succeed())

				health := tm.Status.Health
				g.Expect(health).NotTo(BeNil(), tm.Name)
				g.Expect(health.Services).NotTo(BeEmpty(), tm.Name)

				for _, service := range health.Services {
					g.Expect(service.Running && service.Healthy).To(BeTrue(), "%s: service %s", tm.Name, service.Name)
				}

				g.Expect(health.TimeSynced).To(HaveValue(BeTrue()), tm.Name)
				g.Expect(health.Links).To(ContainElement(HaveField("Up", BeTrue())), tm.Name)

				for _, volume := range []string{"STATE", "EPHEMERAL"} {
					g.Expect(health.Volumes).To(ContainElement(And(HaveField("Name", volume), HaveField("Phase", talos.VolumeReady))),
						"%s: volume %s", tm.Name, volume)
				}

				if !isControlPlane(tm) {
					g.Expect(tm.Status.Etcd).To(BeNil(), tm.Name)

					continue
				}

				g.Expect(tm.Status.Etcd).NotTo(BeNil(), tm.Name)
				g.Expect(tm.Status.Etcd.DBSize).To(BeNumerically(">", 0), tm.Name)
				g.Expect(tm.Status.Etcd.DBSizeInUse).To(And(BeNumerically(">", 0), BeNumerically("<=", tm.Status.Etcd.DBSize)), tm.Name)

				if tm.Status.Etcd.Leader {
					leaders++
				}
			}

			g.Expect(leaders).To(Equal(1), "exactly one control plane leads etcd")
		}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("reading the validity of the operator's certificate and of the cluster's authorities")
		Eventually(func(g Gomega) {
			refresh(ctx, g)

			validity := map[string]time.Duration{}

			for _, cert := range tc.Status.Certificates {
				g.Expect(cert.ExpiresAt.After(time.Now())).To(BeTrue(), cert.Name)
				validity[cert.Name] = cert.ExpiresAt.Sub(cert.IssuedAt.Time)
			}

			g.Expect(validity).To(HaveLen(5))

			// Talos issues the operator's certificate for hours, and an authority for years: one
			// alert covers both by what is left of each (charts/talos-operator/files/alerts.yaml).
			g.Expect(validity["operator"]).To(And(BeNumerically(">", 0), BeNumerically("<=", 24*time.Hour)))

			for _, authority := range []string{"os", "kubernetes", "kubernetes-aggregator", "etcd"} {
				g.Expect(validity[authority]).To(BeNumerically(">", 365*24*time.Hour), authority)
			}

			g.Expect(meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionEtcdMaintained)).NotTo(BeNil())
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		By("finding it in the metrics of every replica")
		Eventually(func(g Gomega) {
			for _, tm := range machines {
				g.Expect(operatorGauge(ctx, g, "talos_operator_machine_service_healthy", "taloscluster", tc.Name, "machine", tm.Name,
					"service", "kubelet")).To(Equal([]float64{1, 1}), tm.Name)
				g.Expect(operatorGauge(ctx, g, "talos_operator_machine_time_synced", "taloscluster", tc.Name, "machine", tm.Name)).
					To(Equal([]float64{1, 1}), tm.Name)
				g.Expect(operatorGauge(ctx, g, "talos_operator_machine_volume_ready", "machine", tm.Name, "volume", "STATE",
					"phase", talos.VolumeReady)).To(Equal([]float64{1, 1}), tm.Name)

				if isControlPlane(tm) {
					g.Expect(operatorGauge(ctx, g, "talos_operator_machine_etcd_db_size_bytes", "machine", tm.Name)).
						To(HaveExactElements(BeNumerically(">", 0), BeNumerically(">", 0)), tm.Name)
				}
			}

			g.Expect(operatorGauge(ctx, g, "talos_operator_cluster_certificate_expiry_timestamp_seconds", "taloscluster", tc.Name,
				"certificate", "operator")).To(HaveExactElements(
				BeNumerically(">", float64(time.Now().Unix())), BeNumerically(">", float64(time.Now().Unix()))))
		}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	}, NodeTimeout(10*time.Minute))

	// A Talos upgrade (docs/scope.md §7.3): every node gets the new system installed, is drained
	// and rebooted, and comes back on it, one at a time. The operator runs on these very nodes, one
	// replica on each, so it upgrades the ground it stands on: whichever replica leads loses its
	// node at some point, and the other finishes what it began.
	// Version awareness (docs/scope.md §7.5), against what really exists upstream: the operator
	// asks from inside the cluster, and the check a repository would run asks from here.
	It("says which versions exist, and passes the check a repository would run on its resources", func(ctx context.Context) {
		By("reading what the operator found out upstream")
		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Versions).NotTo(BeNil())
			g.Expect(tc.Status.Versions.Talos).To(Equal(os.Getenv("E2E_TALOS_VERSION")))
			g.Expect(tc.Status.Versions.Kubernetes).To(Equal(os.Getenv("E2E_KUBERNETES_VERSION")))
			g.Expect(tc.Status.Versions.Available).NotTo(BeNil())
			g.Expect(tc.Status.Versions.Available.Problem).To(BeEmpty())
			g.Expect(tc.Status.Versions.Available.Talos).NotTo(BeEmpty())
			g.Expect(tc.Status.Versions.Available.Kubernetes).NotTo(BeEmpty())
		}).WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())

		// The newest release of the minor the cluster runs is at least the one it is upgraded to.
		if target := os.Getenv("E2E_UPGRADE_TO"); target != "" {
			newest := tc.Status.Versions.Available.Talos[0]
			listed := version.MustParseSemantic(newest).AtLeast(version.MustParseSemantic(target))
			Expect(listed).To(BeTrue(), "%s is listed, %s exists", newest, target)
		}

		conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionVersionsReachable)(Default)
		Expect(tc.Status.Versions.Plan).To(BeEmpty(), "the cluster runs what its spec asks for")

		By("exporting the resources, as someone would who puts them in Git")
		export := func(dir string, mutate func(*talosv1alpha1.TalosCluster)) {
			GinkgoHelper()

			documents := make([][]byte, 0, 1+len(nodes))

			cluster := &talosv1alpha1.TalosCluster{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), cluster)).To(Succeed())
			cluster.TypeMeta = metav1.TypeMeta{APIVersion: talosv1alpha1.GroupVersion.String(), Kind: "TalosCluster"}

			if mutate != nil {
				mutate(cluster)
			}

			raw, err := yaml.Marshal(cluster)
			Expect(err).NotTo(HaveOccurred())

			documents = append(documents, raw)

			var poolList talosv1alpha1.TalosMachinePoolList
			Expect(k8sClient.List(ctx, &poolList, client.InNamespace(testNamespace))).To(Succeed())

			for i := range poolList.Items {
				poolList.Items[i].TypeMeta = metav1.TypeMeta{APIVersion: talosv1alpha1.GroupVersion.String(), Kind: "TalosMachinePool"}

				raw, err := yaml.Marshal(&poolList.Items[i])
				Expect(err).NotTo(HaveOccurred())

				documents = append(documents, raw)
			}

			var machineList talosv1alpha1.TalosMachineList
			Expect(k8sClient.List(ctx, &machineList, client.InNamespace(testNamespace))).To(Succeed())

			for i := range machineList.Items {
				machineList.Items[i].TypeMeta = metav1.TypeMeta{APIVersion: talosv1alpha1.GroupVersion.String(), Kind: "TalosMachine"}

				raw, err := yaml.Marshal(&machineList.Items[i])
				Expect(err).NotTo(HaveOccurred())

				documents = append(documents, raw)
			}

			var configList talosv1alpha1.TalosMachineConfigList
			Expect(k8sClient.List(ctx, &configList, client.InNamespace(testNamespace))).To(Succeed())

			for i := range configList.Items {
				configList.Items[i].TypeMeta = metav1.TypeMeta{APIVersion: talosv1alpha1.GroupVersion.String(), Kind: "TalosMachineConfig"}

				raw, err := yaml.Marshal(&configList.Items[i])
				Expect(err).NotTo(HaveOccurred())

				documents = append(documents, raw)
			}

			Expect(os.WriteFile(filepath.Join(dir, "cluster.yaml"), bytes.Join(documents, []byte("---\n")), 0o600)).To(Succeed())
		}

		check := func(dir string) (problems []string) {
			GinkgoHelper()

			findings, err := validate.Run(ctx, validate.Options{Paths: []string{dir}, Source: &catalog.HTTP{}})
			Expect(err).NotTo(HaveOccurred())

			for _, f := range findings {
				if !f.Skipped {
					problems = append(problems, f.String())
				}
			}

			return problems
		}

		asAdopted := GinkgoT().TempDir()
		export(asAdopted, nil)
		Expect(check(asAdopted)).To(BeEmpty(), "what adoption wrote names versions that go together, an installer that exists, and renders")

		By("bumping to an installer that does not exist, as a mistaken pull request would")
		mistaken := GinkgoT().TempDir()
		export(mistaken, func(cluster *talosv1alpha1.TalosCluster) {
			cluster.Spec.ConfigGeneration.Installer.SchematicID = strings.Repeat("0", 64)
		})
		Expect(check(mistaken)).To(ContainElement(ContainSubstring("Image Factory has no schematic")))
	}, NodeTimeout(10*time.Minute))

	It("upgrades Talos on every node, the ones the operator itself runs on included, with a workload kept up", func(ctx context.Context) {
		target := os.Getenv("E2E_UPGRADE_TO")
		if target == "" || target == os.Getenv("E2E_TALOS_VERSION") {
			Skip("the harness cluster already runs the version to upgrade to")
		}

		before := snapshot(ctx, harness, nodes)

		schematics := map[netip.Addr]string{}
		for _, n := range nodes {
			schematic, err := harness.ImageFactorySchematic(ctx, n)
			Expect(err).NotTo(HaveOccurred())

			schematics[n] = schematic.ID
		}

		By("running a workload that has to stay up: one replica per node, and a budget that lets one go at a time")
		const name = "stays-up"

		labels := map[string]string{"app": name}
		workload := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: appsv1.DeploymentSpec{
				Replicas: new(int32(len(nodes))),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						// The image every Talos node has: the kubelet's own pause container.
						Containers:  []corev1.Container{{Name: "pause", Image: "registry.k8s.io/pause:3.10"}},
						Tolerations: []corev1.Toleration{{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists}},
						Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
								TopologyKey:   "kubernetes.io/hostname",
								LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
							}},
						}},
					},
				},
			},
		}
		budget := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: new(intstr.FromInt32(1)),
				Selector:       &metav1.LabelSelector{MatchLabels: labels},
			},
		}

		Expect(k8sClient.Create(ctx, workload)).To(Succeed())
		Expect(k8sClient.Create(ctx, budget)).To(Succeed())
		DeferCleanup(func(ctx context.Context) {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, budget))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, workload))).To(Succeed())
		})

		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(workload), workload)).To(Succeed())
			g.Expect(workload.Status.AvailableReplicas).To(BeEquivalentTo(len(nodes)))
		}).WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())

		By("asking for the next Talos release, outside the cluster's maintenance window")
		opens := time.Now().UTC().Add(12 * time.Hour)
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Versions.Talos = target
			tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
			tc.Spec.Rollout.MaintenanceWindows = []talosv1alpha1.MaintenanceWindow{{
				Schedule: fmt.Sprintf("%d %d * * *", opens.Minute(), opens.Hour()), Duration: metav1.Duration{Duration: time.Hour},
			}}
		})

		// The bump is planned, and waits: a version change is held to the window like any other.
		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Versions).NotTo(BeNil())
			g.Expect(tc.Status.Versions.Plan).To(Equal([]talosv1alpha1.VersionStep{
				{Kind: plan.StepTalos, From: os.Getenv("E2E_TALOS_VERSION"), To: target},
			}))

			upToDate := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionTalosUpToDate)
			g.Expect(upToDate).NotTo(BeNil())
			g.Expect(upToDate.Reason).To(Equal(talosv1alpha1.ReasonOutsideWindow), upToDate.Message)
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		Consistently(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Operation).To(BeNil())
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(5 * time.Second).Should(Succeed())
		Expect(snapshot(ctx, harness, nodes)).To(Equal(before), "nothing is installed, pulled or drained outside the window")

		By("opening the window")
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.MaintenanceWindows = nil })

		// upgraded finds the record of the upgrade once it has ended.
		upgraded := func() *talosv1alpha1.OperationRecord {
			for i := range tc.Status.History {
				if tc.Status.History[i].Type == talosv1alpha1.OperationTalosUpgrade {
					return &tc.Status.History[i]
				}
			}

			return nil
		}

		By("waiting for the upgrade to go through every node")
		// With a single control plane the Kubernetes API is down while that node reboots, and the
		// test cannot see anything then. Whenever it can, the workload must be up.
		lowest := int32(len(nodes))

		Eventually(func(g Gomega) {
			refresh(ctx, g)

			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(workload), workload); err == nil {
				lowest = min(lowest, workload.Status.AvailableReplicas)
			}

			if op := tc.Status.Operation; op != nil {
				halted(op, "upgrade")
			}

			g.Expect(upgraded()).NotTo(BeNil(), "still upgrading: %+v", tc.Status.Operation)
		}).WithContext(ctx).WithTimeout(time.Duration(len(nodes)) * 15 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		record := upgraded()
		expectSucceeded(*record)
		Expect(record.Completed).To(HaveLen(len(nodes)))
		Expect(lowest).To(BeNumerically(">=", int32(len(nodes))-1), "the budget lets one replica go at a time, never more")

		By("checking every node: rebooted once into the new release, from the installer it had, and schedulable again")
		for _, n := range nodes {
			version, err := harness.Version(ctx, n)
			Expect(err).NotTo(HaveOccurred())
			Expect(version.Tag).To(Equal(target), "%s", n)

			schematic, err := harness.ImageFactorySchematic(ctx, n)
			Expect(err).NotTo(HaveOccurred())
			Expect(schematic.ID).To(Equal(schematics[n]), "%s keeps its schematic", n)

			boot, err := harness.BootID(ctx, n)
			Expect(err).NotTo(HaveOccurred())
			Expect(boot).NotTo(Equal(before[n].BootID), "%s must have rebooted", n)
		}

		var kubeNodes corev1.NodeList
		Expect(k8sClient.List(ctx, &kubeNodes)).To(Succeed())

		for _, n := range kubeNodes.Items {
			Expect(n.Spec.Unschedulable).To(BeFalse(), "%s was uncordoned", n.Name)
		}

		By("waiting for the config to follow: it names the installer each node now runs")
		inSync(ctx, time.Duration(len(nodes))*6*time.Minute)
		Expect(tc.Spec.Versions.Talos).To(Equal(target))
		Eventually(conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionTalosUpToDate)).
			WithContext(ctx).WithTimeout(time.Minute).Should(Succeed())

		By("checking that the operator is whole again: both replicas, and nothing it was not allowed to do")
		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: operatorName}, deployment)).To(Succeed())
			g.Expect(deployment.Status.ReadyReplicas).To(Equal(*deployment.Spec.Replicas))
		}).WithContext(ctx).WithTimeout(5 * time.Minute).Should(Succeed())

		Expect(rejectedRPCs(ctx)).To(BeZero())
	}, NodeTimeout(100*time.Minute))

	// A change of the spec reaches the nodes: applied in try mode, verified from outside, and
	// confirmed only after the node stayed healthy (docs/scope.md §8.5.1).
	It("rolls a label out to the workers without a reboot", func(ctx context.Context) {
		workers := workersOrSkip()

		before := snapshot(ctx, harness, nodes)
		previous := lastEnded(ctx)

		workerConfig := poolConfig(ctx, talosv1alpha1.MachineRoleWorker)
		setPoolConfig(ctx, talosv1alpha1.MachineRoleWorker, with(workerConfig, labelDocument("one")))

		By("waiting for the rollout to finish")
		record := endedSince(ctx, previous, "rollout", time.Duration(len(workers))*5*time.Minute)
		expectSucceeded(record)
		Expect(record.Completed).To(HaveLen(len(workers)))
		inSync(ctx, time.Minute)

		By("checking that the label reached Kubernetes, so the config really took effect")
		expectLabelled(ctx, "one", len(workers))

		By("checking that only the workers changed, persistently, and nothing rebooted")
		after := snapshot(ctx, harness, nodes)
		expectNoReboot(before, after)

		for _, n := range nodes {
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

		Expect(rejectedRPCs(ctx)).To(BeZero())
	}, NodeTimeout(15*time.Minute))

	// Two replicas, and every step persisted before its side effect: when the leader dies in the
	// middle of a node's try, the other replica takes the rollout over from what is in status and
	// on the node, and finishes it (docs/scope.md §4.2, §8.5.1).
	It("finishes a rollout when the operator's leader is killed in the middle of it", func(ctx context.Context) {
		workers := workersOrSkip()

		before := snapshot(ctx, harness, nodes)
		previous := lastEnded(ctx)

		setPoolConfig(ctx, talosv1alpha1.MachineRoleWorker,
			with(poolConfig(ctx, talosv1alpha1.MachineRoleWorker), labelDocument("two")))

		By("waiting until a node's config is tried and being verified")
		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Operation).NotTo(BeNil(), "no rollout in flight")
			g.Expect(tc.Status.Operation.Current).NotTo(BeNil())
			g.Expect(tc.Status.Operation.Current.Step).To(Equal(plan.StepVerifying))
		}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(time.Second).Should(Succeed())

		By("killing the leader: no grace, so it cannot hand its lease over")
		leader := operatorLeader(ctx, Default)
		Expect(leader).NotTo(BeNil(), "the Lease is held by none of the operator's pods")
		Expect(k8sClient.Delete(ctx, leader, client.GracePeriodSeconds(0))).To(Succeed())

		By("waiting for another replica to lead")
		Eventually(func(g Gomega) {
			successor := operatorLeader(ctx, g)
			g.Expect(successor).NotTo(BeNil())
			g.Expect(successor.Name).NotTo(Equal(leader.Name))
		}).WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())

		By("waiting for the rollout to finish under the new leader")
		record := endedSince(ctx, previous, "rollout", time.Duration(len(workers))*6*time.Minute)
		expectSucceeded(record)
		Expect(record.Completed).To(HaveLen(len(workers)))
		inSync(ctx, time.Minute)

		By("checking that the change arrived everywhere, confirmed, and nothing rebooted")
		expectLabelled(ctx, "two", len(workers))
		expectNoReboot(before, snapshot(ctx, harness, nodes))

		for _, n := range nodes {
			if slices.Contains(workers, n) {
				cfgs, err := harness.MachineConfig(ctx, n)
				Expect(err).NotTo(HaveOccurred())
				Expect(cfgs.PendingConfigChange()).To(BeFalse(), "nothing is left tried on %s", n)
			}
		}

		Expect(rejectedRPCs(ctx)).To(BeZero())
	}, NodeTimeout(20*time.Minute))

	// A rollout may touch a node that is unhealthy, as long as it is the only one: that is how a
	// config that broke a node is taken back (docs/scope.md §6.3, §8.3). Here the admin breaks a
	// worker's kubelet behind the operator's back, and enforcing the spec repairs the node.
	It("repairs a worker whose kubelet a config broke, while the rest of the cluster is healthy", func(ctx context.Context) {
		workers := workersOrSkip()

		victim := workers[0]
		before := snapshot(ctx, harness, nodes)
		previous := lastEnded(ctx)

		// ready reports what Kubernetes says about the victim's Node.
		ready := func(g Gomega) corev1.ConditionStatus {
			var all corev1.NodeList
			g.Expect(k8sClient.List(ctx, &all)).To(Succeed())

			for _, n := range all.Items {
				if !slices.ContainsFunc(n.Status.Addresses, func(a corev1.NodeAddress) bool { return a.Address == victim.String() }) {
					continue
				}

				for _, c := range n.Status.Conditions {
					if c.Type == corev1.NodeReady {
						return c.Status
					}
				}
			}

			g.Expect(false).To(BeTrue(), "Kubernetes has no Node with address %s", victim)

			return ""
		}

		By("breaking the worker's kubelet with a config applied outside the operator")
		cfgs, err := harness.MachineConfig(ctx, victim)
		Expect(err).NotTo(HaveOccurred())
		Expect(harnessWriter(ctx).ApplyConfig(ctx, victim, withPatch(cfgs.Active, brokenKubeletPatch), talos.ApplyNoReboot, 0)).
			To(Succeed())

		By("waiting until Talos and Kubernetes both call the node unhealthy, and the operator only reports it")
		Eventually(func(g Gomega) {
			g.Expect(ready(g)).NotTo(Equal(corev1.ConditionTrue), "Kubernetes still calls the node Ready")

			status, err := harness.MachineStatus(ctx, victim)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(status.Ready).To(BeFalse(), "Talos still calls the node ready")

			refresh(ctx, g)

			healthy := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionHealthy)
			g.Expect(healthy).NotTo(BeNil())
			g.Expect(healthy.Status).To(Equal(metav1.ConditionFalse), "Talos's cluster-wide checks still pass")

			drift := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionConfigInSync)
			g.Expect(drift).NotTo(BeNil())
			g.Expect(drift.Reason).To(Equal(talosv1alpha1.ReasonDrift), drift.Message)
			g.Expect(tc.Status.Operation).To(BeNil(), "driftPolicy Report leaves the node alone")
		}).WithContext(ctx).WithTimeout(5 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		for _, n := range nodes {
			if n != victim {
				status, err := harness.MachineStatus(ctx, n)
				Expect(err).NotTo(HaveOccurred())
				Expect(status.Ready).To(BeTrue(), "%s is not the node that was broken and must still be ready", n)
			}
		}

		By("enforcing the spec: the unhealthy node is the one node the operator may touch")
		var policy talosv1alpha1.DriftPolicy

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			policy = tc.Spec.Rollout.DriftPolicy
			tc.Spec.Rollout.DriftPolicy = talosv1alpha1.DriftPolicyEnforce
		})

		record := endedSince(ctx, previous, "rollout that repairs the node", e2eTryTimeout+5*time.Minute)
		expectSucceeded(record)
		Expect(record.Completed).To(HaveLen(1), "only the broken node needed a change")
		inSync(ctx, 2*time.Minute)

		By("checking that the node is whole again, without a reboot, and nothing else changed")
		Eventually(func(g Gomega) {
			g.Expect(ready(g)).To(Equal(corev1.ConditionTrue))
			conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionHealthy)(g)
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		repaired, err := harness.MachineConfig(ctx, victim)
		Expect(err).NotTo(HaveOccurred())
		Expect(bytes.Contains(repaired.Active, []byte(unknownKubeletFlag))).To(BeFalse(), "the broken kubelet flag is gone")
		Expect(repaired.PendingConfigChange()).To(BeFalse(), "the repair is confirmed, not left tried")

		after := snapshot(ctx, harness, nodes)
		expectNoReboot(before, after)

		for _, n := range nodes {
			if n != victim {
				Expect(after[n].ConfigHash).To(Equal(before[n].ConfigHash), "%s was healthy and must not change", n)
			}
		}

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.DriftPolicy = policy })
		inSync(ctx, time.Minute)
		Expect(rejectedRPCs(ctx)).To(BeZero())
	}, NodeTimeout(e2eTryTimeout+15*time.Minute))

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
		workersOrSkip()

		controlPlanes := addrsOf("E2E_CONTROLPLANES")
		before := snapshot(ctx, harness, nodes)
		previous := lastEnded(ctx)

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

		// What the chart's TalosOperationHalted alert is a rule over (charts/talos-operator/files/
		// alerts.yaml, tested on these series by `make alerts-test`): every replica says so, with
		// the labels the alert carries to whoever it reaches.
		By("finding the halt in the metrics of every replica")
		Eventually(func(g Gomega) {
			g.Expect(operatorGauge(ctx, g, "talos_operator_operation_failure_info",
				"namespace", tc.Namespace, "taloscluster", tc.Name, "type", string(op.Type),
				"machine", op.Failure.Machine, "step", op.Failure.Step, "reason", op.Failure.Reason,
			)).To(Equal([]float64{1, 1}))
			g.Expect(operatorGauge(ctx, g, "talos_operator_cluster_operation_halted", "taloscluster", tc.Name)).To(Equal([]float64{1, 1}))
		}).WithContext(ctx).WithTimeout(8 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("checking that every node runs what it ran before: the worker reverted, the rest untouched")
		Expect(snapshot(ctx, harness, nodes)).To(Equal(before))

		Expect(labelled(ctx, Default, rolloutLabel, "never")).To(BeEmpty(), "no control plane was touched")

		// The worker answers again once Talos took the config back, and is asked what it has to
		// say about the time it was cut off (docs/scope.md §14.6).
		By("finding what the worker said when the rollout halted")
		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(tc.Status.Operation).NotTo(BeNil())
			g.Expect(tc.Status.Operation.Failure.Bundle).To(Equal(tc.Name+"-support-bundle"), "no bundle yet")
		}).WithContext(ctx).WithTimeout(7 * time.Minute).WithPolling(10 * time.Second).Should(Succeed())

		bundle := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: tc.Name + "-support-bundle"}, bundle)).To(Succeed())
		Expect(metav1.IsControlledBy(bundle, tc)).To(BeTrue(), "the bundle goes when the TalosCluster does")

		// Only what is there is looked at, never what it says: a log is not for a public CI log.
		for _, key := range []string{"failure.txt", "services.txt", "machined.log.gz", "kubelet.log.gz", "dmesg.log.gz"} {
			Expect(bundle.Data[key]).NotTo(BeEmpty(), "the bundle has no %s", key)
		}

		_, hasEtcd := bundle.Data["etcd.log.gz"]
		Expect(hasEtcd).To(BeFalse(), "a worker has no etcd to ask")

		By("aborting, and taking the change back")
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			if tc.Annotations == nil {
				tc.Annotations = map[string]string{}
			}

			tc.Annotations[talosv1alpha1.AnnotationAbort] = op.Failure.Token
		})

		// The operation is halted until the operator reads the annotation: that ends no wait here.
		record := endedAsTold(ctx, previous, "rollout", 2*time.Minute)
		Expect(record.Outcome).To(Equal(talosv1alpha1.OperationAborted))

		setPoolConfig(ctx, talosv1alpha1.MachineRoleWorker, workerConfig)
		setPoolConfig(ctx, talosv1alpha1.MachineRoleControlPlane, controlPlaneConfig)

		// The cluster is whole again once the worker's kubelet is back.
		inSync(ctx, 5*time.Minute)
		Expect(snapshot(ctx, harness, nodes)).To(Equal(before))
		Expect(rejectedRPCs(ctx)).To(BeZero())

		By("finding nothing of the halt left in the metrics, and every machine read and ready")
		Eventually(func(g Gomega) {
			g.Expect(operatorGauge(ctx, g, "talos_operator_operation_failure_info")).To(BeEmpty())
			g.Expect(operatorGauge(ctx, g, "talos_operator_cluster_operation_halted", "taloscluster", tc.Name)).To(Equal([]float64{0, 0}))

			for _, tm := range machines {
				g.Expect(operatorGauge(ctx, g, "talos_operator_machine_ready", "taloscluster", tc.Name, "machine", tm.Name)).
					To(Equal([]float64{1, 1}), tm.Name)
				g.Expect(operatorGauge(ctx, g, "talos_operator_machine_condition", "machine", tm.Name,
					"condition", talosv1alpha1.ConditionNodeObserved, "status", "True")).To(Equal([]float64{1, 1}), tm.Name)
			}
		}).WithContext(ctx).WithTimeout(5 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	}, NodeTimeout(e2eTryTimeout+40*time.Minute))

	// The rollout policy a user declares (docs/scope.md §6.4), against a real Kubernetes: a
	// maintenance window by the cluster's own clock, a gate read from the API with the permission
	// the chart was given for it, and hooks as Jobs that really run.
	It("holds a rollout back outside its windows and behind a gate, and runs its hooks around it", func(ctx context.Context) {
		workers := workersOrSkip()

		const (
			policyLabel = "e2e.talos-operator/policy"
			gateObject  = "storage"
		)

		previous := lastEnded(ctx)

		// The one image every node is known to have: the operator's own. Its --help exits with 0.
		deployment := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: operatorName}, deployment)).To(Succeed())

		job, err := json.Marshal(batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "hook", Image: deployment.Spec.Template.Spec.Containers[0].Image, Command: []string{"/manager", "--help"},
			}},
			Tolerations: []corev1.Toleration{{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists}},
		}}})
		Expect(err).NotTo(HaveOccurred())

		hook := func(name string) []talosv1alpha1.Hook {
			return []talosv1alpha1.Hook{{Name: name, Job: runtime.RawExtension{Raw: job}}}
		}

		hookJobs := func(ctx context.Context, g Gomega) map[string][]batchv1.Job {
			var list batchv1.JobList
			g.Expect(k8sClient.List(ctx, &list, client.InNamespace(testNamespace), client.HasLabels{talosv1alpha1.LabelHook})).To(Succeed())

			byStage := map[string][]batchv1.Job{}
			for _, j := range list.Items {
				byStage[j.Labels[talosv1alpha1.LabelHook]] = append(byStage[j.Labels[talosv1alpha1.LabelHook]], j)
			}

			return byStage
		}

		// held waits until the rollout is reported as held back for the given reason, and then
		// watches for a while that it really is: no operation, no hook, no node changed.
		held := func(reason, message string) {
			GinkgoHelper()

			before := snapshot(ctx, harness, nodes)

			Eventually(func(g Gomega) {
				refresh(ctx, g)
				blocked := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionRolloutBlocked)
				g.Expect(blocked).NotTo(BeNil())
				g.Expect(blocked.Reason).To(Equal(reason), blocked.Message)
				g.Expect(blocked.Message).To(ContainSubstring(message))
			}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

			Consistently(func(g Gomega) {
				refresh(ctx, g)
				g.Expect(tc.Status.Operation).To(BeNil())
				g.Expect(hookJobs(ctx, g)).To(BeEmpty())
			}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(5 * time.Second).Should(Succeed())

			Expect(snapshot(ctx, harness, nodes)).To(Equal(before))
		}

		By("declaring a window that opens in twelve hours, a gate on an object that does not exist, and a hook at every stage")
		opens := time.Now().UTC().Add(12 * time.Hour)
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Rollout.MaintenanceWindows = []talosv1alpha1.MaintenanceWindow{{
				Schedule: fmt.Sprintf("%d %d * * *", opens.Minute(), opens.Hour()), Duration: metav1.Duration{Duration: time.Hour},
			}}
			tc.Spec.Rollout.Gates = []talosv1alpha1.RolloutGate{{
				Name:       "storage-healthy",
				Object:     talosv1alpha1.GateObject{APIVersion: "v1", Kind: "ConfigMap", Namespace: testNamespace, Name: gateObject},
				Expression: `object.data.health == "HEALTH_OK"`,
			}}
			tc.Spec.Rollout.Hooks = &talosv1alpha1.RolloutHooks{
				PreRollout: hook("announce"), PostRollout: hook("all-clear"), PreNode: hook("prepare"), PostNode: hook("release"),
			}
		})
		DeferCleanup(func(ctx context.Context) {
			updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
				tc.Spec.Rollout.MaintenanceWindows, tc.Spec.Rollout.Gates, tc.Spec.Rollout.Hooks = nil, nil, nil
			})
			Expect(k8sClient.DeleteAllOf(ctx, &batchv1.Job{}, client.InNamespace(testNamespace), client.HasLabels{talosv1alpha1.LabelHook},
				client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx,
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: gateObject, Namespace: testNamespace}}))).To(Succeed())
		})

		workerConfig := poolConfig(ctx, talosv1alpha1.MachineRoleWorker)
		setPoolConfig(ctx, talosv1alpha1.MachineRoleWorker,
			with(workerConfig, "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  "+policyLabel+": held\n"))

		By("waiting outside the window")
		held(talosv1alpha1.ReasonOutsideWindow, "outside the maintenance windows")

		By("opening the window: the gate's object does not exist")
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.MaintenanceWindows = nil })
		held(talosv1alpha1.ReasonGateClosed, "does not exist")

		By("creating the object, with the gate's condition false")
		storage := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: gateObject, Namespace: testNamespace},
			Data:       map[string]string{"health": "HEALTH_WARN"},
		}
		Expect(k8sClient.Create(ctx, storage)).To(Succeed())
		held(talosv1alpha1.ReasonGateClosed, "is false")

		By("making the gate's condition true")
		storage.Data["health"] = "HEALTH_OK"
		Expect(k8sClient.Update(ctx, storage)).To(Succeed())

		record := endedSince(ctx, previous, "rollout", time.Duration(len(workers))*6*time.Minute+5*time.Minute)
		expectSucceeded(record)
		Expect(record.Completed).To(HaveLen(len(workers)))

		By("checking that every hook ran, as a Job that succeeded, for the node it was for")
		jobs := hookJobs(ctx, Default)
		Expect(jobs[talosv1alpha1.HookPreRollout]).To(HaveLen(1))
		Expect(jobs[talosv1alpha1.HookPostRollout]).To(HaveLen(1))
		Expect(jobs[talosv1alpha1.HookPreNode]).To(HaveLen(len(workers)))
		Expect(jobs[talosv1alpha1.HookPostNode]).To(HaveLen(len(workers)))

		changed := labelled(ctx, Default, policyLabel, "held")
		Expect(changed).To(HaveLen(len(workers)), "the label reached Kubernetes, so the config really took effect")

		workerNodes := make([]string, 0, len(changed))
		for _, n := range changed {
			workerNodes = append(workerNodes, n.Name)
		}

		for stage, ran := range jobs {
			for _, j := range ran {
				Expect(j.Status.Succeeded).To(BeEquivalentTo(1), "%s Job %s", stage, j.Name)

				env := map[string]string{}
				for _, e := range j.Spec.Template.Spec.Containers[0].Env {
					env[e.Name] = e.Value
				}

				Expect(env).To(HaveKeyWithValue("TALOS_OPERATION", "ConfigRollout"))
				Expect(env).To(HaveKeyWithValue("TALOS_HOOK", stage))

				if stage == talosv1alpha1.HookPreNode || stage == talosv1alpha1.HookPostNode {
					Expect(workerNodes).To(ContainElement(env["TALOS_NODE"]), "%s Job %s", stage, j.Name)
				}
			}
		}

		inSync(ctx, 2*time.Minute)
		Expect(rejectedRPCs(ctx)).To(BeZero())
	}, NodeTimeout(40*time.Minute))

	// A Kubernetes upgrade (docs/scope.md §7.4): the components Talos runs, one on one node at a
	// time, every API server first. It is the one scenario in which the Kubernetes API the
	// operator itself depends on is restarted under it, on purpose.
	It("upgrades Kubernetes by a minor, after refusing to while a kubelet runs with a removed flag", func(ctx context.Context) {
		from, target := os.Getenv("E2E_KUBERNETES_VERSION"), os.Getenv("E2E_KUBERNETES_UPGRADE_TO")
		if target == "" || target == from {
			Skip("the harness cluster already runs the Kubernetes version to upgrade to")
		}

		controlPlanes := addrsOf("E2E_CONTROLPLANES")

		kubeletVersions := func(ctx context.Context, g Gomega) []string {
			var list corev1.NodeList
			g.Expect(k8sClient.List(ctx, &list)).To(Succeed())

			out := make([]string, 0, len(list.Items))
			for _, n := range list.Items {
				out = append(out, n.Status.NodeInfo.KubeletVersion)
			}

			return out
		}

		upToDate := func() *metav1.Condition {
			return meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionKubernetesUpToDate)
		}

		Expect(tc.Spec.Versions.Kubernetes).To(Equal(from), "adoption read the version the cluster runs")
		Expect(kubeletVersions(ctx, Default)).To(HaveEach("v" + from))

		// What 1.37 removes, by go-kubernetes's list. Other versions remove other things.
		strands := strings.HasPrefix(from, "1.36.") && strings.HasPrefix(target, "1.37.")

		// The pool whose kubelets get the flag: the workers, where there are any.
		role, flagged := talosv1alpha1.MachineRoleWorker, addrsOf("E2E_WORKERS")
		if len(flagged) == 0 {
			role, flagged = talosv1alpha1.MachineRoleControlPlane, controlPlanes
		}

		original := poolConfig(ctx, role)

		if strands {
			By("starting kubelets with a flag the new version removed")
			previous := lastEnded(ctx)
			setPoolConfig(ctx, role, with(original, removedKubeletPatch))

			expectSucceeded(endedSince(ctx, previous, "rollout", time.Duration(len(flagged))*6*time.Minute))
		}

		By("asking for the next Kubernetes minor")
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Versions.Kubernetes = target
			tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
		})

		if strands {
			By("checking that the upgrade is refused before any node is touched")
			before := snapshot(ctx, harness, nodes)

			Eventually(func(g Gomega) {
				refresh(ctx, g)
				g.Expect(upToDate()).NotTo(BeNil())
				g.Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible), upToDate().Message)
				g.Expect(upToDate().Message).To(ContainSubstring("--" + removedKubeletFlag))
			}).WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())

			Consistently(func(g Gomega) {
				refresh(ctx, g)
				g.Expect(tc.Status.Operation).To(BeNil())
			}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(5 * time.Second).Should(Succeed())

			Expect(snapshot(ctx, harness, nodes)).To(Equal(before))
			Expect(kubeletVersions(ctx, Default)).To(HaveEach("v" + from))

			// What clears the way is a config change, and the refused upgrade must not hold it up:
			// the rollout takes the flag out and leaves the kubelet's image as it is.
			By("taking the flag out again")
			setPoolConfig(ctx, role, original)
		}

		upgraded := func() []talosv1alpha1.OperationRecord {
			var out []talosv1alpha1.OperationRecord

			for _, record := range tc.Status.History {
				if record.Type == talosv1alpha1.OperationKubernetesUpgrade {
					out = append(out, record)
				}
			}

			return out
		}

		By("waiting for the upgrade to go through every component on every node")
		// The API server is restarted on the way, and with a single control plane nothing answers
		// meanwhile: the test, like the operator, simply asks again.
		// Three components on every control plane besides the kubelet, and a kubelet on every node,
		// which brings the kube-proxy image of a control plane's config along.
		turns := 3*len(controlPlanes) + len(nodes)
		Eventually(func(g Gomega) {
			refresh(ctx, g)
			if op := tc.Status.Operation; op != nil {
				halted(op, "upgrade")
			}
			g.Expect(upgraded()).NotTo(BeEmpty(), "still upgrading: %+v", tc.Status.Operation)
		}).WithContext(ctx).WithTimeout(time.Duration(turns) * 6 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		record := upgraded()[0]
		expectSucceeded(record)
		Expect(record.Completed).To(HaveLen(turns))
		Expect(record.Completed[0]).To(HavePrefix(plan.ComponentAPIServer+"/"), "the API servers go first")

		By("checking what runs: every kubelet and every control-plane component at the new version")
		Eventually(func(g Gomega) {
			g.Expect(kubeletVersions(ctx, g)).To(HaveEach("v" + target))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		for _, component := range []string{plan.ComponentAPIServer, plan.ComponentControllerManager, plan.ComponentScheduler} {
			var pods corev1.PodList
			Expect(k8sClient.List(ctx, &pods, client.InNamespace("kube-system"), client.MatchingLabels{"k8s-app": component})).To(Succeed())
			Expect(pods.Items).To(HaveLen(len(controlPlanes)), component)

			for _, pod := range pods.Items {
				Expect(pod.Spec.Containers[0].Image).To(HaveSuffix(":v"+target), "%s", pod.Name)
			}
		}

		By("checking what every node's config names, persistently")
		for _, n := range nodes {
			cfgs, err := harness.MachineConfig(ctx, n)
			Expect(err).NotTo(HaveOccurred())
			Expect(cfgs.PendingConfigChange()).To(BeFalse(), "nothing is left tried or staged on %s", n)

			images, err := talos.KubernetesImages(cfgs.Active)
			Expect(err).NotTo(HaveOccurred())
			Expect(images).NotTo(BeEmpty())

			for component, image := range images {
				Expect(image).To(HaveSuffix(":v"+target), "%s on %s", component, n)
			}
		}

		By("checking that nothing is left for a config rollout")
		inSync(ctx, 2*time.Minute)

		Eventually(func(g Gomega) {
			refresh(ctx, g)
			g.Expect(upToDate()).NotTo(BeNil())
			g.Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
		}).WithContext(ctx).WithTimeout(2 * time.Minute).Should(Succeed())

		// Talos applied kube-proxy once, at bootstrap. The operator leaves it alone, and says so.
		proxy := &appsv1.DaemonSet{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: "kube-system", Name: "kube-proxy"}, proxy); err == nil {
			Expect(proxy.Spec.Template.Spec.Containers[0].Image).To(HaveSuffix(":v"+from), "kube-proxy is not the operator's to change")
			Expect(upToDate().Message).To(ContainSubstring("kube-proxy DaemonSet runs"))
		}

		By("checking that the operator is whole again, and did nothing it was not allowed to do")
		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: operatorName}, deployment)).To(Succeed())
			g.Expect(deployment.Status.ReadyReplicas).To(Equal(*deployment.Spec.Replicas))
		}).WithContext(ctx).WithTimeout(5 * time.Minute).Should(Succeed())
		Expect(rejectedRPCs(ctx)).To(BeZero())
	}, NodeTimeout(150*time.Minute))

	// The switch of spec.observability (docs/scope.md §14.2), on real control planes: Talos binds
	// the controller manager and the scheduler to localhost, and a Prometheus is not there.
	It("makes the controller manager and the scheduler answer from outside their node, once asked to", func(ctx context.Context) {
		controlPlanes, workers := addrsOf("E2E_CONTROLPLANES"), addrsOf("E2E_WORKERS")
		before := snapshot(ctx, harness, nodes)
		previous := lastEnded(ctx)

		// The secure ports of the two components. Without a token they refuse to say anything,
		// which is an answer; behind localhost nobody answers at all.
		components := map[string]uint16{"kube-controller-manager": 10257, "kube-scheduler": 10259}

		By("finding that nobody answers before")
		for _, cp := range controlPlanes {
			for name, port := range components {
				_, err := answers(ctx, "https://"+netip.AddrPortFrom(cp, port).String()+"/metrics")
				Expect(err).To(HaveOccurred(), "%s on %s must not be reachable from outside yet", name, cp)
			}
		}

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Observability.ControlPlaneMetrics = talosv1alpha1.CapabilityEnabled
		})

		record := endedSince(ctx, previous, "rollout of the metrics switch", time.Duration(len(controlPlanes))*10*time.Minute)
		Expect(record.Type).To(Equal(talosv1alpha1.OperationConfigRollout))
		expectSucceeded(record)
		Expect(record.Completed).To(HaveLen(len(controlPlanes)), "the control planes, and nobody else")
		inSync(ctx, 2*time.Minute)

		By("asking both components on every control plane, from outside the node")
		Eventually(func(g Gomega) {
			for _, cp := range controlPlanes {
				for name, port := range components {
					status, err := answers(ctx, "https://"+netip.AddrPortFrom(cp, port).String()+"/metrics")
					g.Expect(err).NotTo(HaveOccurred(), "%s on %s", name, cp)
					g.Expect(status).To(BeElementOf(http.StatusUnauthorized, http.StatusForbidden), "%s on %s", name, cp)
				}
			}
		}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("checking that nothing rebooted and no worker changed")
		after := snapshot(ctx, harness, nodes)
		expectNoReboot(before, after)

		for _, n := range nodes {
			if slices.Contains(workers, n) {
				Expect(after[n].ConfigHash).To(Equal(before[n].ConfigHash), "%s is a worker and must not change", n)
			} else {
				Expect(after[n].ConfigHash).NotTo(Equal(before[n].ConfigHash), "%s must run the new config", n)
			}
		}

		Expect(rejectedRPCs(ctx)).To(BeZero())
	}, NodeTimeout(45*time.Minute))

	// The upkeep of etcd (docs/scope.md §14.6). A database on a cluster this young is small and
	// not half empty, so the spec asks for it at a threshold every database is over: what is
	// shown is the operation, on real members, not the arithmetic that starts it.
	It("defragments etcd, one member at a time and the leader last", func(ctx context.Context) {
		before := snapshot(ctx, harness, nodes)
		previous := lastEnded(ctx)

		var (
			members []string
			leader  string
		)

		Eventually(func(g Gomega) {
			members, leader = nil, ""

			for _, tm := range machines {
				if !isControlPlane(tm) {
					continue
				}

				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), tm)).To(Succeed())
				g.Expect(tm.Status.Etcd).NotTo(BeNil(), tm.Name)

				members = append(members, tm.Name)

				if tm.Status.Etcd.Leader {
					g.Expect(leader).To(BeEmpty(), "two control planes say they lead etcd")

					leader = tm.Name
				}
			}

			g.Expect(leader).NotTo(BeEmpty(), "no control plane is known to lead etcd")
		}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Maintenance.Etcd = talosv1alpha1.EtcdMaintenance{FreePercent: new(int32(1)), MinimumSize: new(resource.MustParse("1"))}
			tc.Spec.Management.Maintenance = talosv1alpha1.CapabilityEnabled
		})

		record := endedSince(ctx, previous, "defragmentation", time.Duration(len(members))*8*time.Minute)
		Expect(record.Type).To(Equal(talosv1alpha1.OperationEtcdDefragment))
		expectSucceeded(record)
		Expect(record.Completed).To(ConsistOf(members))
		Expect(record.Completed[len(record.Completed)-1]).To(Equal(leader), "the member that led etcd goes last")

		refresh(ctx, Default)
		Expect(tc.Status.Maintenance).NotTo(BeNil())
		Expect(tc.Status.Maintenance.EtcdDefragmentedAt).NotTo(BeNil())

		By("leaving it alone again: nothing rebooted, no config changed, and it is not started again")
		updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Management.Maintenance = talosv1alpha1.CapabilityDisabled
			tc.Spec.Maintenance = talosv1alpha1.Maintenance{}
		})
		inSync(ctx, 2*time.Minute)

		Expect(snapshot(ctx, harness, nodes)).To(Equal(before))
		Expect(lastEnded(ctx)).To(Equal(record.ID))
		Expect(rejectedRPCs(ctx)).To(BeZero())

		Eventually(func(g Gomega) {
			g.Expect(operatorGauge(ctx, g, "talos_operator_cluster_etcd_defragmented_timestamp_seconds", "taloscluster", tc.Name)).
				To(HaveExactElements(BeNumerically(">", 0), BeNumerically(">", 0)))
		}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	}, NodeTimeout(45*time.Minute))

	// Adding and removing machines (docs/scope.md §7.6, §7.7), on the machine the harness left in
	// maintenance mode. The specs hand that one machine on to each other: added as a worker, taken
	// out again, on three control planes made a control plane in the place of another, and at the
	// end whatever machine is left waiting is added once more to die.
	Context("membership", func() {
		var (
			// spare is the address of the machine that waits in maintenance mode, and spareUUID
			// what it says its hardware is.
			spare     netip.Addr
			spareUUID string
		)

		// candidate asks a machine in maintenance mode what it is, as the operator does: without
		// credentials, because such a machine has none to check.
		candidate := func(ctx context.Context, addr netip.Addr) (talos.Candidate, error) {
			f := talos.NewFactory()
			defer f.Close()

			m, err := f.Maintenance(ctx, addr)
			if err != nil {
				return talos.Candidate{}, err
			}

			defer m.Close() //nolint:errcheck // only read from

			ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			return m.Probe(ctx)
		}

		// declare creates a TalosMachine for the machine at addr, as a user would write it: where
		// it is, what its hardware says it is, and what only this machine has in its config. The
		// taint keeps the operator's own pods off it: its image is in no registry, and was only
		// loaded into the nodes the harness built.
		declare := func(ctx context.Context, name string, role talosv1alpha1.MachineRole, addr netip.Addr, uuid string,
		) *talosv1alpha1.TalosMachine {
			GinkgoHelper()

			tm := &talosv1alpha1.TalosMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name: name, Namespace: testNamespace, Labels: map[string]string{talosv1alpha1.LabelCluster: tc.Name},
				},
				Spec: talosv1alpha1.TalosMachineSpec{
					PoolRef:  corev1.LocalObjectReference{Name: pools[role].Name},
					Address:  addr.String(),
					Identity: talosv1alpha1.MachineIdentity{SystemUUID: uuid},
					Config: []talosv1alpha1.ConfigSource{{Inline: "apiVersion: v1alpha1\nkind: HostnameConfig\nauto: off\nhostname: " + name +
						"\n---\napiVersion: v1alpha1\nkind: KubeNodeConfig\ntaints:\n  e2e.talos-operator/added: \"true:NoSchedule\"\n"}},
				},
			}
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			return tm
		}

		setDesiredState := func(ctx context.Context, tm *talosv1alpha1.TalosMachine, state talosv1alpha1.DesiredState) {
			GinkgoHelper()

			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), tm)).To(Succeed())

				tm.Spec.DesiredState = state
				g.Expect(k8sClient.Update(ctx, tm)).To(Succeed())
			}).WithContext(ctx).WithTimeout(30 * time.Second).Should(Succeed())
		}

		membershipCurrent := func() *metav1.Condition {
			return meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionMembershipCurrent)
		}

		// settled waits until the cluster's machines are the ones that are declared and nothing is
		// in flight. A halted operation ends the wait at once.
		settled := func(ctx context.Context, what string, timeout time.Duration, also func(Gomega)) {
			GinkgoHelper()

			Eventually(func(g Gomega) {
				refresh(ctx, g)
				halted(tc.Status.Operation, what)

				g.Expect(tc.Status.Operation).To(BeNil(), "%+v", tc.Status.Operation)
				g.Expect(membershipCurrent()).NotTo(BeNil())
				g.Expect(membershipCurrent().Status).To(Equal(metav1.ConditionTrue), membershipCurrent().Message)

				if also != nil {
					also(g)
				}
			}).WithContext(ctx).WithTimeout(timeout).WithPolling(10 * time.Second).Should(Succeed())
		}

		// joined checks that a machine is a node of the cluster in every sense.
		joined := func(ctx context.Context, tm *talosv1alpha1.TalosMachine) func(Gomega) {
			return func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), tm)).To(Succeed())
				g.Expect(tm.Status.JoinedAt).NotTo(BeNil())
				g.Expect(tm.Status.RemovedAt).To(BeNil())
				g.Expect(tm.Status.Phase).To(Equal(talosv1alpha1.MachinePhase("Ready")))

				node := &corev1.Node{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: tm.Name}, node)).To(Succeed())
				g.Expect(node.Status.Conditions).To(ContainElement(SatisfyAll(
					HaveField("Type", corev1.NodeReady), HaveField("Status", corev1.ConditionTrue))))
			}
		}

		// gone checks that a machine has left: recorded as removed, its Node deleted, and the
		// machine itself waiting in maintenance mode again, having forgotten the cluster.
		gone := func(ctx context.Context, tm *talosv1alpha1.TalosMachine, uuid string) func(Gomega) {
			return func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), tm)).To(Succeed())
				g.Expect(tm.Status.RemovedAt).NotTo(BeNil())

				err := k8sClient.Get(ctx, client.ObjectKey{Name: tm.Status.Identity.KubernetesNodeName}, &corev1.Node{})
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the machine's Node is still there: %v", err)

				found, err := candidate(ctx, netip.MustParseAddr(tm.Spec.Address))
				g.Expect(err).NotTo(HaveOccurred(), "the machine does not answer in maintenance mode")
				g.Expect(found.Stage).To(Equal(talos.StageMaintenance))
				g.Expect(strings.EqualFold(found.UUID, uuid)).To(BeTrue())
			}
		}

		lastControlPlane := func() *talosv1alpha1.TalosMachine {
			GinkgoHelper()

			controlPlanes := addrsOf("E2E_CONTROLPLANES")
			last := controlPlanes[len(controlPlanes)-1].String()

			for _, tm := range machines {
				if tm.Spec.Address == last {
					return tm
				}
			}

			Fail("no TalosMachine for the control plane at " + last)

			return nil
		}

		BeforeAll(func(ctx context.Context) {
			spares := addrsOf("E2E_MAINTENANCE_NODES")
			if len(spares) == 0 {
				Skip("the harness cluster has no machine in maintenance mode (MAINTENANCE_NODES)")
			}

			spare = spares[0]

			found, err := candidate(ctx, spare)
			Expect(err).NotTo(HaveOccurred(), "the spare machine must wait in maintenance mode")
			Expect(found.Stage).To(Equal(talos.StageMaintenance))

			spareUUID = found.UUID

			inSync(ctx, 5*time.Minute)
			updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Management.Membership = talosv1alpha1.CapabilityEnabled })
		})

		It("adds a machine that waits in maintenance mode, and sends nothing to one that is not the machine declared", func(ctx context.Context) {
			By("declaring the machine with the identity of another")
			wrong := declare(ctx, "e2e-added", talosv1alpha1.MachineRoleWorker, spare, "aaaaaaaa-bbbb-4ccc-8ddd-999999999999")

			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wrong), wrong)).To(Succeed())

				cond := meta.FindStatusCondition(wrong.Status.Conditions, talosv1alpha1.ConditionProvisionable)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonIdentityMismatch), cond.Message)
			}).WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())

			Consistently(func(g Gomega) {
				refresh(ctx, g)
				g.Expect(tc.Status.Operation).To(BeNil(), "nothing is begun for a machine that is another one")

				found, err := candidate(ctx, spare)
				g.Expect(err).NotTo(HaveOccurred(), "the machine was sent something: it no longer waits in maintenance mode")
				g.Expect(found.Stage).To(Equal(talos.StageMaintenance))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(10 * time.Second).Should(Succeed())

			// A machine that never was a member is nobody's to protect.
			deleteMachine(ctx, wrong)

			By("declaring it as what it is")
			added := declare(ctx, "e2e-added", talosv1alpha1.MachineRoleWorker, spare, spareUUID)
			settled(ctx, "addition", 30*time.Minute, joined(ctx, added))

			// The history keeps the last ten operations, so its length says nothing by now.
			Expect(tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationAddMachine))
			expectSucceeded(tc.Status.History[0])
			Expect(tc.Status.History[0].Completed).To(Equal([]string{added.Name}))

			inSync(ctx, 5*time.Minute)
			Expect(rejectedRPCs(ctx)).To(BeZero())
		}, NodeTimeout(45*time.Minute))

		It("refuses to take out a control plane the cluster cannot spare", func(ctx context.Context) {
			controlPlane := lastControlPlane()

			refresh(ctx, Default)
			last := tc.Status.History[0].ID

			setDesiredState(ctx, controlPlane, talosv1alpha1.DesiredStateRemoved)

			Eventually(func(g Gomega) {
				refresh(ctx, g)

				blocked := meta.FindStatusCondition(tc.Status.Conditions, talosv1alpha1.ConditionRolloutBlocked)
				g.Expect(blocked).NotTo(BeNil())
				g.Expect(blocked.Reason).To(BeElementOf(plan.BlockQuorumAtRisk, plan.BlockLastControlPlane), blocked.Message)
			}).WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())

			Consistently(func(g Gomega) {
				refresh(ctx, g)
				g.Expect(tc.Status.Operation).To(BeNil())

				node := &corev1.Node{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: controlPlane.Status.Identity.KubernetesNodeName}, node)).To(Succeed())
				g.Expect(node.Spec.Unschedulable).To(BeFalse(), "a node that may not leave is not cordoned either")
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(10 * time.Second).Should(Succeed())

			setDesiredState(ctx, controlPlane, talosv1alpha1.DesiredStatePresent)
			inSync(ctx, 3*time.Minute)
			Expect(tc.Status.History[0].ID).To(Equal(last), "no operation ran")
		}, NodeTimeout(10*time.Minute))

		It("takes the machine out again: drained, reset, waiting in maintenance mode, its Node deleted", func(ctx context.Context) {
			added := &talosv1alpha1.TalosMachine{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "e2e-added"}, added)).To(Succeed())

			// Asked to leave is not gone: the API server keeps the TalosMachine of a node that is
			// still a member.
			setDesiredState(ctx, added, talosv1alpha1.DesiredStateRemoved)
			Expect(k8sClient.Delete(ctx, added)).To(MatchError(ContainSubstring("can be deleted once it is out of the cluster")))

			settled(ctx, "removal", 30*time.Minute, gone(ctx, added, spareUUID))

			Expect(tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationRemoveMachine))
			expectSucceeded(tc.Status.History[0])

			By("deleting the TalosMachine of the machine that has left")
			Expect(k8sClient.Delete(ctx, added)).To(Succeed())

			inSync(ctx, 5*time.Minute)
			Expect(rejectedRPCs(ctx)).To(BeZero())
		}, NodeTimeout(40*time.Minute))

		It("replaces a control plane, with three voting members of etcd throughout", func(ctx context.Context) {
			if len(addrsOf("E2E_CONTROLPLANES")) < 3 {
				Skip("needs three control planes: one is only taken out of etcd while three voting members remain")
			}

			old := lastControlPlane()
			oldUUID := old.Spec.Identity.SystemUUID

			// One commit: the new machine is declared, and the old one is to go.
			replacement := declare(ctx, "e2e-replacement", talosv1alpha1.MachineRoleControlPlane, spare, spareUUID)
			setDesiredState(ctx, old, talosv1alpha1.DesiredStateRemoved)

			fewest := 99

			settled(ctx, "replacement", 60*time.Minute, func(g Gomega) {
				// Asked at every look, until both are done.
				if tc.Status.Observed != nil && len(tc.Status.Observed.EtcdMembers) > 0 {
					voters := 0

					for _, m := range tc.Status.Observed.EtcdMembers {
						if !m.IsLearner {
							voters++
						}
					}

					fewest = min(fewest, voters)
				}

				joined(ctx, replacement)(g)
				gone(ctx, old, oldUUID)(g)
			})

			Expect(fewest).To(BeNumerically(">=", 3), "etcd must never have had fewer than three voting members")
			Expect(tc.Status.Observed.EtcdMembers).To(HaveLen(len(addrsOf("E2E_CONTROLPLANES"))))
			Expect(tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationRemoveMachine), "the old one left after the new one joined")
			Expect(tc.Status.History[1].Type).To(Equal(talosv1alpha1.OperationAddMachine))

			// The machine that waits now is the one that was a control plane.
			spare, spareUUID = netip.MustParseAddr(old.Spec.Address), oldUUID

			Expect(k8sClient.Delete(ctx, old)).To(Succeed())

			inSync(ctx, 10*time.Minute)
			Expect(rejectedRPCs(ctx)).To(BeZero())
		}, NodeTimeout(75*time.Minute))

		It("takes out a machine that died, once a human says it is dead", func(ctx context.Context) {
			By("adding the machine that waits, to have one to lose")
			doomed := declare(ctx, "e2e-doomed", talosv1alpha1.MachineRoleWorker, spare, spareUUID)
			settled(ctx, "addition", 30*time.Minute, joined(ctx, doomed))

			// As far as the cluster can tell, a machine that drops everything it would send to the
			// control planes is dead: its Talos API answers through none of them, and its kubelet
			// reaches no API server. Nothing short of the harness's own tools gets it back.
			//
			// The control planes are the ones the cluster has now. On three, one was replaced
			// above: the list the cluster was built with names this very machine and leaves out
			// the control plane that took its place, through which it would go on being a member.
			By("cutting the machine off for good, behind the operator's back")
			cfgs, err := harness.MachineConfig(ctx, spare)
			Expect(err).NotTo(HaveOccurred())

			// The config takes away the way its own answer has to come back on, through a control
			// plane: the call ends with the answer or without one, whichever is first. What says
			// that it took is the machine no longer answering.
			isolated := withPatch(cfgs.Active, blackholeDocuments(controlPlaneAddrsNow(ctx)))
			if err := harnessWriter(ctx).ApplyConfig(ctx, spare, isolated, talos.ApplyNoReboot, 0); err != nil {
				Expect(talos.IsUnreachable(err)).To(BeTrue(), "the config was refused: %v", err)
			}

			Eventually(func(g Gomega) {
				_, err := harness.BootID(ctx, spare)
				g.Expect(err).To(HaveOccurred(), "the machine still answers through a control plane")
				g.Expect(talos.IsUnreachable(err)).To(BeTrue(), "%v", err)
			}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(10 * time.Second).Should(Succeed())

			setDesiredState(ctx, doomed, talosv1alpha1.DesiredStateRemoved)

			By("waiting for the removal to halt, with nothing taken from the cluster")
			var token string

			Eventually(func(g Gomega) {
				refresh(ctx, g)
				g.Expect(tc.Status.Operation).NotTo(BeNil(), "no removal in flight")
				g.Expect(tc.Status.Operation.Failure).NotTo(BeNil(), "not halted yet: %+v", tc.Status.Operation.Current)
				g.Expect(tc.Status.Operation.Failure.Reason).To(Equal(plan.FailNodeGone), tc.Status.Operation.Failure.Message)

				token = tc.Status.Operation.Failure.Token
			}).WithContext(ctx).WithTimeout(25 * time.Minute).WithPolling(10 * time.Second).Should(Succeed())

			Expect(tc.Status.Operation.Failure.Remediation).To(ContainSubstring(talosv1alpha1.AnnotationConfirmDead))
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: doomed.Name}, &corev1.Node{})).To(Succeed(),
				"the Node of a machine that only does not answer is not deleted on a guess")

			By("saying that it is dead")
			updateCluster(ctx, func(tc *talosv1alpha1.TalosCluster) {
				if tc.Annotations == nil {
					tc.Annotations = map[string]string{}
				}

				tc.Annotations[talosv1alpha1.AnnotationConfirmDead] = token
			})

			// The operation is halted until the operator has read that; from then on a halt is a new one.
			Eventually(func(g Gomega) {
				refresh(ctx, g)
				g.Expect(tc.Status.Operation == nil || tc.Status.Operation.Failure == nil).To(BeTrue(),
					"the operator has not acted on the confirmation yet")
			}).WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())

			settled(ctx, "removal", 20*time.Minute, func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(doomed), doomed)).To(Succeed())
				g.Expect(doomed.Status.RemovedAt).NotTo(BeNil())

				err := k8sClient.Get(ctx, client.ObjectKey{Name: doomed.Name}, &corev1.Node{})
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the dead machine's Node is still there: %v", err)
			})

			// The cluster's discovery goes on listing the machine. The cluster is whole without it.
			inSync(ctx, 10*time.Minute)
			Eventually(conditionTrue(ctx, tc, clusterConditions, talosv1alpha1.ConditionHealthy)).
				WithContext(ctx).WithTimeout(5 * time.Minute).Should(Succeed())
			Expect(rejectedRPCs(ctx)).To(BeZero())
		}, NodeTimeout(90*time.Minute))
	})
})

// diffOf returns a machine's last reported config diff, for failure messages.
func diffOf(tm *talosv1alpha1.TalosMachine) string {
	if tm.Status.Config == nil {
		return "(none reported)"
	}

	return tm.Status.Config.Diff
}
