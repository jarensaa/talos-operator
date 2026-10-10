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

package controller

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	talosmachine "github.com/siderolabs/talos/pkg/machinery/config/machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	sigsyaml "sigs.k8s.io/yaml"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
	"github.com/jarensaa/talos-operator/internal/target"
)

const (
	talosVersion      = "v1.14.2"
	kubernetesVersion = "1.37.0"
	// newerTalosVersion is what specs upgrade a node to between reconciles.
	newerTalosVersion = "v1.14.3"
	// credentialsSecret names the talosconfig Secret every spec creates.
	credentialsSecret = "talosconfig"
	// clusterName names the TalosCluster every spec creates.
	clusterName = "cluster"
	cpNodename  = "e2e-controlplane-1"
)

var (
	cpAddr     = netip.MustParseAddr("10.5.0.2")
	workerAddr = netip.MustParseAddr("10.5.0.3")
)

const (
	cpUUID     = "aaaaaaaa-bbbb-4ccc-8ddd-000000000001"
	workerUUID = "aaaaaaaa-bbbb-4ccc-8ddd-000000000002"
	schematic  = "376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba"
)

// liveConfig generates a node's config the way talosctl would, signed by the shared test bundle.
// Patches are what someone changed on the node since, the way `talosctl patch machineconfig` would.
func liveConfig(role talosmachine.Type, patches ...string) []byte {
	GinkgoHelper()

	in := render.Input{
		ClusterName:       "e2e",
		Endpoint:          "https://" + cpAddr.String() + ":6443",
		KubernetesVersion: kubernetesVersion,
		Contract:          config.TalosVersion1_14,
		Bundle:            talostest.Bundle(GinkgoT()),
		Role:              role,
		InstallImage:      "factory.talos.dev/metal-installer/" + schematic + ":" + talosVersion,
	}

	for _, p := range patches {
		in.Patches = append(in.Patches, []byte(p))
	}

	cfg, err := render.Generate(in)
	Expect(err).NotTo(HaveOccurred())

	raw, err := cfg.Bytes()
	Expect(err).NotTo(HaveOccurred())

	return raw
}

// reproduceLiveConfigs sets the spec the fake's live configs were generated from (liveConfig).
func reproduceLiveConfigs(tc *talosv1alpha1.TalosCluster) {
	tc.Spec.ClusterName = "e2e"
	tc.Spec.ControlPlaneEndpoint = "https://" + cpAddr.String() + ":6443"
	tc.Spec.Versions = &talosv1alpha1.Versions{Talos: talosVersion, Kubernetes: kubernetesVersion}
	tc.Spec.ConfigGeneration = &talosv1alpha1.ConfigGeneration{
		VersionContract: "v1.14",
		Installer:       &talosv1alpha1.Installer{FactoryURL: "https://factory.talos.dev", Platform: "metal", SchematicID: schematic},
	}
}

// fakeNode is a healthy fake Talos node running the config liveConfig generates for its role.
func fakeNode(addr netip.Addr, machineType, name, uuid string, etcdID uint64) fake.Node {
	GinkgoHelper()

	role := talosmachine.TypeWorker
	if machineType == fake.TypeControlPlane {
		role = talosmachine.TypeControlPlane
	}

	return fake.Node{
		Addr: addr, MachineType: machineType, Nodename: name, EtcdMemberID: etcdID,
		Version:           talos.VersionInfo{Tag: talosVersion},
		MachineStatus:     talos.MachineStatus{Stage: stageRunning, Ready: true},
		BootID:            name + "-boot-1",
		SystemInformation: talos.SystemInformation{UUID: uuid},
		Schematic:         talos.Schematic{ID: schematic},
		// Like a real node booted from disk: no persistent config until something is applied.
		Config: talos.MachineConfigs{Active: liveConfig(role)},
	}
}

// observedCluster is the fake Talos cluster most specs observe: one control plane, one worker.
func observedCluster() *fake.Scenario {
	return fake.NewScenario(
		fakeNode(cpAddr, fake.TypeControlPlane, cpNodename, cpUUID, 0xa1),
		fakeNode(workerAddr, fake.TypeWorker, "e2e-worker-1", workerUUID, 0),
	)
}

// secretCredentials points a TalosCluster at the talosconfig Secret every spec creates.
func secretCredentials() talosv1alpha1.Credentials {
	return talosv1alpha1.Credentials{
		Source:    talosv1alpha1.CredentialsSourceSecret,
		SecretRef: &talosv1alpha1.SecretKeyReference{Name: credentialsSecret},
	}
}

// kubeNode is a Kubernetes Node running the specs' kubelet version.
func kubeNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v" + kubernetesVersion}},
	}
}

func condition(tc *talosv1alpha1.TalosCluster, condType string) *metav1.Condition {
	return meta.FindStatusCondition(tc.Status.Conditions, condType)
}

var _ = Describe("TalosCluster observe loop", func() {
	var (
		ns         string
		scenario   *fake.Scenario
		recorder   *events.FakeRecorder
		reconciler *TalosClusterReconciler
		tc         *talosv1alpha1.TalosCluster
	)

	reconcileOnce := func() {
		GinkgoHelper()

		res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(DefaultObserveInterval))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
	}

	BeforeEach(func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "observe-"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name

		for _, n := range []*corev1.Node{kubeNode(cpNodename), kubeNode("e2e-worker-1")} {
			Expect(k8sClient.Create(ctx, n)).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, n)).To(Succeed())
		}

		DeferCleanup(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
		})

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credentialsSecret, Namespace: ns},
			Data:       map[string][]byte{creds.DefaultTalosconfigKey: talostest.AdminTalosconfig(GinkgoT(), cpAddr.String())},
		})).To(Succeed())

		tc = &talosv1alpha1.TalosCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
			Spec:       talosv1alpha1.TalosClusterSpec{Credentials: secretCredentials()},
		}
		Expect(k8sClient.Create(ctx, tc)).To(Succeed())
		createPools(ns)

		scenario = observedCluster()
		recorder = events.NewFakeRecorder(50)
		reconciler = &TalosClusterReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Resolver: &target.Resolver{Client: k8sClient, Factory: &fake.Factory{Scenario: scenario}},
			Recorder: recorder,
		}
	})

	It("reports a healthy observed cluster without writing to Talos", func() {
		reconcileOnce()

		Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseObserving))
		Expect(condition(tc, talosv1alpha1.ConditionCredentialsValid).Status).To(Equal(metav1.ConditionTrue))
		Expect(condition(tc, talosv1alpha1.ConditionTalosAPIReachable).Status).To(Equal(metav1.ConditionTrue))
		Expect(condition(tc, talosv1alpha1.ConditionHealthy).Status).To(Equal(metav1.ConditionTrue))

		Expect(tc.Status.Observed).NotTo(BeNil())
		Expect(tc.Status.Observed.TalosVersions).To(Equal(map[string]int32{talosVersion: 2}))
		Expect(tc.Status.Observed.KubernetesVersion).To(Equal(kubernetesVersion))
		Expect(tc.Status.Observed.EtcdMembers).To(ConsistOf(talosv1alpha1.EtcdMember{ID: "a1", Hostname: cpNodename}))

		scenario.AssertNoMutations(GinkgoT())
	})

	It("does not write status when nothing changed", func() {
		reconcileOnce()
		rv := tc.ResourceVersion

		reconcileOnce()
		Expect(tc.ResourceVersion).To(Equal(rv))
	})

	It("reports the lowest kubelet version", func() {
		node := &corev1.Node{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "e2e-worker-1"}, node)).To(Succeed())
		node.Status.NodeInfo.KubeletVersion = "v1.36.4"
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

		reconcileOnce()
		Expect(tc.Status.Observed.KubernetesVersion).To(Equal("1.36.4"))
	})

	It("reports missing credentials and does not claim reachability", func() {
		Expect(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecret, Namespace: ns}})).To(Succeed())

		reconcileOnce()

		cond := condition(tc, talosv1alpha1.ConditionCredentialsValid)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonSecretNotFound))
		Expect(condition(tc, talosv1alpha1.ConditionTalosAPIReachable).Status).To(Equal(metav1.ConditionUnknown))
		Expect(scenario.Calls()).To(BeEmpty())
	})

	It("reports an unreachable Talos API and keeps the last observation", func() {
		reconcileOnce()
		previous := tc.Status.Observed.DeepCopy()

		scenario.FailNext(fake.MethodMembers, cpAddr, status.Error(codes.Unavailable, "connection refused"))
		reconcileOnce()

		cond := condition(tc, talosv1alpha1.ConditionTalosAPIReachable)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonUnreachable))
		Expect(tc.Status.Observed).To(Equal(previous))
	})

	It("asks a node that does not answer only once, and says which node it was", func() {
		scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.UnreachableWhen = func([]byte) bool { return true } })

		reconcileOnce()

		asked := 0

		for _, c := range scenario.Calls() {
			if c.Node == workerAddr {
				asked++
			}
		}

		Expect(asked).To(Equal(1), "every further read would only wait for its own timeout")

		reachable := condition(tc, talosv1alpha1.ConditionTalosAPIReachable)
		Expect(reachable.Status).To(Equal(metav1.ConditionFalse))
		Expect(reachable.Message).To(And(ContainSubstring("e2e-worker-1"), ContainSubstring(workerAddr.String())))

		// The node that answers is still read in full.
		Expect(tc.Status.Observed.TalosVersions).To(Equal(map[string]int32{talosVersion: 1}))
		Expect(condition(tc, talosv1alpha1.ConditionAdoptionPreconditionsMet).Status).To(Equal(metav1.ConditionUnknown))
	})

	It("ends a pass that runs out of time with the nodes it did not get to unknown, not fine", func() {
		reconciler.ObserveBudget = time.Nanosecond

		reconcileOnce()

		Expect(scenario.CallsFor(fake.MethodSystemInformation)).To(BeEmpty())

		reachable := condition(tc, talosv1alpha1.ConditionTalosAPIReachable)
		Expect(reachable.Status).To(Equal(metav1.ConditionFalse))
		Expect(reachable.Message).To(ContainSubstring("ran out of time"))
		Expect(condition(tc, talosv1alpha1.ConditionAdoptionPreconditionsMet).Status).To(Equal(metav1.ConditionUnknown))
		Expect(tc.Status.Adoption.ProposalRef).To(BeNil(), "nothing is proposed from a cluster that was not read")
	})

	It("does not blame Talos when the Kubernetes API fails", func() {
		reconcileOnce()
		Expect(tc.Status.Observed.KubernetesVersion).To(Equal(kubernetesVersion))

		watching, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())

		failing := interceptor.NewClient(watching, interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, nodes := list.(*corev1.NodeList); nodes {
					return apierrors.NewServiceUnavailable("etcd is compacting")
				}

				return c.List(ctx, list, opts...)
			},
		})
		reconciler.Client = failing
		reconciler.Resolver.Client = failing

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
		Expect(err).To(MatchError(And(ContainSubstring("Kubernetes API"), ContainSubstring("etcd is compacting"))),
			"the pass fails and is retried")

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
		Expect(condition(tc, talosv1alpha1.ConditionTalosAPIReachable).Status).To(Equal(metav1.ConditionTrue), "Talos answered")
		Expect(tc.Status.Observed.KubernetesVersion).To(Equal(kubernetesVersion), "what was not read keeps its last value")
	})

	It("reports an unhealthy cluster with the failing check", func() {
		scenario.SetHealth(talos.HealthResult{Messages: []string{"waiting for etcd to be healthy"}})

		reconcileOnce()

		cond := condition(tc, talosv1alpha1.ConditionHealthy)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Message).To(Equal("waiting for etcd to be healthy"))
	})

	It("emits events only on condition transitions", func() {
		reconcileOnce()
		Expect(drain(recorder)).To(ContainElement(ContainSubstring("TalosAPIReachable is True")))

		reconcileOnce()
		Expect(drain(recorder)).To(BeEmpty())

		scenario.SetHealth(talos.HealthResult{Messages: []string{"node not ready"}})
		reconcileOnce()
		Expect(drain(recorder)).To(ConsistOf(ContainSubstring("Healthy is False: node not ready")))
	})

	It("reports adoption preconditions and blocks on a pending staged config", func() {
		reconcileOnce()

		met := condition(tc, talosv1alpha1.ConditionAdoptionPreconditionsMet)
		Expect(met.Status).To(Equal(metav1.ConditionTrue), met.Message)
		Expect(tc.Status.Adoption.Preconditions).To(HaveLen(7))

		scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Persistent = []byte("version: v1alpha1\nstaged: true\n") })
		reconcileOnce()

		met = condition(tc, talosv1alpha1.ConditionAdoptionPreconditionsMet)
		Expect(met.Status).To(Equal(metav1.ConditionFalse))
		Expect(met.Message).To(ContainSubstring(plan.PreconditionNoPendingConfig))
	})

	It("lists live members that no TalosMachine claims", func() {
		reconcileOnce()
		Expect(tc.Status.Adoption.UnmatchedMembers).To(ConsistOf(cpNodename, "e2e-worker-1"))

		Expect(k8sClient.Create(ctx, machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID))).To(Succeed())
		reconcileOnce()
		Expect(tc.Status.Adoption.UnmatchedMembers).To(ConsistOf("e2e-worker-1"))

		Expect(k8sClient.Create(ctx, machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID))).To(Succeed())
		reconcileOnce()
		Expect(tc.Status.Adoption.UnmatchedMembers).To(BeEmpty())
	})

	It("records the observation time only when the observation changes", func() {
		reconciler.Now = func() time.Time { return time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC) }
		reconcileOnce()
		first := tc.Status.Observed.ObservedAt.Time

		reconciler.Now = func() time.Time { return first.Add(time.Hour) }
		reconcileOnce()
		Expect(tc.Status.Observed.ObservedAt.Time).To(BeTemporally("==", first))

		scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Version.Tag = newerTalosVersion })
		reconcileOnce()
		Expect(tc.Status.Observed.ObservedAt.Time).To(BeTemporally("==", first.Add(time.Hour)))
		Expect(tc.Status.Observed.TalosVersions).To(Equal(map[string]int32{talosVersion: 1, newerTalosVersion: 1}))
	})

	Context("Talos-issued credentials", func() {
		talosServiceAccount := func() *unstructured.Unstructured {
			GinkgoHelper()

			sa := &unstructured.Unstructured{}
			sa.SetGroupVersionKind(talosServiceAccountGVK)
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: tc.TalosServiceAccountName()}, sa)).To(Succeed())

			return sa
		}

		BeforeEach(func() {
			tc.Spec.Credentials = talosv1alpha1.Credentials{Source: talosv1alpha1.CredentialsSourceTalosServiceAccount}
			Expect(k8sClient.Update(ctx, tc)).To(Succeed())

			// Talos labels control planes; their InternalIPs are the endpoints the operator dials.
			cp := &corev1.Node{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cpNodename}, cp)).To(Succeed())
			cp.Labels = map[string]string{"node-role.kubernetes.io/control-plane": ""}
			Expect(k8sClient.Update(ctx, cp)).To(Succeed())
			cp.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: cpAddr.String()}}
			Expect(k8sClient.Status().Update(ctx, cp)).To(Succeed())
		})

		It("requests an admin certificate from Talos and waits for it", func() {
			reconcileOnce()

			sa := talosServiceAccount()
			roles, _, _ := unstructured.NestedStringSlice(sa.Object, "spec", "roles")
			Expect(roles).To(Equal([]string{"os:admin"}))
			Expect(sa.GetOwnerReferences()).To(ConsistOf(HaveField("UID", tc.UID)))

			cond := condition(tc, talosv1alpha1.ConditionCredentialsValid)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonAwaitingCredentials))
			Expect(scenario.Calls()).To(BeEmpty())
		})

		It("observes the cluster through control-plane IPs once Talos issues the Secret", func() {
			reconcileOnce()

			// Talos's part: issue the Secret named after the ServiceAccount.
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: tc.TalosServiceAccountName(), Namespace: ns},
				Data:       map[string][]byte{talosv1alpha1.TalosServiceAccountKey: talostest.IssuedTalosconfig(GinkgoT())},
			})).To(Succeed())

			reconcileOnce()

			cond := condition(tc, talosv1alpha1.ConditionCredentialsValid)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue), cond.Message)
			Expect(condition(tc, talosv1alpha1.ConditionTalosAPIReachable).Status).To(Equal(metav1.ConditionTrue))

			targets := reconciler.Resolver.Factory.(*fake.Factory).Targets()
			Expect(targets).NotTo(BeEmpty())
			Expect(targets[len(targets)-1].Endpoints).To(ConsistOf(HaveField("Addr", cpAddr)),
				"the control-plane IP is dialled, never talos.default")

			scenario.AssertNoMutations(GinkgoT())
		})

		It("reports Talos's refusal and restores the requested roles", func() {
			reconcileOnce()

			sa := talosServiceAccount()
			Expect(unstructured.SetNestedStringSlice(sa.Object, []string{"os:reader"}, "spec", "roles")).To(Succeed())
			Expect(unstructured.SetNestedField(sa.Object, "Roles not allowed: [os:admin]", "status", "failureReason")).To(Succeed())
			Expect(k8sClient.Update(ctx, sa)).To(Succeed())

			reconcileOnce()

			cond := condition(tc, talosv1alpha1.ConditionCredentialsValid)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonTalosAPIAccessDenied))
			Expect(cond.Message).To(ContainSubstring("Roles not allowed: [os:admin]"))
			Expect(cond.Message).To(ContainSubstring("namespace " + ns))

			sa = talosServiceAccount()
			roles, _, _ := unstructured.NestedStringSlice(sa.Object, "spec", "roles")
			Expect(roles).To(Equal([]string{"os:admin"}))

			reason, _, _ := unstructured.NestedString(sa.Object, "status", "failureReason")
			Expect(reason).NotTo(BeEmpty(), "the status Talos wrote is kept")
		})
	})

	Context("adoption proof", func() {
		var (
			machineReconciler *TalosMachineReconciler
			machines          []*talosv1alpha1.TalosMachine
		)

		adopted := func() *metav1.Condition {
			GinkgoHelper()

			return condition(tc, talosv1alpha1.ConditionAdopted)
		}

		dryRuns := func() int {
			n := 0

			for _, c := range scenario.CallsFor(fake.MethodApplyConfiguration) {
				Expect(c.DryRun).To(BeTrue(), "observe mode only ever dry-runs")

				n++
			}

			return n
		}

		// reconcileAll runs both controllers once, machines first, as the cluster's proof reads
		// their status.
		reconcileAll := func() {
			GinkgoHelper()

			for _, tm := range machines {
				_, err := machineReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tm)})
				Expect(err).NotTo(HaveOccurred())
			}

			reconcileOnce()
		}

		BeforeEach(func() {
			reproduceLiveConfigs(tc)
			Expect(k8sClient.Update(ctx, tc)).To(Succeed())

			// The first pass publishes the proposal and the recovered bundle the machines render with.
			reconcileOnce()
			Expect(condition(tc, talosv1alpha1.ConditionSecretsMatchLive).Status).To(Equal(metav1.ConditionTrue))

			machineReconciler = &TalosMachineReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Resolver: reconciler.Resolver}
			machines = []*talosv1alpha1.TalosMachine{
				machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID),
				machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID),
			}

			for _, tm := range machines {
				Expect(k8sClient.Create(ctx, tm)).To(Succeed())
			}
		})

		It("proves adoption with Talos's own dry run on every node", func() {
			reconcileAll()

			cond := adopted()
			Expect(cond.Status).To(Equal(metav1.ConditionTrue), cond.Message)
			Expect(tc.Status.Adoption.ProvenAt).NotTo(BeNil())
			Expect(tc.Status.Adoption.LiveConfigHashes).To(HaveLen(2))

			for _, tm := range machines {
				got := &talosv1alpha1.TalosMachine{}
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), got)).To(Succeed())
				Expect(tc.Status.Adoption.LiveConfigHashes).To(HaveKeyWithValue(tm.Name, got.Status.Config.LiveHash))
			}

			Expect(dryRuns()).To(Equal(2))
			scenario.AssertNoMutations(GinkgoT())

			// Nothing moved, so the proof stands without asking Talos again.
			reconcileAll()
			Expect(adopted().Status).To(Equal(metav1.ConditionTrue))
			Expect(dryRuns()).To(Equal(2))
		})

		It("refuses when Talos's dry run sees changes, without showing its diff", func() {
			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.DryRunReportsChanges = true })

			reconcileAll()

			cond := adopted()
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonServerReportsChanges))
			Expect(cond.Message).To(ContainSubstring("worker-1"))
			Expect(cond.Message).NotTo(ContainSubstring("cp-1"))
			Expect(tc.Status.Adoption.ProvenAt).To(BeNil())
		})

		It("invalidates the proof when a live config changes", func() {
			reconcileAll()
			Expect(adopted().Status).To(Equal(metav1.ConditionTrue))

			// Someone edits the worker with talosctl. The cluster notices on its next pass, before
			// the TalosMachine has even been re-compared.
			edited := liveConfig(talosmachine.TypeWorker)
			edited = append(edited, []byte("---\napiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  edited: \"true\"\n")...)
			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Active = edited })

			reconcileOnce()

			cond := adopted()
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonProofInvalidated))
			Expect(cond.Message).To(ContainSubstring("worker-1"))
		})

		Context("entering config: Manage", func() {
			setConfig := func(mode talosv1alpha1.ConfigManagement) {
				GinkgoHelper()

				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
				tc.Spec.Management.Config = mode
				Expect(k8sClient.Update(ctx, tc)).To(Succeed())
			}

			BeforeEach(func() {
				reconcileAll()
				Expect(adopted().Status).To(Equal(metav1.ConditionTrue))
			})

			It("hands over only once Talos confirms that taking ownership changes nothing", func() {
				setConfig(talosv1alpha1.ConfigManagementManage)
				reconcileAll()

				handover := condition(tc, talosv1alpha1.ConditionConfigHandover)
				Expect(handover.Status).To(Equal(metav1.ConditionTrue), handover.Message)
				Expect(condition(tc, talosv1alpha1.ConditionConfigInSync).Status).To(Equal(metav1.ConditionTrue))
				Expect(condition(tc, talosv1alpha1.ConditionRolloutBlocked)).To(BeNil())
				Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseManaged))

				// Two dry runs for the proof, two more for the handover; none after that.
				Expect(dryRuns()).To(Equal(4))
				reconcileAll()
				Expect(dryRuns()).To(Equal(4))

				scenario.AssertNoMutations(GinkgoT())
			})

			It("reports drift after the handover and acts on nothing", func() {
				setConfig(talosv1alpha1.ConfigManagementManage)
				reconcileAll()

				edited := liveConfig(talosmachine.TypeWorker, "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  edited: \"true\"\n")
				scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Active = edited })
				reconcileAll()

				inSync := condition(tc, talosv1alpha1.ConditionConfigInSync)
				Expect(inSync.Status).To(Equal(metav1.ConditionFalse), inSync.Message)
				Expect(inSync.Reason).To(Equal(talosv1alpha1.ReasonDrift))
				Expect(inSync.Message).To(ContainSubstring("worker-1"))
				Expect(condition(tc, talosv1alpha1.ConditionConfigHandover).Status).To(Equal(metav1.ConditionTrue), "drift does not undo the handover")
				Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseManaged))

				scenario.AssertNoMutations(GinkgoT())
			})

			It("degrades and blocks rollouts when taking ownership would change a node, until it would not", func() {
				scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.DryRunReportsChanges = true })
				setConfig(talosv1alpha1.ConfigManagementManage)
				reconcileAll()

				handover := condition(tc, talosv1alpha1.ConditionConfigHandover)
				Expect(handover.Status).To(Equal(metav1.ConditionFalse))
				Expect(handover.Reason).To(Equal(talosv1alpha1.ReasonServerReportsChanges))
				Expect(handover.Message).To(ContainSubstring("worker-1"))
				Expect(condition(tc, talosv1alpha1.ConditionRolloutBlocked).Status).To(Equal(metav1.ConditionTrue))
				Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))
				Expect(drain(recorder)).To(ContainElement(ContainSubstring("ConfigHandover is False")))
				scenario.AssertNoMutations(GinkgoT())

				scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.DryRunReportsChanges = false })
				reconcileAll()

				Expect(condition(tc, talosv1alpha1.ConditionConfigHandover).Status).To(Equal(metav1.ConditionTrue))
				Expect(condition(tc, talosv1alpha1.ConditionRolloutBlocked)).To(BeNil())
				Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseManaged))
			})

			It("verifies the handover again after returning to Observe", func() {
				setConfig(talosv1alpha1.ConfigManagementManage)
				reconcileAll()
				Expect(condition(tc, talosv1alpha1.ConditionConfigHandover).Status).To(Equal(metav1.ConditionTrue))

				setConfig(talosv1alpha1.ConfigManagementObserve)
				reconcileAll()
				Expect(condition(tc, talosv1alpha1.ConditionConfigHandover)).To(BeNil())
				Expect(condition(tc, talosv1alpha1.ConditionConfigInSync)).To(BeNil())
				Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseObserving))

				before := dryRuns()
				setConfig(talosv1alpha1.ConfigManagementManage)
				reconcileAll()
				Expect(condition(tc, talosv1alpha1.ConditionConfigHandover).Status).To(Equal(metav1.ConditionTrue))
				Expect(dryRuns()).To(Equal(before + 2))
			})
		})

		It("waits until every live member has a TalosMachine", func() {
			Expect(k8sClient.Delete(ctx, machines[1])).To(Succeed())
			machines = machines[:1]

			reconcileAll()

			cond := adopted()
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonPreconditionUnmet))
			Expect(cond.Message).To(ContainSubstring("e2e-worker-1"))
			Expect(dryRuns()).To(BeZero())
		})
	})

	Context("adoption proposal", func() {
		bundleSecret := func() *corev1.Secret {
			GinkgoHelper()

			s := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName + "-talos-secrets"}, s)).To(Succeed())

			return s
		}

		proposal := func() *corev1.ConfigMap {
			GinkgoHelper()

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName + "-adoption-proposal"}, cm)).To(Succeed())

			return cm
		}

		It("proposes specs that reproduce the cluster and stores the recovered bundle", func() {
			reconcileOnce()

			proposed := condition(tc, talosv1alpha1.ConditionAdoptionProposed)
			Expect(proposed.Status).To(Equal(metav1.ConditionTrue), proposed.Message)
			Expect(tc.Status.Adoption.ProposalRef).To(Equal(&corev1.LocalObjectReference{Name: clusterName + "-adoption-proposal"}))
			Expect(condition(tc, talosv1alpha1.ConditionSecretsMatchLive).Status).To(Equal(metav1.ConditionTrue))

			cm := proposal()
			Expect(cm.OwnerReferences).To(ConsistOf(HaveField("UID", tc.UID)))

			cluster := &talosv1alpha1.TalosCluster{}
			Expect(sigsyaml.Unmarshal([]byte(cm.Data["taloscluster.yaml"]), cluster)).To(Succeed())
			Expect(cluster.Spec.ClusterName).To(Equal("e2e"))
			Expect(cluster.Spec.Versions.Talos).To(Equal(talosVersion))
			Expect(cluster.Spec.Management.Config).To(Equal(talosv1alpha1.ConfigManagementObserve))
			Expect(cm.Data["talosmachines.yaml"]).To(ContainSubstring("systemUUID: " + cpUUID))
			Expect(cm.Data["talosmachines.yaml"]).To(ContainSubstring("systemUUID: " + workerUUID))

			Expect(cm.Data["taloscluster.yaml"]).To(ContainSubstring("# renovate: datasource=github-releases depName=siderolabs/talos"))

			for key, value := range cm.Data {
				for _, secret := range secretMaterial(talostest.Bundle(GinkgoT())) {
					Expect(value).NotTo(ContainSubstring(secret), "key %s of the proposal leaks secret material", key)
				}
			}

			// The proposal applies as is: CRD schemas and CEL rules accept it.
			target := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "proposal-"}}
			Expect(k8sClient.Create(ctx, target)).To(Succeed())

			cluster.Namespace = target.Name
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			// One pool per role, each the cluster's.
			pools := map[string]talosv1alpha1.MachineRole{}

			for doc := range strings.SplitSeq(cm.Data["talosmachinepools.yaml"], "\n---\n") {
				pool := &talosv1alpha1.TalosMachinePool{}
				Expect(sigsyaml.Unmarshal([]byte(doc), pool)).To(Succeed())
				Expect(pool.Spec.ClusterRef.Name).To(Equal(clusterName))

				pools[pool.Name] = pool.Spec.Role
				pool.Namespace = target.Name
				Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			}

			Expect(pools).To(Equal(map[string]talosv1alpha1.MachineRole{
				clusterName + "-control-plane": talosv1alpha1.MachineRoleControlPlane,
				clusterName + "-workers":       talosv1alpha1.MachineRoleWorker,
			}))

			for doc := range strings.SplitSeq(cm.Data["talosmachines.yaml"], "\n---\n") {
				tm := &talosv1alpha1.TalosMachine{}
				Expect(sigsyaml.Unmarshal([]byte(doc), tm)).To(Succeed())
				Expect(pools).To(HaveKey(tm.Spec.PoolRef.Name), "TalosMachine %s joins a proposed pool", tm.Name)

				tm.Namespace = target.Name
				Expect(k8sClient.Create(ctx, tm)).To(Succeed())
			}

			// The PKI must outlive the TalosCluster: no owner reference.
			s := bundleSecret()
			Expect(s.OwnerReferences).To(BeEmpty())
			Expect(s.Labels).To(HaveKeyWithValue(talosv1alpha1.LabelCluster, clusterName))

			recovered, err := adopt.DecodeBundle(s.Data[talosv1alpha1.DefaultSecretsKey])
			Expect(err).NotTo(HaveOccurred())
			Expect(recovered.Certs).To(Equal(talostest.Bundle(GinkgoT()).Certs))

			backup := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName + "-adoption-backup"}, backup)).To(Succeed())
			Expect(backup.OwnerReferences).To(BeEmpty())
			Expect(backup.Data).To(HaveKey(cpNodename + ".yaml"))
			Expect(backup.Data).To(HaveKey("e2e-worker-1.yaml"))

			scenario.AssertNoMutations(GinkgoT())
		})

		It("reports the bundle backed up once the Secret carries its hash", func() {
			reconcileOnce()

			backedUp := condition(tc, talosv1alpha1.ConditionSecretsBackedUp)
			Expect(backedUp.Status).To(Equal(metav1.ConditionFalse))
			Expect(backedUp.Reason).To(Equal(talosv1alpha1.ReasonAwaitingBackup))

			hash, err := adopt.BundleHash(talostest.Bundle(GinkgoT()))
			Expect(err).NotTo(HaveOccurred())
			Expect(backedUp.Message).To(ContainSubstring(hash))

			s := bundleSecret()
			s.Annotations = map[string]string{talosv1alpha1.AnnotationSecretsBackedUp: hash}
			Expect(k8sClient.Update(ctx, s)).To(Succeed())

			reconcileOnce()
			Expect(condition(tc, talosv1alpha1.ConditionSecretsBackedUp).Status).To(Equal(metav1.ConditionTrue))
		})

		It("verifies a supplied bundle and keeps it as is", func() {
			data, err := adopt.EncodeBundle(talostest.Bundle(GinkgoT()))
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-talos-secrets", Namespace: ns},
				Data:       map[string][]byte{talosv1alpha1.DefaultSecretsKey: data},
			})).To(Succeed())

			reconcileOnce()

			Expect(condition(tc, talosv1alpha1.ConditionSecretsMatchLive).Status).To(Equal(metav1.ConditionTrue))
			Expect(condition(tc, talosv1alpha1.ConditionAdoptionProposed).Status).To(Equal(metav1.ConditionTrue))
			Expect(bundleSecret().Data[talosv1alpha1.DefaultSecretsKey]).To(Equal(data))
		})

		It("refuses a supplied bundle that does not match the cluster", func() {
			stale, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
			Expect(err).NotTo(HaveOccurred())

			data, err := adopt.EncodeBundle(stale)
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-talos-secrets", Namespace: ns},
				Data:       map[string][]byte{talosv1alpha1.DefaultSecretsKey: data},
			})).To(Succeed())

			reconcileOnce()

			match := condition(tc, talosv1alpha1.ConditionSecretsMatchLive)
			Expect(match.Status).To(Equal(metav1.ConditionFalse))
			Expect(match.Reason).To(Equal(talosv1alpha1.ReasonMismatch))
			Expect(match.Message).To(ContainSubstring("Talos API CA"))
			Expect(condition(tc, talosv1alpha1.ConditionAdoptionProposed).Status).To(Equal(metav1.ConditionFalse))

			// The user's Secret is never overwritten and no proposal is published.
			Expect(bundleSecret().Data[talosv1alpha1.DefaultSecretsKey]).To(Equal(data))
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName + "-adoption-proposal"}, &corev1.ConfigMap{})).
				To(Satisfy(apierrors.IsNotFound))
		})

		It("rebuilds the proposal only when the live cluster changes", func() {
			reconcileOnce()

			cm := proposal()
			cm.Data["notes.md"] = "edited\n"
			Expect(k8sClient.Update(ctx, cm)).To(Succeed())

			reconcileOnce()
			Expect(proposal().Data["notes.md"]).To(Equal("edited\n"))

			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Version.Tag = newerTalosVersion })
			reconcileOnce()
			Expect(proposal().Data["notes.md"]).To(ContainSubstring("e2e-worker-1 runs Talos " + newerTalosVersion))
		})

		It("does not propose while a precondition is unmet", func() {
			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Persistent = []byte("version: v1alpha1\nstaged: true\n") })

			reconcileOnce()

			Expect(condition(tc, talosv1alpha1.ConditionAdoptionProposed)).To(BeNil())
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName + "-talos-secrets"}, &corev1.Secret{})).
				To(Satisfy(apierrors.IsNotFound))
		})
	})
})

// secretMaterial lists a bundle's secret values as they appear in configs: base64-encoded keys and
// plain tokens.
func secretMaterial(b *secrets.Bundle) []string {
	keys := [][]byte{b.Certs.OS.Key, b.Certs.Etcd.Key, b.Certs.K8s.Key, b.Certs.K8sAggregator.Key, b.Certs.K8sServiceAccount.Key}

	out := make([]string, 0, len(keys)+4)
	out = append(out, b.Secrets.BootstrapToken, b.Secrets.SecretboxEncryptionSecret, b.TrustdInfo.Token, b.Cluster.Secret)

	for _, key := range keys {
		out = append(out, base64.StdEncoding.EncodeToString(key))
	}

	return slices.DeleteFunc(out, func(s string) bool { return s == "" })
}

// Pools of the spec's "cluster", one per role; createPools makes them.
const (
	controlPlanePool = "control-plane"
	workerPool       = "workers"
)

// poolFor names the spec cluster's pool of a role.
func poolFor(role talosv1alpha1.MachineRole) string {
	if role == talosv1alpha1.MachineRoleControlPlane {
		return controlPlanePool
	}

	return workerPool
}

// createPools creates the spec cluster's pools, one per role, with no config of their own.
func createPools(ns string) {
	GinkgoHelper()

	for _, role := range []talosv1alpha1.MachineRole{talosv1alpha1.MachineRoleControlPlane, talosv1alpha1.MachineRoleWorker} {
		Expect(k8sClient.Create(ctx, &talosv1alpha1.TalosMachinePool{
			ObjectMeta: metav1.ObjectMeta{Name: poolFor(role), Namespace: ns},
			Spec: talosv1alpha1.TalosMachinePoolSpec{
				ClusterRef: corev1.LocalObjectReference{Name: clusterName},
				Role:       role,
			},
		})).To(Succeed())
	}
}

// machine returns a TalosMachine in the spec cluster's pool of role.
func machine(ns, name string, role talosv1alpha1.MachineRole, addr netip.Addr, uuid string) *talosv1alpha1.TalosMachine {
	return &talosv1alpha1.TalosMachine{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: talosv1alpha1.TalosMachineSpec{
			PoolRef:  corev1.LocalObjectReference{Name: poolFor(role)},
			Address:  addr.String(),
			Identity: talosv1alpha1.MachineIdentity{SystemUUID: uuid},
		},
	}
}

// drain returns the events recorded so far.
func drain(r *events.FakeRecorder) []string {
	var out []string

	for {
		select {
		case e := <-r.Events:
			out = append(out, fmt.Sprint(e))
		default:
			return out
		}
	}
}
