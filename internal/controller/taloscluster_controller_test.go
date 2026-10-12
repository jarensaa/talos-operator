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
		MachineStatus:     talos.MachineStatus{Stage: talos.StageRunning, Ready: true},
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

		// A pass that changes the TalosCluster itself ends there, and the change brings the next
		// one: the spec filled in while adopting, ownership given up once adopted.
		for range 4 {
			res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
			Expect(err).NotTo(HaveOccurred())

			if res.RequeueAfter != 0 {
				Expect(res.RequeueAfter).To(Equal(DefaultObserveInterval))
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())

				return
			}
		}

		Fail("the reconciler kept changing the TalosCluster")
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

		Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopting))
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
		// The first pass writes the cluster's resources, the second finds them.
		reconcileOnce()
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

		var machines talosv1alpha1.TalosMachineList
		Expect(k8sClient.List(ctx, &machines, client.InNamespace(ns))).To(Succeed())
		Expect(machines.Items).To(BeEmpty(), "nothing is derived from a cluster that was not read")
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
		// A cluster that is not safe to adopt gets no resources written, so its members stay
		// unclaimed until someone writes them.
		scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Persistent = []byte("version: v1alpha1\nstaged: true\n") })

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

			return condition(tc, talosv1alpha1.ConditionConfigProven)
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

			// These specs bring their own resources, under their own names: every node has its
			// TalosMachine before the operator looks, so the operator writes none.
			machineReconciler = &TalosMachineReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Resolver: reconciler.Resolver}
			machines = []*talosv1alpha1.TalosMachine{
				machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID),
				machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID),
			}

			for _, tm := range machines {
				Expect(k8sClient.Create(ctx, tm)).To(Succeed())
			}

			// The first pass stores the recovered bundle the machines render with.
			reconcileOnce()
			Expect(condition(tc, talosv1alpha1.ConditionSecretsMatchLive).Status).To(Equal(metav1.ConditionTrue))
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
				Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopted))

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

	// Adoption writes the resources that describe the live cluster, until the cluster is adopted
	// (docs/scope.md §7.1, §12.14).
	Context("adoption", func() {
		const workerNodename = "e2e-worker-1"

		var (
			machineReconciler *TalosMachineReconciler

			controlPlanes = clusterName + "-control-plane"
			workers       = clusterName + "-workers"
		)

		bundleSecret := func() *corev1.Secret {
			GinkgoHelper()

			s := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName + "-talos-secrets"}, s)).To(Succeed())

			return s
		}

		getPool := func(name string) *talosv1alpha1.TalosMachinePool {
			GinkgoHelper()

			pool := &talosv1alpha1.TalosMachinePool{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, pool)).To(Succeed())

			return pool
		}

		getMachine := func(name string) *talosv1alpha1.TalosMachine {
			GinkgoHelper()

			tm := &talosv1alpha1.TalosMachine{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, tm)).To(Succeed())

			return tm
		}

		machineNames := func() []string {
			GinkgoHelper()

			var list talosv1alpha1.TalosMachineList
			Expect(k8sClient.List(ctx, &list, client.InNamespace(ns))).To(Succeed())

			names := make([]string, 0, len(list.Items))
			for _, tm := range list.Items {
				names = append(names, tm.Name)
			}

			return names
		}

		derived := func() *metav1.Condition {
			GinkgoHelper()

			c := condition(tc, talosv1alpha1.ConditionResourcesDerived)
			Expect(c).NotTo(BeNil())

			return c
		}

		// reconcileAll runs the machines' controller for every TalosMachine, then the cluster's.
		reconcileAll := func() {
			GinkgoHelper()

			for _, name := range machineNames() {
				_, err := machineReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}})
				Expect(err).NotTo(HaveOccurred())
			}

			reconcileOnce()
		}

		// runAdoption lets adoption run to its end.
		runAdoption := func() {
			GinkgoHelper()

			reconcileOnce()
			reconcileAll()
			reconcileAll()

			proven := condition(tc, talosv1alpha1.ConditionConfigProven)
			Expect(proven).NotTo(BeNil())
			Expect(proven.Status).To(Equal(metav1.ConditionTrue), proven.Message)
			Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopted))
		}

		// ownsSpec reports whether the operator is still recorded as owning any of obj's spec.
		ownsSpec := func(obj client.Object) bool {
			for _, e := range obj.GetManagedFields() {
				if e.Manager == adoptionManager && e.Subresource == "" && e.FieldsV1 != nil && strings.Contains(e.FieldsV1.GetRawString(), `"f:spec"`) {
					return true
				}
			}

			return false
		}

		// asGitOps applies a resource the way a GitOps tool does: server-side, under its own
		// name, taking over what it applies.
		asGitOps := func(kind string, obj client.Object, spec any) {
			GinkgoHelper()

			Expect(k8sClient.Apply(ctx, applyConfiguration(clusterName, kind, obj, spec),
				client.FieldOwner("gitops"), client.ForceOwnership)).To(Succeed())
		}

		// editWorker changes the worker's live config, as someone with talosctl would.
		editWorker := func() {
			edited := liveConfig(talosmachine.TypeWorker, editedLabel)
			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Active = edited })
		}

		BeforeEach(func() {
			machineReconciler = &TalosMachineReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Resolver: reconciler.Resolver}
		})

		It("writes the resources that describe the live cluster and arrives at Adopted with nothing applied by hand", func() {
			reconcileOnce()

			Expect(derived().Status).To(Equal(metav1.ConditionTrue), derived().Message)
			Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopting))
			Expect(condition(tc, talosv1alpha1.ConditionSecretsMatchLive).Status).To(Equal(metav1.ConditionTrue))

			By("filling in what the TalosCluster's spec left empty")
			Expect(tc.Spec.ClusterName).To(Equal("e2e"))
			Expect(tc.Spec.ControlPlaneEndpoint).To(Equal("https://" + cpAddr.String() + ":6443"))
			Expect(tc.Spec.Versions).To(Equal(&talosv1alpha1.Versions{Talos: talosVersion, Kubernetes: kubernetesVersion}))
			Expect(tc.Spec.ConfigGeneration.Installer.SchematicID).To(Equal(schematic))
			Expect(tc.Spec.Management.Config).To(Equal(talosv1alpha1.ConfigManagementObserve), "adoption never switches management on")
			Expect(tc.Spec.Credentials).To(Equal(secretCredentials()), "what was set stays")

			By("writing one pool per role and one machine per node, named after it and pinned to its hardware")
			Expect(getPool(controlPlanes).Spec.Role).To(Equal(talosv1alpha1.MachineRoleControlPlane))
			Expect(getPool(workers).Spec.Role).To(Equal(talosv1alpha1.MachineRoleWorker))
			Expect(getPool(workers).Spec.ClusterRef.Name).To(Equal(clusterName))
			Expect(machineNames()).To(ConsistOf(cpNodename, workerNodename))

			cp, worker := getMachine(cpNodename), getMachine(workerNodename)
			Expect(cp.Spec.PoolRef.Name).To(Equal(controlPlanes))
			Expect(cp.Spec.Identity.SystemUUID).To(Equal(cpUUID))
			Expect(cp.Spec.Address).To(Equal(cpAddr.String()))
			Expect(worker.Spec.PoolRef.Name).To(Equal(workers))
			Expect(worker.Spec.Identity.SystemUUID).To(Equal(workerUUID))

			By("keeping every secret out of them")
			for _, obj := range []any{tc.Spec, getPool(controlPlanes).Spec, getPool(workers).Spec, cp.Spec, worker.Spec} {
				raw, err := sigsyaml.Marshal(obj)
				Expect(err).NotTo(HaveOccurred())

				for _, secret := range secretMaterial(talostest.Bundle(GinkgoT())) {
					Expect(string(raw)).NotTo(ContainSubstring(secret), "a derived spec leaks secret material")
				}
			}

			By("storing the recovered bundle and a backup of every config, neither owned by the TalosCluster")
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
			Expect(backup.Data).To(HaveKey(workerNodename + ".yaml"))

			By("proving that they render what the nodes run")
			reconcileAll()
			reconcileAll()

			proven := condition(tc, talosv1alpha1.ConditionConfigProven)
			Expect(proven.Status).To(Equal(metav1.ConditionTrue), proven.Message)
			Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopted))
			Expect(tc.Status.Adoption.AdoptedAt).NotTo(BeNil())
			Expect(derived().Reason).To(Equal(talosv1alpha1.ReasonHandedOver))

			scenario.AssertNoMutations(GinkgoT())
		})

		It("never overwrites a field of the TalosCluster that is set", func() {
			tc.Spec.ControlPlaneEndpoint = "https://cluster.example.com:6443"
			Expect(k8sClient.Update(ctx, tc)).To(Succeed())

			reconcileOnce()

			Expect(tc.Spec.ControlPlaneEndpoint).To(Equal("https://cluster.example.com:6443"))
			Expect(tc.Spec.ClusterName).To(Equal("e2e"), "what was empty is still filled in")

			// The machines then render another endpoint than the nodes run, which they report.
			reconcileAll()
			reconcileAll()

			Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopting))
			Expect(condition(tc, talosv1alpha1.ConditionConfigProven).Status).To(Equal(metav1.ConditionFalse))
			Expect(meta.IsStatusConditionFalse(getMachine(cpNodename).Status.Conditions, talosv1alpha1.ConditionConfigMatchesLive)).To(BeTrue())
		})

		It("does not give a node a second TalosMachine when it has one under another name", func() {
			Expect(k8sClient.Create(ctx, machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID))).To(Succeed())

			reconcileOnce()

			Expect(machineNames()).To(ConsistOf("cp-1", workerNodename))
			Expect(getPool(workers).Spec.Role).To(Equal(talosv1alpha1.MachineRoleWorker))
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: controlPlanes}, &talosv1alpha1.TalosMachinePool{})).
				To(Satisfy(apierrors.IsNotFound), "no pool is written that no written machine is in")
			Expect(getMachine("cp-1").Spec.PoolRef.Name).To(Equal(controlPlanePool), "the machine that was there is untouched")
			Expect(derived().Status).To(Equal(metav1.ConditionTrue), "a node described under another name is described: %s", derived().Message)
		})

		It("keeps the resources in line with the cluster for as long as it is being adopted", func() {
			reconcileOnce()
			Expect(getMachine(workerNodename).Spec.Config).To(BeEmpty())
			Expect(getPool(workers).Spec.Config).To(BeEmpty())

			editWorker()
			reconcileOnce()

			// One worker: what it sets beyond the defaults is what its role shares.
			Expect(getPool(workers).Spec.Config).To(HaveLen(1))
			Expect(getPool(workers).Spec.Config[0].Inline).To(ContainSubstring("edited"))
			Expect(derived().Status).To(Equal(metav1.ConditionTrue), derived().Message)

			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Version.Tag = newerTalosVersion })
			reconcileOnce()
			Expect(tc.Status.Adoption.Notes).To(ContainElement(ContainSubstring(workerNodename + " runs Talos " + newerTalosVersion)))
		})

		It("leaves a resource as it is once someone else changed it, and says which", func() {
			reconcileOnce()

			worker := getMachine(workerNodename)
			worker.Spec.Address = "10.5.0.99"
			Expect(k8sClient.Update(ctx, worker)).To(Succeed())

			// The cluster changes, so the resources are derived again.
			editWorker()
			reconcileOnce()

			Expect(getMachine(workerNodename).Spec.Address).To(Equal("10.5.0.99"), "what someone else set is theirs")
			Expect(derived().Status).To(Equal(metav1.ConditionFalse))
			Expect(derived().Reason).To(Equal(talosv1alpha1.ReasonChangedElsewhere))
			Expect(derived().Message).To(ContainSubstring("TalosMachine " + workerNodename))
			Expect(derived().Message).NotTo(ContainSubstring(cpNodename))

			// What nobody else touched still follows the cluster.
			Expect(getPool(workers).Spec.Config).To(HaveLen(1))
		})

		It("leaves a machine alone that belongs to another cluster's pool", func() {
			foreign := machine(ns, workerNodename, talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
			Expect(k8sClient.Create(ctx, foreign)).To(Succeed())

			reconcileOnce()

			Expect(getMachine(workerNodename).Spec.PoolRef.Name).To(Equal(workerPool))
			Expect(derived().Reason).To(Equal(talosv1alpha1.ReasonChangedElsewhere))
			Expect(derived().Message).To(ContainSubstring("TalosMachine " + workerNodename))
		})

		It("stops writing the resources for good once the cluster is adopted", func() {
			runAdoption()

			generations := map[string]int64{
				workers:        getPool(workers).Generation,
				workerNodename: getMachine(workerNodename).Generation,
				clusterName:    tc.Generation,
			}

			By("changing the cluster under the adopted resources")
			editWorker()
			reconcileAll()
			reconcileAll()

			Expect(getPool(workers).Generation).To(Equal(generations[workers]))
			Expect(getPool(workers).Spec.Config).To(BeEmpty(), "the resources no longer follow the cluster")
			Expect(getMachine(workerNodename).Generation).To(Equal(generations[workerNodename]))
			Expect(tc.Generation).To(Equal(generations[clusterName]))

			By("reporting that they no longer match, without going back")
			proven := condition(tc, talosv1alpha1.ConditionConfigProven)
			Expect(proven.Status).To(Equal(metav1.ConditionFalse))
			Expect(proven.Reason).To(Equal(talosv1alpha1.ReasonProofInvalidated))
			Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopted))

			By("not bringing back a resource that was deleted")
			cp := getMachine(cpNodename)
			cp.Annotations = map[string]string{talosv1alpha1.AnnotationOrphan: "true"}
			Expect(k8sClient.Update(ctx, cp)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cp)).To(Succeed())

			reconcileAll()
			reconcileAll()
			Expect(machineNames()).To(ConsistOf(workerNodename))
		})

		It("gives up its ownership once adopted, so that a field the next owner removes is really removed", func() {
			editWorker()
			runAdoption()
			Expect(getPool(workers).Spec.Config).To(HaveLen(1))

			// Ownership is given up in the passes after the one that adopted.
			reconcileOnce()

			for _, obj := range []client.Object{tc, getPool(workers), getPool(controlPlanes), getMachine(workerNodename), getMachine(cpNodename)} {
				Expect(ownsSpec(obj)).To(BeFalse(), "%T %s still names the operator as an owner of its spec", obj, obj.GetName())
			}

			Expect(getPool(workers).Spec.Config).To(HaveLen(1), "giving up ownership changes nothing of the resource")
			Expect(tc.Spec.ClusterName).To(Equal("e2e"))

			By("taking the pool over as a GitOps tool does, then removing its config there")
			pool := getPool(workers)
			taken := pool.Spec
			asGitOps("TalosMachinePool", pool, taken)
			Expect(getPool(workers).Spec.Config).To(HaveLen(1))

			taken.Config = nil
			asGitOps("TalosMachinePool", pool, taken)
			Expect(getPool(workers).Spec.Config).To(BeEmpty(), "a field its only owner drops must go")

			By("doing the same with a machine, which also carries the status its own controller writes")
			reconcileAll()

			worker := getMachine(workerNodename)
			withConfig := worker.Spec
			withConfig.Config = []talosv1alpha1.ConfigSource{{Inline: editedLabel}}
			asGitOps("TalosMachine", worker, withConfig)
			asGitOps("TalosMachine", worker, worker.Spec)
			Expect(getMachine(workerNodename).Spec.Config).To(BeEmpty())
		})

		It("does not overwrite what someone else manages when its record of the adoption is lost", func() {
			runAdoption()
			reconcileOnce()

			By("taking the worker's machine over and changing it")
			worker := getMachine(workerNodename)
			taken := worker.Spec
			taken.Address = "10.5.0.99"
			asGitOps("TalosMachine", worker, taken)

			By("losing the status, as when the TalosCluster is restored from a backup of its spec")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
			tc.Status = talosv1alpha1.TalosClusterStatus{}
			Expect(k8sClient.Status().Update(ctx, tc)).To(Succeed())

			reconcileOnce()

			Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopting), "without its record the operator is adopting again")
			Expect(getMachine(workerNodename).Spec.Address).To(Equal("10.5.0.99"), "and still does not touch what is someone else's")
			Expect(derived().Reason).To(Equal(talosv1alpha1.ReasonChangedElsewhere))
			Expect(derived().Message).To(ContainSubstring("TalosMachine " + workerNodename))
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
			Expect(derived().Status).To(Equal(metav1.ConditionTrue), derived().Message)
			Expect(bundleSecret().Data[talosv1alpha1.DefaultSecretsKey]).To(Equal(data))
		})

		It("refuses a supplied bundle that does not match the cluster, and writes nothing", func() {
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
			Expect(derived().Status).To(Equal(metav1.ConditionFalse))

			// The user's Secret is never overwritten, and nothing is derived from a cluster whose
			// secrets are not the ones at hand.
			Expect(bundleSecret().Data[talosv1alpha1.DefaultSecretsKey]).To(Equal(data))
			Expect(machineNames()).To(BeEmpty())
			Expect(tc.Spec.ClusterName).To(BeEmpty())
		})

		It("derives the resources again only when the live cluster changes", func() {
			reconcileOnce()

			applied := func() []metav1.ManagedFieldsEntry { return getPool(workers).ManagedFields }
			before := applied()

			reconcileOnce()
			reconcileOnce()
			Expect(applied()).To(Equal(before))
			Expect(getPool(workers).Generation).To(Equal(int64(1)))

			editWorker()
			reconcileOnce()
			Expect(getPool(workers).Generation).To(Equal(int64(2)))
		})

		It("writes nothing while a precondition is unmet", func() {
			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Persistent = []byte("version: v1alpha1\nstaged: true\n") })

			reconcileOnce()

			Expect(condition(tc, talosv1alpha1.ConditionResourcesDerived)).To(BeNil())
			Expect(machineNames()).To(BeEmpty())
			Expect(tc.Spec.ClusterName).To(BeEmpty())
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
