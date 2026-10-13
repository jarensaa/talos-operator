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
	"crypto/x509"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

// A Talos upgrade: another version, or another installer, installed to every node in turn
// (docs/scope.md §7.3).
var _ = Describe("Talos upgrade", func() {
	var h *rolloutHarness

	BeforeEach(func() { h = startRollout() })

	newerImage := "factory.talos.dev/metal-installer/" + schematic + ":" + newerTalosVersion

	// called lists the machines a Talos call went to, in order.
	called := func(method string) []string {
		var out []string

		for _, c := range h.scenario.CallsFor(method) {
			for _, n := range rolloutNodes {
				if n.addr == c.Node {
					out = append(out, n.name)
				}
			}
		}

		return out
	}

	kubeNodeOf := func(machine string) *corev1.Node {
		GinkgoHelper()

		kube := &corev1.Node{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: h.node(machine).node}, kube)).To(Succeed())

		return kube
	}

	cordoned := func(machine string) bool {
		GinkgoHelper()

		return kubeNodeOf(machine).Spec.Unschedulable
	}

	// addPod puts a running pod on a machine's node.
	addPod := func(machine, name string, mutate func(*corev1.Pod)) *corev1.Pod {
		GinkgoHelper()

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.ns, Labels: map[string]string{"app": name}},
			Spec: corev1.PodSpec{
				NodeName:                      h.node(machine).node,
				TerminationGracePeriodSeconds: new(int64),
				Containers:                    []corev1.Container{{Name: "main", Image: "registry.example.com/app:1"}},
			},
		}
		if mutate != nil {
			mutate(pod)
		}

		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		return pod
	}

	exists := func(pod *corev1.Pod) bool {
		GinkgoHelper()

		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
		if err != nil {
			Expect(err).To(Satisfy(apierrors.IsNotFound))
		}

		return err == nil
	}

	// upgradeTo asks for another Talos version, with upgrades enabled.
	upgradeTo := func(version string) {
		GinkgoHelper()

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Versions.Talos = version
			tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
		})
	}

	done := func(n int) func() bool {
		return func() bool { return h.operation() == nil || len(h.operation().Completed) >= n }
	}

	down := func(machine string) func() bool {
		return func() bool { return h.scenario.Down(h.node(machine).addr) }
	}

	// reboots lets the upgrade take a machine down, and brings it back.
	reboots := func(machine string) {
		GinkgoHelper()

		h.runUntil(machine+" is rebooted", down(machine))
		Expect(h.scenario.Boot(h.node(machine).addr)).To(BeTrue())
	}

	upToDate := func() *metav1.Condition {
		GinkgoHelper()

		c := condition(h.tc, talosv1alpha1.ConditionTalosUpToDate)
		Expect(c).NotTo(BeNil())

		return c
	}

	It("reports every node up to date while nothing is asked for", func() {
		h.settle(nil)

		Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
		Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
	})

	It("takes every node to the new version, one at a time: installed, drained, rebooted and verified", func() {
		app := addPod(worker1, "app", nil)
		daemon := addPod(worker1, "daemon", func(pod *corev1.Pod) {
			pod.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "DaemonSet", Name: "daemon", UID: "aaaaaaaa-bbbb-4ccc-8ddd-0000000000aa", Controller: new(true),
			}}
		})
		static := addPod(worker1, "static", func(pod *corev1.Pod) {
			pod.Annotations = map[string]string{mirrorPodAnnotation: "file"}
		})
		elsewhere := addPod(worker2, "elsewhere", nil)

		upgradeTo(newerTalosVersion)
		h.settle(h.started)

		By("planning takes the lock and sends nothing")
		Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationTalosUpgrade))
		Expect(h.operation().Plan).To(Equal([]string{worker1, worker2, cp1Machine}), "workers before the control plane")
		Expect(h.operation().Targets).To(HaveKeyWithValue(worker1, newerImage))
		Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUpgrading))

		By("installing the new system while the node keeps doing its work")
		h.runUntil("worker-1 is to be drained", h.stepIs(worker1, plan.StepDraining))
		Expect(called(fake.MethodImagePull)).To(Equal([]string{worker1}))
		Expect(called(fake.MethodInstall)).To(Equal([]string{worker1}))
		Expect(h.scenario.CallsFor(fake.MethodInstall)[0].Image).To(Equal(newerImage))
		Expect(cordoned(worker1)).To(BeFalse(), "nothing is taken from the cluster before the install is done")
		Expect(exists(app)).To(BeTrue())
		Expect(h.scenario.Node(workerAddr).Version.Tag).To(Equal(talosVersion), "an install changes nothing until the reboot")

		By("draining the node, then rebooting it")
		h.runUntil("worker-1 is rebooted", down(worker1))
		Expect(cordoned(worker1)).To(BeTrue())
		Expect(h.operation().Current.Cordoned).To(BeTrue())
		Expect(exists(app)).To(BeFalse(), "its pods were evicted")
		Expect(exists(daemon)).To(BeTrue(), "a DaemonSet's pod would only come straight back")
		Expect(exists(static)).To(BeTrue(), "a static pod is not Kubernetes' to move")
		Expect(exists(elsewhere)).To(BeTrue(), "no other node is drained")
		Expect(called(fake.MethodReboot)).To(Equal([]string{worker1}))
		Expect(called(fake.MethodEtcdForfeitLeadership)).To(BeEmpty(), "a worker leads no etcd")
		Expect(h.operation().Current.Step).To(Equal(plan.StepBooting))
		Expect(h.operation().Current.PreRebootBootID).To(Equal(h.node(worker1).node + "-boot-1"))

		By("waiting for it, and not asking twice")
		h.advance(time.Minute)
		Expect(called(fake.MethodReboot)).To(Equal([]string{worker1}))
		Expect(called(fake.MethodImagePull)).To(Equal([]string{worker1}), "no other node is begun meanwhile")

		By("taking it back once it runs the new system and stayed healthy")
		Expect(h.scenario.Boot(workerAddr)).To(BeTrue())
		h.runUntil("worker-1 is done", done(1))
		Expect(cordoned(worker1)).To(BeFalse())
		Expect(h.scenario.Node(workerAddr).Version.Tag).To(Equal(newerTalosVersion))

		By("going on with the other worker, and the control plane last")
		reboots(worker2)
		h.runUntil("worker-2 is done", done(2))
		Expect(exists(elsewhere)).To(BeFalse())

		reboots(cp1Machine)
		h.runUntil("the upgrade ends", func() bool { return len(h.tc.Status.History) >= 1 })

		Expect(called(fake.MethodReboot)).To(Equal([]string{worker1, worker2, cp1Machine}), "every node once, in order")
		Expect(called(fake.MethodEtcdForfeitLeadership)).To(Equal([]string{cp1Machine}))

		upgraded := h.tc.Status.History[len(h.tc.Status.History)-1]
		Expect(upgraded.Type).To(Equal(talosv1alpha1.OperationTalosUpgrade))
		Expect(upgraded.Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(upgraded.Completed).To(Equal([]string{worker1, worker2, cp1Machine}))
		Expect(drain(h.recorder)).To(ContainElements(ContainSubstring("UpgradeStarted"), ContainSubstring("MachineUpgraded")))

		By("then bringing each node's config in line: it names the installer the node now runs")
		h.runUntil("the config follows", func() bool { return h.idle() && len(h.tc.Status.History) == 2 })

		configured := h.tc.Status.History[0]
		Expect(configured.Type).To(Equal(talosv1alpha1.OperationConfigRollout))
		Expect(configured.Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(configured.Completed).To(Equal([]string{worker1, worker2, cp1Machine}))
		Expect(string(h.scenario.Node(cpAddr).Config.Active)).To(ContainSubstring(newerImage))

		h.settle(nil)
		Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
		Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
		Expect(h.blocked()).To(BeNil())
		Expect(called(fake.MethodReboot)).To(HaveLen(3), "bringing the config in line reboots nothing")
	})

	It("upgrades nothing while upgrades are not enabled, and says what is waiting", func() {
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Versions.Talos = newerTalosVersion })
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Status).To(Equal(metav1.ConditionFalse))
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUpgradesDisabled))
		Expect(upToDate().Message).To(ContainSubstring("spec.management.upgrades"))

		// The config names an installer no node runs: that is not a config rollout's to apply.
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonAwaitingUpgrade))
		Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonAwaitingUpgrade))
		Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
		Expect(h.applies()).To(BeEmpty())
	})

	It("refuses a downgrade before any node is touched", func() {
		upgradeTo("v1.14.1")
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(upToDate().Message).To(ContainSubstring("downgrades are not supported"))
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(h.blocked().Message).To(ContainSubstring("upgrade"))
		Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())

		// Asking for what the nodes run again clears it.
		upgradeTo(talosVersion)
		h.settle(nil)
		Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
		Expect(h.blocked()).To(BeNil())
	})

	It("refuses an installer that does not exist before a node is asked to pull it, and only when that is certain", func() {
		source := upstream()
		h.reconciler.Catalog = source

		upgradeTo("v1.14.9")
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(upToDate().Message).To(And(ContainSubstring("no installer of Talos v1.14.9"), ContainSubstring(schematic)))
		Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())

		// A source that cannot be reached makes nothing false: the node is where the answer is.
		source.Err = errors.New("no route to host")
		h.reconciler.Catalog = source
		upgradeTo("v1.14.8")
		h.runUntil("a node is asked to pull", func() bool { return len(h.scenario.CallsFor(fake.MethodImagePull)) > 0 })
		Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationTalosUpgrade))
	})

	// Secure Boot (docs/scope.md §7.3): a machine whose firmware enforces it boots only what that
	// firmware trusts, and an upgrade is about to give it something else to boot.
	Context("with Secure Boot on", func() {
		var (
			signer   *x509.Certificate
			stranger *x509.Certificate
			factory  *catalog.Fake
		)

		// trusting is a firmware that trusts whoever signs the installer's boot image; pinned is
		// the one that trusts another certificate and the hashes of the images it has, as a
		// machine does whose images were enrolled one by one.
		trusting := func() talos.SecureBootDB { return talos.SecureBootDB{Certificates: [][]byte{signer.Raw}} }
		pinned := func() talos.SecureBootDB { return talos.SecureBootDB{Certificates: [][]byte{stranger.Raw}, Hashes: 6} }

		firmware := func(machine string, db talos.SecureBootDB) {
			h.scenario.UpdateNode(h.node(machine).addr, func(n *fake.Node) { n.SecureBoot, n.SecureBootDB = true, db })
		}

		// upgradeSigned asks for another Talos version from the installer whose boot image is signed.
		upgradeSigned := func(version string) {
			GinkgoHelper()

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
				tc.Spec.Versions.Talos = version
				tc.Spec.ConfigGeneration.Installer.SecureBoot = true
				tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
			})
		}

		untouched := func() {
			GinkgoHelper()

			Expect(h.operation()).To(BeNil())
			Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
			Expect(h.scenario.CallsFor(fake.MethodInstall)).To(BeEmpty())
			Expect(h.scenario.CallsFor(fake.MethodReboot)).To(BeEmpty())
		}

		pulling := func() bool { return len(h.scenario.CallsFor(fake.MethodImagePull)) > 0 }

		BeforeEach(func() {
			signer = talostest.SigningCertificate(GinkgoT(), "Image signing", nil).Certificate
			stranger = talostest.SigningCertificate(GinkgoT(), "A boot stick's authority", nil).Certificate
			factory = &catalog.Fake{Signer: signer}
			h.reconciler.Signers = &catalog.Cached{Source: factory, Now: func() time.Time { return h.clock }}

			for _, n := range rolloutNodes {
				firmware(n.name, trusting())
			}
		})

		It("refuses to touch any node while one would not boot the new system, and upgrades them all once it would", func() {
			firmware(worker2, pinned())

			upgradeSigned(newerTalosVersion)
			h.advance(time.Hour)

			By("refusing the upgrade of every node, the ones that would boot it included")
			untouched()
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(And(
				ContainSubstring(worker2+": its firmware has Secure Boot on and does not trust"),
				ContainSubstring(talos.CertificateFingerprint(signer)),
				ContainSubstring("A boot stick's authority"), ContainSubstring("6 single image(s)"),
				ContainSubstring("factory.talos.dev/secureboot/signing-cert.pem"),
			))
			Expect(upToDate().Message).NotTo(ContainSubstring(worker1 + ":"))
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))

			By("asking the Image Factory once, however long the refusal stands")
			Expect(factory.Asked).To(Equal(1))

			By("upgrading every node once the certificate is in the firmware")
			firmware(worker2, trusting())
			h.runUntil("a node is asked to pull", pulling)
			Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationTalosUpgrade))
			Expect(h.operation().Plan).To(Equal([]string{worker1, worker2, cp1Machine}))
			Expect(h.operation().Targets[worker2]).To(ContainSubstring("/metal-installer-secureboot/"))
			Expect(factory.Asked).To(Equal(1))
		})

		It("upgrades the other nodes around a machine that is on hold", func() {
			firmware(worker2, pinned())

			tm := h.machine(worker2)
			tm.Spec.Hold = true
			Expect(k8sClient.Update(ctx, tm)).To(Succeed())

			upgradeSigned(newerTalosVersion)
			h.runUntil("a node is asked to pull", pulling)
			Expect(h.operation().Plan).To(Equal([]string{worker1, cp1Machine}))
			Expect(h.scenario.CallsFor(fake.MethodSecureBootDB)).NotTo(ContainElement(HaveField("Node", worker2Addr)),
				"a machine that is not upgraded is not asked about")
		})

		It("does not start while it cannot be told whether every node would boot the new system", func() {
			By("an Image Factory that does not answer")
			factory.Err = errors.New("no route to host")

			upgradeSigned(newerTalosVersion)
			h.advance(time.Hour)

			untouched()
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUnverified))
			Expect(upToDate().Message).To(And(ContainSubstring("could not be asked which certificate"), ContainSubstring("no route to host")))
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonUnverified))

			By("a node whose firmware cannot be read")
			factory.Err = nil
			h.scenario.FailNext(fake.MethodSecureBootDB, cpAddr, status.Error(codes.NotFound, "no such file"))
			h.advance(time.Hour)

			untouched()
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUnverified))
			Expect(upToDate().Message).To(ContainSubstring(cp1Machine + ": what its firmware trusts to boot could not be read"))

			By("starting once both answer")
			h.scenario.FailNext(fake.MethodSecureBootDB, cpAddr, nil)
			h.runUntil("a node is asked to pull", pulling)
			Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationTalosUpgrade))
		})

		It("refuses the installer whose boot image nobody signed, without asking anyone", func() {
			upgradeTo(newerTalosVersion)
			h.advance(time.Hour)

			untouched()
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(And(ContainSubstring("the boot image of "+newerImage+" is not signed"),
				ContainSubstring(worker1+":"), ContainSubstring(cp1Machine+":")))
			Expect(factory.Asked).To(BeZero())
			Expect(h.scenario.CallsFor(fake.MethodSecureBootDB)).To(BeEmpty())

			// A pool says so for its own machines; the others stay refused, and with them the upgrade.
			for _, pool := range []string{poolFor(talosv1alpha1.MachineRoleWorker), poolFor(talosv1alpha1.MachineRoleControlPlane)} {
				h.advance(time.Minute)
				untouched()

				p := &talosv1alpha1.TalosMachinePool{}
				Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: h.ns, Name: pool}, p)).To(Succeed())
				p.Spec.Installer = &talosv1alpha1.PoolInstaller{SecureBoot: new(true)}
				Expect(k8sClient.Update(ctx, p)).To(Succeed())
			}

			h.runUntil("a node is asked to pull", pulling)
			Expect(h.operation().Targets[worker1]).To(ContainSubstring("/metal-installer-secureboot/"))
		})
	})

	It("asks nobody outside the cluster to upgrade machines without Secure Boot", func() {
		factory := &catalog.Fake{}
		h.reconciler.Signers = factory

		upgradeTo(newerTalosVersion)
		h.runUntil("a node is asked to pull", func() bool { return len(h.scenario.CallsFor(fake.MethodImagePull)) > 0 })

		Expect(factory.Asked).To(BeZero())
		Expect(h.scenario.CallsFor(fake.MethodSecureBootDB)).To(BeEmpty())
	})

	It("leaves the node the operator runs on for last", func() {
		h.reconciler.NodeName = h.node(worker1).node

		upgradeTo(newerTalosVersion)
		h.settle(h.started)

		Expect(h.operation().Plan).To(Equal([]string{worker2, worker1, cp1Machine}))
	})

	It("is carried on by another replica from what is persisted, without a second reboot", func() {
		upgradeTo(newerTalosVersion)
		h.runUntil("worker-1 is rebooted", down(worker1))

		// The leader is gone with its node, or for any other reason. The other replica knows
		// nothing but what is in status and on the nodes.
		h.takeOver()

		h.advance(time.Minute)
		Expect(h.scenario.Boot(workerAddr)).To(BeTrue())
		h.runUntil("worker-1 is done", done(1))

		Expect(called(fake.MethodReboot)).To(Equal([]string{worker1}))
		Expect(called(fake.MethodInstall)).To(Equal([]string{worker1}))
		Expect(cordoned(worker1)).To(BeFalse())
	})

	It("halts when a node comes back on the system it ran before, leaving it cordoned and every other node alone", func() {
		h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.FallsBack = true })

		upgradeTo(newerTalosVersion)
		reboots(worker1)
		h.runUntil("the upgrade halts", h.failed)

		f := h.operation().Failure
		Expect(f.Machine).To(Equal(worker1))
		Expect(f.Reason).To(Equal(plan.FailFellBack))
		Expect(f.Remediation).To(ContainSubstring(talosv1alpha1.AnnotationRetry + "=" + f.Token))
		Expect(cordoned(worker1)).To(BeTrue(), "a node that did not come back as expected is not handed back to the scheduler")
		Expect(h.scenario.Node(workerAddr).Version.Tag).To(Equal(talosVersion))

		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
		Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))
		Expect(drain(h.recorder)).To(ContainElement(ContainSubstring("UpgradeHalted")))

		h.advance(time.Hour)
		Expect(called(fake.MethodReboot)).To(Equal([]string{worker1}), "it stays halted: no second reboot, no other node")
		Expect(called(fake.MethodImagePull)).To(Equal([]string{worker1}))

		By("retrying once the cause is found")
		h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.FallsBack = false })
		h.annotate(talosv1alpha1.AnnotationRetry, f.Token)

		reboots(worker1)
		h.runUntil("worker-1 is done", done(1))
		Expect(cordoned(worker1)).To(BeFalse(), "the cordon the operator set is still its own to undo")
		Expect(h.scenario.Node(workerAddr).Version.Tag).To(Equal(newerTalosVersion))
	})

	It("halts on a drain that a disruption budget never allows, with the node cordoned and not rebooted", func() {
		guarded := addPod(worker1, "guarded", nil)
		budget := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: "guarded", Namespace: h.ns},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: new(intstr.FromInt32(0)),
				Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "guarded"}},
			},
		}
		Expect(k8sClient.Create(ctx, budget)).To(Succeed())

		upgradeTo(newerTalosVersion)
		h.runUntil("worker-1 is being drained", func() bool { return h.stepIs(worker1, plan.StepDraining)() && cordoned(worker1) })

		h.advance(time.Minute)
		Expect(exists(guarded)).To(BeTrue(), "an eviction the budget refuses does not happen")
		Expect(condition(h.tc, talosv1alpha1.ConditionTalosUpToDate).Message).To(ContainSubstring("PodDisruptionBudget"))

		h.runEvery(time.Minute, "the upgrade halts", h.failed)

		f := h.operation().Failure
		Expect(f.Reason).To(Equal(plan.FailDrainTimeout))
		Expect(f.Message).To(ContainSubstring("1 pod"))
		Expect(cordoned(worker1)).To(BeTrue())
		Expect(called(fake.MethodReboot)).To(BeEmpty(), "a node that is not drained is not rebooted")
		Expect(exists(guarded)).To(BeTrue())

		By("retrying once the budget lets the pod go")
		Expect(k8sClient.Delete(ctx, budget)).To(Succeed())
		h.annotate(talosv1alpha1.AnnotationRetry, f.Token)

		reboots(worker1)
		h.runUntil("worker-1 is done", done(1))
		Expect(exists(guarded)).To(BeFalse())
		Expect(cordoned(worker1)).To(BeFalse())
	})

	It("halts on an installer that cannot be pulled, before anything changed, and does not start again after an abort", func() {
		h.scenario.FailNext(fake.MethodImagePull, workerAddr, status.Error(codes.NotFound, "manifest unknown"))

		upgradeTo(newerTalosVersion)
		h.runUntil("the upgrade halts", h.failed)

		f := h.operation().Failure
		Expect(f.Reason).To(Equal(plan.FailImagePull))
		Expect(f.Message).To(ContainSubstring(newerImage))
		Expect(cordoned(worker1)).To(BeFalse())
		Expect(called(fake.MethodInstall)).To(BeEmpty())
		Expect(called(fake.MethodReboot)).To(BeEmpty())

		h.annotate(talosv1alpha1.AnnotationAbort, f.Token)
		h.scenario.FailNext(fake.MethodImagePull, workerAddr, nil)
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
		Expect(h.tc.Status.Holds[talosv1alpha1.OperationTalosUpgrade].Aborted).To(HaveKeyWithValue(worker1, newerImage))
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonAborted))
		Expect(called(fake.MethodImagePull)).To(HaveLen(1), "an aborted upgrade is not started again as it is")

		By("asking for what the nodes run again")
		upgradeTo(talosVersion)
		h.settle(nil)
		Expect(h.tc.Status.Holds).To(BeEmpty(), "the abort is spent")
		Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
	})

	It("leaves out a machine that is skipped, and does not come back for it with the same installer", func() {
		h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.InstallExitCode = 1 })

		upgradeTo(newerTalosVersion)
		h.runUntil("the upgrade halts", h.failed)

		f := h.operation().Failure
		Expect(f.Reason).To(Equal(plan.FailInstall))
		Expect(cordoned(worker1)).To(BeFalse(), "a failed install leaves the node as it was")

		h.annotate(talosv1alpha1.AnnotationSkipMachine, f.Token)
		reboots(worker2)
		h.runUntil("worker-2 is done", done(1))
		reboots(cp1Machine)
		h.runUntil("the upgrade ends", func() bool { return h.idle() && len(h.tc.Status.History) >= 1 })

		h.advance(time.Hour)
		Expect(h.tc.Status.Holds[talosv1alpha1.OperationTalosUpgrade].Skipped).To(HaveKeyWithValue(worker1, newerImage))
		Expect(upToDate().Status).To(Equal(metav1.ConditionFalse))
		Expect(upToDate().Message).To(ContainSubstring(worker1))
		Expect(called(fake.MethodInstall)).To(Equal([]string{worker1, worker2, cp1Machine}), "the skipped machine is not tried again")
	})

	It("does not undo a cordon that someone else set", func() {
		kube := kubeNodeOf(worker1)
		kube.Spec.Unschedulable = true
		Expect(k8sClient.Update(ctx, kube)).To(Succeed())

		// A cordoned node is someone's work in progress: nothing else is begun, but the node
		// itself may be upgraded.
		upgradeTo(newerTalosVersion)
		reboots(worker1)
		h.runUntil("worker-1 is settling", h.stepIs(worker1, plan.StepVerifying))

		Expect(h.operation().Current.Cordoned).To(BeFalse())
		Expect(cordoned(worker1)).To(BeTrue(), "whoever cordoned it uncordons it")
	})

	It("leaves a node it has not taken anything from alone while paused, and finishes one it has", func() {
		upgradeTo(newerTalosVersion)
		h.runUntil("worker-1 is to be drained", h.stepIs(worker1, plan.StepDraining))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		h.advance(time.Hour)

		Expect(h.operation()).NotTo(BeNil())
		Expect(h.operation().Failure).To(BeNil(), "waiting out a pause is not a failure")
		Expect(h.operation().Current).To(BeNil())
		Expect(cordoned(worker1)).To(BeFalse())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		h.runUntil("worker-1 is cordoned", func() bool { return cordoned(worker1) })

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		reboots(worker1)
		h.runUntil("worker-1 is done", done(1))
		Expect(cordoned(worker1)).To(BeFalse(), "a node that was taken down is brought all the way back")

		h.advance(time.Hour)
		Expect(called(fake.MethodImagePull)).To(Equal([]string{worker1, worker1}), "and no other node is begun")
	})
})
