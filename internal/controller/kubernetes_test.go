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
	"errors"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
)

// A Kubernetes upgrade: the components Talos runs, one on one node at a time, every API server
// first (docs/scope.md §7.4).
var _ = Describe("Kubernetes upgrade", func() {
	const (
		patch     = "1.37.1"
		apiServer = "registry.k8s.io/kube-apiserver:v"
		kubelet   = "ghcr.io/siderolabs/kubelet:v"
	)

	var (
		h *rolloutHarness
		// stuck lists the turns whose component does not come up on the new image: it keeps
		// running the old one.
		stuck map[string]bool
		// running is what each component really runs, by turn.
		running map[string]string
	)

	imagesOf := func(machine string) map[string]string {
		GinkgoHelper()

		images, err := talos.KubernetesImages(h.scenario.Node(h.node(machine).addr).Config.Active)
		Expect(err).NotTo(HaveOccurred())

		return images
	}

	// pulled lists the images pulled for an upgrade, in order, as "<machine> <image>".
	pulled := func() []string {
		var out []string

		for _, c := range h.scenario.CallsFor(fake.MethodKubernetesImagePull) {
			for _, n := range rolloutNodes {
				if n.addr == c.Node {
					out = append(out, n.name+" "+c.Image)
				}
			}
		}

		return out
	}

	upgradeKubernetes := func(version string) {
		GinkgoHelper()
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Versions.Kubernetes = version
			tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
		})
	}

	upToDate := func() *metav1.Condition {
		GinkgoHelper()

		c := condition(h.tc, talosv1alpha1.ConditionKubernetesUpToDate)
		Expect(c).NotTo(BeNil())

		return c
	}

	turnIs := func(component, machine, step string) func() bool {
		return func() bool {
			cur := h.operation()

			return cur != nil && cur.Current != nil && cur.Current.Component == component && cur.Current.Machine == machine &&
				cur.Current.Step == step
		}
	}

	kubernetesOperations := func() []talosv1alpha1.OperationRecord {
		var out []talosv1alpha1.OperationRecord

		for _, record := range h.tc.Status.History {
			if record.Type == talosv1alpha1.OperationKubernetesUpgrade {
				out = append(out, record)
			}
		}

		return out
	}

	settled := func() bool {
		return h.idle() && upToDate().Status == metav1.ConditionTrue && h.inSync().Status == metav1.ConditionTrue
	}

	BeforeEach(func() {
		h = startRollout()
		stuck, running = map[string]bool{}, map[string]string{}

		// The kubelets, as far as Kubernetes itself goes: each runs the components its node's
		// active config names, mirrors the static pods into the API, and reports its own version.
		h.kubelets = func() {
			GinkgoHelper()

			for _, n := range rolloutNodes {
				for component, image := range imagesOf(n.name) {
					key := plan.TurnKey(component, n.name)
					if !stuck[key] || running[key] == "" {
						running[key] = image
					}

					if !plan.Runs(component) {
						continue
					}

					if component == plan.ComponentKubelet {
						version, _ := plan.VersionOf(running[key])

						kube := &corev1.Node{}
						Expect(k8sClient.Get(ctx, client.ObjectKey{Name: n.node}, kube)).To(Succeed())

						if kube.Status.NodeInfo.KubeletVersion != "v"+version {
							kube.Status.NodeInfo.KubeletVersion = "v" + version
							Expect(k8sClient.Status().Update(ctx, kube)).To(Succeed())
						}

						continue
					}

					pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: component + "-" + n.node, Namespace: kubeSystem}}

					switch err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod); {
					case apierrors.IsNotFound(err):
						pod.Labels = map[string]string{"k8s-app": component}
						pod.Spec = corev1.PodSpec{NodeName: n.node, Containers: []corev1.Container{{Name: component, Image: running[key]}}}
						Expect(k8sClient.Create(ctx, pod)).To(Succeed())

						pod.Status.Phase = corev1.PodRunning
						pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
						Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
					case err != nil:
						Expect(err).NotTo(HaveOccurred())
					case pod.Spec.Containers[0].Image != running[key]:
						pod.Spec.Containers[0].Image = running[key]
						Expect(k8sClient.Update(ctx, pod)).To(Succeed())
					}
				}
			}
		}

		DeferCleanup(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace(kubeSystem), client.GracePeriodSeconds(0))).To(Succeed())
		})

		h.settle(nil)
	})

	It("reports every node up to date while nothing is asked for", func() {
		Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
		Expect(pulled()).To(BeEmpty())
		Expect(h.applies()).To(BeEmpty())
	})

	It("takes every component to the new version, the API server first, each seen running before it is confirmed", func() {
		upgradeKubernetes(patch)
		h.settle(h.started)

		Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationKubernetesUpgrade))
		Expect(h.operation().Target).To(Equal(patch))
		Expect(h.operation().Plan).To(Equal([]string{
			"kube-apiserver/" + cp1Machine, "kube-controller-manager/" + cp1Machine, "kube-scheduler/" + cp1Machine,
			"kube-proxy/" + cp1Machine, "kubelet/" + cp1Machine, "kubelet/" + worker1, "kubelet/" + worker2,
		}))
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUpgrading))

		h.runUntil("the API server is tried", turnIs(plan.ComponentAPIServer, cp1Machine, plan.StepVerifying))
		Expect(pulled()).To(Equal([]string{cp1Machine + " " + apiServer + patch}), "the image is on the node before its config changes")
		Expect(h.applies()).To(Equal([]string{tried(cp1Machine)}))
		Expect(imagesOf(cp1Machine)[plan.ComponentAPIServer]).To(Equal(apiServer + patch))
		Expect(imagesOf(cp1Machine)[plan.ComponentControllerManager]).To(HaveSuffix(":v"+kubernetesVersion), "one component at a time")
		Expect(h.scenario.TryPending(cpAddr)).To(BeTrue(), "tried, not yet kept")
		Expect(h.operation().Current.Deadline.Sub(h.clock)).To(BeNumerically(">", plan.DefaultStepTimeout),
			"an API server's restart leaves a control plane the time to settle that a reboot leaves a node")

		h.runUntil("the controller manager is tried", turnIs(plan.ComponentControllerManager, cp1Machine, plan.StepVerifying))
		Expect(h.operation().Current.Deadline.Sub(h.clock)).To(BeNumerically("<=", plan.DefaultTryTimeout),
			"no other turn gets more than any change")

		h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
		Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))

		Expect(pulled()).To(Equal([]string{
			cp1Machine + " " + apiServer + patch,
			cp1Machine + " registry.k8s.io/kube-controller-manager:v" + patch,
			cp1Machine + " registry.k8s.io/kube-scheduler:v" + patch,
			cp1Machine + " " + kubelet + patch, worker1 + " " + kubelet + patch, worker2 + " " + kubelet + patch,
		}))
		// Five changes of the control plane's config, the kube-proxy image among them, for which
		// nothing is pulled; then one for each worker's kubelet.
		Expect(h.applies()).To(Equal([]string{
			tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine), confirmed(cp1Machine),
			tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine), confirmed(cp1Machine),
			tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2),
		}))

		for _, n := range rolloutNodes {
			for component, image := range imagesOf(n.name) {
				Expect(image).To(HaveSuffix(":v"+patch), "%s on %s", component, n.name)
			}

			Expect(h.scenario.TryPending(n.addr)).To(BeFalse())
		}

		// Nothing is left for a config rollout: every image the version names is the upgrade's.
		h.runUntil("everything settles", settled)
		Expect(upToDate().Message).To(ContainSubstring("all 3 nodes"))
		Expect(h.applies()).To(HaveLen(14))
		Expect(h.tc.Status.History).To(HaveLen(1))
		Expect(drain(h.recorder)).To(ContainElement(ContainSubstring("KubernetesUpgradeStarted")))
	})

	It("changes nothing while upgrades are not enabled, and says what is waiting", func() {
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Versions.Kubernetes = patch })
		h.advance(time.Hour)

		Expect(upToDate().Status).To(Equal(metav1.ConditionFalse))
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUpgradesDisabled))
		Expect(upToDate().Message).To(ContainSubstring(cp1Machine))
		Expect(h.operation()).To(BeNil())
		Expect(pulled()).To(BeEmpty())

		// And no config rollout applies the images behind the upgrade's back.
		Expect(h.applies()).To(BeEmpty())

		for _, n := range rolloutNodes {
			for component, image := range imagesOf(n.name) {
				Expect(image).To(HaveSuffix(":v"+kubernetesVersion), "%s on %s", component, n.name)
			}
		}

		Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
		matches := apimeta.FindStatusCondition(h.machine(worker1).Status.Conditions, talosv1alpha1.ConditionConfigMatchesLive)
		Expect(matches.Status).To(Equal(metav1.ConditionTrue))
		Expect(matches.Message).To(ContainSubstring("apart from the Kubernetes version"), "a machine says what its comparison leaves out")
	})

	// What stands in the way of an upgrade is often something in the config: a flag the new
	// version removed. Taking it out is a config change, and must not wait for the upgrade
	// that waits for it.
	It("rolls out a config change while the upgrade is refused, without touching the images", func() {
		h.scenario.SetRemovedInKubernetes(patch, "kubelet on 10.5.0.3 is started with --container-hints, which the new version removed")
		upgradeKubernetes(patch)
		h.advance(time.Minute)
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(h.applies()).To(BeEmpty())

		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("the config change is rolled out", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })

		Expect(h.tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationConfigRollout))
		Expect(h.tc.Status.History[0].Completed).To(Equal([]string{worker1, worker2}))
		Expect(string(h.scenario.Node(workerAddr).Config.Active)).To(ContainSubstring("rollout: one"))
		Expect(imagesOf(worker1)[plan.ComponentKubelet]).To(HaveSuffix(":v"+kubernetesVersion), "the image is not a config rollout's to change")
		Expect(pulled()).To(BeEmpty())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible), "and the upgrade is still refused")

		// With the way cleared, the upgrade goes ahead, and keeps what the rollout applied.
		h.scenario.SetRemovedInKubernetes(patch)
		h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
		Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(imagesOf(worker1)[plan.ComponentKubelet]).To(HaveSuffix(":v" + patch))
		Expect(string(h.scenario.Node(workerAddr).Config.Active)).To(ContainSubstring("rollout: one"))

		h.runUntil("everything settles", settled)
	})

	It("refuses a version the nodes' Talos does not run, before any node is touched, and says which to upgrade first", func() {
		upgradeKubernetes("1.38.0")
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(upToDate().Message).To(And(ContainSubstring("does not support Kubernetes 1.38.0"), ContainSubstring("upgrade Talos first"),
			ContainSubstring(h.node(worker1).node+" ("+talosVersion+")")))
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(pulled()).To(BeEmpty())
		Expect(h.applies()).To(BeEmpty())
	})

	It("refuses a downgrade", func() {
		upgradeKubernetes("1.36.4")
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(upToDate().Message).To(ContainSubstring("not downgraded"))
		Expect(pulled()).To(BeEmpty())
	})

	It("refuses an upgrade that would strand what the new version removes, before any node is touched", func() {
		// A flag a component is started with.
		h.scenario.SetRemovedInKubernetes(patch, "kubelet on 10.5.0.3 is started with --container-hints, which the new version removed")
		upgradeKubernetes(patch)
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(upToDate().Message).To(ContainSubstring("--container-hints"))
		Expect(pulled()).To(BeEmpty())

		// Objects of an API it no longer serves.
		h.scenario.SetRemovedInKubernetes(patch)
		h.reconciler.KubernetesChecks = KubernetesCheckerFunc(func(context.Context, string, string) ([]string, error) {
			return []string{"2 object(s) of workloads.v1alpha2.scheduling.k8s.io exist, an API the new version no longer serves"}, nil
		})
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Message).To(ContainSubstring("workloads.v1alpha2.scheduling.k8s.io"))
		Expect(pulled()).To(BeEmpty())
		Expect(h.applies()).To(BeEmpty())

		// What could not be asked is not fine either.
		h.reconciler.KubernetesChecks = KubernetesCheckerFunc(func(context.Context, string, string) ([]string, error) {
			return nil, errors.New("the server is currently unable to handle the request")
		})
		h.advance(time.Hour)
		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonAwaitingStatus))
		Expect(upToDate().Message).To(ContainSubstring("could not be checked"))

		h.reconciler.KubernetesChecks = nil
		h.advance(time.Hour)
		Expect(h.operation()).To(BeNil(), "an operator that cannot ask does not assume")

		h.reconciler.KubernetesChecks = nothingStranded
		h.scenario.FailNext(fake.MethodKubernetesUpgradeChecks, workerAddr, status.Error(codes.Unavailable, "connection refused"))
		h.advance(time.Hour)
		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonAwaitingStatus))

		h.scenario.FailNext(fake.MethodKubernetesUpgradeChecks, workerAddr, nil)
		h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
		Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
	})

	It("asks the Kubernetes API itself for objects of removed APIs", func() {
		checks := KubernetesCheckerFunc(func(ctx context.Context, from, to string) ([]string, error) {
			return talos.RemovedAPIObjects(ctx, cfg, from, to)
		})

		// The API server of these specs serves none of the APIs that 1.37 removes.
		Expect(checks.RemovedAPIObjects(ctx, "1.36.2", "1.37.1")).To(BeEmpty())
	})

	Context("with a component that does not come up", func() {
		var deadline time.Time

		BeforeEach(func() {
			stuck[plan.TurnKey(plan.ComponentControllerManager, cp1Machine)] = true

			upgradeKubernetes(patch)
			h.runUntil("the controller manager is tried", turnIs(plan.ComponentControllerManager, cp1Machine, plan.StepVerifying))
			h.advance(time.Minute)
			Expect(h.operation().Current.HealthySince).To(BeNil())
			Expect(upToDate().Message).To(ContainSubstring("kube-controller-manager on " + h.node(cp1Machine).node + " does not run"))

			// Talos takes the image back when the try ends.
			deadline = h.operation().Current.Deadline.Time
			h.at(deadline)
			h.expireTry(cp1Machine)
			h.at(deadline.Add(plan.RollbackGrace))
		})

		It("halts, with the component back on its previous image and nothing after it touched", func() {
			Expect(h.failed()).To(BeTrue())

			f := h.operation().Failure
			Expect(f.Reason).To(Equal(plan.FailRolledBack))
			Expect(f.Machine).To(Equal(cp1Machine))
			Expect(f.Remediation).To(And(ContainSubstring(talosv1alpha1.AnnotationRetry+"="+f.Token),
				ContainSubstring(talosv1alpha1.AnnotationAbort+"="+f.Token)))
			Expect(f.Remediation).NotTo(ContainSubstring(talosv1alpha1.AnnotationSkipMachine),
				"a Kubernetes upgrade cannot leave a node out")

			Expect(imagesOf(cp1Machine)[plan.ComponentControllerManager]).To(HaveSuffix(":v" + kubernetesVersion))
			Expect(imagesOf(cp1Machine)[plan.ComponentAPIServer]).To(HaveSuffix(":v"+patch), "what was confirmed stays")
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
			Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))

			before := h.applies()
			h.annotate(talosv1alpha1.AnnotationSkipMachine, f.Token)
			h.advance(time.Hour)
			Expect(h.failed()).To(BeTrue(), "and an annotation that asks for it is not followed")
			Expect(h.applies()).To(Equal(before))
			Expect(pulled()).To(HaveLen(2))
		})

		It("runs the turn again on retry", func() {
			stuck = map[string]bool{}
			h.annotate(talosv1alpha1.AnnotationRetry, h.operation().Failure.Token)
			h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })

			Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(imagesOf(worker2)[plan.ComponentKubelet]).To(HaveSuffix(":v" + patch))
		})

		It("ends on abort, and does not start again until the spec asks for another version", func() {
			stuck = map[string]bool{}
			h.annotate(talosv1alpha1.AnnotationAbort, h.operation().Failure.Token)
			h.settle(h.idle)

			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
			Expect(h.tc.Status.Upgrades.Kubernetes).To(Equal(patch))

			sent := len(h.applies())
			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil())
			Expect(h.applies()).To(HaveLen(sent))
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonAborted))

			upgradeKubernetes("1.37.2")
			h.runUntil("the upgrade to the other version ends", func() bool { return len(kubernetesOperations()) == 2 })
			Expect(h.tc.Status.Upgrades).To(BeNil())
			Expect(imagesOf(cp1Machine)[plan.ComponentControllerManager]).To(HaveSuffix(":v1.37.2"))
		})
	})

	It("halts on an image that cannot be pulled, with nothing changed", func() {
		h.scenario.FailNext(fake.MethodKubernetesImagePull, cpAddr, status.Error(codes.NotFound, "manifest unknown"))
		upgradeKubernetes(patch)
		h.runUntil("the upgrade halts", h.failed)

		Expect(h.operation().Failure.Reason).To(Equal(plan.FailImagePull))
		Expect(h.operation().Failure.Message).To(ContainSubstring(apiServer + patch))
		Expect(h.applies()).To(BeEmpty())
		Expect(imagesOf(cp1Machine)[plan.ComponentAPIServer]).To(HaveSuffix(":v" + kubernetesVersion))
	})

	It("takes a version that is more than a minor away one minor at a time", func() {
		// The cluster is two minors behind what its spec asks for.
		for _, n := range rolloutNodes {
			h.scenario.UpdateNode(n.addr, func(fn *fake.Node) {
				images, err := talos.KubernetesImages(fn.Config.Active)
				Expect(err).NotTo(HaveOccurred())

				for component, image := range images {
					fn.Config.Active, err = talos.WithKubernetesImage(fn.Config.Active, component, strings.Replace(image, kubernetesVersion, "1.35.4", 1))
					Expect(err).NotTo(HaveOccurred())
				}
			})
		}

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled })
		h.settle(h.started)
		Expect(h.operation().Target).To(Equal("1.36.0"), "the first release of the minor in between")

		h.runUntil("the first hop ends", func() bool { return len(kubernetesOperations()) == 1 })
		for _, n := range rolloutNodes {
			Expect(imagesOf(n.name)[plan.ComponentKubelet]).To(Equal(kubelet + "1.36.0"))
		}

		h.runUntil("the second hop ends", func() bool { return len(kubernetesOperations()) == 2 })
		for _, n := range rolloutNodes {
			for _, image := range imagesOf(n.name) {
				Expect(image).To(HaveSuffix(":v" + kubernetesVersion))
			}
		}

		h.runUntil("everything settles", settled)
		Expect(pulled()).To(HaveLen(12))
	})

	It("goes through the newest release it knows of a minor on the way, and refuses a release that does not exist", func() {
		h.reconciler.Catalog = upstream()

		upgradeKubernetes("1.37.9")
		h.advance(time.Hour)
		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(upToDate().Message).To(And(ContainSubstring("these images do not exist"),
			ContainSubstring("registry.k8s.io/kube-apiserver:v1.37.9"), ContainSubstring("ghcr.io/siderolabs/kubelet:v1.37.9")))
		Expect(pulled()).To(BeEmpty())

		// Two minors behind what the spec asks for now.
		for _, n := range rolloutNodes {
			h.scenario.UpdateNode(n.addr, func(fn *fake.Node) {
				images, err := talos.KubernetesImages(fn.Config.Active)
				Expect(err).NotTo(HaveOccurred())

				for component, image := range images {
					fn.Config.Active, err = talos.WithKubernetesImage(fn.Config.Active, component, strings.Replace(image, kubernetesVersion, "1.35.4", 1))
					Expect(err).NotTo(HaveOccurred())
				}
			})
		}

		upgradeKubernetes(kubernetesVersion)
		h.settle(h.started)
		Expect(h.operation().Target).To(Equal("1.36.5"), "not the first release of the minor in between, but the newest")
		Expect(h.tc.Status.Versions.Plan).To(Equal([]talosv1alpha1.VersionStep{
			{Kind: plan.StepKubernetes, From: "1.35.4", To: "1.36.5"}, {Kind: plan.StepKubernetes, From: "1.36.5", To: kubernetesVersion},
		}))

		h.runUntil("both hops end", func() bool { return len(kubernetesOperations()) == 2 })
		Expect(imagesOf(worker1)[plan.ComponentKubelet]).To(HaveSuffix(":v" + kubernetesVersion))
	})

	It("is carried on by another replica from what is persisted, without a second try", func() {
		upgradeKubernetes(patch)
		h.runUntil("the API server is tried", turnIs(plan.ComponentAPIServer, cp1Machine, plan.StepVerifying))

		h.takeOver()

		h.runUntil("the scheduler's turn", turnIs(plan.ComponentScheduler, cp1Machine, plan.StepVerifying))
		Expect(h.applies()).To(Equal([]string{
			tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine),
		}))
	})

	It("finishes the component it is at when paused, and begins no other", func() {
		upgradeKubernetes(patch)
		h.runUntil("the API server is tried", turnIs(plan.ComponentAPIServer, cp1Machine, plan.StepVerifying))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		h.runUntil("the API server is confirmed", func() bool { return len(h.operation().Completed) == 1 })
		h.advance(time.Hour)

		Expect(h.applies()).To(Equal([]string{tried(cp1Machine), confirmed(cp1Machine)}))
		Expect(pulled()).To(HaveLen(1), "not even the next image is pulled")
		Expect(h.started()).To(BeTrue())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
	})

	It("pulls nothing while paused, and does not count the pause against the turn", func() {
		// The first pull does not get through, so the turn is still at its beginning when the
		// pause comes.
		h.scenario.FailNext(fake.MethodKubernetesImagePull, cpAddr, status.Error(codes.Unavailable, "connection refused"))
		upgradeKubernetes(patch)
		h.runUntil("the first turn begins", turnIs(plan.ComponentAPIServer, cp1Machine, plan.StepPulling))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		h.scenario.FailNext(fake.MethodKubernetesImagePull, cpAddr, nil)
		tries := len(h.scenario.CallsFor(fake.MethodKubernetesImagePull))
		h.advance(time.Hour)

		Expect(h.scenario.CallsFor(fake.MethodKubernetesImagePull)).To(HaveLen(tries), "a paused upgrade asks nothing of a node")
		Expect(h.applies()).To(BeEmpty())
		Expect(h.failed()).To(BeFalse(), "a pause is not a timeout")
		Expect(h.operation().Current).To(BeNil(), "the turn is dropped, and begins again later")

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
		Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
	})

	It("does not start on a cluster that is not healthy", func() {
		h.sicken(worker2, "")
		upgradeKubernetes(patch)
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
		Expect(pulled()).To(BeEmpty())
	})

	It("refuses to leave a control plane out, and leaves a worker on hold behind", func() {
		hold := func(machine string, held bool) {
			GinkgoHelper()

			tm := h.machine(machine)
			tm.Spec.Hold = held
			Expect(k8sClient.Update(ctx, tm)).To(Succeed())
		}

		hold(cp1Machine, true)
		upgradeKubernetes(patch)
		h.advance(time.Hour)
		Expect(h.operation()).To(BeNil())
		Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(upToDate().Message).To(ContainSubstring("cannot leave a control plane out"))

		hold(cp1Machine, false)
		hold(worker2, true)
		h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })

		Expect(imagesOf(worker1)[plan.ComponentKubelet]).To(HaveSuffix(":v" + patch))
		Expect(imagesOf(worker2)[plan.ComponentKubelet]).To(HaveSuffix(":v" + kubernetesVersion))
		h.runUntil("the rest settles", func() bool { return h.idle() && upToDate().Reason == talosv1alpha1.ReasonHeld })
		Expect(upToDate().Message).To(ContainSubstring(worker2))
	})

	It("runs the rollout's hooks around the upgrade, and none around its nodes", func() {
		var hooks []string

		inner := h.kubelets
		h.kubelets = func() {
			GinkgoHelper()
			inner()

			var list batchv1.JobList
			Expect(k8sClient.List(ctx, &list, client.InNamespace(h.ns), client.HasLabels{talosv1alpha1.LabelHook})).To(Succeed())

			for i := range list.Items {
				job := &list.Items[i]
				if len(job.Status.Conditions) > 0 {
					continue
				}

				hooks = append(hooks, job.Labels[talosv1alpha1.LabelHook])

				now := metav1.NewTime(h.clock)
				job.Status.StartTime, job.Status.CompletionTime, job.Status.Succeeded = &now, &now, 1
				job.Status.Conditions = []batchv1.JobCondition{
					{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
					{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
				}
				Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
			}
		}

		job := runtime.RawExtension{Raw: []byte(hookJob)}
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Rollout.Hooks = &talosv1alpha1.RolloutHooks{
				PreRollout: []talosv1alpha1.Hook{{Name: "announce", Job: job}}, PostRollout: []talosv1alpha1.Hook{{Name: "all-clear", Job: job}},
				PreNode: []talosv1alpha1.Hook{{Name: "prepare", Job: job}}, PostNode: []talosv1alpha1.Hook{{Name: "release", Job: job}},
			}
		})

		upgradeKubernetes(patch)
		h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
		Expect(hooks).To(Equal([]string{talosv1alpha1.HookPreRollout, talosv1alpha1.HookPostRollout}))
	})
})
