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
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
)

// upstream is what exists upstream in the specs that ask: a newer Talos patch release and its
// installer, a Talos minor this operator does not support, and the newest Kubernetes releases.
func upstream() *catalog.Fake {
	return &catalog.Fake{
		Talos: []string{"v1.13.9", talosVersion, newerTalosVersion, "v1.15.0"},
		Installers: map[string]bool{
			schematic + " " + talosVersion: true, schematic + " " + newerTalosVersion: true,
		},
		Latest: map[string]string{"1.36": "1.36.5", "1.37": "1.37.1"},
		Tags:   map[string]bool{"v" + kubernetesVersion: true, "v1.37.2": true},
	}
}

// What the operator knows of the versions that exist, and of the way to the ones a spec asks for
// (docs/scope.md §7.5).
var _ = Describe("Version awareness", func() {
	var (
		h      *rolloutHarness
		source *catalog.Fake
	)

	versions := func() *talosv1alpha1.VersionsStatus {
		GinkgoHelper()
		Expect(h.tc.Status.Versions).NotTo(BeNil())

		return h.tc.Status.Versions
	}

	reachable := func() *metav1.Condition {
		GinkgoHelper()

		c := condition(h.tc, talosv1alpha1.ConditionVersionsReachable)
		Expect(c).NotTo(BeNil())

		return c
	}

	BeforeEach(func() {
		h = startRollout()

		source = upstream()
		h.reconciler.Catalog = &catalog.Cached{Source: source, Now: func() time.Time { return h.clock }}
		h.settle(nil)
	})

	It("says what the cluster runs and what exists, and that there is nothing to do", func() {
		Expect(versions().Talos).To(Equal(talosVersion))
		Expect(versions().Kubernetes).To(Equal(kubernetesVersion))
		Expect(versions().Plan).To(BeEmpty())
		Expect(reachable().Status).To(Equal(metav1.ConditionTrue))
		Expect(reachable().Reason).To(Equal(talosv1alpha1.ReasonUpToDate))

		// Of Talos: the newest release of each minor this operator supports, nothing older than
		// what runs. Of Kubernetes: up to the newest minor one of those runs.
		Expect(versions().Available.Talos).To(Equal([]string{newerTalosVersion}))
		Expect(versions().Available.Kubernetes).To(Equal([]string{"1.37.1"}))
		Expect(versions().Available.Problem).To(BeEmpty())

		// What exists is information. Nothing is upgraded because of it.
		h.advance(time.Hour)
		Expect(h.operation()).To(BeNil())
		Expect(h.tc.Status.History).To(BeEmpty())
		Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
	})

	It("asks upstream rarely, and not at all while an operation is in flight", func() {
		asked := source.Asked
		for range 20 {
			h.round()
		}

		Expect(source.Asked).To(Equal(asked), "an answer is kept for a while")

		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.settle(h.started)

		asked = source.Asked
		h.clock = h.clock.Add(catalog.DefaultKeep)
		h.settle(nil)
		Expect(h.started()).To(BeTrue())
		Expect(source.Asked).To(Equal(asked), "an operation in flight has deadlines to keep")
	})

	It("plans the way to what the spec asks for, and goes it in that order", func() {
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Versions.Talos, tc.Spec.Versions.Kubernetes = newerTalosVersion, "1.37.1"
			tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
		})
		h.settle(h.started)

		Expect(versions().Plan).To(Equal([]talosv1alpha1.VersionStep{
			{Kind: plan.StepTalos, From: talosVersion, To: newerTalosVersion},
			{Kind: plan.StepKubernetes, From: kubernetesVersion, To: "1.37.1"},
		}))
		Expect(reachable().Reason).To(Equal(talosv1alpha1.ReasonPlanned))
		Expect(reachable().Message).To(ContainSubstring("2 upgrade(s)"))
		Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationTalosUpgrade), "the first step of the plan is the first operation")
	})

	It("says why the versions a spec asks for cannot be reached", func() {
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Versions.Kubernetes = "1.38.0"
			tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
		})
		h.advance(time.Minute)

		Expect(reachable().Status).To(Equal(metav1.ConditionFalse))
		Expect(reachable().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
		Expect(reachable().Message).To(ContainSubstring("the spec asks for Talos " + talosVersion + " with Kubernetes 1.38.0"))
		Expect(versions().Plan).To(BeEmpty())
		Expect(h.operation()).To(BeNil())

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Versions.Kubernetes = "1.36.5" })
		h.advance(time.Minute)
		Expect(reachable().Message).To(ContainSubstring("not downgraded"))
	})

	It("keeps what it knew when upstream does not answer, and says so", func() {
		source.Err = errors.New("no route to host")
		h.clock = h.clock.Add(catalog.DefaultKeep)
		h.settle(nil)

		Expect(versions().Available.Talos).To(Equal([]string{newerTalosVersion}))
		Expect(versions().Available.Kubernetes).To(Equal([]string{"1.37.1"}))
		Expect(versions().Available.Problem).To(ContainSubstring("no route to host"))
		Expect(reachable().Status).To(Equal(metav1.ConditionTrue), "what the cluster runs and may be upgraded to does not depend on it")

		source.Err = nil
		h.clock = h.clock.Add(catalog.DefaultRetry)
		h.settle(nil)
		Expect(versions().Available.Problem).To(BeEmpty())
	})

	It("asks nobody when it was started without a source", func() {
		h.reconciler.Catalog = nil
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		asked := source.Asked
		h.clock = h.clock.Add(2 * catalog.DefaultKeep)
		h.settle(nil)

		Expect(source.Asked).To(Equal(asked))
		Expect(reachable().Status).To(Equal(metav1.ConditionTrue))
	})
})
