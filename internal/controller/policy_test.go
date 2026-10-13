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
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
)

// What a hook's Job does in the specs that play the Job controller.
const (
	hookSucceeds = "succeed"
	hookFails    = "fail"
	// hookJob is the JobSpec of the specs' hooks.
	hookJob = `{"template":{"spec":{"containers":[{"name":"hook","image":"registry.example.com/hook:1"}]}}}`
)

// What a user declares about every operation (docs/scope.md §6.4): the windows it may begin a
// node in, the gates it asks before and after one, and the hooks it runs around them.
var _ = Describe("Rollout policy", func() {
	var h *rolloutHarness

	BeforeEach(func() { h = startRollout() })

	// daily is a maintenance window that opens at the given minute every day.
	daily := func(opens time.Time, lasts time.Duration) talosv1alpha1.MaintenanceWindow {
		utc := opens.UTC()

		return talosv1alpha1.MaintenanceWindow{
			Schedule: fmt.Sprintf("%d %d * * *", utc.Minute(), utc.Hour()), Duration: metav1.Duration{Duration: lasts},
		}
	}

	setWindows := func(windows ...talosv1alpha1.MaintenanceWindow) {
		GinkgoHelper()
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.MaintenanceWindows = windows })
	}

	It("starts nothing outside its maintenance windows, and starts when one opens", func() {
		opens := h.clock.Truncate(time.Minute).Add(time.Hour)
		setWindows(daily(opens, 30*time.Minute))
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.advance(10 * time.Minute)

		Expect(h.operation()).To(BeNil())
		Expect(h.applies()).To(BeEmpty())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOutsideWindow))
		Expect(h.blocked().Message).To(ContainSubstring(opens.UTC().Format(time.RFC3339)), "it says when the next window opens")
		Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonOutsideWindow))

		h.at(opens.Add(-time.Second))
		Expect(h.operation()).To(BeNil(), "not a second early")

		h.at(opens)
		h.runUntil("the rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })
		Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
		Expect(h.blocked()).To(BeNil())
	})

	It("finishes the node it is at when the window closes, and begins the next when one opens again", func() {
		opened := h.clock.Truncate(time.Minute)
		// It closes half a minute from now: time for one node's try, not for its soak.
		setWindows(daily(opened, h.clock.Sub(opened)+30*time.Second))
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.settle(nil)
		Expect(h.stepIs(worker1, plan.StepVerifying)()).To(BeTrue(), "%+v", h.operation())

		h.advance(10 * time.Second)
		Expect(h.operation().Current.HealthySince).NotTo(BeNil(), h.inSync().Message)
		Expect(h.blocked()).To(BeNil(), "the window is still open")

		// The window closes in the middle of the node's soak.
		h.advance(30 * time.Second)
		Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}), "the node that was being changed is finished")
		Expect(h.started()).To(BeTrue(), "the rollout is not over: it waits")
		Expect(h.operation().Completed).To(Equal([]string{worker1}))
		Expect(h.operation().Current).To(BeNil())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOutsideWindow))
		Expect(h.inSync().Message).To(ContainSubstring("outside the maintenance windows"))

		h.advance(time.Hour)
		Expect(h.applies()).To(HaveLen(2), "and no other node is begun")

		h.at(opened.Add(24 * time.Hour))
		h.runUntil("the rollout ends", h.idle)
		Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
	})

	It("starts nothing while a window cannot be understood, rather than at any time", func() {
		setWindows(talosv1alpha1.MaintenanceWindow{Schedule: "61 2 * * *", Duration: metav1.Duration{Duration: time.Hour}})
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(h.applies()).To(BeEmpty())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonInvalidRollout))
		Expect(h.blocked().Message).To(ContainSubstring("maintenanceWindows[0]"))

		setWindows(talosv1alpha1.MaintenanceWindow{
			Schedule: "0 2 * * *", Duration: metav1.Duration{Duration: time.Hour}, TimeZone: "Europe/Atlantis",
		})
		h.advance(time.Minute)
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonInvalidRollout))
		Expect(h.blocked().Message).To(ContainSubstring("Europe/Atlantis"))
		Expect(h.applies()).To(BeEmpty())
	})

	Context("with a gate on an object in the cluster", func() {
		const healthy, degraded = "HEALTH_OK", "HEALTH_WARN"

		// storage plays the object a gate asks about: a storage cluster's health.
		storage := func(health string) {
			GinkgoHelper()

			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "storage", Namespace: h.ns}}
			_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, cm, func() error {
				cm.Data = map[string]string{"health": health}

				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		}

		gate := func(expression string) {
			GinkgoHelper()
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
				tc.Spec.Rollout.Gates = []talosv1alpha1.RolloutGate{{
					Name:       "storage-healthy",
					Object:     talosv1alpha1.GateObject{APIVersion: "v1", Kind: "ConfigMap", Namespace: h.ns, Name: "storage"},
					Expression: expression,
				}}
			})
		}

		BeforeEach(func() { gate(`object.data.health == "` + healthy + `"`) })

		It("starts nothing while the gate is not true, and says what it said", func() {
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.advance(time.Minute)
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
			Expect(h.blocked().Message).To(And(ContainSubstring("gate storage-healthy"), ContainSubstring("does not exist")))

			storage(degraded)
			h.advance(time.Minute)
			Expect(h.blocked().Message).To(And(ContainSubstring("gate storage-healthy"), ContainSubstring("is false")))
			Expect(h.operation()).To(BeNil())
			Expect(h.applies()).To(BeEmpty())

			// A field that is not there is not true either.
			gate(`object.status.health == "` + healthy + `"`)
			h.advance(time.Minute)
			Expect(h.blocked().Message).To(ContainSubstring("no such key"))

			gate(`object.data.health ==`)
			h.advance(time.Minute)
			Expect(h.blocked().Message).To(ContainSubstring("does not compile"))
			Expect(h.applies()).To(BeEmpty())

			gate(`object.data.health == "` + healthy + `"`)
			storage(healthy)
			h.runUntil("the rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })
			Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
		})

		It("does not confirm a node, or begin the next, until the gate is true again after it", func() {
			storage(healthy)
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.settle(nil)
			Expect(h.stepIs(worker1, plan.StepVerifying)()).To(BeTrue(), "%+v", h.operation())

			// The change of the node set something off that the gate watches.
			storage(degraded)
			h.advance(time.Minute)
			Expect(h.applies()).To(Equal([]string{tried(worker1)}), "a node is not done while a gate is closed")
			Expect(h.inSync().Message).To(ContainSubstring("gate storage-healthy"))

			storage(healthy)
			h.advance(10 * time.Second)
			Expect(h.applies()).To(Equal([]string{tried(worker1)}), "the soak starts over once the gate is true")

			h.runUntil("the rollout ends", h.idle)
			Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
		})

		It("begins no other node while the gate is closed, and halts when it stays closed", func() {
			storage(healthy)

			// The gate closes the moment the first node is done.
			h.kubelets = func() {
				GinkgoHelper()

				if slices.Contains(h.applies(), confirmed(worker1)) {
					storage(degraded)
				}
			}

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the first node is done", func() bool { return h.started() && len(h.operation().Completed) == 1 })
			h.advance(time.Minute)

			Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}), "nothing is sent to a node behind a closed gate")
			Expect(h.inSync().Message).To(ContainSubstring("gate storage-healthy"))
			Expect(h.failed()).To(BeFalse())

			// Like the cluster's own health: a wait for it is bounded, and ends in a halt.
			h.runUntil("the wait for the gate runs out", h.failed)
			Expect(h.operation().Failure.Reason).To(Equal(plan.FailTimeout))
			Expect(h.operation().Failure.Message).To(ContainSubstring("gate storage-healthy"))
			Expect(h.applies()).To(HaveLen(2))
		})

		It("lets Talos take a config back when the gate stays closed for the whole try", func() {
			storage(healthy)
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.settle(nil)
			storage(degraded)

			deadline := h.operation().Current.Deadline.Time
			h.at(deadline)
			h.expireTry(worker1)
			h.at(deadline.Add(plan.RollbackGrace))

			Expect(h.failed()).To(BeTrue())
			Expect(h.operation().Failure.Reason).To(Equal(plan.FailRolledBack))
			Expect(h.applies()).To(Equal([]string{tried(worker1)}), "never confirmed, and no other node touched")
		})
	})

	Context("with hooks", func() {
		var (
			// seen lists the hooks' Jobs in the order they were created, as "<stage> <machine>".
			seen []string
			// jobs maps what seen lists to the Job's name.
			jobs map[string]string
			// outcome is what a hook's Job does: succeed, fail, or run on ("").
			outcome func(stage, machine string) string
		)

		hook := func(name string, timeout time.Duration) talosv1alpha1.Hook {
			out := talosv1alpha1.Hook{
				Name: name,
				Job:  runtime.RawExtension{Raw: []byte(hookJob)},
			}
			if timeout > 0 {
				out.Timeout = &metav1.Duration{Duration: timeout}
			}

			return out
		}

		setHooks := func(hooks talosv1alpha1.RolloutHooks) {
			GinkgoHelper()
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.Hooks = &hooks })
		}

		everyStage := func() talosv1alpha1.RolloutHooks {
			return talosv1alpha1.RolloutHooks{
				PreRollout: []talosv1alpha1.Hook{hook("announce", 0)}, PostRollout: []talosv1alpha1.Hook{hook("all-clear", 0)},
				PreNode: []talosv1alpha1.Hook{hook("prepare", 0)}, PostNode: []talosv1alpha1.Hook{hook("release", 0)},
			}
		}

		envOf := func(job *batchv1.Job) map[string]string {
			env := map[string]string{}
			for _, e := range job.Spec.Template.Spec.Containers[0].Env {
				env[e.Name] = e.Value
			}

			return env
		}

		getJob := func(key string) *batchv1.Job {
			GinkgoHelper()

			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: h.ns, Name: jobs[key]}, job)).To(Succeed(), "no Job for "+key)

			return job
		}

		BeforeEach(func() {
			seen, jobs = nil, map[string]string{}
			outcome = func(string, string) string { return hookSucceeds }

			// The Job controller, and the pods it would run: each hook's Job ends as the spec says.
			h.kubelets = func() {
				GinkgoHelper()

				var list batchv1.JobList
				Expect(k8sClient.List(ctx, &list, client.InNamespace(h.ns), client.HasLabels{talosv1alpha1.LabelHook})).To(Succeed())

				for i := range list.Items {
					job := &list.Items[i]
					env := envOf(job)
					key := strings.TrimSpace(env["TALOS_HOOK"] + " " + env["TALOS_MACHINE"])

					if _, known := jobs[key+" "+job.Name]; !known {
						jobs[key+" "+job.Name] = job.Name
						jobs[key] = job.Name
						seen = append(seen, key)
					}

					if len(job.Status.Conditions) > 0 {
						continue
					}

					now := metav1.NewTime(h.clock)

					switch outcome(env["TALOS_HOOK"], env["TALOS_MACHINE"]) {
					case hookSucceeds:
						job.Status.StartTime, job.Status.CompletionTime, job.Status.Succeeded = &now, &now, 1
						job.Status.Conditions = []batchv1.JobCondition{
							{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
							{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
						}
					case hookFails:
						job.Status.StartTime, job.Status.Failed = &now, 1
						job.Status.Conditions = []batchv1.JobCondition{
							{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now, Reason: "BackoffLimitExceeded"},
							{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now, Reason: "BackoffLimitExceeded",
								Message: "Job has reached the specified backoff limit"},
						}
					default:
						continue
					}

					Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
				}
			}
		})

		It("runs them around the rollout and around each node, in order, and ends only when the last is through", func() {
			setHooks(everyStage())

			// The first hook of a node runs on: nothing is sent to the node while it does.
			outcome = func(stage, _ string) string {
				if stage == talosv1alpha1.HookPreNode {
					return ""
				}

				return hookSucceeds
			}

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the node's hook is started", func() bool { return len(seen) == 2 })
			h.advance(time.Minute)
			Expect(seen).To(Equal([]string{talosv1alpha1.HookPreRollout, "preNode " + worker1}))
			Expect(h.applies()).To(BeEmpty(), "nothing is sent to a node before its preNode hooks are through")
			Expect(h.inSync().Message).To(ContainSubstring("preNode hook prepare"))

			job := getJob("preNode " + worker1)
			Expect(envOf(job)).To(Equal(map[string]string{
				"TALOS_CLUSTER": h.tc.Name, "TALOS_OPERATION": "ConfigRollout", "TALOS_OPERATION_ID": h.operation().ID,
				"TALOS_HOOK": "preNode", "TALOS_MACHINE": worker1, "TALOS_NODE": h.node(worker1).node,
			}))
			Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(*job.Spec.BackoffLimit).To(BeZero(), "a hook that fails is not run again behind the operator's back")
			Expect(*job.Spec.ActiveDeadlineSeconds).To(Equal(int64(plan.DefaultStepTimeout.Seconds())))
			Expect(metav1.IsControlledBy(job, h.tc)).To(BeTrue())
			Expect(job.Name).To(HavePrefix(h.tc.Name + "-prepare-"))

			outcome = func(string, string) string { return hookSucceeds }
			h.runUntil("the rollout ends", h.idle)

			Expect(seen).To(Equal([]string{
				talosv1alpha1.HookPreRollout,
				"preNode " + worker1, "postNode " + worker1,
				"preNode " + worker2, "postNode " + worker2,
				talosv1alpha1.HookPostRollout,
			}))
			Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(h.tc.Status.History[0].Completed).To(Equal([]string{worker1, worker2}))
			Expect(envOf(getJob(talosv1alpha1.HookPostRollout))).NotTo(HaveKey("TALOS_MACHINE"))
		})

		It("holds the lock until the postRollout hooks are through", func() {
			setHooks(talosv1alpha1.RolloutHooks{PostRollout: []talosv1alpha1.Hook{hook("all-clear", 0)}})
			outcome = func(string, string) string { return "" }

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the last hook is started", func() bool { return len(seen) == 1 })
			h.advance(time.Minute)

			Expect(h.started()).To(BeTrue(), "the operation is not over while it cleans up")
			Expect(h.operation().Ending.Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(h.applies()).To(HaveLen(4))

			// Nothing else starts meanwhile: the lock is the operation's.
			h.setPoolConfig(talosv1alpha1.MachineRoleControlPlane, labelConfig("two"))
			h.advance(time.Minute)
			Expect(h.applies()).To(HaveLen(4))

			outcome = func(string, string) string { return hookSucceeds }
			h.runUntil("both rollouts end", func() bool { return h.idle() && len(h.tc.Status.History) == 2 })
			Expect(h.applies()).To(HaveLen(6))
		})

		It("halts on a hook that fails, with the node untouched, and runs it again on retry", func() {
			setHooks(everyStage())
			outcome = func(stage, machine string) string {
				if stage == talosv1alpha1.HookPreNode && machine == worker1 {
					return hookFails
				}

				return hookSucceeds
			}

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the rollout halts", h.failed)

			f := h.operation().Failure
			Expect(f.Reason).To(Equal(plan.FailHook))
			Expect(f.Hook).To(Equal(talosv1alpha1.HookPreNode))
			Expect(f.Machine).To(Equal(worker1))
			Expect(f.Step).To(Equal("preNode/prepare"))
			Expect(f.Message).To(ContainSubstring("BackoffLimitExceeded"))
			Expect(f.Remediation).To(And(ContainSubstring("kubectl logs -n "+h.ns+" job/"+jobs["preNode "+worker1]),
				ContainSubstring(talosv1alpha1.AnnotationRetry+"="+f.Token), ContainSubstring(talosv1alpha1.AnnotationSkipMachine+"="+f.Token)))
			Expect(h.applies()).To(BeEmpty())
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
			Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))

			h.advance(time.Hour)
			Expect(h.applies()).To(BeEmpty(), "it stays halted, whatever time passes")

			first := jobs["preNode "+worker1]
			outcome = func(string, string) string { return hookSucceeds }
			h.annotate(talosv1alpha1.AnnotationRetry, f.Token)
			h.runUntil("the rollout ends", h.idle)

			Expect(jobs["preNode "+worker1]).NotTo(Equal(first), "a retry is a Job of its own")
			Expect(seen).To(Equal([]string{
				talosv1alpha1.HookPreRollout, "preNode " + worker1, "preNode " + worker1, "postNode " + worker1,
				"preNode " + worker2, "postNode " + worker2, talosv1alpha1.HookPostRollout,
			}))
			Expect(h.applies()).To(HaveLen(4))
		})

		It("leaves a node out when its hook failed and a human says so, and still cleans up", func() {
			setHooks(everyStage())
			outcome = func(stage, machine string) string {
				if stage == talosv1alpha1.HookPreNode && machine == worker1 {
					return hookFails
				}

				return hookSucceeds
			}

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the rollout halts", h.failed)
			h.annotate(talosv1alpha1.AnnotationSkipMachine, h.operation().Failure.Token)
			h.runUntil("the rollout ends", h.idle)

			Expect(h.applies()).To(Equal([]string{tried(worker2), confirmed(worker2)}))
			Expect(h.tc.Status.History[0].Skipped).To(Equal([]string{worker1}))
			Expect(seen).To(Equal([]string{
				talosv1alpha1.HookPreRollout, "preNode " + worker1, "preNode " + worker2, "postNode " + worker2, talosv1alpha1.HookPostRollout,
			}))

			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil(), "and the rollout does not come back for it")
			Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonHeld))
		})

		It("runs the postRollout hooks of a rollout a human aborts, and not of one that never announced itself", func() {
			setHooks(everyStage())
			outcome = func(stage, _ string) string {
				if stage == talosv1alpha1.HookPreNode {
					return hookFails
				}

				return hookSucceeds
			}

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the rollout halts", h.failed)
			h.annotate(talosv1alpha1.AnnotationAbort, h.operation().Failure.Token)
			h.runUntil("the rollout ends", h.idle)

			Expect(seen).To(Equal([]string{talosv1alpha1.HookPreRollout, "preNode " + worker1, talosv1alpha1.HookPostRollout}))
			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
			Expect(h.applies()).To(BeEmpty())
			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil(), "an aborted rollout is not started again")

			// Another change, whose very first hook fails: what was not set up is not taken down.
			seen = nil
			outcome = func(string, string) string { return hookFails }

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("two"))
			h.runUntil("the rollout halts", h.failed)
			Expect(h.operation().Failure.Hook).To(Equal(talosv1alpha1.HookPreRollout))
			Expect(h.operation().Failure.Remediation).NotTo(ContainSubstring(talosv1alpha1.AnnotationSkipMachine))

			h.annotate(talosv1alpha1.AnnotationAbort, h.operation().Failure.Token)
			h.runUntil("the rollout ends", h.idle)
			Expect(seen).To(Equal([]string{talosv1alpha1.HookPreRollout}))
		})

		It("halts on a hook that does not finish in its time", func() {
			setHooks(talosv1alpha1.RolloutHooks{PreNode: []talosv1alpha1.Hook{hook("prepare", 2*time.Minute)}})
			outcome = func(string, string) string { return "" }

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the hook is started", func() bool { return len(seen) == 1 })
			Expect(*getJob("preNode " + worker1).Spec.ActiveDeadlineSeconds).To(Equal(int64(120)))

			since := h.operation().Hook.Since.Time
			h.at(since.Add(2*time.Minute - time.Second))
			Expect(h.failed()).To(BeFalse())

			h.at(since.Add(2 * time.Minute))
			Expect(h.failed()).To(BeTrue())
			Expect(h.operation().Failure.Message).To(ContainSubstring("did not succeed within 2m0s"))
			Expect(h.applies()).To(BeEmpty())
		})

		It("halts on a hook whose Job the API server refuses", func() {
			setHooks(talosv1alpha1.RolloutHooks{PreRollout: []talosv1alpha1.Hook{{
				Name: "broken", Job: runtime.RawExtension{Raw: []byte(`{"template":{"spec":{"containers":[]}}}`)},
			}}})

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the rollout halts", h.failed)
			Expect(h.operation().Failure.Message).To(ContainSubstring("could not be created"))
			Expect(seen).To(BeEmpty())
			Expect(h.applies()).To(BeEmpty())
		})

		It("is carried on by another replica without running a hook twice", func() {
			setHooks(everyStage())
			outcome = func(stage, _ string) string {
				if stage == talosv1alpha1.HookPreNode {
					return ""
				}

				return hookSucceeds
			}

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the node's hook is started", func() bool { return len(seen) == 2 })

			// A new process: nothing but status.operation tells it a hook is running.
			h.takeOver()
			h.advance(time.Minute)
			Expect(seen).To(HaveLen(2), "the Job that runs is found, not started again")

			outcome = func(string, string) string { return hookSucceeds }
			h.runUntil("the rollout ends", h.idle)
			Expect(seen).To(HaveLen(6))
		})
	})
})
