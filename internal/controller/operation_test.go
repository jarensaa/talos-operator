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
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// operationTypes reads the types of operation the CRD admits from the marker on the type, so
// that a type added to the API cannot be forgotten here.
func operationTypes(t *testing.T) []talosv1alpha1.OperationType {
	t.Helper()

	source, err := os.ReadFile(filepath.Join("..", "..", "api", "v1alpha1", "taloscluster_types.go"))
	if err != nil {
		t.Fatal(err)
	}

	marker := regexp.MustCompile(`\+kubebuilder:validation:Enum=(\S+)\ntype OperationType string`).FindSubmatch(source)
	if marker == nil {
		t.Fatal("the enum marker of OperationType was not found")
	}

	names := strings.Split(string(marker[1]), ";")
	types := make([]talosv1alpha1.OperationType, 0, len(names))

	for _, name := range names {
		types = append(types, talosv1alpha1.OperationType(name))
	}

	return types
}

// Every type of operation the API admits is a kind the operator knows: what it is called, which
// condition reports on it, what its Events are called, and what its turns do.
func TestEveryTypeOfOperationIsAKind(t *testing.T) {
	types := operationTypes(t)
	if len(types) != len(kinds) {
		t.Fatalf("the API admits %d types of operation and the operator knows %d kinds", len(types), len(kinds))
	}

	named, done := map[string]talosv1alpha1.OperationType{}, map[string]talosv1alpha1.OperationType{}

	for _, of := range types {
		k, known := kinds[of]
		if !known {
			t.Fatalf("an operation of type %s is none the operator knows", of)
		}

		for what, value := range map[string]string{
			"name": k.name, "condition": k.condition, "reason while in flight": k.running, "what it still finishes": k.finishes,
			"event": k.event, "event of a turn that is done": k.done, "what a turn that is done does": k.does,
		} {
			if value == "" {
				t.Errorf("%s: no %s", of, what)
			}
		}

		if k.turns == nil {
			t.Errorf("%s: no turns", of)
		}

		if (k.allowed == nil) != (k.disabled == "") {
			t.Errorf("%s: a kind that can be switched off says so when it is, and no other does", of)
		}

		// The Events of two kinds must not be told apart by their message alone.
		if other, taken := named[k.event]; taken {
			t.Errorf("%s and %s are both called %s in their Events", of, other, k.event)
		}

		if other, taken := done[k.done]; taken {
			t.Errorf("a turn of %s and one of %s are both reported as %s", of, other, k.done)
		}

		named[k.event], done[k.done] = of, of
	}

	// Config management is all a rollout needs, and what ends one is its return to Observe.
	if kinds[talosv1alpha1.OperationConfigRollout].allowed != nil {
		t.Error("a config rollout is switched off by nothing but config management")
	}
}

// An operation tells its story under the name of its own kind. An etcd defragmentation used to
// tell that of a config rollout: started as a rollout, each member "running its rendered config".
func TestEventsNameTheKindOfOperation(t *testing.T) {
	for of, k := range kinds {
		t.Run(string(of), func(t *testing.T) {
			var told eventsOf

			r := &TalosClusterReconciler{Recorder: &told}
			started := metav1.NewTime(time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC))

			before := &talosv1alpha1.TalosCluster{}
			running := &talosv1alpha1.TalosCluster{}
			running.Status.Operation = &talosv1alpha1.Operation{ID: "a-1", Type: of, Plan: []string{"cp-1"}, StartedAt: started}

			done := running.DeepCopy()
			done.Status.Operation.Completed = []string{"cp-1"}

			ended := &talosv1alpha1.TalosCluster{}
			ended.Status.History = []talosv1alpha1.OperationRecord{{
				ID: "a-1", Type: of, Outcome: talosv1alpha1.OperationSucceeded, Completed: []string{"cp-1"}, StartedAt: started, FinishedAt: started,
			}}

			r.emitOperation(context.Background(), before, running)
			r.emitOperation(context.Background(), running, done)
			r.emitOperation(context.Background(), done, ended)

			want := []string{k.event + "Started", k.done, k.event + talosv1alpha1.OperationSucceeded}
			if !slices.Equal(told.reasons, want) {
				t.Fatalf("told as %v, want %v", told.reasons, want)
			}

			if of != talosv1alpha1.OperationConfigRollout && slices.ContainsFunc(told.reasons, func(reason string) bool {
				return strings.Contains(reason, "Rollout") || reason == "MachineConfigured"
			}) {
				t.Fatalf("a %s is told as a config rollout: %v", of, told.reasons)
			}
		})
	}
}

// eventsOf records the reasons of the Events it is handed.
type eventsOf struct{ reasons []string }

var _ events.EventRecorder = (*eventsOf)(nil)

func (e *eventsOf) Eventf(_, _ runtime.Object, _, reason, _, _ string, _ ...any) {
	e.reasons = append(e.reasons, reason)
}

func TestHoldsKeepNothingOfAKindThatHasNothingToRemember(t *testing.T) {
	tc := &talosv1alpha1.TalosCluster{}

	hold(tc, talosv1alpha1.OperationTalosUpgrade, func(h *talosv1alpha1.Holds) { h.Skipped["w-1"] = "image" })
	hold(tc, talosv1alpha1.OperationConfigRollout, func(h *talosv1alpha1.Holds) { h.Aborted["w-2"] = "hash" })

	if got := held(tc, talosv1alpha1.OperationTalosUpgrade); got.Skipped["w-1"] != "image" || got.Aborted != nil {
		t.Fatalf("got %+v", got)
	}

	// Looking at the holds of a kind that has none leaves nothing behind: a cluster at rest is not
	// written to.
	hold(tc, talosv1alpha1.OperationAddMachine, func(*talosv1alpha1.Holds) {})

	if _, kept := tc.Status.Holds[talosv1alpha1.OperationAddMachine]; kept || len(tc.Status.Holds) != 2 {
		t.Fatalf("got %+v", tc.Status.Holds)
	}

	hold(tc, talosv1alpha1.OperationTalosUpgrade, func(h *talosv1alpha1.Holds) { delete(h.Skipped, "w-1") })
	hold(tc, talosv1alpha1.OperationConfigRollout, func(h *talosv1alpha1.Holds) { clear(h.Aborted) })

	if tc.Status.Holds != nil {
		t.Fatalf("holds that are spent are forgotten altogether, got %+v", tc.Status.Holds)
	}
}

// What a human ended is remembered turn by turn, and added to what was ended before: a machine
// an earlier operation was ended at is not let go because a later one was ended too.
func TestAbortRemembersWhatWasLeft(t *testing.T) {
	tc := &talosv1alpha1.TalosCluster{}
	tc.Status.Holds = map[talosv1alpha1.OperationType]talosv1alpha1.Holds{
		talosv1alpha1.OperationRemoveMachine: {Aborted: map[string]string{"w-1": ""}},
	}
	tc.Status.Operation = &talosv1alpha1.Operation{
		Type: talosv1alpha1.OperationRemoveMachine, Plan: []string{"w-2", "w-3"}, Completed: []string{"w-2"},
	}

	abort(tc, kinds[talosv1alpha1.OperationRemoveMachine], turns{})

	got := held(tc, talosv1alpha1.OperationRemoveMachine).Aborted
	if _, kept := got["w-1"]; !kept || len(got) != 2 {
		t.Fatalf("got %v", got)
	}

	if _, done := got["w-2"]; done {
		t.Fatal("a turn that was done is not remembered as one that was left")
	}

	// A kind that has nothing to remember remembers nothing.
	tc.Status.Operation = &talosv1alpha1.Operation{Type: talosv1alpha1.OperationEtcdDefragment, Plan: []string{"cp-1"}}
	abort(tc, kinds[talosv1alpha1.OperationEtcdDefragment], turns{})

	if _, kept := tc.Status.Holds[talosv1alpha1.OperationEtcdDefragment]; kept {
		t.Fatal("a defragmentation that was ended leaves nothing behind: its interval keeps the next one away")
	}
}

// An answer a kind of step does not carry out is an error of the pass, never a silent success.
func TestAnAnswerNoStepCarriesOutIsRefused(t *testing.T) {
	op := &talosv1alpha1.Operation{
		Type:    talosv1alpha1.OperationConfigRollout,
		Current: &talosv1alpha1.OperationStep{Machine: "w-1", Step: plan.StepTrying},
	}
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)

	for _, kind := range []plan.ActionKind{plan.Try, plan.Reboot, plan.Reset, plan.Defrag, plan.Run, plan.Complete} {
		if note, done := answered(op, plan.Action{Kind: kind}, now); done || note != "" {
			t.Fatalf("%s is no answer every kind of step has, and was carried out as one", kind)
		}
	}

	if err := unexpected(op, plan.Action{Kind: plan.Reboot}); err == nil || !strings.Contains(err.Error(), "Reboot") {
		t.Fatalf("got %v", err)
	}

	if note, done := answered(op, plan.Action{Kind: plan.Wait, Message: "waiting"}, now); !done || note != "waiting" {
		t.Fatalf("got %q, %v", note, done)
	}

	if _, done := answered(op, plan.Action{Kind: plan.Fail, Reason: plan.FailTimeout, Message: "late"}, now); !done || op.Failure == nil {
		t.Fatal("a failure halts the operation")
	}
}

// The planners judge a node by the stage Talos gives a booted, configured one. They may not ask
// the Talos machinery for its name, so this holds the two to each other.
func TestThePlannersCallARunningNodeWhatTalosDoes(t *testing.T) {
	if plan.StageRunning != talos.StageRunning {
		t.Fatalf("the planners call it %q and Talos %q", plan.StageRunning, talos.StageRunning)
	}
}

// A condition that is cleared and set again within a pass has not changed. Its transition time
// moves only when its status is another than it was when the pass began.
func TestAConditionSetAgainHasNotChanged(t *testing.T) {
	then := metav1.NewTime(time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC))
	now := metav1.NewTime(then.Add(time.Hour))

	before := []metav1.Condition{
		{Type: talosv1alpha1.ConditionRolloutBlocked, Status: metav1.ConditionTrue, LastTransitionTime: then},
		{Type: talosv1alpha1.ConditionConfigInSync, Status: metav1.ConditionFalse, LastTransitionTime: then},
	}
	after := []metav1.Condition{
		{Type: talosv1alpha1.ConditionConfigInSync, Status: metav1.ConditionTrue, LastTransitionTime: now},
		{Type: talosv1alpha1.ConditionRolloutBlocked, Status: metav1.ConditionTrue, LastTransitionTime: now},
		{Type: talosv1alpha1.ConditionTalosUpToDate, Status: metav1.ConditionTrue, LastTransitionTime: now},
	}

	keepTransitions(before, after)

	for i, want := range []metav1.Time{now, then, now} {
		if !after[i].LastTransitionTime.Equal(&want) {
			t.Errorf("%s: its transition is at %s, want %s", after[i].Type, after[i].LastTransitionTime, want)
		}
	}
}
