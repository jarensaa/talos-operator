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
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/gates"
	"github.com/jarensaa/talos-operator/internal/plan"
)

// The rollout policy of a cluster as it stands in this pass (docs/scope.md §6.4): whether a
// maintenance window is open, and what the user's gates say. internal/plan decides what follows
// from it; this file only finds out.

// gateReadTimeout bounds the reading and evaluating of one gate.
const gateReadTimeout = 10 * time.Second

// policy evaluates spec.rollout's windows and gates. It also returns when the answer about the
// windows changes next, if it does: the cluster is looked at again then.
//
// Nothing here is an error of the pass. A window that cannot be understood and a gate that
// cannot be read are both answers: closed, with the reason.
func (r *TalosClusterReconciler) policy(ctx context.Context, tc *talosv1alpha1.TalosCluster) (plan.Policy, time.Time) {
	policy := plan.Policy{Paused: tc.Spec.Paused}

	// Timings under which a tried config could never be confirmed start nothing.
	if err := plan.TimingOf(tc.Spec.Rollout).Validate(); err != nil {
		policy.Invalid = "spec.rollout: " + err.Error()
	}

	windows := make([]plan.Window, 0, len(tc.Spec.Rollout.MaintenanceWindows))
	for _, w := range tc.Spec.Rollout.MaintenanceWindows {
		windows = append(windows, plan.Window{Schedule: w.Schedule, Duration: w.Duration.Duration, TimeZone: w.TimeZone})
	}

	window, changes, err := plan.Windows(windows, r.now())
	if !window.Open {
		policy.OutsideWindow, policy.WindowsInvalid = window.Reason, err != nil
	}

	for _, gate := range tc.Spec.Rollout.Gates {
		if why := r.gateClosed(ctx, gate); why != "" {
			policy.GateClosed = fmt.Sprintf("gate %s: %s", gate.Name, why)

			break
		}
	}

	return policy, changes
}

// gateClosed says why a gate is closed, or returns "" for one that is open: its object exists,
// could be read, and the expression is true of it.
func (r *TalosClusterReconciler) gateClosed(ctx context.Context, gate talosv1alpha1.RolloutGate) string {
	ctx, cancel := context.WithTimeout(ctx, gateReadTimeout)
	defer cancel()

	ref := gate.Object
	what := fmt.Sprintf("%s %s", ref.Kind, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name})

	if err := gates.Check(gate.Expression); err != nil {
		return err.Error()
	}

	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return fmt.Sprintf("apiVersion %q: %v", ref.APIVersion, err)
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gv.WithKind(ref.Kind))

	// Uncached: a gate may name any kind, and a cache would watch every object of it.
	switch err := r.apiReader().Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, obj); {
	case err == nil:
	case apierrors.IsNotFound(err):
		return what + " does not exist"
	case apierrors.IsForbidden(err):
		return fmt.Sprintf("the operator may not read %s; grant it get on the kind in the chart's gates.rules", what)
	case meta.IsNoMatchError(err):
		return fmt.Sprintf("the cluster has no kind %s in %s", ref.Kind, ref.APIVersion)
	default:
		logf.FromContext(ctx).Info("reading a gate's object failed", "gate", gate.Name, "error", err.Error())

		return fmt.Sprintf("%s could not be read: %v", what, err)
	}

	open, err := gates.Evaluate(ctx, gate.Expression, obj.Object)
	switch {
	case err != nil:
		return err.Error()
	case !open:
		return fmt.Sprintf("%s is false of %s", gate.Expression, what)
	}

	return ""
}
