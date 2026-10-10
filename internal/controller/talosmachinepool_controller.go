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
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// maxListedNames bounds the names listed in a condition message.
const maxListedNames = 10

// TalosMachinePoolReconciler reports a pool's membership and keeps a pool with members from being
// deleted (docs/scope.md §5.2). It reads Kubernetes objects only.
type TalosMachinePoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachinepools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachinepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachinepools/finalizers,verbs=update

// Reconcile updates one pool's status and its in-use finalizer.
func (r *TalosMachinePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pool := &talosv1alpha1.TalosMachinePool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	members, err := machinesOfPool(ctx, r.Client, pool.Namespace, pool.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	if done, err := protectInUse(ctx, r.Client, pool, len(members) > 0); done || err != nil {
		return ctrl.Result{}, err
	}

	original := pool.DeepCopy()
	st := &pool.Status
	st.ObservedGeneration = pool.Generation
	st.Members, st.Matched, st.ConfigMatchesLive = int32(len(members)), 0, 0 //nolint:gosec // bounded by the namespace

	names := make([]string, 0, len(members))

	for _, tm := range members {
		names = append(names, tm.Name)

		if meta.IsStatusConditionTrue(tm.Status.Conditions, talosv1alpha1.ConditionMatched) {
			st.Matched++
		}

		if meta.IsStatusConditionTrue(tm.Status.Conditions, talosv1alpha1.ConditionConfigMatchesLive) {
			st.ConfigMatchesLive++
		}
	}

	if len(members) == 0 {
		setConditionIn(&st.Conditions, pool.Generation, talosv1alpha1.ConditionInUse, metav1.ConditionFalse, talosv1alpha1.ReasonSucceeded,
			"the pool has no members")
	} else {
		msg := fmt.Sprintf("%d member(s): %s", len(members), listNames(names))
		if !pool.DeletionTimestamp.IsZero() {
			msg += "; deletion waits until they move to another pool or are deleted"
		}

		setConditionIn(&st.Conditions, pool.Generation, talosv1alpha1.ConditionInUse, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, msg)
	}

	if !equality.Semantic.DeepEqual(original.Status, pool.Status) {
		if err := r.Status().Patch(ctx, pool, client.MergeFrom(original)); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch status: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// protectInUse keeps FinalizerInUse on obj until it is being deleted and no longer in use, the way
// Kubernetes protects bound PVCs. It returns true when it changed obj, which triggers a new
// reconcile; if the operator is down, a deletion waits, which is the safe direction.
func protectInUse(ctx context.Context, c client.Client, obj client.Object, inUse bool) (bool, error) {
	var changed bool

	if obj.GetDeletionTimestamp().IsZero() {
		changed = controllerutil.AddFinalizer(obj, talosv1alpha1.FinalizerInUse)
	} else if !inUse {
		changed = controllerutil.RemoveFinalizer(obj, talosv1alpha1.FinalizerInUse)
	}

	if !changed {
		return false, nil
	}

	if err := c.Update(ctx, obj); err != nil {
		return true, client.IgnoreNotFound(err)
	}

	return true, nil
}

func listNames(names []string) string {
	if len(names) <= maxListedNames {
		return strings.Join(names, ", ")
	}

	return fmt.Sprintf("%s and %d more", strings.Join(names[:maxListedNames], ", "), len(names)-maxListedNames)
}

// poolOfMachine re-reconciles a machine's pool. On a pool change both the old and the new pool are
// mapped, since the update event carries both objects.
func poolOfMachine(_ context.Context, obj client.Object) []reconcile.Request {
	tm, ok := obj.(*talosv1alpha1.TalosMachine)
	if !ok {
		return nil
	}

	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: tm.Namespace, Name: tm.Spec.PoolRef.Name}}}
}

// SetupWithManager sets up the controller with the Manager.
func (r *TalosMachinePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&talosv1alpha1.TalosMachinePool{}).
		Watches(&talosv1alpha1.TalosMachine{}, handler.EnqueueRequestsFromMapFunc(poolOfMachine)).
		Named("talosmachinepool").
		Complete(r)
}
