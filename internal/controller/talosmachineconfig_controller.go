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
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/render"
)

// TalosMachineConfigReconciler validates a TalosMachineConfig, reports what uses it (the blast
// radius of an edit) and keeps it from being deleted while it is referenced (docs/scope.md §5.3).
// It reads Kubernetes objects only.
type TalosMachineConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachineconfigs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachineconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachineconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachinepools,verbs=get;list;watch

// Reconcile updates one TalosMachineConfig's status and its in-use finalizer.
func (r *TalosMachineConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	mc := &talosv1alpha1.TalosMachineConfig{}
	if err := r.Get(ctx, req.NamespacedName, mc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	layers, err := listLayers(ctx, r.Client, mc.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	usage := usageOf(layers, mc.Name)
	inUse := len(usage.Clusters) > 0 || len(usage.Pools) > 0 || usage.Machines > 0

	if done, err := protectInUse(ctx, r.Client, mc, inUse); done || err != nil {
		return ctrl.Result{}, err
	}

	original := mc.DeepCopy()
	st := &mc.Status
	st.ObservedGeneration = mc.Generation
	st.UsedBy = &usage

	keys, err := render.PatchDocKeys([]byte(mc.Spec.Documents))

	switch {
	case errors.Is(err, render.ErrJSONPatch):
		st.DocumentKeys = nil
		setConditionIn(&st.Conditions, mc.Generation, talosv1alpha1.ConditionValid, metav1.ConditionFalse, talosv1alpha1.ReasonParseError,
			"documents are a JSON6902 patch; only config documents (strategic merge) are supported")
	case err != nil:
		// The parser's error can quote the documents.
		st.DocumentKeys = nil
		setConditionIn(&st.Conditions, mc.Generation, talosv1alpha1.ConditionValid, metav1.ConditionFalse, talosv1alpha1.ReasonParseError,
			"documents do not parse as Talos config documents")
	default:
		st.DocumentKeys = make([]string, 0, len(keys))
		for _, k := range keys {
			st.DocumentKeys = append(st.DocumentKeys, string(k))
		}

		setConditionIn(&st.Conditions, mc.Generation, talosv1alpha1.ConditionValid, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			fmt.Sprintf("%d config document(s)", len(keys)))
	}

	if inUse {
		msg := fmt.Sprintf("used by %d machine(s) through %d cluster(s) and %d pool(s)", usage.Machines, len(usage.Clusters), len(usage.Pools))
		if !mc.DeletionTimestamp.IsZero() {
			msg += "; deletion waits until nothing references it"
		}

		setConditionIn(&st.Conditions, mc.Generation, talosv1alpha1.ConditionInUse, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, msg)
	} else {
		setConditionIn(&st.Conditions, mc.Generation, talosv1alpha1.ConditionInUse, metav1.ConditionFalse, talosv1alpha1.ReasonSucceeded,
			"nothing references it")
	}

	if !equality.Semantic.DeepEqual(original.Status, mc.Status) {
		if err := r.Status().Patch(ctx, mc, client.MergeFrom(original)); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch status: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// usageOf finds what references the TalosMachineConfig name: clusters and pools whose layer does,
// and machines whose rendered config includes it through any layer.
func usageOf(l *namespaceLayers, name string) talosv1alpha1.ConfigUsage {
	ref := objectRef{config: name}

	var usage talosv1alpha1.ConfigUsage

	for _, tc := range l.clusters {
		if ref.in(tc.Spec.Config) {
			usage.Clusters = append(usage.Clusters, tc.Name)
		}
	}

	for _, pool := range l.pools {
		if ref.in(pool.Spec.Config) {
			usage.Pools = append(usage.Pools, pool.Name)
		}
	}

	for i := range l.machines {
		if l.machineDependsOn(&l.machines[i], ref) {
			usage.Machines++
		}
	}

	slices.Sort(usage.Clusters)
	slices.Sort(usage.Pools)

	return usage
}

// configsInNamespace re-reconciles every TalosMachineConfig of the namespace. A spec change to a
// cluster, pool or machine can add or drop a reference directly or through the layers above it;
// namespaces hold few configs, so re-evaluating all of them is simpler than tracking each path.
func configsInNamespace(c client.Reader) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		var list talosv1alpha1.TalosMachineConfigList
		if err := c.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
			mapFailed(ctx, err, "configs of the namespace", obj)

			return nil
		}

		reqs := make([]reconcile.Request, 0, len(list.Items))
		for i := range list.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}

		return reqs
	}
}

// SetupWithManager sets up the controller with the Manager. References live in specs, so status
// updates of clusters, pools and machines are ignored.
func (r *TalosMachineConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	specChanges := builder.WithPredicates(predicate.GenerationChangedPredicate{})
	mapAll := handler.EnqueueRequestsFromMapFunc(configsInNamespace(mgr.GetClient()))

	return ctrl.NewControllerManagedBy(mgr).
		For(&talosv1alpha1.TalosMachineConfig{}).
		Watches(&talosv1alpha1.TalosCluster{}, mapAll, specChanges).
		Watches(&talosv1alpha1.TalosMachinePool{}, mapAll, specChanges).
		Watches(&talosv1alpha1.TalosMachine{}, mapAll, specChanges).
		Named("talosmachineconfig").
		Complete(r)
}
