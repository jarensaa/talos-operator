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
	"slices"

	"github.com/siderolabs/talos/pkg/machinery/constants"
	"github.com/siderolabs/talos/pkg/machinery/role"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
)

// talosServiceAccountGVK is the type through which Talos issues API credentials to workloads.
// Talos installs its CRD only while a KubeTalosAPIAccessConfig is active, so the operator uses it
// unstructured and never watches it: a watch on a missing CRD would stop the manager from
// starting. Re-issued Secrets trigger reconciles through the Secret watch instead.
var talosServiceAccountGVK = schema.GroupVersionKind{
	Group:   constants.ServiceAccountResourceGroup,
	Version: constants.ServiceAccountResourceVersion,
	Kind:    constants.ServiceAccountResourceKind,
}

// operatorRoles are the Talos roles the operator requests. os:admin is required to read machine
// configs (docs/scope.md §7.1).
var operatorRoles = []string{string(role.Admin)}

// +kubebuilder:rbac:groups=talos.dev,resources=serviceaccounts,verbs=get;create;update

// requestCredentials keeps a talos.dev ServiceAccount with the operator's roles in the
// TalosCluster's namespace, so Talos issues the operator's certificate (docs/scope.md §9). This
// writes to Kubernetes only, never to Talos, and runs in every management mode. Problems Talos
// reports are returned as *creds.Error.
func (r *TalosClusterReconciler) requestCredentials(ctx context.Context, tc *talosv1alpha1.TalosCluster) error {
	if !tc.UsesTalosServiceAccount() {
		return nil
	}

	disabled := &creds.Error{
		Reason: talosv1alpha1.ReasonTalosAPIAccessDisabled,
		Message: fmt.Sprintf("Talos API access for workloads is not enabled: add a KubeTalosAPIAccessConfig to the control planes "+
			"that allows %s for namespace %s", role.Admin, tc.Namespace),
	}

	sa := &unstructured.Unstructured{}
	sa.SetGroupVersionKind(talosServiceAccountGVK)

	err := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: tc.TalosServiceAccountName()}, sa)

	switch {
	case meta.IsNoMatchError(err):
		return disabled
	case apierrors.IsNotFound(err):
		sa.SetNamespace(tc.Namespace)
		sa.SetName(tc.TalosServiceAccountName())
		sa.SetLabels(map[string]string{talosv1alpha1.LabelCluster: tc.Name})

		if err := unstructured.SetNestedStringSlice(sa.Object, operatorRoles, "spec", "roles"); err != nil {
			return err
		}

		// Deleting the TalosCluster deletes the ServiceAccount, and Talos then deletes its Secret.
		if err := controllerutil.SetControllerReference(tc, sa, r.Scheme); err != nil {
			return err
		}

		if err := r.Create(ctx, sa); err != nil {
			if meta.IsNoMatchError(err) {
				return disabled
			}

			return fmt.Errorf("create Talos ServiceAccount: %w", err)
		}

		logf.FromContext(ctx).Info("requested credentials from Talos", "serviceAccount", sa.GetName(), "roles", operatorRoles)

		return nil
	case err != nil:
		return fmt.Errorf("get Talos ServiceAccount: %w", err)
	}

	if roles, _, _ := unstructured.NestedStringSlice(sa.Object, "spec", "roles"); !slices.Equal(roles, operatorRoles) {
		// The object was read whole, so the update keeps the status Talos wrote.
		if err := unstructured.SetNestedStringSlice(sa.Object, operatorRoles, "spec", "roles"); err != nil {
			return err
		}

		if err := r.Update(ctx, sa); err != nil {
			return fmt.Errorf("update Talos ServiceAccount: %w", err)
		}

		logf.FromContext(ctx).Info("restored the roles of the Talos ServiceAccount", "serviceAccount", sa.GetName(),
			"found", roles, "roles", operatorRoles)
	}

	if reason, _, _ := unstructured.NestedString(sa.Object, "status", "failureReason"); reason != "" {
		return &creds.Error{
			Reason: talosv1alpha1.ReasonTalosAPIAccessDenied,
			Message: fmt.Sprintf("Talos refused ServiceAccount %s: %s; the KubeTalosAPIAccessConfig must allow %s for namespace %s",
				tc.TalosServiceAccountName(), reason, role.Admin, tc.Namespace),
		}
	}

	return nil
}
