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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	talosmachine "github.com/siderolabs/talos/pkg/machinery/config/machine"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/adopt/bundle"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// adoptionManager is the field manager under which the operator writes the resources of a
// cluster it is adopting. It owns nothing once the cluster is adopted (see release).
const adoptionManager = "talos-operator-adoption"

// adopted reports whether the cluster has been adopted: proven once, or managed. From then on
// the resources are the user's and the operator never writes a spec again. Nothing takes it back
// (docs/scope.md §12.14).
func adopted(tc *talosv1alpha1.TalosCluster) bool {
	return managing(tc) || (tc.Status.Adoption != nil && tc.Status.Adoption.AdoptedAt != nil)
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=create
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachinepools;talosmachines,verbs=create;patch

// adopt runs while the cluster is observed only and its preconditions hold. It writes Kubernetes
// objects only, never Talos: a one-time backup of the live configs, the secrets bundle Secret (if
// absent) and, until the cluster is adopted, the resources that describe it (docs/scope.md §7.1
// stage 1). It returns true when it changed the TalosCluster's spec: the pass ends there, and the
// next one works from the spec as stored.
func (r *TalosClusterReconciler) adopt(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) (bool, error) {
	st := &tc.Status
	gen := tc.Generation
	adopting := !adopted(tc)

	derived := func(status metav1.ConditionStatus, reason, format string, args ...any) {
		if adopting {
			setCondition(st, gen, talosv1alpha1.ConditionResourcesDerived, status, reason, fmt.Sprintf(format, args...))
		}
	}

	nodes, missing := liveNodes(obs.facts)
	if len(missing) > 0 {
		derived(metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable, "could not read identity or config of %s", strings.Join(missing, ", "))

		return false, nil
	}

	// Capture the pre-adoption state first, whatever happens next.
	if err := r.writeBackup(ctx, tc, nodes); err != nil {
		return false, err
	}

	secretsRef := tc.SecretsReference()

	existing := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: secretsRef.Name}, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("get secrets bundle: %w", err)
		}

		existing = nil
	}

	var (
		supplied     *secrets.Bundle
		suppliedHash string
	)

	if existing != nil {
		b, err := adopt.DecodeBundle(existing.Data[secretsRef.Key])
		if err != nil {
			msg := fmt.Sprintf("secret %s key %s is not a secrets bundle", secretsRef.Name, secretsRef.Key)
			setCondition(st, gen, talosv1alpha1.ConditionSecretsMatchLive, metav1.ConditionFalse, talosv1alpha1.ReasonParseError, msg)
			derived(metav1.ConditionFalse, talosv1alpha1.ReasonParseError, "%s", msg)

			return false, nil //nolint:nilerr // reported in SecretsMatchLive; the parser's error may quote the bundle
		}

		if suppliedHash, err = adopt.BundleHash(b); err != nil {
			return false, err
		}

		supplied = b

		// Nothing the resources depend on moved, and every node still has its machine. Without a
		// stored bundle they are always derived again, which recreates the Secret.
		source := adoptionSource(nodes, secretsRef, suppliedHash)
		if st.Adoption != nil && st.Adoption.Source == source && (!adopting || (obs.unmatched != nil && len(obs.unmatched) == 0)) {
			reportBackup(tc, secretsRef, existing.Annotations, suppliedHash)

			return false, nil
		}
	}

	p, err := adopt.Build(adopt.Request{Nodes: nodes, Cluster: tc, Bundle: supplied})
	if err != nil {
		var mismatch *bundle.MismatchError
		if errors.As(err, &mismatch) {
			setCondition(st, gen, talosv1alpha1.ConditionSecretsMatchLive, metav1.ConditionFalse, talosv1alpha1.ReasonMismatch, err.Error())
		}

		derived(metav1.ConditionFalse, talosv1alpha1.ReasonFailed, "%s", err.Error())

		return false, nil
	}

	setCondition(st, gen, talosv1alpha1.ConditionSecretsMatchLive, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
		"the secrets bundle matches every node")

	bundleHash := suppliedHash
	annotations := map[string]string(nil)

	if existing != nil {
		annotations = existing.Annotations
	} else {
		if bundleHash, err = adopt.BundleHash(p.Bundle); err != nil {
			return false, err
		}

		if err := r.createBundleSecret(ctx, tc, secretsRef, p.Bundle); err != nil {
			return false, err
		}
	}

	if adopting {
		if filled, err := r.fillClusterSpec(ctx, tc, p.Cluster); err != nil || filled {
			return filled, err
		}

		written, left, err := r.applyResources(ctx, tc, p)
		if err != nil {
			return false, err
		}

		if len(left) > 0 {
			derived(metav1.ConditionFalse, talosv1alpha1.ReasonChangedElsewhere,
				"left as they are, because someone else set what the live cluster says differently: %s", strings.Join(left, ", "))
		} else {
			derived(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "%d TalosMachinePools and %d TalosMachines describe the live cluster",
				written.pools, written.machines)
		}
	}

	if st.Adoption == nil {
		st.Adoption = &talosv1alpha1.AdoptionStatus{}
	}

	st.Adoption.Source = adoptionSource(nodes, secretsRef, bundleHash)
	st.Adoption.Notes = p.Notes

	reportBackup(tc, secretsRef, annotations, bundleHash)

	return false, nil
}

// fillClusterSpec sets what the TalosCluster's spec leaves empty to what the live cluster says,
// and never touches a field that is set. It reports whether it wrote anything.
func (r *TalosClusterReconciler) fillClusterSpec(ctx context.Context, tc, proposed *talosv1alpha1.TalosCluster) (bool, error) {
	filled := tc.DeepCopy()
	spec, from := &filled.Spec, proposed.Spec

	if spec.ClusterName == "" {
		spec.ClusterName = from.ClusterName
	}

	if spec.ControlPlaneEndpoint == "" {
		spec.ControlPlaneEndpoint = from.ControlPlaneEndpoint
	}

	if spec.Versions == nil {
		spec.Versions = from.Versions
	}

	if spec.ConfigGeneration == nil {
		spec.ConfigGeneration = from.ConfigGeneration
	}

	if len(spec.Config) == 0 {
		spec.Config = from.Config
	}

	if equality.Semantic.DeepEqual(tc.Spec, filled.Spec) {
		return false, nil
	}

	// On a copy: the caller still holds the object it read, and its status patch is refused
	// because the spec moved, which is what ends this pass.
	if err := r.Patch(ctx, filled, client.MergeFrom(tc), client.FieldOwner(adoptionManager)); err != nil {
		return false, fmt.Errorf("fill in the TalosCluster's spec: %w", err)
	}

	logf.FromContext(ctx).Info("filled in what the TalosCluster's spec left empty from the live cluster")

	return true, nil
}

type resourceCount struct{ pools, machines int }

// applyResources writes the pools and machines that describe the live cluster, by server-side
// apply without force. That is what keeps the operator's hands off anything someone else set: a
// field another manager owns with a different value is a conflict, and the object is left as it
// is and named. Fields nobody else touched follow the live cluster for as long as the cluster is
// being adopted.
//
// A node that already has a TalosMachine under another name is not given a second one, and a
// pool is only written for the machines written here.
func (r *TalosClusterReconciler) applyResources(ctx context.Context, tc *talosv1alpha1.TalosCluster, p *adopt.Proposal,
) (resourceCount, []string, error) {
	var (
		count resourceCount
		left  []string
		all   talosv1alpha1.TalosMachineList
	)

	if err := r.List(ctx, &all, client.InNamespace(tc.Namespace)); err != nil {
		return count, nil, fmt.Errorf("list machines: %w", err)
	}

	claimedBy := make(map[string]*talosv1alpha1.TalosMachine, len(all.Items))
	for i := range all.Items {
		claimedBy[strings.ToLower(all.Items[i].Spec.Identity.SystemUUID)] = &all.Items[i]
	}

	needed := map[string]bool{}

	apply := func(kind string, obj client.Object, spec any) (bool, error) {
		switch err := r.Apply(ctx, applyConfiguration(tc.Name, kind, obj, spec), client.FieldOwner(adoptionManager)); {
		case err == nil:
			return true, nil
		case apierrors.IsConflict(err):
			left = append(left, kind+" "+obj.GetName())

			return false, nil
		default:
			return false, fmt.Errorf("apply %s %s: %w", kind, obj.GetName(), err)
		}
	}

	for _, tm := range p.Machines {
		if claim := claimedBy[strings.ToLower(tm.Spec.Identity.SystemUUID)]; claim != nil {
			if claim.Name != tm.Name {
				continue
			}

			// The same name in a pool that is not this cluster's: another TalosCluster describes
			// the node, and applying would quietly move its machine here.
			if claim.Spec.PoolRef.Name != tm.Spec.PoolRef.Name {
				left = append(left, "TalosMachine "+tm.Name)

				continue
			}
		}

		needed[tm.Spec.PoolRef.Name] = true

		ok, err := apply("TalosMachine", tm, tm.Spec)
		if err != nil {
			return count, nil, err
		}

		if ok {
			count.machines++
		}
	}

	for _, pool := range p.Pools {
		if !needed[pool.Name] {
			continue
		}

		ok, err := apply("TalosMachinePool", pool, pool.Spec)
		if err != nil {
			return count, nil, err
		}

		if ok {
			count.pools++
		}
	}

	slices.Sort(left)

	return count, left, nil
}

// applyConfiguration is what the operator claims of a resource it derives: its spec, and of its
// metadata the label naming its cluster.
func applyConfiguration(cluster, kind string, obj client.Object, spec any) runtime.ApplyConfiguration {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(talosv1alpha1.GroupVersion.String())
	u.SetKind(kind)
	u.SetNamespace(obj.GetNamespace())
	u.SetName(obj.GetName())
	u.SetLabels(map[string]string{talosv1alpha1.LabelCluster: cluster})

	// A spec is plain data; converting it cannot fail.
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&struct {
		Spec any `json:"spec"`
	}{spec})
	if err != nil {
		panic(fmt.Sprintf("spec of %s %s: %v", kind, obj.GetName(), err))
	}

	u.Object["spec"] = content["spec"]

	return client.ApplyConfigurationFromUnstructured(u)
}

// release gives up what the operator is recorded as owning of an adopted cluster's resources: the
// spec fields it filled in on the TalosCluster, and the spec of every pool and machine it wrote.
// Whoever manages the resources next is then their only owner, so a field they later remove is
// really removed and not kept alive by an owner that has long let go (docs/scope.md §12.14). It
// reports whether it changed the TalosCluster itself.
func (r *TalosClusterReconciler) release(ctx context.Context, tc *talosv1alpha1.TalosCluster) (bool, error) {
	pools, machines, err := membersOf(ctx, r.Client, tc.Namespace, tc.Name)
	if err != nil {
		return false, err
	}

	for _, pool := range pools {
		if _, err := r.releaseObject(ctx, pool); err != nil {
			return false, err
		}
	}

	for i := range machines {
		if _, err := r.releaseObject(ctx, &machines[i]); err != nil {
			return false, err
		}
	}

	return r.releaseObject(ctx, tc)
}

// labelOnly is the ownership the operator keeps of a resource it wrote: the label naming its
// cluster, in the notation of managed fields.
const labelOnly = `{"f:metadata":{"f:labels":{"f:` + talosv1alpha1.LabelCluster + `":{}}}}`

// releaseObject rewrites the operator's entries in an object's managed fields. What it applied
// shrinks to the cluster label; what it set any other way goes.
//
// The entry it applied is kept on purpose. An object with no managed fields at all gets a
// "before-first-apply" owner of everything from the API server at the next apply
// (managedfields/internal/skipnonapplied.go), which would hold on to every field exactly as the
// operator's own entry did.
func (r *TalosClusterReconciler) releaseObject(ctx context.Context, obj client.Object) (bool, error) {
	kept := make([]metav1.ManagedFieldsEntry, 0, len(obj.GetManagedFields()))
	changed := false

	for _, e := range obj.GetManagedFields() {
		switch {
		case e.Manager != adoptionManager:
		case e.Operation != metav1.ManagedFieldsOperationApply:
			changed = true

			continue
		case e.FieldsV1 == nil || e.FieldsV1.GetRawString() != labelOnly:
			e.FieldsV1 = metav1.NewFieldsV1(labelOnly)
			changed = true
		}

		kept = append(kept, e)
	}

	if !changed {
		return false, nil
	}

	released, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return false, fmt.Errorf("%T is not an object", obj)
	}

	released.SetManagedFields(kept)

	if err := r.Patch(ctx, released, client.MergeFrom(obj)); err != nil {
		return false, client.IgnoreNotFound(fmt.Errorf("release %s: %w", obj.GetName(), err))
	}

	logf.FromContext(ctx).Info("gave up ownership of a resource written during adoption",
		"kind", fmt.Sprintf("%T", obj), "name", obj.GetName())

	return true, nil
}

// liveNodes turns observed facts into adoption input, listing nodes whose data was unreadable.
func liveNodes(facts *clusterFacts) ([]adopt.LiveNode, []string) {
	var (
		nodes   []adopt.LiveNode
		missing []string
	)

	for _, n := range facts.nodes {
		if n.systemUUID == "" || len(n.activeConfig) == 0 || n.version == "" {
			missing = append(missing, n.hostname)

			continue
		}

		role := talosmachine.TypeWorker
		if n.machineType == machineTypeControlPlane {
			role = talosmachine.TypeControlPlane
		}

		nodes = append(nodes, adopt.LiveNode{
			Hostname:     n.hostname,
			Address:      n.address.String(),
			Role:         role,
			SystemUUID:   n.systemUUID,
			TalosVersion: n.version,
			Config:       n.activeConfig,
		})
	}

	return nodes, missing
}

// adoptionSource fingerprints everything the derived resources depend on, so they are derived
// again only when the live cluster, the bundle or the references change.
func adoptionSource(nodes []adopt.LiveNode, ref talosv1alpha1.SecretKeyReference, bundleHash string) string {
	sorted := slices.Clone(nodes)
	slices.SortFunc(sorted, func(a, b adopt.LiveNode) int { return strings.Compare(a.Hostname, b.Hostname) })

	lines := make([]string, 0, len(sorted)+1)
	for _, n := range sorted {
		lines = append(lines, strings.Join([]string{
			n.Hostname, n.Address, n.Role.String(), n.SystemUUID, n.TalosVersion, talos.HashConfig(n.Config),
		}, "|"))
	}

	lines = append(lines, "secrets="+ref.Name+"/"+ref.Key+" bundle="+bundleHash)

	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))

	return hex.EncodeToString(sum[:16])
}

func backupName(tc *talosv1alpha1.TalosCluster) string { return tc.Name + "-adoption-backup" }

// createBundleSecret stores a recovered bundle. It has no owner reference: deleting the
// TalosCluster must never delete the cluster's PKI (docs/scope.md §9).
func (r *TalosClusterReconciler) createBundleSecret(ctx context.Context, tc *talosv1alpha1.TalosCluster,
	ref talosv1alpha1.SecretKeyReference, b *secrets.Bundle,
) error {
	data, err := adopt.EncodeBundle(b)
	if err != nil {
		return fmt.Errorf("encode bundle: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ref.Name,
			Namespace: tc.Namespace,
			Labels:    map[string]string{talosv1alpha1.LabelCluster: tc.Name},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{ref.Key: data},
	}

	switch err := r.Create(ctx, secret); {
	case err == nil:
		logf.FromContext(ctx).Info("stored the secrets bundle recovered from the cluster", "secret", secret.Name)
	case !apierrors.IsAlreadyExists(err):
		return fmt.Errorf("create secrets bundle: %w", err)
	}

	return nil
}

// writeBackup saves every node's live config once, before anything else happens. It is never
// updated, so it keeps the pre-adoption state; it has no owner reference.
func (r *TalosClusterReconciler) writeBackup(ctx context.Context, tc *talosv1alpha1.TalosCluster, nodes []adopt.LiveNode) error {
	data := make(map[string][]byte, len(nodes))
	for _, n := range nodes {
		data[n.Hostname+".yaml"] = n.Config
	}

	backup := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupName(tc),
			Namespace: tc.Namespace,
			Labels:    map[string]string{talosv1alpha1.LabelCluster: tc.Name},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}

	switch err := r.Create(ctx, backup); {
	case err == nil:
		logf.FromContext(ctx).Info("backed up every node's machine config as it was before adoption", "secret", backup.Name)
	case !apierrors.IsAlreadyExists(err):
		return fmt.Errorf("create config backup: %w", err)
	}

	return nil
}

// reportBackup sets SecretsBackedUp from the annotation the user puts on the bundle Secret.
func reportBackup(tc *talosv1alpha1.TalosCluster, ref talosv1alpha1.SecretKeyReference, annotations map[string]string, hash string) {
	if annotations[talosv1alpha1.AnnotationSecretsBackedUp] == hash {
		setCondition(&tc.Status, tc.Generation, talosv1alpha1.ConditionSecretsBackedUp, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			"the secrets bundle is confirmed backed up off-cluster")

		return
	}

	setCondition(&tc.Status, tc.Generation, talosv1alpha1.ConditionSecretsBackedUp, metav1.ConditionFalse, talosv1alpha1.ReasonAwaitingBackup,
		fmt.Sprintf("export key %s of Secret %s off-cluster, then annotate the Secret with %s=%s",
			ref.Key, ref.Name, talosv1alpha1.AnnotationSecretsBackedUp, hash))
}
