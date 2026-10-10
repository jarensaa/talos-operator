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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/adopt/bundle"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// Keys of the proposal ConfigMap (docs/scope.md §7.1 stage 1).
const (
	proposalClusterKey  = "taloscluster.yaml"
	proposalPoolsKey    = "talosmachinepools.yaml"
	proposalMachinesKey = "talosmachines.yaml"
	proposalNotesKey    = "notes.md"
)

// +kubebuilder:rbac:groups="",resources=secrets,verbs=create
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update

// propose builds the adoption proposal once the preconditions hold. It writes Kubernetes objects
// only, never Talos: a one-time backup of the live configs, the secrets bundle Secret (if absent)
// and the proposal ConfigMap.
func (r *TalosClusterReconciler) propose(ctx context.Context, tc *talosv1alpha1.TalosCluster, facts *clusterFacts) error {
	st := &tc.Status
	gen := tc.Generation

	nodes, missing := liveNodes(facts)
	if len(missing) > 0 {
		setCondition(st, gen, talosv1alpha1.ConditionAdoptionProposed, metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable,
			"could not read identity or config of "+strings.Join(missing, ", "))

		return nil
	}

	// Capture the pre-adoption state first, whatever happens next.
	if err := r.writeBackup(ctx, tc, nodes); err != nil {
		return err
	}

	secretsRef := tc.SecretsReference()

	existing := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: secretsRef.Name}, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get secrets bundle: %w", err)
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
			setCondition(st, gen, talosv1alpha1.ConditionAdoptionProposed, metav1.ConditionFalse, talosv1alpha1.ReasonParseError, msg)

			return nil //nolint:nilerr // reported in SecretsMatchLive; the parser's error may quote the bundle
		}

		if suppliedHash, err = adopt.BundleHash(b); err != nil {
			return err
		}

		supplied = b

		// Without a stored bundle the proposal is always rebuilt, which recreates the Secret.
		if r.proposalCurrent(ctx, tc, proposalSource(nodes, secretsRef, suppliedHash)) {
			reportBackup(tc, secretsRef, existing.Annotations, suppliedHash)

			return nil
		}
	}

	p, err := adopt.Build(adopt.Request{Nodes: nodes, Cluster: tc, Bundle: supplied})
	if err != nil {
		var mismatch *bundle.MismatchError
		if errors.As(err, &mismatch) {
			setCondition(st, gen, talosv1alpha1.ConditionSecretsMatchLive, metav1.ConditionFalse, talosv1alpha1.ReasonMismatch, err.Error())
		}

		setCondition(st, gen, talosv1alpha1.ConditionAdoptionProposed, metav1.ConditionFalse, talosv1alpha1.ReasonFailed, err.Error())

		return nil
	}

	setCondition(st, gen, talosv1alpha1.ConditionSecretsMatchLive, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
		"the secrets bundle matches every node")

	bundleHash := suppliedHash
	annotations := map[string]string(nil)

	if existing != nil {
		annotations = existing.Annotations
	} else {
		if bundleHash, err = adopt.BundleHash(p.Bundle); err != nil {
			return err
		}

		if err := r.createBundleSecret(ctx, tc, secretsRef, p.Bundle); err != nil {
			return err
		}
	}

	name, err := r.writeProposal(ctx, tc, p, proposalSource(nodes, secretsRef, bundleHash))
	if err != nil {
		return err
	}

	if st.Adoption == nil {
		st.Adoption = &talosv1alpha1.AdoptionStatus{}
	}

	st.Adoption.ProposalRef = &corev1.LocalObjectReference{Name: name}
	setCondition(st, gen, talosv1alpha1.ConditionAdoptionProposed, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
		fmt.Sprintf("review and commit the TalosCluster, TalosMachinePools and TalosMachines in ConfigMap %s", name))

	reportBackup(tc, secretsRef, annotations, bundleHash)

	return nil
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

// proposalSource fingerprints everything a proposal depends on, so it is rebuilt only when the
// live cluster, the bundle or the references change.
func proposalSource(nodes []adopt.LiveNode, ref talosv1alpha1.SecretKeyReference, bundleHash string) string {
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

func proposalName(tc *talosv1alpha1.TalosCluster) string { return tc.Name + "-adoption-proposal" }

func backupName(tc *talosv1alpha1.TalosCluster) string { return tc.Name + "-adoption-backup" }

func (r *TalosClusterReconciler) proposalCurrent(ctx context.Context, tc *talosv1alpha1.TalosCluster, source string) bool {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: proposalName(tc)}, cm); err != nil {
		return false
	}

	return cm.Annotations[talosv1alpha1.AnnotationProposalSource] == source &&
		tc.Status.Adoption != nil && tc.Status.Adoption.ProposalRef != nil
}

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

// writeProposal creates or updates the proposal ConfigMap, owned by the TalosCluster.
func (r *TalosClusterReconciler) writeProposal(ctx context.Context, tc *talosv1alpha1.TalosCluster, p *adopt.Proposal, source string,
) (string, error) {
	clusterYAML, err := p.ClusterYAML()
	if err != nil {
		return "", err
	}

	poolsYAML, err := p.PoolsYAML()
	if err != nil {
		return "", err
	}

	machinesYAML, err := p.MachinesYAML()
	if err != nil {
		return "", err
	}

	notes := "No notes.\n"
	if len(p.Notes) > 0 {
		notes = "- " + strings.Join(p.Notes, "\n- ") + "\n"
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: proposalName(tc), Namespace: tc.Namespace}}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}

		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}

		cm.Labels[talosv1alpha1.LabelCluster] = tc.Name
		cm.Annotations[talosv1alpha1.AnnotationProposalSource] = source
		cm.Data = map[string]string{
			proposalClusterKey:  string(clusterYAML),
			proposalPoolsKey:    string(poolsYAML),
			proposalMachinesKey: string(machinesYAML),
			proposalNotesKey:    notes,
		}

		return controllerutil.SetControllerReference(tc, cm, r.Scheme)
	})
	if err != nil {
		return "", fmt.Errorf("write proposal: %w", err)
	}

	if result != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("wrote the adoption proposal", "configMap", cm.Name, "result", string(result))
	}

	return cm.Name, nil
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
