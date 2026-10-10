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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/classify"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// maxStatusDiff bounds status.config.diff, leaving room under the CRD's 16 KiB limit.
const maxStatusDiff = 8 << 10

// maxChangeReasons bounds status.config.changeReasons, as the CRD does.
const maxChangeReasons = 16

// configError is a config source that cannot be used. Its message names the source, never content.
type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }

// resolvedConfig is a machine's config documents in apply order, plus the documents set by
// sources read from Secrets, whose diffs must not be shown.
type resolvedConfig struct {
	contents    [][]byte
	fromSecrets map[render.DocKey]bool
}

// resolveConfig reads every config source. A missing source is a configError; other errors are
// API failures.
func resolveConfig(ctx context.Context, c client.Reader, namespace string, sources []talosv1alpha1.ConfigSource) (resolvedConfig, error) {
	out := resolvedConfig{fromSecrets: map[render.DocKey]bool{}}

	for _, src := range sources {
		switch {
		case src.ConfigRef != nil:
			mc := &talosv1alpha1.TalosMachineConfig{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: src.ConfigRef.Name}, mc); err != nil {
				return out, sourceError(err, "TalosMachineConfig", src.ConfigRef.Name)
			}

			out.contents = append(out.contents, []byte(mc.Spec.Documents))
		case src.SecretKeyRef != nil:
			ref := src.SecretKeyRef

			secret := &corev1.Secret{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, secret); err != nil {
				return out, sourceError(err, "Secret", ref.Name)
			}

			data, ok := secret.Data[ref.Key]
			if !ok {
				return out, &configError{fmt.Sprintf("Secret %s has no key %s", ref.Name, ref.Key)}
			}

			keys, err := render.PatchDocKeys(data)
			if err != nil {
				// The parser's error may quote the content.
				return out, &configError{fmt.Sprintf("Secret %s key %s is not strategic-merge config", ref.Name, ref.Key)}
			}

			for _, k := range keys {
				out.fromSecrets[k] = true
			}

			out.contents = append(out.contents, data)
		default:
			out.contents = append(out.contents, []byte(src.Inline))
		}
	}

	return out, nil
}

func sourceError(err error, kind, name string) error {
	if apierrors.IsNotFound(err) {
		return &configError{fmt.Sprintf("%s %s not found", kind, name)}
	}

	return fmt.Errorf("get %s %s: %w", kind, name, err)
}

// configProblem is why a machine's config cannot be rendered: something for the user to fix,
// reported in a condition rather than returned as an error.
type configProblem struct {
	status metav1.ConditionStatus
	reason string
	msg    string
}

func (p *configProblem) Error() string { return p.msg }

// renderMachine renders tm's desired config from its cluster and pool, the secrets bundle and
// every config layer, exactly as a rollout would. Problems are *configProblem; other errors are
// API failures of the management cluster. Both TalosMachine comparison and the cluster's adoption
// proof render through it, so they cannot disagree about what "desired" means.
func renderMachine(ctx context.Context, c client.Reader, tc *talosv1alpha1.TalosCluster, pool *talosv1alpha1.TalosMachinePool,
	tm *talosv1alpha1.TalosMachine,
) (*render.Config, resolvedConfig, error) {
	ref := tc.SecretsReference()

	secret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: tm.Namespace, Name: ref.Name}, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, resolvedConfig{}, fmt.Errorf("get secrets bundle: %w", err)
		}

		return nil, resolvedConfig{}, &configProblem{metav1.ConditionUnknown, talosv1alpha1.ReasonSecretNotFound,
			fmt.Sprintf("secrets bundle Secret %s not found; adoption writes it once the cluster's preconditions hold", ref.Name)}
	}

	bundle, err := adopt.DecodeBundle(secret.Data[ref.Key])
	if err != nil {
		return nil, resolvedConfig{}, &configProblem{metav1.ConditionFalse, talosv1alpha1.ReasonParseError,
			fmt.Sprintf("Secret %s key %s is not a secrets bundle", ref.Name, ref.Key)}
	}

	resolved, err := resolveConfig(ctx, c, tm.Namespace, adopt.ConfigSources(&tc.Spec, &pool.Spec, &tm.Spec))
	if err != nil {
		var ce *configError
		if errors.As(err, &ce) {
			return nil, resolved, &configProblem{metav1.ConditionFalse, talosv1alpha1.ReasonConfigUnresolved, ce.msg}
		}

		return nil, resolved, err
	}

	in, err := adopt.RenderInput(&tc.Spec, &pool.Spec, bundle, resolved.contents)
	if errors.Is(err, adopt.ErrIncompleteSpec) {
		return nil, resolved, &configProblem{
			metav1.ConditionUnknown, talosv1alpha1.ReasonPreconditionUnmet, "TalosCluster " + tc.Name + ": " + err.Error(),
		}
	} else if err != nil {
		return nil, resolved, &configProblem{metav1.ConditionFalse, talosv1alpha1.ReasonFailed, err.Error()}
	}

	desired, err := render.Render(in)
	if err != nil {
		msg := "render: " + err.Error()
		if len(resolved.fromSecrets) > 0 {
			// Patch and validation errors can quote values, and some came from a Secret.
			msg = "render failed; the error is withheld because a config source is a Secret"
		}

		return nil, resolved, &configProblem{metav1.ConditionFalse, talosv1alpha1.ReasonFailed, msg}
	}

	return desired, resolved, nil
}

// observeConfig renders the machine's desired config and compares it with the live one per
// document (docs/scope.md §7.1 stage 2). It reads Kubernetes objects only and never writes to
// Talos. Only API errors of the management cluster are returned.
func (r *TalosMachineReconciler) observeConfig(ctx context.Context, tc *talosv1alpha1.TalosCluster, pool *talosv1alpha1.TalosMachinePool,
	tm *talosv1alpha1.TalosMachine, raw []byte,
) error {
	if tm.Status.Config == nil {
		tm.Status.Config = &talosv1alpha1.ConfigState{}
	}

	cfg := tm.Status.Config

	report := func(status metav1.ConditionStatus, reason, message string) {
		setMachineCondition(tm, talosv1alpha1.ConditionConfigMatchesLive, status, reason, message)
	}

	parsed, err := talos.ParseMachineConfig(raw)
	if err != nil {
		cfg.LiveHash = ""
		report(metav1.ConditionFalse, talosv1alpha1.ReasonParseError, "live config: "+err.Error())

		return nil //nolint:nilerr // reported in ConfigMatchesLive
	}

	live, err := render.Normalize(parsed)
	if err != nil {
		cfg.LiveHash = ""
		report(metav1.ConditionFalse, talosv1alpha1.ReasonParseError, "live config: "+err.Error())

		return nil //nolint:nilerr // reported in ConfigMatchesLive
	}

	cfg.LiveHash = live.Hash

	desired, resolved, err := renderMachine(ctx, r.Client, tc, pool, tm)
	if err != nil {
		var problem *configProblem
		if errors.As(err, &problem) {
			report(problem.status, problem.reason, problem.msg)

			return nil
		}

		return err
	}

	cfg.DesiredHash = desired.Hash

	diffs, err := render.Diff(live, desired)
	if err != nil {
		report(metav1.ConditionFalse, talosv1alpha1.ReasonFailed, err.Error())

		return nil
	}

	diffs = render.Withhold(diffs, resolved.fromSecrets)
	cfg.Diff = statusDiff(diffs)

	// The class of the change decides how a rollout applies it (docs/scope.md §6.3). A node that
	// already runs what the config's installer installs takes that part like any other change.
	var opts []classify.Option

	if install, err := adopt.InstallTarget(&tc.Spec, &pool.Spec); err == nil && tm.Status.Talos != nil &&
		tm.Status.Talos.Version == install.Version && tm.Status.Talos.SchematicID == install.SchematicID {
		opts = append(opts, classify.Installed())
	}

	change, err := classify.Classify(live, desired, opts...)
	if err != nil {
		report(metav1.ConditionFalse, talosv1alpha1.ReasonFailed, err.Error())

		return nil
	}

	cfg.ChangeClass = string(change.Class)
	cfg.ChangeReasons = change.Reasons

	if len(cfg.ChangeReasons) > maxChangeReasons {
		cfg.ChangeReasons = cfg.ChangeReasons[:maxChangeReasons]
	}

	if len(diffs) == 0 {
		report(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "the rendered config equals the live config")

		return nil
	}

	keys := make([]string, 0, len(diffs))
	for _, d := range diffs {
		keys = append(keys, string(d.Key)+" ("+string(d.Op)+")")
	}

	report(metav1.ConditionFalse, talosv1alpha1.ReasonMismatch,
		fmt.Sprintf("%d document(s) differ from the live config: %s; see status.config.diff", len(diffs), strings.Join(keys, ", ")))

	return nil
}

// statusDiff joins diffs, cutting at a line boundary once maxStatusDiff is reached.
func statusDiff(diffs []render.DocDiff) string {
	var b strings.Builder

	for i, d := range diffs {
		room := maxStatusDiff - b.Len()
		if len(d.Text) <= room {
			b.WriteString(d.Text)

			continue
		}

		if cut := strings.LastIndexByte(d.Text[:room], '\n'); cut > 0 {
			b.WriteString(d.Text[:cut+1])
		}

		b.WriteString("... truncated; " + strconv.Itoa(len(diffs)-i) + " document(s) not shown in full\n")

		break
	}

	return b.String()
}

// machinesForObject re-reconciles the machines whose config depends on a TalosMachineConfig or a
// Secret: a config source in any of their layers, or their cluster's secrets bundle or
// credentials.
func machinesForObject(c client.Reader) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		var ref objectRef

		switch obj.(type) {
		case *corev1.Secret:
			ref = objectRef{secret: obj.GetName()}
		case *talosv1alpha1.TalosMachineConfig:
			ref = objectRef{config: obj.GetName()}
		default:
			return nil
		}

		layers, err := listLayers(ctx, c, obj.GetNamespace())
		if err != nil {
			mapFailed(ctx, err, "machines depending on the object", obj)

			return nil
		}

		var reqs []reconcile.Request

		for i := range layers.machines {
			tm := &layers.machines[i]

			if layers.machineDependsOn(tm, ref) {
				reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tm)})
			}
		}

		return reqs
	}
}

// objectRef is a Secret or a TalosMachineConfig a machine's config may depend on.
type objectRef struct {
	secret string
	config string
}

func (o objectRef) in(sources []talosv1alpha1.ConfigSource) bool {
	for _, src := range sources {
		if o.secret != "" && src.SecretKeyRef != nil && src.SecretKeyRef.Name == o.secret {
			return true
		}

		if o.config != "" && src.ConfigRef != nil && src.ConfigRef.Name == o.config {
			return true
		}
	}

	return false
}

// namespaceLayers is every cluster, pool and machine of a namespace, indexed by name. Clusters
// have a handful of each, so listing works with any client, cached or direct, without an index.
type namespaceLayers struct {
	clusters map[string]*talosv1alpha1.TalosCluster
	pools    map[string]*talosv1alpha1.TalosMachinePool
	machines []talosv1alpha1.TalosMachine
}

func listLayers(ctx context.Context, c client.Reader, namespace string) (*namespaceLayers, error) {
	var (
		clusters talosv1alpha1.TalosClusterList
		pools    talosv1alpha1.TalosMachinePoolList
		machines talosv1alpha1.TalosMachineList
	)

	for _, list := range []client.ObjectList{&clusters, &pools, &machines} {
		if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
			return nil, fmt.Errorf("list %T: %w", list, err)
		}
	}

	l := &namespaceLayers{
		clusters: make(map[string]*talosv1alpha1.TalosCluster, len(clusters.Items)),
		pools:    make(map[string]*talosv1alpha1.TalosMachinePool, len(pools.Items)),
		machines: machines.Items,
	}

	for i := range clusters.Items {
		l.clusters[clusters.Items[i].Name] = &clusters.Items[i]
	}

	for i := range pools.Items {
		l.pools[pools.Items[i].Name] = &pools.Items[i]
	}

	return l, nil
}

// machineDependsOn reports whether tm's rendered config reads ref, through any layer.
func (l *namespaceLayers) machineDependsOn(tm *talosv1alpha1.TalosMachine, ref objectRef) bool {
	if ref.in(tm.Spec.Config) {
		return true
	}

	pool := l.pools[tm.Spec.PoolRef.Name]
	if pool == nil {
		return false
	}

	if ref.in(pool.Spec.Config) {
		return true
	}

	tc := l.clusters[pool.Spec.ClusterRef.Name]
	if tc == nil {
		return false
	}

	if ref.secret != "" && (tc.SecretsReference().Name == ref.secret || tc.CredentialsSecret().Name == ref.secret) {
		return true
	}

	return ref.in(tc.Spec.Config)
}
