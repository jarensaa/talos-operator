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
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// Version awareness (docs/scope.md §7.5): what the cluster runs, what exists upstream, and the
// sequence of upgrades that leads to what the spec asks for. This file reports; it starts
// nothing. What it finds out upstream is used in two places only: to choose the release of a
// Kubernetes minor on the way to the version the spec asks for, and to refuse an upgrade to
// something that certainly does not exist before a node is asked to pull it.

const (
	// maxKubernetesAhead bounds how many Kubernetes minors ahead of the cluster's are asked about.
	maxKubernetesAhead = 6
	// upstreamTimeout bounds what one pass spends asking upstream. What is not answered in time is
	// not known, which refuses nothing.
	upstreamTimeout = 20 * time.Second
)

// versionRules is what Talos says about versions, as the version planner asks it.
func versionRules() plan.VersionRules {
	return plan.VersionRules{TalosUpgrade: talos.UpgradeProblem, Runs: talos.RunsKubernetes}
}

// running is the versions the cluster runs: the lowest Talos among its nodes and the lowest
// kubelet. A version that was not read from every node is empty.
func running(obs observation) plan.VersionState {
	var (
		state  plan.VersionState
		lowest *version.Version
	)

	for _, n := range obs.facts.nodes {
		v, err := version.ParseSemantic(n.version)
		if err != nil {
			return plan.VersionState{Kubernetes: obs.kubernetes}
		}

		if lowest == nil || v.LessThan(lowest) {
			lowest, state.Talos = v, n.version
		}
	}

	state.Kubernetes = obs.kubernetes

	return state
}

// releasesOf is what status knows of upstream, as the planners use it. Status is where it is
// taken from, not the network: every replica then plans with the same releases, and an operation
// in flight is not replanned because a release came out.
func releasesOf(tc *talosv1alpha1.TalosCluster) plan.Releases {
	releases := plan.Releases{}

	if tc.Status.Versions == nil || tc.Status.Versions.Available == nil {
		return releases
	}

	for _, release := range tc.Status.Versions.Available.Kubernetes {
		if v, err := version.ParseSemantic(release); err == nil {
			releases[fmt.Sprintf("%d.%d", v.Major(), v.Minor())] = release
		}
	}

	return releases
}

// observeVersions owns status.versions and VersionsReachable.
func (r *TalosClusterReconciler) observeVersions(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) {
	if obs.facts == nil {
		return
	}

	st, gen := &tc.Status, tc.Generation

	versions := &talosv1alpha1.VersionsStatus{}
	if st.Versions != nil {
		versions.Available = st.Versions.Available
	}

	current := running(obs)
	versions.Talos, versions.Kubernetes = current.Talos, current.Kubernetes

	// Upstream is only asked between operations. One in flight has deadlines to keep, and plans
	// with what was known when it started.
	if st.Operation == nil {
		versions.Available = r.available(ctx, tc, current, versions.Available)
	}

	st.Versions = versions

	reachable := func(status metav1.ConditionStatus, reason, format string, args ...any) {
		setCondition(st, gen, talosv1alpha1.ConditionVersionsReachable, status, reason, fmt.Sprintf(format, args...))
	}

	if current.Talos == "" || current.Kubernetes == "" {
		reachable(metav1.ConditionUnknown, talosv1alpha1.ReasonAwaitingStatus, "the versions the cluster runs were not all read")

		return
	}

	var desired plan.VersionState
	if tc.Spec.Versions != nil {
		desired = plan.VersionState{Talos: tc.Spec.Versions.Talos, Kubernetes: tc.Spec.Versions.Kubernetes}
	}

	way := plan.PlanVersions(current, desired, versionRules(), releasesOf(tc))

	steps := make([]string, 0, len(way.Steps))
	for _, step := range way.Steps {
		versions.Plan = append(versions.Plan, talosv1alpha1.VersionStep{Kind: step.Kind, From: step.From, To: step.To})
		steps = append(steps, step.String())
	}

	switch {
	case way.Problem != "":
		reachable(metav1.ConditionFalse, talosv1alpha1.ReasonIncompatible, "%s", way.Problem)
	case len(steps) == 0:
		reachable(metav1.ConditionTrue, talosv1alpha1.ReasonUpToDate, "the cluster runs Talos %s and Kubernetes %s, as its spec asks",
			current.Talos, current.Kubernetes)
	default:
		reachable(metav1.ConditionTrue, talosv1alpha1.ReasonPlanned, "%d upgrade(s) lead to what the spec asks for: %s",
			len(steps), strings.Join(steps, "; "))
	}
}

// available asks what exists upstream. A source that does not answer leaves what was known
// before as it is, and says so.
func (r *TalosClusterReconciler) available(ctx context.Context, tc *talosv1alpha1.TalosCluster, current plan.VersionState,
	known *talosv1alpha1.AvailableVersions,
) *talosv1alpha1.AvailableVersions {
	if r.Catalog == nil || tc.Spec.ConfigGeneration == nil || tc.Spec.ConfigGeneration.Installer == nil || current.Talos == "" {
		return known
	}

	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()

	out := &talosv1alpha1.AvailableVersions{}
	if known != nil {
		out.Talos, out.Kubernetes = known.Talos, known.Kubernetes
	}

	var problems []string

	tags, err := r.Catalog.TalosVersions(ctx, tc.Spec.ConfigGeneration.Installer.FactoryURL)
	if err != nil {
		problems = append(problems, "Talos releases: "+err.Error())
	} else {
		out.Talos = newestPerMinor(tags, current.Talos)
	}

	if current.Kubernetes != "" {
		latest, err := r.kubernetesAhead(ctx, tc, current, out.Talos)
		if err != nil {
			problems = append(problems, "Kubernetes releases: "+err.Error())
		} else {
			out.Kubernetes = latest
		}
	}

	out.Problem = strings.Join(problems, "; ")
	if out.Problem != "" {
		logf.FromContext(ctx).V(1).Info("what exists upstream could not all be asked", "problem", out.Problem)
	}

	return out
}

// newestPerMinor keeps, of the releases Image Factory lists, the newest of each minor that this
// operator supports and that is not older than the release the cluster runs.
func newestPerMinor(tags []string, running string) []string {
	floor, err := version.ParseSemantic(running)
	if err != nil {
		return nil
	}

	newest := map[uint]*version.Version{}

	for _, tag := range tags {
		v, err := version.ParseSemantic(tag)
		if err != nil || v.LessThan(floor) {
			continue
		}

		if ok, _ := talos.SupportsTalos(tag); !ok {
			continue
		}

		if have := newest[v.Minor()]; have == nil || have.LessThan(v) {
			newest[v.Minor()] = v
		}
	}

	minors := make([]uint, 0, len(newest))
	for minor := range newest {
		minors = append(minors, minor)
	}

	slices.Sort(minors)

	out := make([]string, 0, len(newest))
	for _, minor := range minors {
		out = append(out, "v"+newest[minor].String())
	}

	return out
}

// kubernetesAhead lists the newest release of each Kubernetes minor from the cluster's own to
// the newest that any Talos release in play runs: the one it runs, the ones available, and the
// one its spec asks for.
func (r *TalosClusterReconciler) kubernetesAhead(ctx context.Context, tc *talosv1alpha1.TalosCluster, current plan.VersionState,
	talosReleases []string,
) ([]string, error) {
	from, err := version.ParseSemantic(current.Kubernetes)
	if err != nil {
		return nil, fmt.Errorf("%q is no Kubernetes version", current.Kubernetes)
	}

	hosts := append([]string{current.Talos}, talosReleases...)
	if tc.Spec.Versions != nil && tc.Spec.Versions.Talos != "" {
		hosts = append(hosts, tc.Spec.Versions.Talos)
	}

	var out []string

	for minor := from.Minor(); minor <= from.Minor()+maxKubernetesAhead; minor++ {
		first := fmt.Sprintf("%d.%d.0", from.Major(), minor)
		if !slices.ContainsFunc(hosts, func(host string) bool { return talos.RunsKubernetes(host, first) == "" }) {
			break
		}

		latest, err := r.Catalog.KubernetesLatest(ctx, fmt.Sprintf("%d.%d", from.Major(), minor))
		if errors.Is(err, catalog.ErrNotFound) {
			if minor == from.Minor() {
				// The cluster's own minor is no longer listed. That says nothing about the next.
				continue
			}

			// Not released yet, and so nothing after it is either.
			break
		}

		if err != nil {
			return nil, err
		}

		out = append(out, latest)
	}

	return out, nil
}

// installerProblem says that the installer a machine is to be upgraded with certainly does not
// exist, or returns "". Whatever could not be asked is no problem here: the node pulls through
// its own registry configuration and is where the answer is (docs/scope.md §7.3).
func (r *TalosClusterReconciler) installerProblem(ctx context.Context, tc *talosv1alpha1.TalosCluster, install adopt.Install) string {
	if r.Catalog == nil || tc.Spec.ConfigGeneration == nil || tc.Spec.ConfigGeneration.Installer == nil {
		return ""
	}

	installer := tc.Spec.ConfigGeneration.Installer

	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()

	err := r.Catalog.Installer(ctx, installer.FactoryURL, installer.Platform, install.SchematicID, install.Version)
	if errors.Is(err, catalog.ErrNotFound) {
		return err.Error()
	}

	return ""
}

// imagesProblem says which of the images a Kubernetes upgrade is about to pull certainly do not
// exist, or returns "". The newest release of Kubernetes is published a while before the kubelet
// image Talos runs is: asking for the images themselves is what tells.
func (r *TalosClusterReconciler) imagesProblem(ctx context.Context, images map[string]string) string {
	if r.Catalog == nil {
		return ""
	}

	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()

	unique := make([]string, 0, len(images))
	for _, image := range images {
		if !slices.Contains(unique, image) {
			unique = append(unique, image)
		}
	}

	slices.Sort(unique)

	var missing []string

	for _, image := range unique {
		if err := r.Catalog.Image(ctx, image); errors.Is(err, catalog.ErrNotFound) {
			missing = append(missing, image)
		}
	}

	if len(missing) == 0 {
		return ""
	}

	return "these images do not exist: " + strings.Join(missing, ", ")
}
