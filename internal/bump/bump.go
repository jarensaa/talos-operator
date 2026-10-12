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

// Package bump writes the next Talos or Kubernetes version into the manifests of a cluster
// (docs/scope.md §7.5): what a repository runs on a schedule, so that a cluster is upgraded
// without anyone picking versions. It needs no cluster and no secret.
//
// It is not the operator choosing a version. The operator in a cluster rolls out what its spec
// asks for and never changes that; this edits a file, and whoever runs it commits the file or
// does not. What it writes is one step: the next release of Talos, or else of Kubernetes, that
// this operator upgrades to from what the file asks for, and that the validate check passes.
package bump

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/util/version"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/validate"
)

// Options says what to bump.
type Options struct {
	// Paths are the files and directories that hold the manifests.
	Paths []string
	// Source answers what exists upstream. There is no bumping without one.
	Source catalog.Source
	// Talos and Kubernetes say which of the two may be bumped. Neither means either.
	Talos, Kubernetes bool
	// PatchOnly keeps a cluster on the minors its file names.
	PatchOnly bool
	// DryRun works out the bump and writes nothing.
	DryRun bool
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Outcome is what a run did about one TalosCluster.
type Outcome struct {
	// Object is the cluster, such as "TalosCluster default/my-cluster", and File where it is.
	Object, File string
	// Step is the bump that was written, or that a dry run would write. Nil when there is none.
	Step *plan.VersionStep
	// Held says what was not taken although nothing is wrong: a release this operator does not
	// upgrade to, or one whose turn has not come.
	Held []string
	// Problems says what stood in the way of a bump and should not have: a question upstream
	// that was not answered, a release the check turned down.
	Problems []string
}

// Result is what a run did.
type Result struct {
	// Outcomes has one entry for every TalosCluster, in the order the files hold them.
	Outcomes []Outcome
	// Invalid is what the check finds wrong with the manifests as they are. Nothing is bumped on
	// top of that, and Outcomes is empty.
	Invalid []validate.Finding
	// NotChecked is what the check says it did not check.
	NotChecked []validate.Finding
}

// Run bumps every TalosCluster under the paths by one step and returns what it did. An error is
// a failure to read or write the manifests.
func Run(ctx context.Context, opts Options) (*Result, error) {
	if opts.Source == nil {
		return nil, errors.New("which releases exist cannot be asked without a source")
	}

	result := &Result{}

	// The manifests as they are come first: a bump on top of something that is wrong would be
	// turned down for that, and would be blamed for it.
	found, err := validate.Run(ctx, validate.Options{Paths: opts.Paths, Source: opts.Source, Now: opts.Now})
	if err != nil {
		return nil, err
	}

	for _, f := range found {
		if f.Skipped {
			result.NotChecked = append(result.NotChecked, f)
		} else {
			result.Invalid = append(result.Invalid, f)
		}
	}

	if len(result.Invalid) > 0 {
		return result, nil
	}

	files, err := validate.Files(opts.Paths)
	if err != nil {
		return nil, err
	}

	r := &run{ctx: ctx, opts: opts, edited: map[string][]byte{}}

	for _, file := range files {
		content, err := os.ReadFile(file) //nolint:gosec // the files the caller named
		if err != nil {
			return nil, err
		}

		for index := range clusters(content) {
			// A bump changes no line into two: the clusters of a file stay where they are counted.
			if edited, ok := r.edited[file]; ok {
				content = edited
			}

			outcome, err := r.cluster(file, content, index)
			if err != nil {
				return nil, err
			}

			result.Outcomes = append(result.Outcomes, outcome)
		}
	}

	if opts.DryRun {
		return result, nil
	}

	for _, file := range files {
		edited, ok := r.edited[file]
		if !ok {
			continue
		}

		// The file exists, and keeps the mode it has.
		if err := os.WriteFile(file, edited, 0o600); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// run is one Run.
type run struct {
	ctx  context.Context //nolint:containedctx // lives for one Run
	opts Options
	// edited holds the files that were changed, as they are to be written.
	edited map[string][]byte
}

// kinds lists what may be bumped, in the order it is tried. Talos comes first, as it does where
// the operator could upgrade either (plan.PlanVersions): a newer Talos never runs fewer of the
// Kubernetes versions still ahead.
func (r *run) kinds() []string {
	if r.opts.Talos == r.opts.Kubernetes {
		return []string{plan.StepTalos, plan.StepKubernetes}
	}

	if r.opts.Talos {
		return []string{plan.StepTalos}
	}

	return []string{plan.StepKubernetes}
}

// cluster bumps the index-th TalosCluster of a file by the first step that can be taken.
func (r *run) cluster(file string, content []byte, index int) (Outcome, error) {
	found := clusters(content)[index]
	tc := found.cluster

	out := Outcome{Object: "TalosCluster " + tc.Name, File: file}
	if tc.Namespace != "" {
		out.Object = "TalosCluster " + tc.Namespace + "/" + tc.Name
	}

	if tc.Spec.Versions == nil || tc.Spec.Versions.Talos == "" || tc.Spec.Versions.Kubernetes == "" {
		out.Held = append(out.Held, "it names no versions yet (adoption fills them in), so there is none to bump")

		return out, nil
	}

	for _, kind := range r.kinds() {
		step := plan.VersionStep{Kind: kind, From: tc.Spec.Versions.Talos}
		next := r.nextTalos

		if kind == plan.StepKubernetes {
			step.From, next = tc.Spec.Versions.Kubernetes, r.nextKubernetes
		}

		var held, problem string
		if step.To, held, problem = next(tc); held != "" {
			out.Held = append(out.Held, held)
		}

		if problem != "" {
			out.Problems = append(out.Problems, problem)
		}

		if step.To == "" || held != "" || problem != "" {
			continue
		}

		edited, err := found.write(content, kind, step.To)
		if err != nil {
			out.Problems = append(out.Problems, fmt.Sprintf("%s was not written: %v", step, err))

			continue
		}

		refused, err := r.check(file, edited)
		if err != nil {
			return out, err
		}

		if len(refused) > 0 {
			for _, why := range refused {
				out.Problems = append(out.Problems, fmt.Sprintf("%s was not written: %s", step, why))
			}

			continue
		}

		r.edited[file] = edited
		out.Step = &step

		break
	}

	return out, nil
}

// check runs the validate check on the manifests with one file as it would be written, against
// the manifests as they are, and returns what it finds wrong.
func (r *run) check(file string, content []byte) ([]string, error) {
	edited := map[string][]byte{file: content}
	for name, other := range r.edited {
		if name != file {
			edited[name] = other
		}
	}

	found, err := validate.Run(r.ctx, validate.Options{
		Paths: r.opts.Paths, Base: r.opts.Paths, Edited: edited, Source: r.opts.Source, Now: r.opts.Now,
	})
	if err != nil {
		return nil, fmt.Errorf("%s as it would be written: %w", file, err)
	}

	var problems []string

	for _, f := range found {
		if !f.Skipped {
			problems = append(problems, f.String())
		}
	}

	return problems, nil
}

// nextTalos is the Talos release a cluster is bumped to: the newest of the minor its file names,
// and once it is on that, the newest of the next minor. held says that there is one and why it
// is not taken; problem, that it could not be found out.
func (r *run) nextTalos(tc *talosv1alpha1.TalosCluster) (to, held, problem string) {
	from := tc.Spec.Versions.Talos

	gen := tc.Spec.ConfigGeneration
	if gen == nil || gen.Installer == nil || gen.Installer.FactoryURL == "" {
		return "", "it names no Image Factory (configGeneration.installer.factoryURL), so which Talos releases exist was not asked", ""
	}

	tags, err := r.opts.Source.TalosVersions(r.ctx, gen.Installer.FactoryURL)
	if err != nil {
		return "", "", fmt.Sprintf("which Talos releases exist could not be asked: %v", err)
	}

	if to = newerTalos(from, tags, r.opts.PatchOnly); to == "" {
		return "", "", ""
	}

	// The rules are the ones the operator refuses an upgrade by: its own range of Talos, the
	// steps Talos takes, and the Kubernetes version a Talos release runs.
	if why := talos.UpgradeProblem(from, to, tc.Spec.Versions.Kubernetes); why != "" {
		return to, fmt.Sprintf("Talos %s exists and is not taken: %s", to, why), ""
	}

	return to, "", ""
}

// newerTalos picks, of the releases an Image Factory lists, the one to go to from a release: the
// newest of the same minor, and where there is none newer, the newest of the minor after it.
// Talos is upgraded to the last release of a minor before it is upgraded to the next.
func newerTalos(from string, tags []string, patchOnly bool) string {
	current, err := version.ParseSemantic(from)
	if err != nil {
		return ""
	}

	var patch, minor *version.Version

	for _, tag := range tags {
		v, err := version.ParseSemantic(tag)
		if err != nil || v.Major() != current.Major() || !current.LessThan(v) {
			continue
		}

		switch {
		case v.Minor() == current.Minor() && (patch == nil || patch.LessThan(v)):
			patch = v
		case v.Minor() == current.Minor()+1 && (minor == nil || minor.LessThan(v)):
			minor = v
		}
	}

	switch {
	case patch != nil:
		return "v" + patch.String()
	case minor != nil && !patchOnly:
		return "v" + minor.String()
	}

	return ""
}

// nextKubernetes is the Kubernetes release a cluster is bumped to: the newest of the minor its
// file names, and once it is on that, the newest of the next minor. Kubernetes skips no minor.
func (r *run) nextKubernetes(tc *talosv1alpha1.TalosCluster) (to, held, problem string) {
	from := tc.Spec.Versions.Kubernetes

	current, err := version.ParseGeneric(from)
	if err != nil {
		return "", "", fmt.Sprintf("%q is no Kubernetes version", from)
	}

	minors := []uint{current.Minor()}
	if !r.opts.PatchOnly {
		minors = append(minors, current.Minor()+1)
	}

	for _, minor := range minors {
		latest, err := r.opts.Source.KubernetesLatest(r.ctx, fmt.Sprintf("%d.%d", current.Major(), minor))
		if errors.Is(err, catalog.ErrNotFound) {
			// A minor that is no longer listed, or not released yet.
			continue
		}

		if err != nil {
			return "", "", fmt.Sprintf("which Kubernetes releases exist could not be asked: %v", err)
		}

		if v, err := version.ParseGeneric(latest); err == nil && current.LessThan(v) {
			to = latest

			break
		}
	}

	if to == "" {
		return "", "", ""
	}

	why := talos.RunsKubernetes(tc.Spec.Versions.Talos, to)
	if why == "" {
		// No node is asked here: only whether Kubernetes has an upgrade from the one to the other.
		why = talos.KubernetesUpgradeProblem(from, to, nil)
	}

	if why != "" {
		return to, fmt.Sprintf("Kubernetes %s exists and is not taken: %s", to, why), ""
	}

	return to, "", ""
}
