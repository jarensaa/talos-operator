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

// Package validate checks the manifests of a cluster before they reach it (docs/scope.md §7.5):
// the check a repository runs on a change that bumps a version. It needs no cluster and no
// secret. What it can tell from the files alone, it tells offline; what exists upstream, it asks.
//
// The operator makes the same checks again when the spec arrives. This one only finds a problem
// earlier, where a change can still be turned down.
package validate

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/controller"
	"github.com/jarensaa/talos-operator/internal/gates"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// Finding is one thing that is wrong, or that could not be checked.
type Finding struct {
	// Object is what the finding is about, such as "TalosCluster default/my-cluster".
	Object string
	// Message says what is wrong.
	Message string
	// Skipped marks something that was not checked and why. It fails nothing.
	Skipped bool
}

func (f Finding) String() string { return f.Object + ": " + f.Message }

// Options says what to check.
type Options struct {
	// Paths are the files and directories that hold the manifests.
	Paths []string
	// Base holds the same manifests as they were before the change, if that is known. With it,
	// the step from the one to the other is checked: no downgrade, and no version the cluster
	// could not get to from where the base left it.
	Base []string
	// Edited holds what to check in place of a file's own content, by the file's name as Files
	// lists it: a change that is not written yet. The base is read as it is.
	Edited map[string][]byte
	// Source answers what exists upstream. Nil checks offline.
	Source catalog.Source
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Summary, if set, is written a summary in Markdown: for every cluster the versions asked
	// for, the upgrades that lead there from the base, and what was found.
	Summary io.Writer
}

// manifests is what the files hold of the operator's kinds.
type manifests struct {
	clusters []*talosv1alpha1.TalosCluster
	pools    []*talosv1alpha1.TalosMachinePool
	machines []*talosv1alpha1.TalosMachine
	configs  []*talosv1alpha1.TalosMachineConfig
}

// Run checks the manifests and returns what it found, problems first. An error is a failure to
// read them at all.
func Run(ctx context.Context, opts Options) ([]Finding, error) {
	loaded, err := load(opts.Paths, opts.Edited)
	if err != nil {
		return nil, err
	}

	if len(loaded.clusters) == 0 {
		return nil, fmt.Errorf("no TalosCluster found in %s", strings.Join(opts.Paths, ", "))
	}

	var base *manifests
	if len(opts.Base) > 0 {
		if base, err = load(opts.Base, nil); err != nil {
			return nil, fmt.Errorf("base: %w", err)
		}
	}

	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}

	var findings []Finding

	for _, tc := range loaded.clusters {
		c := &check{ctx: ctx, opts: opts, now: now(), all: loaded, tc: tc, object: "TalosCluster " + named(tc)}
		if base != nil {
			c.before = find(base.clusters, tc.Namespace, tc.Name)
		}

		c.cluster()
		findings = append(findings, c.findings...)

		if opts.Summary != nil {
			c.summarize(opts.Summary)
		}
	}

	slices.SortStableFunc(findings, func(a, b Finding) int {
		switch {
		case a.Skipped == b.Skipped:
			return 0
		case a.Skipped:
			return 1
		}

		return -1
	})

	return findings, nil
}

func named(obj client.Object) string {
	if obj.GetNamespace() == "" {
		return obj.GetName()
	}

	return obj.GetNamespace() + "/" + obj.GetName()
}

func find[T client.Object](objects []T, namespace, name string) T {
	for _, obj := range objects {
		if obj.GetNamespace() == namespace && obj.GetName() == name {
			return obj
		}
	}

	var none T

	return none
}

// Files lists the files under paths that Run reads: every .yaml and .yml file, in the order it
// reads them.
func Files(paths []string) ([]string, error) {
	var out []string

	for _, path := range paths {
		err := filepath.WalkDir(path, func(file string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if !entry.IsDir() && slices.Contains([]string{".yaml", ".yml"}, filepath.Ext(file)) {
				out = append(out, file)
			}

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// load reads every YAML document under paths and keeps the operator's kinds. Everything else a
// repository holds next to them is none of this check's business. A file that edited names is
// read from there.
func load(paths []string, edited map[string][]byte) (*manifests, error) {
	out := &manifests{}

	files, err := Files(paths)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		raw, ok := edited[file]
		if !ok {
			if raw, err = os.ReadFile(file); err != nil { //nolint:gosec // the files the caller named
				return nil, err
			}
		}

		if err := out.add(file, raw); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (m *manifests) add(file string, raw []byte) error {
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))

	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}

		var meta metav1.TypeMeta
		// A document that is not an object, such as a values file, is skipped like any other kind.
		if err := yaml.Unmarshal(doc, &meta); err != nil {
			continue
		}

		if gv, err := schema.ParseGroupVersion(meta.APIVersion); err != nil || gv.Group != talosv1alpha1.GroupVersion.Group {
			continue
		}

		// Strictly: a field the API does not have is a typo, and the API server would drop it
		// without a word.
		switch meta.Kind {
		case "TalosCluster":
			err = decode(doc, &m.clusters)
		case "TalosMachinePool":
			err = decode(doc, &m.pools)
		case "TalosMachine":
			err = decode(doc, &m.machines)
		case "TalosMachineConfig":
			err = decode(doc, &m.configs)
		}

		if err != nil {
			return fmt.Errorf("%s: %s: %w", file, meta.Kind, err)
		}
	}
}

func decode[T any](doc []byte, into *[]*T) error {
	obj := new(T)
	if err := yaml.UnmarshalStrict(doc, obj); err != nil {
		return err
	}

	*into = append(*into, obj)

	return nil
}

// check is the checking of one TalosCluster.
type check struct {
	ctx    context.Context //nolint:containedctx // lives for one Run
	opts   Options
	now    time.Time
	all    *manifests
	tc     *talosv1alpha1.TalosCluster
	before *talosv1alpha1.TalosCluster
	object string

	findings []Finding
	// way is the upgrades from what the base asks for to what the change asks for, if both are
	// known and there is a way.
	way []plan.VersionStep
}

// summarize writes what was checked about the cluster, for a human who reads a pull request.
func (c *check) summarize(w io.Writer) {
	var b strings.Builder

	fmt.Fprintf(&b, "### %s\n\n", c.object)

	if v := c.tc.Spec.Versions; v != nil && v.Talos != "" {
		fmt.Fprintf(&b, "Asks for Talos %s and Kubernetes %s.\n\n", v.Talos, v.Kubernetes)
	}

	if len(c.way) > 0 {
		b.WriteString("Upgrades from what the base asks for, in the order the operator takes them:\n\n")

		for i, step := range c.way {
			fmt.Fprintf(&b, "%d. %s\n", i+1, step)
		}

		b.WriteString("\n")
	}

	problems := 0

	for _, f := range c.findings {
		if f.Skipped {
			fmt.Fprintf(&b, "- Not checked: %s\n", f)
		} else {
			problems++

			fmt.Fprintf(&b, "- **Problem:** %s\n", f)
		}
	}

	if problems == 0 {
		b.WriteString("\nNothing wrong.\n")
	}

	b.WriteString("\n")

	_, _ = io.WriteString(w, b.String()) //nolint:errcheck // a summary nobody may be reading
}

func (c *check) problem(format string, args ...any) {
	c.findings = append(c.findings, Finding{Object: c.object, Message: fmt.Sprintf(format, args...)})
}

func (c *check) skipped(message string) {
	c.findings = append(c.findings, Finding{Object: c.object, Message: message, Skipped: true})
}

func (c *check) cluster() {
	c.policy()

	versions, named := adopt.Versions(&c.tc.Spec)
	if !named {
		c.skipped("it names no versions yet (adoption fills them in), so there is nothing to check about them")

		return
	}

	if !c.versions(versions) {
		return
	}

	c.upstream()
	c.renders()
}

// versions checks what can be told from the versions the spec names alone. It reports whether
// they make sense enough to go on.
func (c *check) versions(to talosv1alpha1.Versions) bool {
	want := plan.VersionState{Talos: to.Talos, Kubernetes: to.Kubernetes}

	if ok, why := talos.SupportsTalos(want.Talos); !ok {
		c.problem("versions.talos: %s", why)

		return false
	}

	if why := talos.RunsKubernetes(want.Talos, want.Kubernetes); why != "" {
		c.problem("versions: %s", why)

		return false
	}

	if c.before == nil {
		return true
	}

	before, named := adopt.Versions(&c.before.Spec)
	if !named {
		return true
	}

	from := plan.VersionState{Talos: before.Talos, Kubernetes: before.Kubernetes}
	rules := plan.VersionRules{TalosUpgrade: talos.UpgradeProblem, Runs: talos.RunsKubernetes}

	way := plan.PlanVersions(from, want, rules, nil)
	if way.Problem != "" {
		c.problem("versions: there is no way from Talos %s and Kubernetes %s, which the base asks for: %s",
			from.Talos, from.Kubernetes, way.Problem)

		return false
	}

	c.way = way.Steps

	return true
}

// installers are the installers the cluster's machines are installed from: the cluster's own, and
// one for every pool that has a schematic or a Secure Boot setting of its own.
func (c *check) installers() []adopt.Install {
	var out []adopt.Install

	add := func(pool *talosv1alpha1.TalosMachinePoolSpec) {
		install, err := adopt.InstallTarget(&c.tc.Spec, pool)
		if err != nil {
			return
		}

		if !slices.ContainsFunc(out, func(have adopt.Install) bool { return have.Image == install.Image }) {
			out = append(out, install)
		}
	}

	add(&talosv1alpha1.TalosMachinePoolSpec{})

	for _, pool := range c.pools() {
		add(&pool.Spec)
	}

	return out
}

func (c *check) pools() []*talosv1alpha1.TalosMachinePool {
	var out []*talosv1alpha1.TalosMachinePool

	for _, pool := range c.all.pools {
		if pool.Namespace == c.tc.Namespace && pool.Spec.ClusterRef.Name == c.tc.Name {
			out = append(out, pool)
		}
	}

	return out
}

// asked turns an answer from upstream into a finding, if it is one.
func (c *check) asked(what string, err error) {
	switch {
	case err == nil:
	case errors.Is(err, catalog.ErrNotFound):
		c.problem("%s", err)
	default:
		// Not knowing is not passing: a check that lets a change through because a site was down is
		// not one to rely on.
		c.problem("%s could not be checked: %v", what, err)
	}
}

// upstream asks whether the installers the spec names exist.
func (c *check) upstream() {
	if c.opts.Source == nil {
		c.skipped("offline: whether the installer and the Kubernetes images exist was not asked")

		return
	}

	installer := adopt.InstallerOf(&c.tc.Spec)
	if installer == nil {
		c.skipped("it names no installer yet (adoption fills it in), so there is none to look for")

		return
	}

	for _, install := range c.installers() {
		c.asked("the installer "+install.Image,
			c.opts.Source.Installer(c.ctx, install.FactoryURL, installer.Platform, install.SchematicID, install.Version, install.SecureBoot))
	}
}

// policy checks the parts of spec.rollout the API server takes as they are: a schedule, a time
// zone and an expression are strings to it.
func (c *check) policy() {
	ro := c.tc.Spec.Rollout

	windows := make([]plan.Window, 0, len(ro.MaintenanceWindows))
	for _, w := range ro.MaintenanceWindows {
		windows = append(windows, plan.Window{Schedule: w.Schedule, Duration: w.Duration.Duration, TimeZone: w.TimeZone})
	}

	if _, _, err := plan.Windows(windows, c.now); err != nil {
		c.problem("rollout: %v", err)
	}

	for _, gate := range ro.Gates {
		if err := gates.Check(gate.Expression); err != nil {
			c.problem("rollout: gate %s: %v", gate.Name, err)
		}
	}
}

// renders renders the config of every machine the files hold, with secrets made up for the
// purpose. It finds a spec that does not render at the versions it names, and a config document
// Talos does not take, and it asks whether the images of the Kubernetes components the rendered
// configs name exist: those are what a Kubernetes upgrade pulls. It says nothing about what the
// change would do to a node: that takes the node's own config, which only the operator sees.
func (c *check) renders() {
	spec := &c.tc.Spec
	pinned := adopt.VersionContract(spec)
	if pinned == "" {
		c.skipped("it names no version contract yet (adoption fills it in), so no config was rendered")

		return
	}

	contract, err := config.ParseContractFromVersion(pinned)
	if err != nil {
		c.problem("configGeneration.versionContract: %v", err)

		return
	}

	bundle, err := secrets.NewBundle(secrets.NewClock(), contract)
	if err != nil {
		c.problem("no secrets could be made up to render with: %v", err)

		return
	}

	encoded, err := adopt.EncodeBundle(bundle)
	if err != nil {
		c.problem("no secrets could be made up to render with: %v", err)

		return
	}

	ref := c.tc.SecretsReference()
	reader := &files{all: c.all, namespace: c.tc.Namespace, secret: &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: c.tc.Namespace, Name: ref.Name},
		Data:       map[string][]byte{ref.Key: encoded},
	}}

	rendered := 0

	var images []string

	for _, pool := range c.pools() {
		for _, tm := range c.all.machines {
			if tm.Namespace != c.tc.Namespace || tm.Spec.PoolRef.Name != pool.Name {
				continue
			}

			about := "TalosMachine " + named(tm)

			tc, pool, tm, fromSecrets := withoutSecrets(c.tc, pool, tm)
			if fromSecrets > 0 {
				c.findings = append(c.findings, Finding{Object: about, Skipped: true, Message: fmt.Sprintf(
					"%d of its config sources come from a Secret, which this check does not have: it was rendered without them, "+
						"and what they hold was not checked", fromSecrets)})
			}

			rendering, problem, err := controller.RenderMachine(c.ctx, reader, tc, pool, tm)
			if err != nil {
				problem = err.Error()
			}

			if problem != "" {
				c.findings = append(c.findings, Finding{Object: about, Message: "its config does not render: " + problem + withoutThem(fromSecrets)})

				continue
			}

			rendered++

			named, err := talos.KubernetesImages(rendering.Bytes)
			if err != nil {
				c.findings = append(c.findings, Finding{Object: about, Message: "its rendered config cannot be read back: " + err.Error()})

				continue
			}

			for _, image := range named {
				if !slices.Contains(images, image) {
					images = append(images, image)
				}
			}
		}
	}

	if rendered == 0 {
		named, why := c.alone(reader)
		if why != "" {
			c.skipped("no machine of it was rendered, and " + why + ", so the images of its Kubernetes components were not looked for")

			return
		}

		images = named
	}

	if c.opts.Source == nil {
		return
	}

	slices.Sort(images)

	for _, image := range images {
		c.asked("the image "+image, c.opts.Source.Image(c.ctx, image))
	}
}

// alone renders a control plane from the cluster and nothing else, and returns the images of the
// Kubernetes components its config names, or why there is none. It is what is left to look at
// where the files hold the cluster and none of its machines, as they do where the operator wrote
// those when it adopted the cluster: a control plane names every image, and names the ones the
// version brings unless a pool or a machine says otherwise.
func (c *check) alone(reader client.Reader) (images []string, why string) {
	pool := &talosv1alpha1.TalosMachinePool{Spec: talosv1alpha1.TalosMachinePoolSpec{Role: talosv1alpha1.MachineRoleControlPlane}}
	tm := &talosv1alpha1.TalosMachine{ObjectMeta: metav1.ObjectMeta{Namespace: c.tc.Namespace}}

	tc, pool, tm, fromSecrets := withoutSecrets(c.tc, pool, tm)
	if fromSecrets > 0 {
		c.skipped(fmt.Sprintf("%d of the cluster's own config sources come from a Secret, which this check does not have: "+
			"a control plane was rendered without them, and an image they name was not looked for", fromSecrets))
	}

	rendering, problem, err := controller.RenderMachine(c.ctx, reader, tc, pool, tm)
	if err != nil {
		problem = err.Error()
	}

	if problem != "" {
		return nil, "a control plane does not render from the cluster alone (" + problem + withoutThem(fromSecrets) + ")"
	}

	named, err := talos.KubernetesImages(rendering.Bytes)
	if err != nil {
		return nil, "a control plane rendered from the cluster alone cannot be read back (" + err.Error() + ")"
	}

	for _, image := range named {
		if !slices.Contains(images, image) {
			images = append(images, image)
		}
	}

	return images, ""
}

// withoutSecrets returns copies of a machine, its pool and its cluster without the config sources
// that come from a Secret, which this check does not have, and how many it left out. What the
// other sources hold is checked all the same.
func withoutSecrets(tc *talosv1alpha1.TalosCluster, pool *talosv1alpha1.TalosMachinePool, tm *talosv1alpha1.TalosMachine,
) (*talosv1alpha1.TalosCluster, *talosv1alpha1.TalosMachinePool, *talosv1alpha1.TalosMachine, int) {
	tc, pool, tm = tc.DeepCopy(), pool.DeepCopy(), tm.DeepCopy()
	left := 0

	for _, sources := range []*[]talosv1alpha1.ConfigSource{&tc.Spec.Config, &pool.Spec.Config, &tm.Spec.Config} {
		before := len(*sources)
		*sources = slices.DeleteFunc(*sources, func(src talosv1alpha1.ConfigSource) bool { return src.SecretKeyRef != nil })
		left += before - len(*sources)
	}

	return tc, pool, tm, left
}

// withoutThem says, after why a config does not render, that it was rendered without the sources
// from Secrets, if there were any: a document there may be what the others need.
func withoutThem(fromSecrets int) string {
	if fromSecrets == 0 {
		return ""
	}

	return fmt.Sprintf(" (rendered without its %d config source(s) from a Secret, which may be why)", fromSecrets)
}

// files answers the reads of rendering from the manifests: the shared configs they hold, and the
// one Secret that was made up.
type files struct {
	all       *manifests
	namespace string
	secret    *corev1.Secret
}

var _ client.Reader = (*files)(nil)

func (f *files) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch into := obj.(type) {
	case *talosv1alpha1.TalosMachineConfig:
		if found := find(f.all.configs, key.Namespace, key.Name); found != nil {
			found.DeepCopyInto(into)

			return nil
		}

		return apierrors.NewNotFound(schema.GroupResource{Group: talosv1alpha1.GroupVersion.Group, Resource: "talosmachineconfigs"}, key.Name)
	case *corev1.Secret:
		if key.Namespace == f.secret.Namespace && key.Name == f.secret.Name {
			f.secret.DeepCopyInto(into)

			return nil
		}

		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, key.Name)
	}

	return fmt.Errorf("the check holds no %T", obj)
}

func (f *files) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("the check lists nothing")
}
