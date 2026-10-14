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

package plan

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// Versions and names the Kubernetes specs keep coming back to.
const (
	kubeOld      = "1.36.2"
	kubeHop      = "1.36.0"
	kubeNewFirst = "1.37.0"
	kubeNew      = "1.37.1"
	kubeCP1      = "cp1"
)

// Images as Talos names them by default.
var kubeRepos = map[string]string{
	ComponentAPIServer:         "registry.k8s.io/kube-apiserver",
	ComponentControllerManager: "registry.k8s.io/kube-controller-manager",
	ComponentScheduler:         "registry.k8s.io/kube-scheduler",
	ComponentProxy:             "registry.k8s.io/kube-proxy",
	ComponentKubelet:           "ghcr.io/siderolabs/kubelet",
}

func kubeImage(component, v string) string { return kubeRepos[component] + ":v" + v }

func kubeMachine(name string, controlPlane bool, live, desired string) KubernetesMachine {
	m := KubernetesMachine{Name: name, Node: name, ControlPlane: controlPlane, Current: true, Images: map[string]KubernetesImage{}}

	components := []string{ComponentKubelet}
	if controlPlane {
		components = KubernetesComponents
	}

	for _, c := range components {
		m.Images[c] = KubernetesImage{Live: kubeImage(c, live), Desired: kubeImage(c, desired)}
	}

	return m
}

func kubeWorker(name, live, desired string) KubernetesMachine {
	return kubeMachine(name, false, live, desired)
}

func kubeControlPlane(name, live, desired string) KubernetesMachine {
	return kubeMachine(name, true, live, desired)
}

// running sets the version one component of the machine's config names.
func (m KubernetesMachine) running(component, v string) KubernetesMachine {
	images := map[string]KubernetesImage{}
	for c, image := range m.Images {
		images[c] = image
	}

	image := images[component]
	image.Live = kubeImage(component, v)
	images[component] = image
	m.Images = images

	return m
}

func TestImageVersion(t *testing.T) {
	for image, want := range map[string]string{
		"registry.k8s.io/kube-apiserver:v1.37.1":                     kubeNew,
		"ghcr.io/siderolabs/kubelet:v1.36.0":                         kubeHop,
		"registry.example.com:5000/mirror/kube-scheduler:v1.37.1":    kubeNew,
		"ghcr.io/siderolabs/kubelet:v1.37.1-fat":                     kubeNew,
		"registry.k8s.io/kube-apiserver:v1.37.1@sha256:0123456789ab": kubeNew,
		"registry.k8s.io/kube-apiserver:1.37.1":                      kubeNew,
		// Nothing to compare: no tag, a tag that is no version, a digest alone.
		"registry.k8s.io/kube-apiserver":                     "",
		"registry.k8s.io/kube-apiserver:latest":              "",
		"registry.k8s.io/kube-apiserver:v1.37":               "",
		"registry.example.com:5000/mirror/kube-scheduler":    "",
		"registry.k8s.io/kube-apiserver@sha256:0123456789ab": "",
		"": "",
	} {
		got := ""
		if v, ok := ImageVersion(image); ok {
			got = core(v)
		}

		if got != want {
			t.Errorf("%q: got %q, want %q", image, got, want)
		}
	}

	for _, tt := range [][3]string{
		{"registry.k8s.io/kube-apiserver:v1.38.1", kubeNewFirst, "registry.k8s.io/kube-apiserver:v1.37.0"},
		{"ghcr.io/siderolabs/kubelet:v1.38.1-fat", kubeNewFirst, "ghcr.io/siderolabs/kubelet:v1.37.0-fat"},
		{"registry.example.com:5000/v1.38.1/kubelet:v1.38.1", kubeNewFirst, "registry.example.com:5000/v1.38.1/kubelet:v1.37.0"},
	} {
		if got := withVersion(tt[0], tt[1]); got != tt[2] {
			t.Errorf("withVersion(%q, %q): got %q, want %q", tt[0], tt[1], got, tt[2])
		}
	}
}

func TestHopOf(t *testing.T) {
	tests := []struct {
		name          string
		machines      []KubernetesMachine
		from, hop, to string
		problem       string
	}{
		{"nothing to do", []KubernetesMachine{kubeControlPlane(kubeCP1, kubeNew, kubeNew), kubeWorker("w1", kubeNew, kubeNew)}, "", "", "", ""},
		{"a patch release", []KubernetesMachine{kubeControlPlane(kubeCP1, kubeNewFirst, kubeNew)}, kubeNewFirst, kubeNew, kubeNew, ""},
		{"a worker alone", []KubernetesMachine{kubeWorker("w1", kubeHop, kubeNewFirst)}, kubeHop, kubeNewFirst, kubeNewFirst, ""},
		{"the next minor", []KubernetesMachine{kubeControlPlane(kubeCP1, kubeOld, kubeNew), kubeWorker("w1", kubeOld, kubeNew)},
			kubeOld, kubeNew, kubeNew, ""},
		// Kubernetes does not skip a minor: the way there is the first release of each one between.
		{"two minors", []KubernetesMachine{kubeControlPlane(kubeCP1, "1.35.4", kubeNew)}, "1.35.4", kubeHop, kubeNew, ""},
		{"three minors", []KubernetesMachine{kubeControlPlane(kubeCP1, "1.34.9", kubeNew)}, "1.34.9", "1.35.0", kubeNew, ""},
		// A hop that was interrupted goes on from the component that is furthest behind.
		{"half way", []KubernetesMachine{
			kubeControlPlane(kubeCP1, kubeOld, kubeNew).running(ComponentAPIServer, kubeNew), kubeWorker("w1", kubeOld, kubeNew),
		}, kubeOld, kubeNew, kubeNew, ""},
		{"half way through the first of two hops", []KubernetesMachine{
			kubeControlPlane(kubeCP1, "1.35.4", kubeNew).running(ComponentAPIServer, kubeHop),
		}, "1.35.4", kubeHop, kubeNew, ""},
		{"the second of two hops", []KubernetesMachine{kubeControlPlane(kubeCP1, kubeHop, kubeNew)}, kubeHop, kubeNew, kubeNew, ""},
		// A machine that cannot be judged is left out here; the planner blocks on it.
		{"a stale machine", []KubernetesMachine{kubeControlPlane(kubeCP1, kubeOld, kubeNew), {Name: "w1"}}, kubeOld, kubeNew, kubeNew, ""},
		{"an image without a version", []KubernetesMachine{{
			Name: "w1", Current: true,
			Images: map[string]KubernetesImage{
				ComponentKubelet: {Live: "ghcr.io/siderolabs/kubelet:latest", Desired: kubeImage(ComponentKubelet, kubeNew)},
			},
		}}, "", "", "", "names no version"},
		{"another major", []KubernetesMachine{kubeControlPlane(kubeCP1, kubeNew, "2.0.0")}, kubeNew, "", "2.0.0",
			"no upgrade from Kubernetes 1.37.1 to 2.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HopOf(tt.machines, nil)
			if got.From != tt.from || got.Version != tt.hop || got.To != tt.to {
				t.Fatalf("got from %q via %q to %q, want from %q via %q to %q", got.From, got.Version, got.To, tt.from, tt.hop, tt.to)
			}

			if (tt.problem == "") != (got.Problem == "") || !strings.Contains(got.Problem, tt.problem) {
				t.Fatalf("problem: got %q, want %q", got.Problem, tt.problem)
			}

			if got.Due() != (tt.hop != "") {
				t.Fatalf("due: got %v", got.Due())
			}
		})
	}
}

// Every API server before anything else: no other component may be newer than an API server.
func TestPlanKubernetesOrder(t *testing.T) {
	machines := []KubernetesMachine{
		kubeWorker("w2", kubeOld, kubeNew), kubeControlPlane("cp2", kubeOld, kubeNew),
		kubeControlPlane(kubeCP1, kubeOld, kubeNew), kubeWorker("w1", kubeOld, kubeNew),
	}

	got := PlanKubernetes(machines, KubernetesSettings{}, healthyPass())

	want := []string{
		"kube-apiserver/cp1", "kube-apiserver/cp2",
		"kube-controller-manager/cp1", "kube-controller-manager/cp2",
		"kube-scheduler/cp1", "kube-scheduler/cp2",
		"kubelet/cp1", "kubelet/cp2", "kubelet/w1", "kubelet/w2",
	}
	if !slices.Equal(got.Pending, want) || !got.MayStart() {
		t.Fatalf("got %v (%s: %s)\nwant %v", got.Pending, got.BlockReason, got.BlockMessage, want)
	}

	if got.Targets["kube-apiserver/cp1"] != "registry.k8s.io/kube-apiserver:v1.37.1" ||
		got.Targets["kubelet/w2"] != "ghcr.io/siderolabs/kubelet:v1.37.1" ||
		len(got.Targets) != len(want)+2 {
		t.Fatalf("images: %v", got.Targets)
	}

	// kube-proxy has no turn: a control plane's kubelet brings its image along.
	if images := TurnImages(got.Targets, "kubelet/cp1"); len(images) != 2 ||
		images[ComponentKubelet] != "ghcr.io/siderolabs/kubelet:v1.37.1" ||
		images[ComponentProxy] != "registry.k8s.io/kube-proxy:v1.37.1" {
		t.Fatalf("the kubelet's turn on a control plane: %v", images)
	}

	if images := TurnImages(got.Targets, "kubelet/w1"); len(images) != 1 {
		t.Fatalf("the kubelet's turn on a worker: %v", images)
	}

	if want := []string{kubeCP1, "cp2", "w1", "w2"}; !slices.Equal(got.Behind, want) {
		t.Fatalf("behind: got %v, want %v", got.Behind, want)
	}

	for key := range got.Targets {
		component, machine := SplitTurn(key)
		if TurnKey(component, machine) != key || component == "" {
			t.Fatalf("turn key %q does not split into a component and a machine", key)
		}
	}

	if component, machine := SplitTurn("w1"); component != "" || machine != "w1" {
		t.Fatalf("a machine is a turn without a component: %q %q", component, machine)
	}

	// What already names the hop's version is left out, wherever the upgrade stopped.
	machines[2] = machines[2].running(ComponentAPIServer, kubeNew).running(ComponentControllerManager, kubeNew)
	machines[3] = kubeWorker("w1", kubeNew, kubeNew)
	resumed := PlanKubernetes(machines, KubernetesSettings{}, healthyPass())

	want = []string{
		"kube-apiserver/cp2", "kube-controller-manager/cp2", "kube-scheduler/cp1", "kube-scheduler/cp2",
		"kubelet/cp1", "kubelet/cp2", "kubelet/w2",
	}
	if !slices.Equal(resumed.Pending, want) {
		t.Fatalf("resumed: got %v\nwant %v", resumed.Pending, want)
	}

	// A kubelet that names the hop's version already still has its turn while the kube-proxy image
	// of its config does not, and keeps its own image.
	machines[2] = machines[2].running(ComponentKubelet, kubeNew)
	proxyOnly := PlanKubernetes(machines, KubernetesSettings{}, healthyPass())

	if !slices.Contains(proxyOnly.Pending, "kubelet/cp1") ||
		proxyOnly.Targets["kubelet/cp1"] != "ghcr.io/siderolabs/kubelet:v1.37.1" ||
		proxyOnly.Targets["kube-proxy/cp1"] != "registry.k8s.io/kube-proxy:v1.37.1" {
		t.Fatalf("proxy only: %v %v", proxyOnly.Pending, proxyOnly.Targets)
	}

	if resumed.Change == got.Change || resumed.Change == "" {
		t.Fatal("another set of turns is another change")
	}

	// A hop on the way brings every component to the first release of the next minor, in the
	// image its rendered config names.
	far := PlanKubernetes([]KubernetesMachine{kubeControlPlane(kubeCP1, "1.35.4", kubeNew), kubeWorker("w1", "1.35.4", kubeNew)},
		KubernetesSettings{}, healthyPass())
	if far.Hop.Version != "1.36.0" || far.Targets["kubelet/w1"] != "ghcr.io/siderolabs/kubelet:v1.36.0" || len(far.Pending) != 5 {
		t.Fatalf("far: %+v", far)
	}

	done := PlanKubernetes([]KubernetesMachine{kubeControlPlane(kubeCP1, kubeNew, kubeNew)}, KubernetesSettings{}, healthyPass())
	if len(done.Pending) != 0 || done.BlockReason != "" || len(done.Behind) != 0 {
		t.Fatalf("nothing is due: %+v", done)
	}
}

func TestPlanKubernetesBlocks(t *testing.T) {
	cluster := func(mutate ...func([]KubernetesMachine)) []KubernetesMachine {
		machines := []KubernetesMachine{
			kubeControlPlane(kubeCP1, kubeOld, kubeNew), kubeControlPlane("cp2", kubeOld, kubeNew), kubeWorker("w1", kubeOld, kubeNew),
		}
		for _, m := range mutate {
			m(machines)
		}

		return machines
	}

	tests := []struct {
		name     string
		machines []KubernetesMachine
		settings KubernetesSettings
		pass     Pass
		reason   string
		message  string
	}{
		{"nothing in the way", cluster(), KubernetesSettings{}, healthyPass(), "", ""},
		{"a check found something", cluster(), KubernetesSettings{Problems: []string{"cp1 runs Talos v1.13.0, which does not support it"}},
			healthyPass(), BlockIncompatible, "v1.13.0"},
		{"the checks did not run", cluster(), KubernetesSettings{Unchecked: "cp1 did not answer"}, healthyPass(), BlockNotCurrent,
			"cp1 did not answer"},
		{wordPaused, cluster(), KubernetesSettings{Policy: pausedPolicy}, healthyPass(), BlockPaused, wordPaused},
		{"outside the windows", cluster(), KubernetesSettings{Policy: Policy{OutsideWindow: wordOutside}}, healthyPass(), BlockOutsideWindow,
			wordOutside},
		{"a gate of the user's", cluster(), KubernetesSettings{Policy: Policy{GateClosed: "gate ceph"}}, healthyPass(), BlockGateClosed,
			"gate ceph"},
		{wordAborted, cluster(), KubernetesSettings{Aborted: kubeNew}, healthyPass(), BlockAborted, wordAborted},
		{"another version was aborted", cluster(), KubernetesSettings{Aborted: kubeNewFirst}, healthyPass(), "", ""},
		// An upgrade is no repair: it starts on a healthy cluster or not at all, whichever node ails.
		{"an unhealthy node", cluster(), KubernetesSettings{}, ailing(nodeA), BlockGateClosed, nodeA},
		{"a machine that cannot be judged", cluster(func(m []KubernetesMachine) { m[2] = KubernetesMachine{Name: "w1"} }),
			KubernetesSettings{}, healthyPass(), BlockNotCurrent, "w1"},
		// A control plane left out would end up behind components that must not be newer than it.
		{"a control plane on hold", cluster(func(m []KubernetesMachine) { m[1].Hold = true }), KubernetesSettings{}, healthyPass(),
			BlockIncompatible, "cp2"},
		{"a downgrade", cluster(func(m []KubernetesMachine) { m[0] = kubeControlPlane(kubeCP1, "1.37.2", kubeNew) }), KubernetesSettings{},
			healthyPass(), BlockIncompatible, "not downgraded"},
		{"an image without a version", cluster(func(m []KubernetesMachine) {
			m[2].Images[ComponentKubelet] = KubernetesImage{Live: "ghcr.io/siderolabs/kubelet:latest", Desired: kubeImage(ComponentKubelet, kubeNew)}
		}), KubernetesSettings{}, healthyPass(), BlockIncompatible, "names no version"},
		// What cannot work is said before what is merely not yet.
		{"incompatible and paused", cluster(), KubernetesSettings{Policy: pausedPolicy, Problems: []string{"a removed flag"}}, healthyPass(),
			BlockIncompatible, "a removed flag"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PlanKubernetes(tt.machines, tt.settings, tt.pass)
			if got.BlockReason != tt.reason || !strings.Contains(got.BlockMessage, tt.message) {
				t.Fatalf("got %q (%s), want %q (%s)", got.BlockReason, got.BlockMessage, tt.reason, tt.message)
			}

			if len(got.Pending) == 0 {
				t.Fatal("an upgrade that is due and blocked must still say what is pending")
			}

			if got.MayStart() != (tt.reason == "") {
				t.Fatalf("may start: %v", got.MayStart())
			}
		})
	}

	// A worker on hold is left out and named; the kubelet may lag behind the API server.
	held := PlanKubernetes(cluster(func(m []KubernetesMachine) { m[2].Hold = true }), KubernetesSettings{}, healthyPass())
	if !held.MayStart() || !slices.Equal(held.Held, []string{"w1"}) || slices.Contains(held.Pending, "kubelet/w1") {
		t.Fatalf("held worker: %+v", held)
	}

	// An abort covers the version the spec asks for, through every hop on the way there.
	far := []KubernetesMachine{kubeControlPlane(kubeCP1, "1.35.4", kubeNew)}
	if got := PlanKubernetes(far, KubernetesSettings{Aborted: kubeNew}, healthyPass()); got.BlockReason != BlockAborted || !got.AbortStands {
		t.Fatalf("an aborted upgrade does not go on with its next hop: %+v", got)
	}

	if got := PlanKubernetes(far, KubernetesSettings{Aborted: kubeHop}, healthyPass()); got.AbortStands {
		t.Fatal("an abort of another version is spent")
	}
}

func TestNextKubernetesStep(t *testing.T) {
	pulling := Step{Name: StepPulling, PreviousHash: prev, DesiredHash: desired, Since: t0}
	late := t0.Add(timing.StepTimeout)

	tests := []struct {
		name string
		node Node
		pre  Gate
		now  time.Time
		want ActionKind
		// reason of a failure
		reason string
	}{
		{"the gate is open", on(prev, prev), open, t0, Pull, ""},
		{"the gate is closed", on(prev, prev), closed, t0, Wait, ""},
		{"the node does not answer", Node{}, open, t0, Wait, ""},
		{"the gate stays closed", on(prev, prev), closed, late, Fail, FailTimeout},
		{"the node never answers", Node{}, open, late, Fail, FailTimeout},
		// Whatever config the node has by now is the one the pull starts from.
		{"another config than before", on(foreign, foreign), open, t0, Pull, ""},
	}

	for _, tt := range tests {
		got := NextKubernetesStep(pulling, tt.node, tt.pre, closed, timing, tt.now)
		if got.Kind != tt.want || got.Reason != tt.reason {
			t.Errorf("%s: got %s %q (%s), want %s %q", tt.name, got.Kind, got.Reason, got.Message, tt.want, tt.reason)
		}
	}

	// Once the image is on the node, the turn is a config change like any other.
	for _, name := range []string{StepTrying, StepVerifying, StepConfirming, StepRollingBack} {
		step := tryStep(name)
		for _, node := range []Node{on(prev, prev), on(desired, prev), on(desired, desired), {}} {
			for _, now := range []time.Time{t0, t0.Add(time.Minute), t0.Add(timing.TryTimeout), t0.Add(time.Hour)} {
				if got, want := NextKubernetesStep(step, node, open, open, timing, now), NextStep(step, node, open, open, timing, now); got != want {
					t.Fatalf("%s on %+v at %s: got %+v, want %+v", name, node, now, got, want)
				}
			}
		}
	}

	// The image is never pulled, and so nothing ever tried, while the gate is closed.
	for _, now := range []time.Time{t0, t0.Add(time.Minute), late, late.Add(time.Hour)} {
		if got := NextKubernetesStep(pulling, on(prev, prev), closed, open, timing, now); got.Kind == Pull {
			t.Fatalf("pulled behind a closed gate at %s", now)
		}
	}
}

// Every image a config names is changed by exactly one turn, and only kube-proxy, which Talos does
// not run, by another component's.
func TestChanges(t *testing.T) {
	changedBy := map[string][]string{}

	for _, turn := range KubernetesTurns {
		if Changes(turn)[0] != turn {
			t.Errorf("%s: its turn changes %v, its own image not first", turn, Changes(turn))
		}

		for _, c := range Changes(turn) {
			changedBy[c] = append(changedBy[c], turn)
		}
	}

	for _, component := range KubernetesComponents {
		if len(changedBy[component]) != 1 {
			t.Errorf("%s: changed by %v", component, changedBy[component])
		}
	}

	if slices.Contains(KubernetesTurns, ComponentProxy) || !slices.Equal(changedBy[ComponentProxy], []string{ComponentKubelet}) {
		t.Errorf("kube-proxy: a turn of its own %v, changed by %v", slices.Contains(KubernetesTurns, ComponentProxy), changedBy[ComponentProxy])
	}
}

// The API server's restart unsettles a control plane for longer than a config change does, most
// of all where there is only one. Its turn gets the time a rebooted node gets; no other turn does.
func TestKubernetesTryTimeout(t *testing.T) {
	for _, component := range KubernetesComponents {
		want := timing.TryTimeout
		if component == ComponentAPIServer {
			want = timing.StepTimeout + timing.Soak + ConfirmMargin
		}

		if got := KubernetesTryTimeout(component, timing); got != want {
			t.Errorf("%s: got %s, want %s", component, got, want)
		}
	}

	// Never less than any other change gets.
	long := Timing{TryTimeout: time.Hour, Soak: time.Minute, StepTimeout: 5 * time.Minute}
	if got := KubernetesTryTimeout(ComponentAPIServer, long); got != time.Hour {
		t.Fatalf("got %s", got)
	}

	// With the defaults, a control plane has stepTimeout to settle in before the soak must begin.
	window := KubernetesTryTimeout(ComponentAPIServer, timing)
	if settle := window - timing.Soak - ConfirmMargin; settle != timing.StepTimeout {
		t.Fatalf("time to settle in: %s", settle)
	}
}
