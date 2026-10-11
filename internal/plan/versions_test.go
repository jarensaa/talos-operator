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
	"fmt"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/version"
)

// runs is which Kubernetes minors each Talos minor of these tests runs, lowest and highest.
var runs = map[uint][2]uint{13: {31, 36}, 14: {32, 37}, 15: {33, 38}, 16: {34, 39}}

// testRules are rules shaped like Talos's: a release runs a window of Kubernetes minors, is
// upgraded to from at most two minors back and never downgraded, and is only installed while it
// runs the Kubernetes version the cluster has.
func testRules() VersionRules {
	doesRun := func(talos, kubernetes string) string {
		t, k := version.MustParseGeneric(talos), version.MustParseGeneric(kubernetes)
		if window := runs[t.Minor()]; k.Minor() < window[0] || k.Minor() > window[1] {
			return fmt.Sprintf("Talos %s does not run Kubernetes %s", talos, kubernetes)
		}

		return ""
	}

	return VersionRules{
		Runs: doesRun,
		TalosUpgrade: func(from, to, kubernetes string) string {
			f, t := version.MustParseGeneric(from), version.MustParseGeneric(to)

			switch {
			case t.LessThan(f):
				return "downgrades are not supported"
			case t.Minor() > f.Minor()+2:
				return fmt.Sprintf("Talos does not upgrade from %s to %s", from, to)
			}

			return doesRun(to, kubernetes)
		},
	}
}

func steps(plan VersionPlan) []string {
	out := make([]string, 0, len(plan.Steps))
	for _, s := range plan.Steps {
		out = append(out, s.String())
	}

	return out
}

func TestPlanVersions(t *testing.T) {
	releases := Releases{"1.35": "1.35.9", "1.36": "1.36.5", "1.37": "1.37.1"}

	tests := []struct {
		name             string
		current, desired VersionState
		want             []string
		// problem is part of why there is no way; empty when there is one.
		problem string
	}{
		{"nothing to do", VersionState{"v1.14.2", "1.37.1"}, VersionState{"v1.14.2", "1.37.1"}, nil, ""},
		{"nothing asked for", VersionState{"v1.14.2", "1.37.1"}, VersionState{}, nil, ""},
		{"a Talos patch", VersionState{"v1.14.1", "1.37.1"}, VersionState{"v1.14.2", "1.37.1"}, []string{"Talos v1.14.1 to v1.14.2"}, ""},
		{"a Kubernetes patch", VersionState{"v1.14.2", "1.37.0"}, VersionState{"v1.14.2", "1.37.1"}, []string{"Kubernetes 1.37.0 to 1.37.1"}, ""},
		{"only Kubernetes asked for", VersionState{"v1.14.2", "1.36.2"}, VersionState{Kubernetes: "1.37.1"},
			[]string{"Kubernetes 1.36.2 to 1.37.1"}, ""},
		// A minor is not skipped: the way goes through the newest release known of each.
		{"two Kubernetes minors", VersionState{"v1.14.2", "1.35.4"}, VersionState{"v1.14.2", "1.37.1"},
			[]string{"Kubernetes 1.35.4 to 1.36.5", "Kubernetes 1.36.5 to 1.37.1"}, ""},
		// Both can go: Talos does, as the operator would.
		{"both, in either order", VersionState{"v1.14.1", "1.36.2"}, VersionState{"v1.14.2", "1.37.1"},
			[]string{"Talos v1.14.1 to v1.14.2", "Kubernetes 1.36.2 to 1.37.1"}, ""},
		// The new Kubernetes needs the new Talos: Talos first.
		{"Kubernetes needs the new Talos", VersionState{"v1.13.5", "1.36.2"}, VersionState{"v1.14.2", "1.37.1"},
			[]string{"Talos v1.13.5 to v1.14.2", "Kubernetes 1.36.2 to 1.37.1"}, ""},
		// The new Talos no longer runs the old Kubernetes: Kubernetes first.
		{"Talos drops the old Kubernetes", VersionState{"v1.14.2", "1.32.4"}, VersionState{"v1.15.0", "1.33.2"},
			[]string{"Kubernetes 1.32.4 to 1.33.2", "Talos v1.14.2 to v1.15.0"}, ""},
		// And both at once: as far as the old Talos carries, then Talos, then the rest.
		{"taking turns", VersionState{"v1.13.5", "1.31.9"}, VersionState{"v1.15.0", "1.38.0"}, []string{
			"Kubernetes 1.31.9 to 1.32.0", "Kubernetes 1.32.0 to 1.33.0", "Talos v1.13.5 to v1.15.0", "Kubernetes 1.33.0 to 1.34.0",
			"Kubernetes 1.34.0 to 1.35.9", "Kubernetes 1.35.9 to 1.36.5", "Kubernetes 1.36.5 to 1.37.1", "Kubernetes 1.37.1 to 1.38.0",
		}, ""},
		// No way.
		{"a place that is none", VersionState{"v1.14.2", "1.37.1"}, VersionState{"v1.15.0", "1.32.0"}, nil, "not downgraded"},
		{"a Talos that does not run the Kubernetes asked for", VersionState{"v1.14.2", "1.37.1"}, VersionState{"v1.14.2", "1.38.0"}, nil,
			"the spec asks for Talos v1.14.2 with Kubernetes 1.38.0"},
		{"a Talos bump alone that leaves Kubernetes behind", VersionState{"v1.14.2", "1.32.4"}, VersionState{"v1.15.0", "1.32.4"}, nil,
			"Talos v1.15.0 does not run Kubernetes 1.32.4"},
		{"a Kubernetes downgrade", VersionState{"v1.14.2", "1.37.1"}, VersionState{"v1.14.2", "1.36.5"}, nil, "not downgraded"},
		{"a Talos downgrade", VersionState{"v1.14.2", "1.37.1"}, VersionState{"v1.14.1", "1.37.1"}, nil, "downgrades are not supported"},
		{"a Talos jump too far", VersionState{"v1.13.5", "1.36.2"}, VersionState{"v1.16.0", "1.36.2"}, nil, "does not upgrade from v1.13.5"},
		{"a version that is none", VersionState{"v1.14.2", "1.37.1"}, VersionState{"v1.14.2", "latest"}, nil, "is no Kubernetes version"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PlanVersions(tt.current, tt.desired, testRules(), releases)

			if (tt.problem == "") != (got.Problem == "") || !strings.Contains(got.Problem, tt.problem) {
				t.Fatalf("problem: got %q, want %q", got.Problem, tt.problem)
			}

			if tt.problem == "" && !slices.Equal(steps(got), tt.want) {
				t.Fatalf("got  %q\nwant %q", steps(got), tt.want)
			}
		})
	}
}

// Every way the planner finds can be gone: each step is one the rules allow from where the step
// before it left the cluster, no Kubernetes minor is skipped, and the way ends where the spec
// asks. And where it finds none, there is none of the kind it looks for.
func TestPlanVersionsFindsOnlyWaysThatCanBeGone(t *testing.T) {
	rules := testRules()

	var talos, kubernetes []string
	for minor := uint(13); minor <= 16; minor++ {
		talos = append(talos, fmt.Sprintf("v1.%d.0", minor), fmt.Sprintf("v1.%d.3", minor))
	}

	for minor := uint(31); minor <= 39; minor++ {
		kubernetes = append(kubernetes, fmt.Sprintf("1.%d.0", minor), fmt.Sprintf("1.%d.4", minor))
	}

	found, checked := 0, 0

	for _, t0 := range talos {
		for _, k0 := range kubernetes {
			if rules.Runs(t0, k0) != "" {
				continue // not a cluster that runs
			}

			for _, t1 := range talos {
				for _, k1 := range kubernetes {
					checked++

					current, desired := VersionState{t0, k0}, VersionState{t1, k1}
					plan := PlanVersions(current, desired, rules, nil)

					at := current

					for _, step := range plan.Steps {
						switch step.Kind {
						case StepTalos:
							if step.From != at.Talos || rules.TalosUpgrade(at.Talos, step.To, at.Kubernetes) != "" {
								t.Fatalf("%v to %v: step %s cannot be gone from %v", current, desired, step, at)
							}

							at.Talos = step.To
						case StepKubernetes:
							from, to := version.MustParseGeneric(at.Kubernetes), version.MustParseGeneric(step.To)
							if step.From != at.Kubernetes || rules.Runs(at.Talos, step.To) != "" || to.Minor() > from.Minor()+1 || to.LessThan(from) {
								t.Fatalf("%v to %v: step %s cannot be gone from %v", current, desired, step, at)
							}

							at.Kubernetes = step.To
						}
					}

					if plan.Problem == "" {
						found++

						if at != desired {
							t.Fatalf("%v to %v: the way ends at %v", current, desired, at)
						}

						continue
					}

					// No way found. One exists exactly when the place asked for is one to be, nothing
					// goes backwards, and Talos can get there from here: then the turns can be taken.
					t0v, t1v := version.MustParseGeneric(t0), version.MustParseGeneric(t1)
					k0v, k1v := version.MustParseGeneric(k0), version.MustParseGeneric(k1)

					possible := rules.Runs(t1, k1) == "" && !t1v.LessThan(t0v) && !k1v.LessThan(k0v) && t1v.Minor() <= t0v.Minor()+2
					if possible {
						t.Fatalf("%v to %v: a way exists and none was found: %s", current, desired, plan.Problem)
					}
				}
			}
		}
	}

	if found < 500 || checked < 5000 {
		t.Fatalf("only %d ways found in %d cases", found, checked)
	}
}

func TestKubernetesHops(t *testing.T) {
	releases := Releases{"1.36": "1.36.5", "1.37": "v1.37.1", "1.38": "1.39.0", "1.35": "newest"}

	for _, tt := range []struct {
		from, to string
		want     []string
	}{
		{"1.36.2", "1.36.2", nil},
		{"1.36.2", "", nil},
		{"", "1.36.2", nil},
		{"v1.36.2", "1.36.2", nil},
		{"1.36.2", "1.36.4", []string{"1.36.4"}},
		{"1.36.2", "1.37.0", []string{"1.37.0"}},
		// What is known of a minor on the way is used; what is asked for is never replaced by it.
		{"1.35.4", "1.37.0", []string{"1.36.5", "1.37.0"}},
		{"1.35.4", "1.38.2", []string{"1.36.5", "1.37.1", "1.38.2"}},
		// What is known and makes no sense is not used: the first release of the minor is.
		{"1.34.1", "1.36.0", []string{"1.35.0", "1.36.0"}},
		{"1.37.1", "1.39.0", []string{"1.38.0", "1.39.0"}},
		{"1.33.0", "1.35.0", []string{"1.34.0", "1.35.0"}},
	} {
		got, problem := KubernetesHops(tt.from, tt.to, releases)
		if problem != "" || !slices.Equal(got, tt.want) {
			t.Errorf("%s to %s: got %v (%s), want %v", tt.from, tt.to, got, problem, tt.want)
		}
	}

	for _, tt := range [][2]string{{"1.37.1", "1.36.5"}, {"1.37.1", "2.0.0"}, {"1.37.1", "next"}, {"now", "1.37.1"}} {
		if _, problem := KubernetesHops(tt[0], tt[1], releases); problem == "" {
			t.Errorf("%s to %s: there is no such way", tt[0], tt[1])
		}
	}
}

// A hop on the way uses the newest release known of its minor, and a hop that was begun is
// finished with the release it was begun with, whatever is newest by then.
func TestHopOfUsesKnownReleases(t *testing.T) {
	far := []KubernetesMachine{kubeControlPlane(kubeCP1, "1.35.4", kubeNew), kubeWorker("w1", "1.35.4", kubeNew)}

	if got := HopOf(far, Releases{"1.36": "1.36.5"}); got.Version != "1.36.5" {
		t.Fatalf("got %+v", got)
	}

	if got := HopOf(far, nil); got.Version != kubeHop {
		t.Fatalf("without knowing, the first release of the minor: got %+v", got)
	}

	begun := []KubernetesMachine{
		kubeControlPlane(kubeCP1, "1.35.4", kubeNew).running(ComponentAPIServer, "1.36.3"), kubeWorker("w1", "1.35.4", kubeNew),
	}
	if got := HopOf(begun, Releases{"1.36": "1.36.5"}); got.Version != "1.36.3" {
		t.Fatalf("a hop that was begun is finished as it was begun: got %+v", got)
	}

	// The version asked for is the last hop, whatever is known.
	last := []KubernetesMachine{kubeControlPlane(kubeCP1, kubeOld, kubeNewFirst)}
	if got := HopOf(last, Releases{"1.37": kubeNew}); got.Version != kubeNewFirst {
		t.Fatalf("got %+v", got)
	}

	plan := PlanKubernetes(far, KubernetesSettings{Releases: Releases{"1.36": "1.36.5"}}, healthyPass())
	if plan.Images["kubelet/w1"] != "ghcr.io/siderolabs/kubelet:v1.36.5" {
		t.Fatalf("images: %v", plan.Images)
	}
}
