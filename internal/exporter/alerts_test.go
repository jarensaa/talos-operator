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

package exporter

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// The chart's alerts are rules over the series this package exports, and a rule that names a
// series or a label wrongly is silent for good. So they are tested on what the collector really
// exports: each scenario here is a state of the resources and the alerts it has to raise, and
// from them this test writes a test of the rules for promtool, with the collector's own output
// as its input. `make alerts-test` runs it; `make alerts` writes it again.
const (
	rulesFile = "../../charts/talos-operator/files/alerts.yaml"
	testFile  = "../../hack/chart/alerts_test.yaml"

	// A scenario lasts two hours, sampled every minute: longer than any rule waits.
	samples  = 120
	evalTime = "2h"
)

var update = flag.Bool("update", false, "write hack/chart/alerts_test.yaml instead of comparing with it")

// fired is an alert a scenario has to raise.
type fired struct {
	alert, severity      string
	summary, description string
	// labels are the alert's own, in pairs, besides its severity and, unless bare, the cluster.
	labels []string
	bare   bool
}

type scenario struct {
	name string
	// change makes the scenario out of a healthy cluster.
	change func(f *fixture)
	// noOperator: nothing is scraped. cannotRead: the operator's reads fail. notObserving: the
	// last observation stays where it was, where otherwise the operator keeps observing.
	noOperator, cannotRead, notObserving bool
	// replicas is how many replicas are scraped, each exporting the same; one if not set.
	replicas int
	fires    []fired
}

func scenarios() []scenario {
	halted := func(f *fixture) {
		f.cluster.Status.Operation = &talosv1alpha1.Operation{
			ID: "op-1", Type: talosv1alpha1.OperationConfigRollout, StartedAt: epoch,
			Plan: []string{"node-d", "node-a", "node-b", "node-c"}, Completed: []string{"node-d"},
			Failure: &talosv1alpha1.OperationFailure{Machine: "node-a", Step: "Verifying", Reason: "RolledBack", At: &epoch},
		}
		f.is(talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonOperationFailed)
		f.is(talosv1alpha1.ConditionConfigInSync, metav1.ConditionFalse, talosv1alpha1.ReasonOperationFailed)
	}

	haltedAlert := fired{
		alert: "TalosOperationHalted", severity: "critical",
		labels:  []string{"type", "ConfigRollout", "machine", "node-a", "step", "Verifying", "reason", "RolledBack"},
		summary: "A ConfigRollout of my-cluster has failed and waits for a person.",
		description: "The ConfigRollout of my-cluster is halted at Verifying on node-a (RolledBack). It holds the cluster until " +
			"someone retries it, skips the machine or aborts it: status.operation.failure of the TalosCluster says what " +
			"happened and what to do.",
	}

	blocked := func(reason string) func(f *fixture) {
		return func(f *fixture) {
			f.is(talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, reason)
			f.is(talosv1alpha1.ConditionConfigInSync, metav1.ConditionFalse, reason)
		}
	}

	blockedAlert := func(reason string) fired {
		return fired{
			alert: "TalosRolloutBlocked", severity: "warning", labels: []string{"reason", reason},
			summary: "A change to my-cluster is not being rolled out.",
			description: "A change to my-cluster has been waiting for 30 minutes without being paused or outside its " +
				"maintenance window: " + reason + ". The RolloutBlocked condition of the TalosCluster says what it waits " +
				"for, a person's approval included.",
		}
	}

	notObserved := func(machine string) fired {
		return fired{
			alert: "TalosMachineNotObserved", severity: "warning", labels: []string{"machine", machine},
			summary: "talos-operator cannot read " + machine + ".",
			description: "The operator has not been able to read the node of " + machine + " in my-cluster for 10 minutes. " +
				"What is not read is not known: no other node is touched while it lasts.",
		}
	}

	unobserved := func(tm *talosv1alpha1.TalosMachine) {
		set(&tm.Status.Conditions, condition(talosv1alpha1.ConditionNodeObserved, metav1.ConditionFalse, talosv1alpha1.ReasonUnreachable))
		set(&tm.Status.Conditions, condition(talosv1alpha1.ConditionConfigMatchesLive, metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable))
		tm.Status.Talos.Ready = false
	}

	return []scenario{
		{name: "a cluster that is managed, in sync and healthy"},
		{
			name: "no operator is scraped", noOperator: true,
			fires: []fired{{
				alert: "TalosOperatorDown", severity: "critical", bare: true,
				summary: "No talos-operator is being scraped.",
				description: "No replica of talos-operator has exported metrics for 10 minutes. Nothing is rolled out or upgraded, " +
					"and none of the other alerts about the cluster can fire while this lasts.",
			}},
		},
		{
			name: "an operator that cannot read its resources", cannotRead: true,
			fires: []fired{{
				alert: "TalosOperatorCannotReadState", severity: "warning", bare: true,
				summary: "talos-operator cannot read its own resources.",
				description: "The operator fails to read its TalosClusters and TalosMachines when it is scraped, and exports " +
					"nothing about them. None of the other alerts about the cluster can fire while this lasts.",
			}},
		},
		{
			name: "an operator that has stopped observing", notObserving: true,
			fires: []fired{{
				alert: "TalosClusterNotObserved", severity: "warning",
				summary: "talos-operator has stopped observing my-cluster.",
				description: "The last observation of my-cluster that the operator finished is 2h 0m 0s old. Its status, and " +
					"every metric and alert that comes from it, is that old.",
			}},
		},
		{name: "a config rollout halted at a node that rolled back", change: halted, fires: []fired{haltedAlert}},
		{
			// Every replica exports the same state, and it is one alert all the same.
			name: "a halted rollout, seen through two replicas", change: halted, replicas: 2, fires: []fired{haltedAlert},
		},
		{
			name: "a Talos upgrade halted by a hook before its first node",
			change: func(f *fixture) {
				f.cluster.Status.Operation = &talosv1alpha1.Operation{
					ID: "op-2", Type: talosv1alpha1.OperationTalosUpgrade, Target: "v1.14.2", StartedAt: epoch,
					Plan: []string{"node-d", "node-a", "node-b", "node-c"},
					Failure: &talosv1alpha1.OperationFailure{
						Step: "preRollout/announce", Reason: "HookFailed", Hook: talosv1alpha1.HookPreRollout, At: &epoch,
					},
				}
				f.is(talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonOperationFailed)
			},
			fires: []fired{{
				alert: "TalosOperationHalted", severity: "critical",
				labels:  []string{"type", "TalosUpgrade", "step", "preRollout/announce", "reason", "HookFailed", "hook", "preRollout"},
				summary: "A TalosUpgrade of my-cluster has failed and waits for a person.",
				description: "The TalosUpgrade of my-cluster is halted at preRollout/announce in a preRollout hook (HookFailed). " +
					"It holds the cluster until someone retries it, skips the machine or aborts it: status.operation.failure of " +
					"the TalosCluster says what happened and what to do.",
			}},
		},
		{
			// Running is not a reason to wake anybody.
			name: "an upgrade that is running",
			change: func(f *fixture) {
				f.cluster.Status.Operation = &talosv1alpha1.Operation{
					ID: "op-3", Type: talosv1alpha1.OperationKubernetesUpgrade, Target: "1.37.1", StartedAt: epoch,
					Plan: []string{"kube-apiserver/node-a", "kubelet/node-a"}, Completed: []string{"kube-apiserver/node-a"},
				}
			},
		},
		// Waiting as it was told to is not a reason either.
		{name: "a rollout that waits for its maintenance window", change: blocked(talosv1alpha1.ReasonOutsideWindow)},
		{name: "a rollout that is paused", change: blocked(talosv1alpha1.ReasonPaused)},
		{
			name: "a rollout held by a gate", change: blocked(talosv1alpha1.ReasonGateClosed),
			fires: []fired{blockedAlert(talosv1alpha1.ReasonGateClosed)},
		},
		{
			name: "a change that waits for approval",
			change: func(f *fixture) {
				blocked(talosv1alpha1.ReasonAwaitingApproval)(f)
				f.cluster.Status.Approval = &talosv1alpha1.PendingApproval{Hash: "abc", Machines: []string{"node-a"}}
			},
			fires: []fired{blockedAlert(talosv1alpha1.ReasonAwaitingApproval)},
		},
		{
			name: "drift that the policy leaves as it is",
			change: func(f *fixture) {
				f.is(talosv1alpha1.ConditionConfigInSync, metav1.ConditionFalse, talosv1alpha1.ReasonDrift)
				set(&f.machine("node-d").Status.Conditions,
					condition(talosv1alpha1.ConditionConfigMatchesLive, metav1.ConditionFalse, talosv1alpha1.ReasonMismatch))
			},
			fires: []fired{{
				alert: "TalosConfigDrift", severity: "warning",
				summary: "Nodes of my-cluster no longer run the config the operator applied.",
				description: "The config of at least one node of my-cluster was changed behind the operator's back, and the drift " +
					"policy leaves it as it is. status.config.diff of each TalosMachine shows the difference.",
			}},
		},
		{
			name: "a node that no TalosMachine describes",
			change: func(f *fixture) {
				f.is(talosv1alpha1.ConditionConfigInSync, metav1.ConditionFalse, talosv1alpha1.ReasonMismatch)
			},
			fires: []fired{{
				alert: "TalosNodeWithoutMachine", severity: "warning",
				summary: "my-cluster has a node that no TalosMachine describes.",
				description: "A member of my-cluster has no TalosMachine, so the operator does not manage its config. The " +
					"ConfigInSync condition of the TalosCluster names it.",
			}},
		},
		{
			name: "a spec that asks for versions there is no way to",
			change: func(f *fixture) {
				f.is(talosv1alpha1.ConditionVersionsReachable, metav1.ConditionFalse, talosv1alpha1.ReasonIncompatible)
			},
			fires: []fired{{
				alert: "TalosVersionsUnreachable", severity: "warning",
				summary: "my-cluster is asked for versions it cannot be taken to.",
				description: "There is no sequence of upgrades from the versions my-cluster runs to the ones its spec asks for. " +
					"The VersionsReachable condition of the TalosCluster says why; nothing is upgraded until the spec changes.",
			}},
		},
		{
			name: "a Talos API that cannot be reached",
			change: func(f *fixture) {
				f.is(talosv1alpha1.ConditionTalosAPIReachable, metav1.ConditionFalse, talosv1alpha1.ReasonUnreachable)
				f.is(talosv1alpha1.ConditionHealthy, metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable)

				for _, tm := range f.machines {
					unobserved(tm)
				}
			},
			fires: []fired{
				{
					alert: "TalosAPIUnreachable", severity: "critical", labels: []string{"reason", "Unreachable"},
					summary: "talos-operator cannot talk to the Talos API of my-cluster.",
					description: "The operator has had no working connection to the Talos API of my-cluster for 10 minutes: " +
						"Unreachable. It observes nothing and rolls out nothing until it has one.",
				},
				notObserved("node-a"), notObserved("node-b"), notObserved("node-c"), notObserved("node-d"),
			},
		},
		{
			name: "credentials that Talos does not issue",
			change: func(f *fixture) {
				f.is(talosv1alpha1.ConditionCredentialsValid, metav1.ConditionFalse, talosv1alpha1.ReasonAwaitingCredentials)
				f.is(talosv1alpha1.ConditionTalosAPIReachable, metav1.ConditionUnknown, talosv1alpha1.ReasonPreconditionUnmet)
			},
			fires: []fired{{
				alert: "TalosAPIUnreachable", severity: "critical", labels: []string{"reason", "AwaitingCredentials"},
				summary: "talos-operator cannot talk to the Talos API of my-cluster.",
				description: "The operator has had no working connection to the Talos API of my-cluster for 10 minutes: " +
					"AwaitingCredentials. It observes nothing and rolls out nothing until it has one.",
			}},
		},
		{
			name: "a health check of Talos that fails",
			change: func(f *fixture) {
				f.is(talosv1alpha1.ConditionHealthy, metav1.ConditionFalse, talosv1alpha1.ReasonFailed)
			},
			fires: []fired{{
				alert: "TalosClusterUnhealthy", severity: "warning",
				summary: "Talos's health check of my-cluster fails.",
				description: "Talos's own cluster health check of my-cluster has been failing for 15 minutes. The Healthy " +
					"condition of the TalosCluster says which check; no node is touched while it lasts.",
			}},
		},
		{
			// It is not also reported as not ready: what cannot be read is not known to be anything.
			name:   "a node that cannot be read",
			change: func(f *fixture) { unobserved(f.machine("node-d")) },
			fires:  []fired{notObserved("node-d")},
		},
		{
			name:   "a node that answers and is not ready",
			change: func(f *fixture) { f.machine("node-b").Status.Talos.Ready = false },
			fires: []fired{{
				alert: "TalosMachineNotReady", severity: "warning", labels: []string{"machine", "node-b"},
				summary:     "node-b is not ready.",
				description: "The node of node-b in my-cluster answers and has reported itself not ready for 15 minutes.",
			}},
		},
		{
			// A machine that is declared and has not joined is not a node that cannot be read, and
			// not a control plane that is missing from etcd. Neither is one that has left.
			name: "a control plane that waits to be added, and a worker that has left",
			change: func(f *fixture) {
				notAMember := func(tm *talosv1alpha1.TalosMachine) {
					set(&tm.Status.Conditions, condition(talosv1alpha1.ConditionNodeObserved, metav1.ConditionFalse, talosv1alpha1.ReasonNotAMember))
					set(&tm.Status.Conditions,
						condition(talosv1alpha1.ConditionConfigMatchesLive, metav1.ConditionUnknown, talosv1alpha1.ReasonNotAMember))
					set(&tm.Status.Conditions, condition(talosv1alpha1.ConditionMatched, metav1.ConditionFalse, talosv1alpha1.ReasonNotAMember))
					tm.Status.Talos.Ready = false
				}

				waiting := f.machine("node-c")
				notAMember(waiting)
				waiting.Status.JoinedAt, waiting.Status.Etcd = nil, nil
				set(&waiting.Status.Conditions, condition(talosv1alpha1.ConditionProvisionable, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded))

				left := f.machine("node-d")
				notAMember(left)
				left.Status.RemovedAt = &epoch
				left.Spec.DesiredState = talosv1alpha1.DesiredStateRemoved

				f.cluster.Status.Observed.EtcdMembers = f.cluster.Status.Observed.EtcdMembers[:2]
				f.is(talosv1alpha1.ConditionMembershipCurrent, metav1.ConditionFalse, talosv1alpha1.ReasonAwaitingMachine)
			},
		},
		{
			name: "a control plane that stays a learner",
			change: func(f *fixture) {
				f.machine("node-c").Status.Etcd.IsLearner = true
				f.cluster.Status.Observed.EtcdMembers[2].IsLearner = true
			},
			fires: []fired{{
				alert: "TalosControlPlaneNotEtcdVoter", severity: "warning", labels: []string{"machine", "node-c"},
				summary: "node-c is a control plane and not a voting member of etcd.",
				description: "The control plane node-c of my-cluster has not been a voting member of etcd for 15 minutes: it is no " +
					"member, or still a learner. The cluster tolerates one control plane fewer than it should.",
			}},
		},
	}
}

// What promtool reads.
type (
	ruleTests struct {
		RuleFiles          []string    `json:"rule_files"`
		EvaluationInterval string      `json:"evaluation_interval"`
		Tests              []testGroup `json:"tests"`
	}
	testGroup struct {
		Name          string        `json:"name"`
		Interval      string        `json:"interval"`
		InputSeries   []inputSeries `json:"input_series"`
		AlertRuleTest []alertTest   `json:"alert_rule_test"`
	}
	inputSeries struct {
		Series string `json:"series"`
		Values string `json:"values"`
	}
	alertTest struct {
		EvalTime  string     `json:"eval_time"`
		Alertname string     `json:"alertname"`
		ExpAlerts []expAlert `json:"exp_alerts"`
	}
	expAlert struct {
		ExpLabels      map[string]string `json:"exp_labels"`
		ExpAnnotations map[string]string `json:"exp_annotations"`
	}
)

// alertNames lists the alerts of the chart, in the order of the file.
func alertNames(t *testing.T) []string {
	t.Helper()

	raw, err := os.ReadFile(rulesFile)
	if err != nil {
		t.Fatal(err)
	}

	var rules struct {
		Groups []struct {
			Rules []struct {
				Alert string `json:"alert"`
			} `json:"rules"`
		} `json:"groups"`
	}

	if err := yaml.Unmarshal(raw, &rules); err != nil {
		t.Fatal(err)
	}

	var names []string

	for _, group := range rules.Groups {
		for _, rule := range group.Rules {
			names = append(names, rule.Alert)
		}
	}

	return names
}

// series is what a Prometheus would have of the scenario after two hours.
func (s scenario) series(t *testing.T) []inputSeries {
	t.Helper()

	if s.noOperator {
		return []inputSeries{}
	}

	f := healthy()
	if s.change != nil {
		s.change(f)
	}

	var reader client.Reader = fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(f.objects()...).Build()
	if s.cannotRead {
		reader = failing{Reader: reader, kind: "*v1alpha1.TalosClusterList"}
	}

	replicas := max(s.replicas, 1)

	var out []inputSeries

	for _, sample := range scrape(t, New(reader)) {
		values := fmt.Sprintf("%gx%d", sample.value, samples)

		switch sample.name {
		case "talos_operator_info":
			// The build is the test binary's.
			sample.labels = map[string]string{"version": "v0.0.0", "revision": "000000000000"}
		case "talos_operator_cluster_observed_timestamp_seconds":
			if !s.notObserving {
				values = fmt.Sprintf("0+60x%d", samples)
			}
		case "talos_operator_state_read_errors_total":
			if s.cannotRead {
				values = fmt.Sprintf("0+1x%d", samples)
			}
		}

		for replica := range replicas {
			if replicas > 1 {
				sample.labels["pod"] = fmt.Sprintf("talos-operator-%d", replica)
			}

			out = append(out, inputSeries{Series: sample.String(), Values: values})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Series < out[j].Series })

	return out
}

func TestAlertScenarios(t *testing.T) {
	names := alertNames(t)
	if len(names) == 0 {
		t.Fatal("the chart has no alerts")
	}

	file := ruleTests{RuleFiles: []string{"../../charts/talos-operator/files/alerts.yaml"}, EvaluationInterval: "1m"}
	raised := map[string]bool{}

	for _, s := range scenarios() {
		group := testGroup{Name: s.name, Interval: "1m", InputSeries: s.series(t)}

		// Every alert is asked about in every scenario: the ones that must stay quiet as well.
		for _, name := range names {
			test := alertTest{EvalTime: evalTime, Alertname: name, ExpAlerts: []expAlert{}}

			for _, f := range s.fires {
				if f.alert != name {
					continue
				}

				raised[name] = true

				labels := map[string]string{"severity": f.severity}
				if !f.bare {
					labels["namespace"], labels["taloscluster"] = namespace, cluster
				}

				for i := 0; i+1 < len(f.labels); i += 2 {
					labels[f.labels[i]] = f.labels[i+1]
				}

				test.ExpAlerts = append(test.ExpAlerts, expAlert{
					ExpLabels:      labels,
					ExpAnnotations: map[string]string{"summary": f.summary, "description": f.description},
				})
			}

			group.AlertRuleTest = append(group.AlertRuleTest, test)
		}

		for _, f := range s.fires {
			if !strings.Contains(strings.Join(names, " "), f.alert) {
				t.Errorf("%s expects %s, which the chart does not have", s.name, f.alert)
			}
		}

		file.Tests = append(file.Tests, group)
	}

	// An alert that no scenario raises is one nobody has seen fire.
	for _, name := range names {
		if !raised[name] {
			t.Errorf("no scenario raises %s", name)
		}
	}

	body, err := yaml.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}

	want := "# Written by internal/exporter (TestAlertScenarios); `make alerts` writes it again. Do not edit.\n" +
		"# A test of the chart's alert rules for promtool, on what the exporter exports: `make alerts-test`.\n" + string(body)

	if *update {
		if err := os.WriteFile(testFile, []byte(want), 0o644); err != nil { //nolint:gosec // a file of the repository
			t.Fatal(err)
		}

		return
	}

	got, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("%v: run `make alerts`", err)
	}

	if string(got) != want {
		t.Errorf("%s is not what the scenarios and the exporter produce: run `make alerts` and look at the diff", testFile)
	}
}
