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

package gates_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jarensaa/talos-operator/internal/gates"
)

const (
	statusField = "status"
	notBoolean  = "not a boolean"
	noCompile   = "does not compile"
)

func cephCluster(health string) map[string]any {
	return map[string]any{
		"apiVersion": "ceph.rook.io/v1",
		"kind":       "CephCluster",
		"metadata":   map[string]any{"name": "storage", "namespace": "storage", "generation": int64(3)},
		statusField: map[string]any{
			"ceph":               map[string]any{"health": health},
			"observedGeneration": int64(3),
			"conditions": []any{
				map[string]any{"type": "Ready", statusField: "True"},
				map[string]any{"type": "Progressing", statusField: "False"},
			},
		},
	}
}

func TestEvaluate(t *testing.T) {
	const healthy = `object.status.ceph.health == "HEALTH_OK"`

	tests := []struct {
		name       string
		expression string
		object     map[string]any
		want       bool
		// problem is part of the error; empty when the expression evaluates.
		problem string
	}{
		{"true", healthy, cephCluster("HEALTH_OK"), true, ""},
		{"false", healthy, cephCluster("HEALTH_WARN"), false, ""},
		{"a list of conditions", `object.status.conditions.exists(c, c.type == "Ready" && c.status == "True")`,
			cephCluster("HEALTH_OK"), true, ""},
		{"numbers", `object.status.observedGeneration == object.metadata.generation`, cephCluster("HEALTH_OK"), true, ""},
		{"a field that may be absent, asked for", `has(object.status.phase) && object.status.phase == "Ready"`,
			cephCluster("HEALTH_OK"), false, ""},
		{"a boolean field", `object.spec.ready`, map[string]any{"spec": map[string]any{"ready": true}}, true, ""},
		// Everything below is not a plain true or false, and so an error: the gate stays closed.
		{"a field that is not there", healthy, map[string]any{statusField: map[string]any{}}, false, "no such key"},
		{"an object without a status", healthy, map[string]any{"metadata": map[string]any{}}, false, "no such key"},
		{"no object", healthy, nil, false, "no object"},
		{notBoolean, `object.status.ceph.health`, cephCluster("HEALTH_OK"), false, notBoolean},
		{"never a boolean", `"always"`, cephCluster("HEALTH_OK"), false, notBoolean},
		{noCompile, `object.status.ceph.health ==`, cephCluster("HEALTH_OK"), false, noCompile},
		{"another variable", `cluster.status.healthy`, cephCluster("HEALTH_OK"), false, noCompile},
		{"compares what cannot be compared", `object.status.ceph.health > 3`, cephCluster("HEALTH_OK"), false, "could not be evaluated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := gates.Evaluate(context.Background(), tt.expression, tt.object)
			if tt.problem == "" && err != nil {
				t.Fatal(err)
			}

			if tt.problem != "" && (err == nil || !strings.Contains(err.Error(), tt.problem)) {
				t.Fatalf("got %v, want an error about %q", err, tt.problem)
			}

			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// An expression is a user's input evaluated inside a reconcile: it gets a budget, not the benefit
// of the doubt.
func TestEvaluateIsBounded(t *testing.T) {
	items := make([]any, 400)
	for i := range items {
		items[i] = int64(i)
	}

	object := map[string]any{"items": items}

	// 400^3 comparisons.
	const cubic = `object.items.all(a, object.items.all(b, object.items.all(c, a + b + c >= 0)))`

	if _, err := gates.Evaluate(context.Background(), cubic, object); err == nil || !strings.Contains(err.Error(), "cost") {
		t.Fatalf("got %v, want the cost limit", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if got, err := gates.Evaluate(ctx, `object.items.all(a, a >= 0)`, object); err == nil || got {
		t.Fatalf("a cancelled evaluation is no answer: got %v, %v", got, err)
	}
}

func TestCheck(t *testing.T) {
	if err := gates.Check(`object.status.ceph.health == "HEALTH_OK"`); err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{``, `object.`, `1 + 1`, `"text"`, `unknown == 1`} {
		if err := gates.Check(bad); err == nil {
			t.Errorf("%q must be refused", bad)
		}
	}

	// The same expression, asked again and again, is compiled once and still answers.
	for range 1000 {
		if ok, err := gates.Evaluate(context.Background(), `object.ok`, map[string]any{"ok": true}); err != nil || !ok {
			t.Fatalf("got %v, %v", ok, err)
		}
	}
}
