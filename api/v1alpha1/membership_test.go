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

package v1alpha1_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// A machine is a member from the moment it joined until the moment it was taken out.
func TestIsMember(t *testing.T) {
	at := metav1.Now()

	for name, tt := range map[string]struct {
		joined, removed *metav1.Time
		want            bool
	}{
		"declared, not joined yet": {nil, nil, false},
		"joined":                   {&at, nil, true},
		"joined and taken out":     {&at, &at, false},
		// joinedAt is never unset, so this is not a state the operator writes. It is no member either.
		"taken out, never joined": {nil, &at, false},
	} {
		tm := &talosv1alpha1.TalosMachine{Status: talosv1alpha1.TalosMachineStatus{JoinedAt: tt.joined, RemovedAt: tt.removed}}

		if got := tm.IsMember(); got != tt.want {
			t.Errorf("%s: got %v, want %v", name, got, tt.want)
		}
	}
}
