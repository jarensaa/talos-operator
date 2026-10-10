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

package talos

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"k8s.io/apimachinery/pkg/util/version"
)

// TestMaxSupportedMinorMatchesMachinery fails when the machinery requirement in go.mod moves to
// another minor without MaxSupportedTalosMinor following it. (Test binaries carry no dependency
// build info, so go.mod is read directly.)
func TestMaxSupportedMinorMatchesMachinery(t *testing.T) {
	gomod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}

	m := regexp.MustCompile(`(?m)^\s*github\.com/siderolabs/talos/pkg/machinery (v\S+)`).FindSubmatch(gomod)
	if m == nil {
		t.Fatal("machinery requirement not found in go.mod")
	}

	v, err := version.ParseSemantic(string(m[1]))
	if err != nil {
		t.Fatal(err)
	}

	if int(v.Minor()) != MaxSupportedTalosMinor {
		t.Fatalf("go.mod requires machinery %s but MaxSupportedTalosMinor is %d; update it with the dependency", m[1], MaxSupportedTalosMinor)
	}
}

func TestSupportsTalos(t *testing.T) {
	for tag, want := range map[string]bool{
		apiVersion: true, "v1.14.0": true, "v1.13.5": true,
		"v1.12.4": false, "v1.15.0": false, "v2.0.0": false, garbage: false,
	} {
		if got, why := SupportsTalos(tag); got != want {
			t.Errorf("%s: got %v (%s), want %v", tag, got, why, want)
		}
	}
}
