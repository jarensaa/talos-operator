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

package fake

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/netip"
	"strconv"
	"strings"
	"testing"
)

// methodConstants reads the Method constants out of fake.go: their names and the method each
// names. Go keeps no list of a package's constants to ask at run time.
func methodConstants(t *testing.T) map[string]string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), "fake.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	constants := map[string]string{}

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}

		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}

			for i, name := range value.Names {
				if !strings.HasPrefix(name.Name, "Method") {
					continue
				}

				lit, ok := value.Values[i].(*ast.BasicLit)
				if !ok {
					t.Fatalf("%s is not a string written out", name.Name)
				}

				method, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}

				constants[name.Name] = method
			}
		}
	}

	if len(constants) == 0 {
		t.Fatal("found no Method constants in fake.go")
	}

	return constants
}

// AssertNoMutations is only as good as what it knows of each method. Every method the fake
// records is said to read or to change a node, and nothing is said of a method there is not.
func TestEveryMethodIsSaidToReadOrToChange(t *testing.T) {
	constants := methodConstants(t)

	named := map[string]bool{}

	for name, method := range constants {
		named[method] = true

		if _, said := methodReads[method]; !said {
			t.Errorf("%s (%q) has no entry in methodReads: say whether it reads or changes a node", name, method)
		}
	}

	for method := range methodReads {
		if !named[method] {
			t.Errorf("methodReads says something of %q, which no Method constant names", method)
		}
	}
}

// A method nobody said anything about is taken to change a node: an assertion that observe mode
// changed nothing must not pass because a line is missing.
func TestAMethodNobodyListedCountsAsAMutation(t *testing.T) {
	node := netip.MustParseAddr("10.0.0.11")

	for _, tt := range []struct {
		call    Call
		mutates bool
	}{
		{Call{Method: MethodVersion, Node: node}, false},
		{Call{Method: MethodMaintenanceProbe, Node: node}, false},
		{Call{Method: MethodApplyConfiguration, Node: node, DryRun: true}, false},
		{Call{Method: MethodApplyConfiguration, Node: node}, true},
		{Call{Method: MethodEtcdDefragment, Node: node}, true},
		{Call{Method: "SomethingAddedLater", Node: node}, true},
	} {
		s := NewScenario(Node{Addr: node})
		s.calls = append(s.calls, tt.call)

		failed := &failure{}
		s.AssertNoMutations(failed)

		if failed.failed != tt.mutates {
			t.Errorf("%+v: counted as a mutation: %t, want %t", tt.call, failed.failed, tt.mutates)
		}
	}
}

// failure is a TestingT that notes a failure instead of ending the test.
type failure struct{ failed bool }

func (f *failure) Helper() {}

func (f *failure) Fatalf(string, ...any) { f.failed = true }
