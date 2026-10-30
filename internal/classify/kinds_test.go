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

package classify

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/jarensaa/talos-operator/internal/render"
)

// machineryModule is the module whose document kinds the classifier must know.
const machineryModule = "github.com/siderolabs/talos/pkg/machinery"

// Unknown is for a kind Talos adds later, not for one it has: every kind the machinery the
// operator is built with registers has a rule. A machinery release that adds one fails this test,
// which names it.
func TestEveryDocumentKindOfMachineryHasAClass(t *testing.T) {
	kinds := registeredKinds(t)

	// The legacy document is classified by path (pathRules).
	kinds = slices.DeleteFunc(kinds, func(kind string) bool { return kind == "v1alpha1" })

	if len(kinds) < 50 {
		t.Fatalf("only %d document kinds found in %s: the search for them is broken", len(kinds), machineryModule)
	}

	for _, kind := range kinds {
		if _, ok := kindClasses[kind]; !ok {
			t.Errorf("machinery registers the document kind %s, which has no class: give it one in kindClasses", kind)
		}
	}
}

// A kind Talos adds later is Unknown until it has a rule, and so needs approval.
func TestAKindWithoutARuleIsUnknown(t *testing.T) {
	if got := kindClass(render.KeyOf("v1alpha1", "SomethingNewConfig", "")); got != Unknown {
		t.Fatalf("a kind without a rule: got %s, want %s", got, Unknown)
	}
}

// Configs of an older contract hold the kubelet's settings and the node's taints in the legacy
// document, where a 1.14 one may not set them: what they restart is read there too.
func TestTheLegacyDocumentRestartsTheKubelet(t *testing.T) {
	for doc, want := range map[string]string{
		"machine:\n  kubelet:\n    extraArgs:\n      max-pods: \"200\"\n":                    "v1alpha1: machine.kubelet.extraArgs.max-pods",
		"machine:\n  nodeTaints:\n    example.com/gpu: x:NoSchedule\n":                       "v1alpha1: machine.nodeTaints.example.com/gpu",
		"machine:\n  baseRuntimeSpecOverrides:\n    process:\n      noNewPrivileges: true\n": "",
	} {
		got, err := rootRestarts([]byte("machine: {}\n"), []byte(doc))
		if err != nil {
			t.Fatal(err)
		}

		if want != "" && got[restartsKubelet] != want {
			t.Errorf("%q: kubelet restarted by %q, want %q", doc, got[restartsKubelet], want)
		}

		if want == "" && got[restartsCRI] != "v1alpha1: machine.baseRuntimeSpecOverrides.process.noNewPrivileges" {
			t.Errorf("%q: the runtime must restart for its base spec, got %v", doc, got)
		}
	}
}

// registeredKinds reads, from the source of the machinery module the operator is built with,
// every kind its config document types register (registry.Register in config/types). The
// registry itself is internal to machinery, so its source is what can be asked.
func registeredKinds(t *testing.T) []string {
	t.Helper()

	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", machineryModule).Output()
	if err != nil {
		t.Fatalf("find the source of %s: %v", machineryModule, err)
	}

	root := filepath.Join(strings.TrimSpace(string(out)), "config", "types")

	// The files of a directory are one package: its constants are looked up among them.
	packages := map[string][]*ast.File{}
	fset := token.NewFileSet()

	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}

		packages[filepath.Dir(path)] = append(packages[filepath.Dir(path)], file)

		return nil
	})
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}

	kinds := make([]string, 0, len(packages))

	for dir, files := range packages {
		kinds = append(kinds, packageKinds(t, dir, files)...)
	}

	slices.Sort(kinds)

	return slices.Compact(kinds)
}

// packageKinds returns the kinds the files of one package register: the first argument of each
// registry.Register call, a string or a constant of the package.
func packageKinds(t *testing.T, dir string, files []*ast.File) []string {
	t.Helper()

	constants := map[string]string{}

	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}

			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, name := range vs.Names {
					if i < len(vs.Values) {
						if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
							constants[name.Name], _ = strconv.Unquote(lit.Value)
						}
					}
				}
			}
		}
	}

	var kinds []string

	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}

			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Register" {
				return true
			}

			if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "registry" {
				return true
			}

			switch arg := call.Args[0].(type) {
			case *ast.BasicLit:
				kind, _ := strconv.Unquote(arg.Value)
				kinds = append(kinds, kind)
			case *ast.Ident:
				kind, ok := constants[arg.Name]
				if !ok {
					t.Errorf("%s registers a kind by %s, which is not a string constant of the package", dir, arg.Name)
				}

				kinds = append(kinds, kind)
			default:
				t.Errorf("%s registers a kind this test cannot read", dir)
			}

			return true
		})
	}

	return kinds
}
