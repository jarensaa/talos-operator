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
	"encoding/json"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A panel that asks for a series nobody exports, or groups by a label the series does not have,
// is empty for good and looks like a cluster with nothing to show. So every expression of the
// chart's dashboards is held against what the collector describes.

var (
	descName   = regexp.MustCompile(`fqName: "([^"]+)"`)
	descLabels = regexp.MustCompile(`variableLabels: \{([^}]*)\}`)
	seriesName = regexp.MustCompile(`talos_operator_[a-z0-9_]+`)
	selector   = regexp.MustCompile(`(talos_operator_[a-z0-9_]+)\{([^}]*)\}`)
	matcher    = regexp.MustCompile(`([a-z_]+)\s*(?:=~|!~|!=|=)`)
	grouping   = regexp.MustCompile(`by \(([^)]*)\)`)
)

// exported is every series the collector describes, with its labels.
func exported(t *testing.T) map[string][]string {
	t.Helper()

	descs := make(chan *prometheus.Desc, 256)
	New(fake.NewClientBuilder().WithScheme(scheme(t)).Build(), nil).Describe(descs)
	close(descs)

	out := map[string][]string{
		// Counted where the calls are refused, in internal/talos.
		"talos_operator_rejected_rpcs_total": {"method"},
	}

	for d := range descs {
		name := descName.FindStringSubmatch(d.String())
		if name == nil {
			t.Fatalf("a description without a name: %s", d)
		}

		var labels []string

		if m := descLabels.FindStringSubmatch(d.String()); m != nil && m[1] != "" {
			labels = strings.Split(m[1], ",")
		}

		out[name[1]] = labels
	}

	return out
}

func TestDashboardsAskOnlyForWhatIsExported(t *testing.T) {
	series := exported(t)
	if len(series["talos_operator_machine_info"]) == 0 {
		t.Fatalf("the collector's descriptions were not understood: %v", series)
	}

	for _, file := range []string{"dashboard.json", "dashboard-nodes.json"} {
		raw, err := os.ReadFile("../../charts/talos-operator/files/" + file)
		if err != nil {
			t.Fatal(err)
		}

		var dashboard struct {
			Panels []struct {
				Title   string `json:"title"`
				Type    string `json:"type"`
				Targets []struct {
					Expr string `json:"expr"`
				} `json:"targets"`
			} `json:"panels"`
		}

		if err := json.Unmarshal(raw, &dashboard); err != nil {
			t.Fatalf("%s: %v", file, err)
		}

		asked := 0

		for _, panel := range dashboard.Panels {
			if panel.Type != "row" && len(panel.Targets) == 0 {
				t.Errorf("%s: the panel %q asks for nothing", file, panel.Title)
			}

			for _, target := range panel.Targets {
				asked++

				// What a panel may group by is what the series it asks for carry between them.
				var known []string

				for _, name := range seriesName.FindAllString(target.Expr, -1) {
					labels, ok := series[name]
					if !ok {
						t.Errorf("%s: the panel %q asks for %s, which nothing exports", file, panel.Title, name)
					}

					known = append(known, labels...)
				}

				for _, m := range selector.FindAllStringSubmatch(target.Expr, -1) {
					for _, label := range matcher.FindAllStringSubmatch(m[2], -1) {
						if !slices.Contains(series[m[1]], label[1]) {
							t.Errorf("%s: the panel %q selects %s by %s, which it does not carry", file, panel.Title, m[1], label[1])
						}
					}
				}

				for _, by := range grouping.FindAllStringSubmatch(target.Expr, -1) {
					for label := range strings.SplitSeq(by[1], ",") {
						if label = strings.TrimSpace(label); label != "" && len(known) > 0 && !slices.Contains(known, label) {
							t.Errorf("%s: the panel %q groups by %s, which none of its series carries", file, panel.Title, label)
						}
					}
				}
			}
		}

		if asked == 0 {
			t.Errorf("%s has no panel that asks for anything", file)
		}
	}
}
