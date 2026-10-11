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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/validate"
)

// validateTimeout bounds one run of the check, questions to upstream included.
const validateTimeout = 3 * time.Minute

const validateUsage = `Usage: manager validate [flags] <file or directory>...

Checks the manifests of a cluster before they reach it: TalosCluster, TalosMachinePool,
TalosMachine and TalosMachineConfig documents in the files given, and in every .yaml and
.yml file under the directories given. Other documents are ignored.

It checks that the Talos and Kubernetes versions go together and are ones this operator
upgrades to, that the installer image and the Kubernetes release exist, that maintenance
windows and gates can be understood, and that every machine's config renders. It needs no
cluster and no secret.

Exit status: 0 if nothing is wrong, 1 if something is, 2 if the files could not be read.

Flags:
`

// paths is a flag that may be given more than once.
type paths []string

func (p *paths) String() string { return strings.Join(*p, ",") }

func (p *paths) Set(value string) error {
	*p = append(*p, value)

	return nil
}

// validateCommand runs `manager validate` and returns its exit status.
func validateCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	var (
		base     paths
		offline  bool
		releases string
	)

	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Var(&base, "base", "The same manifests as they were before the change, a file or a directory; may be repeated. "+
		"With it, the step from the one to the other is checked: no downgrade, and no version that cannot be reached.")
	flags.BoolVar(&offline, "offline", false, "Ask nothing upstream: whether the installer and the Kubernetes release exist is not checked.")
	flags.StringVar(&releases, "kubernetes-releases", catalog.DefaultKubernetesReleases, "Where Kubernetes publishes its releases.")
	flags.Usage = func() {
		fmt.Fprint(stderr, validateUsage) //nolint:errcheck // usage, to a terminal
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}

		return 2
	}

	if flags.NArg() == 0 {
		flags.Usage()

		return 2
	}

	opts := validate.Options{Paths: flags.Args(), Base: base}
	if !offline {
		opts.Source = &catalog.HTTP{KubernetesReleases: releases}
	}

	ctx, cancel := context.WithTimeout(ctx, validateTimeout)
	defer cancel()

	findings, err := validate.Run(ctx, opts)
	if err != nil {
		fmt.Fprintln(stderr, "validate:", err) //nolint:errcheck // to a terminal

		return 2
	}

	// On GitHub Actions a problem is also an annotation on the run.
	annotate := os.Getenv("GITHUB_ACTIONS") == "true"
	problems := 0

	for _, f := range findings {
		if f.Skipped {
			fmt.Fprintln(stdout, "not checked:", f) //nolint:errcheck // to a terminal

			continue
		}

		problems++

		fmt.Fprintln(stdout, "problem:", f) //nolint:errcheck // to a terminal

		if annotate {
			fmt.Fprintf(stdout, "::error title=talos-operator validate::%s\n", strings.ReplaceAll(f.String(), "\n", " ")) //nolint:errcheck // to a terminal
		}
	}

	if problems > 0 {
		fmt.Fprintf(stdout, "%d problem(s)\n", problems) //nolint:errcheck // to a terminal

		return 1
	}

	fmt.Fprintln(stdout, "nothing wrong") //nolint:errcheck // to a terminal

	return 0
}
