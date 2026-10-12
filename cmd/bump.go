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
	"time"

	"github.com/jarensaa/talos-operator/internal/bump"
	"github.com/jarensaa/talos-operator/internal/catalog"
)

// bumpTimeout bounds one run. It asks what the check asks, and for a release nobody asked about
// before: Image Factory builds an installer when it is first asked for.
const bumpTimeout = 10 * time.Minute

const bumpUsage = `Usage: manager bump [flags] <file or directory>...

Writes the next Talos or Kubernetes version into the TalosCluster it finds in the files given,
and in every .yaml and .yml file under the directories given. Run on a schedule in the
repository a cluster is applied from, it upgrades the cluster without anyone picking versions:
the operator rolls out what is committed, inside the cluster's maintenance windows.

One run takes one step: the newest release of the minor the file names, and once it is on that,
the newest release of the next minor. Talos goes before Kubernetes wherever it can; a Talos
release that does not run the Kubernetes version the file names waits for Kubernetes, and the
other way round. Only a release this operator upgrades to is taken, and only one that passes
what "manager validate" checks. Nothing but the version is changed in the file.

It asks the Image Factory the TalosCluster names, the Kubernetes release site and the registries
of the images which releases exist. It needs no cluster and no secret.

What was written is printed on standard output, one line for every cluster that was bumped, and
everything else on standard error.

Exit status: 0 if a version was written or there is none newer to take, 1 if none was written
because a newer release could not be taken for a reason that is not a rule of Talos or
Kubernetes, or because the manifests have a problem as they are, 2 if the files could not be
read or written.

Flags:
`

// bumpCommand runs `manager bump` and returns its exit status.
func bumpCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	opts := bump.Options{}

	var releases string

	flags := flag.NewFlagSet("bump", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.BoolVar(&opts.Talos, "talos", false, "Bump Talos only.")
	flags.BoolVar(&opts.Kubernetes, "kubernetes", false, "Bump Kubernetes only.")
	flags.BoolVar(&opts.PatchOnly, "patch", false, "Take patch releases only: stay on the minors the file names.")
	flags.BoolVar(&opts.DryRun, "dry-run", false, "Print what would be written, and write nothing.")
	flags.StringVar(&releases, "kubernetes-releases", catalog.DefaultKubernetesReleases, "Where Kubernetes publishes its releases.")
	flags.Usage = func() {
		fmt.Fprint(stderr, bumpUsage) //nolint:errcheck // usage, to a terminal
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

	opts.Paths = flags.Args()
	// The check runs before a bump and again on it: what was answered once is not asked twice.
	opts.Source = &catalog.Cached{Source: &catalog.HTTP{KubernetesReleases: releases}}

	return bumpRun(ctx, opts, stdout, stderr)
}

// bumpRun bumps, says what came of it, and returns the exit status.
func bumpRun(ctx context.Context, opts bump.Options, stdout, stderr io.Writer) int {
	ctx, cancel := context.WithTimeout(ctx, bumpTimeout)
	defer cancel()

	result, err := bump.Run(ctx, opts)
	if err != nil {
		fmt.Fprintln(stderr, "bump:", err) //nolint:errcheck // to a terminal

		return 2
	}

	for _, f := range result.NotChecked {
		fmt.Fprintln(stderr, "not checked:", f) //nolint:errcheck // to a terminal
	}

	if len(result.Invalid) > 0 {
		for _, f := range result.Invalid {
			fmt.Fprintln(stderr, "problem:", f) //nolint:errcheck // to a terminal
		}

		fmt.Fprintln(stderr, "nothing was bumped: the manifests have a problem as they are") //nolint:errcheck // to a terminal

		return 1
	}

	// What was written is to be committed, whatever else there is to say: the run only fails when
	// it wrote nothing and should have.
	written, problems := false, false

	for _, outcome := range result.Outcomes {
		for _, held := range outcome.Held {
			fmt.Fprintf(stderr, "%s: %s\n", outcome.Object, held) //nolint:errcheck // to a terminal
		}

		for _, problem := range outcome.Problems {
			fmt.Fprintf(stderr, "problem: %s: %s\n", outcome.Object, problem) //nolint:errcheck // to a terminal
		}

		switch {
		case outcome.Step != nil:
			fmt.Fprintf(stdout, "%s: %s: %s\n", outcome.File, outcome.Object, outcome.Step) //nolint:errcheck // to a terminal

			written = true

			if opts.DryRun {
				fmt.Fprintf(stderr, "%s: a dry run, so it was not written\n", outcome.Object) //nolint:errcheck // to a terminal
			}
		case len(outcome.Problems) > 0:
			problems = true
		case len(outcome.Held) == 0:
			fmt.Fprintf(stderr, "%s: there is no newer release to take\n", outcome.Object) //nolint:errcheck // to a terminal
		}
	}

	if problems && !written {
		return 1
	}

	return 0
}
