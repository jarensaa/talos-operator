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

// Command import-image loads an image tarball into every node of the e2e cluster, so that the
// operator image built from this checkout can run there without a registry (docs/dev/e2e.md).
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: %s <image tarball>, with the environment of .e2e/env", os.Args[0])
	}

	talosconfig := os.Getenv("E2E_TALOSCONFIG")
	nodes := strings.Fields(os.Getenv("E2E_CONTROLPLANES") + " " + os.Getenv("E2E_WORKERS"))

	if talosconfig == "" || len(nodes) == 0 {
		return fmt.Errorf("E2E_TALOSCONFIG, E2E_CONTROLPLANES and E2E_WORKERS must be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for _, node := range nodes {
		tarball, err := os.Open(os.Args[1])
		if err != nil {
			return err
		}

		name, err := talostest.ImportImage(ctx, talosconfig, node, tarball)

		_ = tarball.Close()

		if err != nil {
			return err
		}

		fmt.Printf("%s: imported %s\n", node, name)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
