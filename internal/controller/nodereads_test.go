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

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jarensaa/talos-operator/internal/talos"
)

func TestNodeReadsStopAfterTheNodeFailedToAnswer(t *testing.T) {
	reads := newNodeReads(context.Background(), netip.MustParseAddr("10.5.0.3"))
	asked := 0

	answer := func(err error) func(context.Context) error {
		return func(context.Context) error {
			asked++

			return err
		}
	}

	if !reads.read("version", answer(nil)) || !reads.ok() || reads.reason() != "" {
		t.Fatal("a read that succeeds leaves nothing behind")
	}

	// The node answered: it just could not serve this. It is asked for the next thing.
	if reads.read("schematic", answer(fmt.Errorf("schematic: %w", status.Error(codes.Internal, "boom")))) {
		t.Fatal("a failed read must say so")
	}

	if reads.dark || !reads.read("boot ID", answer(nil)) {
		t.Fatal("an error that is an answer does not make the node dark")
	}

	// A caller that treats a missing resource as an answer returns nil; one that does not gets a
	// failed read, and the node is still there.
	if reads.read("install status", answer(fmt.Errorf("install status: %w", talos.ErrNotFound))) || reads.dark {
		t.Fatal("a missing resource is a failed read of a node that answers")
	}

	if reads.read("machine status", answer(fmt.Errorf("machine status: %w", status.Error(codes.Unavailable, "no route")))) || !reads.dark {
		t.Fatal("a node that does not answer is dark")
	}

	before := asked

	for _, what := range []string{"nodename", "machine config"} {
		if reads.read(what, answer(nil)) {
			t.Fatalf("%s: a dark node is not asked, and nothing is learned from it", what)
		}
	}

	if asked != before {
		t.Fatalf("a dark node was asked %d more time(s)", asked-before)
	}

	if got, want := reads.missing(), "schematic, install status, machine status, nodename, machine config"; got != want {
		t.Fatalf("missing: got %q, want %q", got, want)
	}

	if reads.ok() || reads.reason() != "schematic: rpc error: code = Internal desc = boom" {
		t.Fatalf("the reason is the first failure, the rest follow from it: %q", reads.reason())
	}
}

func TestNodeReadsBoundEveryRead(t *testing.T) {
	reads := newNodeReads(context.Background(), netip.MustParseAddr("10.5.0.3"))

	var left time.Duration

	reads.read("version", func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("a read must carry a deadline")
		}

		left = time.Until(deadline)

		return nil
	})

	if left <= 0 || left > nodeReadTimeout {
		t.Fatalf("expected a deadline within %s, got %s", nodeReadTimeout, left)
	}

	// A pass that has run out of time ends its reads at once, and the node counts as not read.
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	late := newNodeReads(expired, netip.MustParseAddr("10.5.0.3"))

	if late.read("version", func(ctx context.Context) error { return ctx.Err() }) || !late.dark {
		t.Fatal("a read past the pass's budget fails, and the node is not asked again")
	}

	if !errors.Is(late.firstErr, context.DeadlineExceeded) {
		t.Fatalf("unexpected error: %v", late.firstErr)
	}
}
