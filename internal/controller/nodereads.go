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
	"net/netip"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/jarensaa/talos-operator/internal/talos"
)

const (
	// nodeReadTimeout bounds one read of a node while observing. A healthy node answers in
	// milliseconds; the Talos client's own default of 30s is for calls that do work.
	nodeReadTimeout = 10 * time.Second
	// DefaultObserveBudget bounds the reading of all nodes in one pass over a cluster. Nodes not
	// reached within it count as not read, which every gate treats as unknown.
	DefaultObserveBudget = time.Minute
)

// nodeReads is the reads of one node during one pass. It exists so that a read which fails is
// never silent, and so that a node which does not answer is not asked again: every further read
// would only wait for its own timeout, which matters most while that node's config is being
// tried (docs/scope.md §8.5.1).
type nodeReads struct {
	ctx  context.Context
	node netip.Addr
	// failed names what was not read, in the order it was asked for.
	failed []string
	// firstErr is the first failure; the ones after it are usually its consequences.
	firstErr error
	// dark is set once the node failed to answer at all.
	dark bool
}

func newNodeReads(ctx context.Context, node netip.Addr) *nodeReads {
	return &nodeReads{ctx: ctx, node: node}
}

// read runs one read and reports whether it succeeded. A read the caller wants to treat as
// answered, such as a resource the node does not have, must return nil.
func (n *nodeReads) read(what string, call func(ctx context.Context) error) bool {
	if n.dark {
		n.failed = append(n.failed, what)

		return false
	}

	ctx, cancel := context.WithTimeout(n.ctx, nodeReadTimeout)
	defer cancel()

	err := call(ctx)
	if err == nil {
		return true
	}

	n.failed = append(n.failed, what)
	n.dark = talos.IsUnreachable(err)

	if n.firstErr == nil {
		n.firstErr = err
	}

	// Errors of the Talos API name the call and the node, never config content.
	logf.FromContext(n.ctx).Info("could not read from node", "node", n.node.String(), "what", what,
		"error", err.Error(), "skippingFurtherReads", n.dark)

	return false
}

// mapFailed leaves a trace of a watch mapping that could not look up what an event affects. The
// event is then lost to those objects until something else reconciles them.
func mapFailed(ctx context.Context, err error, lookup string, obj client.Object) {
	logf.FromContext(ctx).Error(err, "watch mapping failed; the objects this event affects are not reconciled for it",
		"lookup", lookup, "namespace", obj.GetNamespace(), "name", obj.GetName())
}

// logTransitions logs every condition whose status changed in this pass.
func logTransitions(ctx context.Context, before, after []metav1.Condition) {
	log := logf.FromContext(ctx)

	for _, c := range after {
		if prev := meta.FindStatusCondition(before, c.Type); prev != nil && prev.Status == c.Status {
			continue
		}

		log.Info("condition changed", "condition", c.Type, "status", string(c.Status), "reason", c.Reason, "message", c.Message)
	}
}

// ok reports whether every read succeeded.
func (n *nodeReads) ok() bool { return len(n.failed) == 0 }

// missing lists what was not read, for a condition message. It does not change between passes
// while the same reads fail.
func (n *nodeReads) missing() string { return strings.Join(n.failed, ", ") }

// reason is the first failure, or "".
func (n *nodeReads) reason() string {
	if n.firstErr == nil {
		return ""
	}

	return n.firstErr.Error()
}
