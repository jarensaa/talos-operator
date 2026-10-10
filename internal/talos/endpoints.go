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
	"errors"
	"net/netip"

	"github.com/jarensaa/talos-operator/internal/talos/creds"
)

// ErrNoEndpoints is returned when selection leaves no usable endpoint.
var ErrNoEndpoints = errors.New("no usable Talos API endpoint")

// SelectOptions shape endpoint selection.
type SelectOptions struct {
	// Exclude is a node that must never be used as the endpoint, e.g. the node being drained
	// or rebooted. The target is still reached through another endpoint via client.WithNode.
	Exclude netip.Addr
	// Healthy marks endpoints known to be healthy; they are tried first.
	Healthy map[netip.Addr]bool
}

// SelectEndpoints orders candidate control-plane endpoints for a client: healthy ones first,
// original order otherwise preserved, and the excluded node removed.
func SelectEndpoints(candidates []creds.Endpoint, opts SelectOptions) ([]creds.Endpoint, error) {
	healthy := make([]creds.Endpoint, 0, len(candidates))
	rest := make([]creds.Endpoint, 0, len(candidates))

	for _, ep := range candidates {
		if opts.Exclude.IsValid() && ep.Addr == opts.Exclude {
			continue
		}

		if opts.Healthy[ep.Addr] {
			healthy = append(healthy, ep)
		} else {
			rest = append(rest, ep)
		}
	}

	selected := append(healthy, rest...)
	if len(selected) == 0 {
		return nil, ErrNoEndpoints
	}

	return selected, nil
}
