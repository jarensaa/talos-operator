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
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsRejected reports whether Talos, or the operator's own allowlist, refused a request for what
// it asks: a machine config that does not parse or validate (ApplyConfiguration answers
// InvalidArgument, v1alpha1_server.go:207-236 in Talos v1.14), or a call the client may not make.
// Sending the same request again cannot succeed.
//
// Everything else may pass: a node that is not reachable, a timeout, and FailedPrecondition,
// which is what Talos answers while another apply is in progress (v1alpha1_server.go:187).
func IsRejected(err error) bool {
	switch status.Code(err) { //nolint:exhaustive // every other code may be transient
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unimplemented:
		return true
	default:
		return false
	}
}

// IsUnreachable reports whether a call failed because the node did not answer: no route to it,
// or no answer in time. Asking the same node for something else right away would only wait for
// the same timeout again.
func IsUnreachable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	switch status.Code(err) { //nolint:exhaustive // every other code is an answer
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}
