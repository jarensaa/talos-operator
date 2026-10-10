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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
)

// ErrUnsupportedConfig means this operator's machinery cannot parse a node's machine config,
// typically because the node runs a newer Talos with document kinds the operator does not know.
// The operator fails closed: it never acts on a config it cannot fully parse (docs/scope.md §11).
var ErrUnsupportedConfig = errors.New("machine config cannot be parsed by this operator version")

var unregisteredDocument = regexp.MustCompile(`"([A-Za-z0-9]+)" "([A-Za-z0-9]*)": not registered`)

// ParseMachineConfig parses raw machine config bytes. Parser errors can quote config content,
// which carries secrets, so they are never included, except for unknown document kinds, whose
// message names only the kind and version.
func ParseMachineConfig(raw []byte) (config.Provider, error) {
	cfg, err := configloader.NewFromBytes(raw)
	if err == nil {
		return cfg, nil
	}

	// The registry's ErrNotRegistered lives in an internal machinery package, so it can only be
	// recognised by its message: `"<kind>" "<version>": not registered`.
	if m := unregisteredDocument.FindStringSubmatch(err.Error()); m != nil {
		return nil, fmt.Errorf("%w: unknown document kind %s/%s", ErrUnsupportedConfig, m[2], m[1])
	}

	return nil, fmt.Errorf("%w: parse failed", ErrUnsupportedConfig)
}

// HashConfig returns a stable fingerprint of raw config bytes, safe to put in status and logs.
func HashConfig(raw []byte) string {
	sum := sha256.Sum256(bytes.TrimSpace(raw))

	return "sha256:" + hex.EncodeToString(sum[:])
}
