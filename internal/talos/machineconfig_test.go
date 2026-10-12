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
	"errors"
	"strings"
	"testing"

	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	configres "github.com/siderolabs/talos/pkg/machinery/resources/config"

	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

func TestParseMachineConfigAcceptsGeneratedMultiDocConfig(t *testing.T) {
	cfg, err := ParseMachineConfig(talostest.Config(t, talostest.Input(t)))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Machine().Type() != machine.TypeControlPlane {
		t.Fatalf("machine type: %s", cfg.Machine().Type())
	}
}

func TestParseMachineConfigFailsClosedOnUnknownDocumentKind(t *testing.T) {
	raw := append(talostest.Config(t, talostest.Input(t)), []byte("---\napiVersion: v1alpha1\nkind: FutureTalosConfig\nenabled: true\n")...)

	_, err := ParseMachineConfig(raw)
	if !errors.Is(err, ErrUnsupportedConfig) {
		t.Fatalf("expected ErrUnsupportedConfig, got %v", err)
	}

	if !strings.Contains(err.Error(), "v1alpha1/FutureTalosConfig") {
		t.Fatalf("expected the unknown kind to be named: %v", err)
	}
}

func TestParseMachineConfigErrorsNeverQuoteContent(t *testing.T) {
	raw := talostest.Config(t, talostest.Input(t))

	// Corrupt the YAML right after the cluster token so a parser error would quote nearby secrets.
	corrupt := strings.Replace(string(raw), "token: ", "token: [unclosed ", 1)

	_, err := ParseMachineConfig([]byte(corrupt))
	if !errors.Is(err, ErrUnsupportedConfig) {
		t.Fatalf("expected ErrUnsupportedConfig, got %v", err)
	}

	if err.Error() != ErrUnsupportedConfig.Error()+": parse failed" {
		t.Fatalf("parse errors must use the fixed message, got: %v", err)
	}
}

func TestHashConfigIsStableAndIgnoresSurroundingWhitespace(t *testing.T) {
	raw := talostest.Config(t, talostest.Input(t))

	if HashConfig(raw) != HashConfig(append([]byte("\n"), append(raw, '\n')...)) {
		t.Fatal("hash must ignore surrounding whitespace")
	}

	if !strings.HasPrefix(HashConfig(raw), "sha256:") {
		t.Fatal("hash must be prefixed with its algorithm")
	}
}

func TestPendingConfigChange(t *testing.T) {
	active := []byte("version: v1alpha1\n")

	for name, tc := range map[string]struct {
		persistent []byte
		want       bool
	}{
		"booted from disk, nothing applied": {nil, false},
		"applied without reboot":            {active, false},
		"staged for next reboot":            {[]byte("version: v1alpha1\nstaged: true\n"), true},
	} {
		if got := (MachineConfigs{Active: active, Persistent: tc.persistent}).PendingConfigChange(); got != tc.want {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}

// TestMachineConfigBodyFromWireResource encodes a MachineConfig resource the way Talos sends it
// and checks that the extracted bytes are the original multi-document config. Parsing the YAML
// spec instead (a string scalar) is what broke e2e.
func TestMachineConfigBodyFromWireResource(t *testing.T) {
	raw := talostest.Config(t, talostest.Input(t))

	cfg, err := ParseMachineConfig(raw)
	if err != nil {
		t.Fatal(err)
	}

	onWire, err := protobuf.FromResource(configres.NewMachineConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}

	body, err := machineConfigBody(onWire)
	if err != nil {
		t.Fatal(err)
	}

	want, err := cfg.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(body, want) {
		t.Fatal("extracted body differs from the config bytes")
	}

	if _, err := ParseMachineConfig(body); err != nil {
		t.Fatalf("extracted body does not parse: %v", err)
	}
}
