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
	"context"
	"encoding/binary"
	"slices"
	"strings"
	"testing"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

// signatureList is an EFI_SIGNATURE_LIST of one kind, every signature the same size, as firmware
// stores it.
func signatureList(kind [16]byte, signatures ...[]byte) []byte {
	size := signatureOwner + len(signatures[0])
	list := slices.Clone(kind[:])
	list = binary.LittleEndian.AppendUint32(list, uint32(signatureListHeader+len(signatures)*size))
	list = binary.LittleEndian.AppendUint32(list, 0)
	list = binary.LittleEndian.AppendUint32(list, uint32(size))

	for _, signature := range signatures {
		list = append(list, bytes.Repeat([]byte{0xee}, signatureOwner)...)
		list = append(list, signature...)
	}

	return list
}

// dbVariable is a db as Linux shows it: the variable's attributes, then its signature lists.
func dbVariable(lists ...[]byte) []byte {
	return append([]byte{0x27, 0, 0, 0}, bytes.Join(lists, nil)...)
}

func hash(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func TestParseSecureBootDB(t *testing.T) {
	signer := talostest.SigningCertificate(t, "Image signing", nil).Certificate
	vendor := talostest.SigningCertificate(t, "A firmware vendor's authority", nil).Certificate
	// EFI_CERT_X509_SHA256_GUID: a kind that says nothing about a Talos image.
	other := [16]byte{0x92, 0xa4, 0xd2, 0x3b, 0xc0, 0x96, 0x79, 0x40, 0xb4, 0x20, 0xfc, 0xf9, 0x8e, 0xf1, 0x03, 0xed}

	db, err := ParseSecureBootDB(dbVariable(
		signatureList(certX509, vendor.Raw),
		signatureList(certSHA256, hash(1), hash(2), hash(3)),
		signatureList(certX509, signer.Raw),
		signatureList(other, bytes.Repeat([]byte{7}, 48)),
	))
	if err != nil {
		t.Fatal(err)
	}

	if len(db.Certificates) != 2 || !bytes.Equal(db.Certificates[0], vendor.Raw) || !bytes.Equal(db.Certificates[1], signer.Raw) ||
		db.Hashes != 3 || db.Others != 1 {
		t.Fatalf("got %d certificates, %d hashes, %d others", len(db.Certificates), db.Hashes, db.Others)
	}

	// A db with nothing in it is an answer: firmware that trusts nothing.
	if db, err := ParseSecureBootDB(dbVariable()); err != nil || len(db.Certificates)+db.Hashes+db.Others != 0 {
		t.Fatalf("an empty db: got %+v, %v", db, err)
	}

	whole := dbVariable(signatureList(certX509, signer.Raw), signatureList(certSHA256, hash(1)))

	oversized := slices.Clone(whole)
	binary.LittleEndian.PutUint32(oversized[efivarAttributes+16:], uint32(len(whole)))

	uneven := dbVariable(signatureList(certSHA256, hash(1)))
	binary.LittleEndian.PutUint32(uneven[efivarAttributes+24:], signatureOwner+31)

	noData := dbVariable(signatureList(certSHA256, hash(1)))
	binary.LittleEndian.PutUint32(noData[efivarAttributes+24:], signatureOwner)

	// What a machine trusts is never guessed from a variable that is not a db.
	for name, variable := range map[string][]byte{
		"shorter than the attributes":       {0x27, 0},
		"cut off inside a list":             whole[:len(whole)-5],
		"cut off inside a header":           whole[:efivarAttributes+20],
		"a list larger than the variable":   oversized,
		"signatures that do not fill it":    uneven,
		"signatures with no room for data":  noData,
		"a header larger than its own list": dbVariable(append(slices.Clone(certSHA256[:]), 28, 0, 0, 0, 1, 0, 0, 0, 48, 0, 0, 0)),
	} {
		if db, err := ParseSecureBootDB(variable); err == nil {
			t.Errorf("%s: expected an error, got %+v", name, db)
		}
	}
}

func TestSecureBootDBTrusts(t *testing.T) {
	authority := talostest.SigningCertificate(t, "An owner's authority", nil)
	issued := talostest.SigningCertificate(t, "Image signing, issued", authority).Certificate
	signer := talostest.SigningCertificate(t, "Image signing", nil).Certificate
	stranger := talostest.SigningCertificate(t, "A boot stick's authority", nil).Certificate

	tests := []struct {
		name   string
		db     SecureBootDB
		signer string
		trusts bool
	}{
		{"the certificate itself", SecureBootDB{Certificates: [][]byte{stranger.Raw, signer.Raw}}, "self", true},
		{"the certificate that issued it", SecureBootDB{Certificates: [][]byte{authority.Certificate.Raw}}, "issued", true},
		// The machine this is about: another certificate, and the hashes of the images it has.
		{"hashes and someone else's certificate", SecureBootDB{Certificates: [][]byte{stranger.Raw}, Hashes: 6}, "self", false},
		{"an authority that did not issue it", SecureBootDB{Certificates: [][]byte{authority.Certificate.Raw}}, "self", false},
		{"nothing", SecureBootDB{}, "self", false},
		{"a certificate that cannot be read", SecureBootDB{Certificates: [][]byte{{0x30, 0x03, 1, 2, 3}}}, "self", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := signer
			if tt.signer == "issued" {
				cert = issued
			}

			if got := tt.db.Trusts(cert); got != tt.trusts {
				t.Fatalf("Trusts = %t, want %t", got, tt.trusts)
			}

			problem := SecureBootTrustProblem(tt.db, cert, "https://factory.example.com/")
			if tt.trusts != (problem == "") {
				t.Fatalf("problem %q for a db that trusts the signer: %t", problem, tt.trusts)
			}
		})
	}

	// What is refused says what to enrol, where to get it, and what the firmware has instead.
	problem := SecureBootTrustProblem(SecureBootDB{Certificates: [][]byte{stranger.Raw}, Hashes: 6}, signer, "https://factory.example.com/")
	for _, want := range []string{
		"Image signing", CertificateFingerprint(signer), "https://factory.example.com/secureboot/signing-cert.pem",
		"1 certificate(s) (A boot stick's authority)", "6 single image(s)",
	} {
		if !strings.Contains(problem, want) {
			t.Errorf("the problem does not say %q: %s", want, problem)
		}
	}
}

func TestSecureBootInstallerProblem(t *testing.T) {
	const image = "factory.example.com/metal-installer/0000:v1.14.2"

	for _, tt := range []struct {
		enforced, signed, problem bool
	}{
		{enforced: true, signed: false, problem: true},
		{enforced: true, signed: true},
		// A signed boot image boots where nothing checks the signature.
		{enforced: false, signed: true},
		{enforced: false, signed: false},
	} {
		got := SecureBootInstallerProblem(tt.enforced, tt.signed, image)
		if tt.problem != (got != "") || tt.problem && !strings.Contains(got, image) {
			t.Errorf("Secure Boot %t, signed installer %t: got %q", tt.enforced, tt.signed, got)
		}
	}
}

// What the operator really asks a node about its firmware, and that the connection it asks over
// reads that one file and no other.
func TestMachineryReadsWhatFirmwareTrusts(t *testing.T) {
	signer := talostest.SigningCertificate(t, "Image signing", nil).Certificate
	ctx := context.Background()

	for _, a := range []access{accessReadOnly, accessConfig, accessUpgrade, accessMembership, accessUpkeep} {
		api, c := serve(t, dial(a)...)
		api.files = map[string][]byte{
			SecureBootDBPath:            dbVariable(signatureList(certX509, signer.Raw), signatureList(certSHA256, hash(1), hash(2))),
			"/system/state/config.yaml": []byte("secrets"),
		}

		state := runtime.NewSecurityStateSpec(runtime.NamespaceName)
		state.TypedSpec().SecureBoot = true
		create(t, api, state)

		r := reader(c)

		if enforced, err := r.SecureBoot(ctx, apiNode); err != nil || !enforced {
			t.Fatalf("Secure Boot: got %t, %v", enforced, err)
		}

		db, err := r.SecureBootDB(ctx, apiNode)
		if err != nil || !db.Trusts(signer) || db.Hashes != 2 {
			t.Fatalf("db: got %+v, %v", db, err)
		}

		if to := api.addressedTo(machineapi.MachineService_Read_FullMethodName); !slices.Equal(to, []string{apiNode.String()}) {
			t.Fatalf("the read must be addressed to the node, got %v", to)
		}

		// Past the Reader, with the raw client: any other file is refused before it is asked for.
		stream, err := c.MachineClient.Read(client.WithNode(ctx, apiNode.String()), &machineapi.ReadRequest{Path: "/system/state/config.yaml"})
		if status.Code(err) != codes.PermissionDenied || stream != nil {
			t.Fatalf("a read of another file must be refused, got %v", err)
		}

		if !slices.Equal(api.reads, []string{SecureBootDBPath}) {
			t.Fatalf("only the Secure Boot db may have been read of the node, got %v", api.reads)
		}
	}

	// A node whose firmware has no such variable does not answer the question; that is an error,
	// not a db that trusts nothing.
	_, c := serve(t, dial(accessReadOnly)...)
	if db, err := reader(c).SecureBootDB(ctx, apiNode); err == nil {
		t.Fatalf("expected an error, got %+v", db)
	}
}
