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
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
)

// Secure Boot (docs/scope.md §7.3). A machine whose firmware enforces it boots an image only if
// the firmware's signature database, db, holds the image's own hash, the certificate that signed
// it, or the certificate that issued that one. A hash covers the one image it is of: a machine
// that trusts its boot image that way boots no other, and an upgrade installs another.

// SecureBootDBPath is where Linux shows the firmware's db: the variable's name and the GUID of
// the image security database (EFI_IMAGE_SECURITY_DATABASE_GUID, UEFI 2.10 §32.6.1). It is the
// one file of a node the operator reads, and the allowlist admits no other (allowlist.go).
const SecureBootDBPath = "/sys/firmware/efi/efivars/db-d719b2cb-3d3a-4596-a3bc-dad00e67656f"

const (
	// maxSecureBootDB bounds what is read of the variable. Firmware keeps a few tens of kilobytes
	// for all of its variables together.
	maxSecureBootDB = 1 << 20
	// efivarAttributes is what Linux puts before a variable's data: its attributes.
	efivarAttributes = 4
	// signatureListHeader is an EFI_SIGNATURE_LIST up to its signatures: the type, and the sizes
	// of the list, of its header and of each signature. signatureOwner is what every signature
	// starts with: the GUID of whoever enrolled it.
	signatureListHeader = 28
	signatureOwner      = 16
)

// The kinds of signature a db holds that say something about a Talos image, as their GUIDs are
// stored: the first three fields little-endian (UEFI 2.10 §32.4.1).
var (
	// EFI_CERT_X509_GUID, a5c059a1-94e4-4aa7-87b5-ab155c2bf072.
	certX509 = [16]byte{0xa1, 0x59, 0xc0, 0xa5, 0xe4, 0x94, 0xa7, 0x4a, 0x87, 0xb5, 0xab, 0x15, 0x5c, 0x2b, 0xf0, 0x72}
	// EFI_CERT_SHA256_GUID, c1c41626-504c-4092-aca9-41f936934328.
	certSHA256 = [16]byte{0x26, 0x16, 0xc4, 0xc1, 0x4c, 0x50, 0x92, 0x40, 0xac, 0xa9, 0x41, 0xf9, 0x36, 0x93, 0x43, 0x28}
)

// SecureBootDB is what a machine's firmware trusts to boot.
type SecureBootDB struct {
	// Certificates are the certificates in db, as they are stored (DER). Whatever one of them
	// signed, or issued the signer of, boots.
	Certificates [][]byte
	// Hashes counts the single images db trusts by their SHA-256 hash.
	Hashes int
	// Others counts the entries of every other kind, which say nothing about a Talos image.
	Others int
}

// ParseSecureBootDB reads a db variable as Linux shows it: its attributes, then signature lists
// (EFI_SIGNATURE_LIST, UEFI 2.10 §32.4.1). A variable that is not that is an error, never an
// empty db: what a machine trusts is not guessed.
func ParseSecureBootDB(variable []byte) (SecureBootDB, error) {
	var db SecureBootDB

	if len(variable) < efivarAttributes {
		return db, errors.New("the Secure Boot db is shorter than a variable's attributes")
	}

	for rest := variable[efivarAttributes:]; len(rest) > 0; {
		if len(rest) < signatureListHeader {
			return SecureBootDB{}, errors.New("the Secure Boot db ends inside a signature list")
		}

		listSize := int(binary.LittleEndian.Uint32(rest[16:20]))
		headerSize := int(binary.LittleEndian.Uint32(rest[20:24]))
		size := int(binary.LittleEndian.Uint32(rest[24:28]))

		if listSize > len(rest) || headerSize > listSize-signatureListHeader || size <= signatureOwner {
			return SecureBootDB{}, errors.New("the Secure Boot db holds a signature list whose sizes do not add up")
		}

		signatures := rest[signatureListHeader+headerSize : listSize]
		if len(signatures)%size != 0 {
			return SecureBootDB{}, errors.New("the Secure Boot db holds a signature list whose sizes do not add up")
		}

		kind := [16]byte(rest[:16])

		for ; len(signatures) > 0; signatures = signatures[size:] {
			switch kind {
			case certX509:
				db.Certificates = append(db.Certificates, bytes.Clone(signatures[signatureOwner:size]))
			case certSHA256:
				db.Hashes++
			default:
				db.Others++
			}
		}

		rest = rest[listSize:]
	}

	return db, nil
}

// Trusts reports whether firmware with this db boots what signer signed: signer is in db, or a
// certificate in db issued it.
func (db SecureBootDB) Trusts(signer *x509.Certificate) bool {
	for _, der := range db.Certificates {
		if bytes.Equal(der, signer.Raw) {
			return true
		}

		// Firmware vendors ship certificates that a strict parser refuses. One that cannot be
		// read here is not the issuer of anything as far as this can tell.
		if issuer, err := x509.ParseCertificate(der); err == nil && signer.CheckSignatureFrom(issuer) == nil {
			return true
		}
	}

	return false
}

// holds describes the db for a message: how many certificates, whose, and how many hashes.
func (db SecureBootDB) holds() string {
	names := make([]string, 0, len(db.Certificates))

	for _, der := range db.Certificates {
		if cert, err := x509.ParseCertificate(der); err == nil {
			names = append(names, certificateName(cert))
		} else {
			names = append(names, "one that cannot be read")
		}
	}

	certificates := "no certificate"
	if len(names) > 0 {
		certificates = fmt.Sprintf("%d certificate(s) (%s)", len(names), strings.Join(names, "; "))
	}

	return fmt.Sprintf("%s and the hashes of %d single image(s)", certificates, db.Hashes)
}

func certificateName(cert *x509.Certificate) string {
	if cert.Subject.CommonName != "" {
		return cert.Subject.CommonName
	}

	return cert.Subject.String()
}

// CertificateFingerprint is the SHA-256 fingerprint of a certificate, as firmware setup screens
// and openssl show it.
func CertificateFingerprint(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)

	return hex.EncodeToString(sum[:])
}

// SecureBootInstallerProblem says why a machine cannot be installed from an installer because of
// what the installer is, or returns "". enforced is whether the machine's firmware has Secure
// Boot on, signed whether the installer is the one whose boot image Image Factory signs.
//
// The plain installer carries a boot image nobody signed (pkg/imager/imager.go:472-498 in Talos
// v1.14: BuildSigned only for a Secure Boot profile). Firmware that enforces Secure Boot refuses
// it, and the machine does not come back from the reboot.
func SecureBootInstallerProblem(enforced, signed bool, image string) string {
	if !enforced || signed {
		return ""
	}

	return fmt.Sprintf("its firmware has Secure Boot on, and the boot image of %s is not signed: it would not boot. "+
		"Set secureBoot on the installer of the cluster, or of the machine's pool", image)
}

// SecureBootTrustProblem says why a machine whose firmware has Secure Boot on would not boot an
// image that signer signed, or returns "". factoryURL is where signer was asked for.
func SecureBootTrustProblem(db SecureBootDB, signer *x509.Certificate, factoryURL string) string {
	if db.Trusts(signer) {
		return ""
	}

	return fmt.Sprintf("its firmware has Secure Boot on and does not trust the certificate the installer's boot image is signed with "+
		"(%s, SHA-256 %s): its db holds %s, and a hash is of one image, which an upgrade replaces. "+
		"Add the certificate (%s/secureboot/signing-cert.pem) to the db in the machine's firmware setup, or put the machine on hold",
		certificateName(signer), CertificateFingerprint(signer), db.holds(), strings.TrimSuffix(factoryURL, "/"))
}

// SecureBoot implements Reader. Talos reports Secure Boot as on only while the firmware enforces
// it: not in setup mode (controllers/runtime/security_state.go:97 in Talos v1.14).
func (r *machineryReader) SecureBoot(ctx context.Context, node netip.Addr) (bool, error) {
	res, err := cosiGet[*runtime.SecurityState](r, ctx, node, "security state", runtime.SecurityStateID)
	if err != nil {
		return false, err
	}

	return res.TypedSpec().SecureBoot, nil
}

// SecureBootDB implements Reader.
func (r *machineryReader) SecureBootDB(ctx context.Context, node netip.Addr) (SecureBootDB, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	stream, err := r.client.Read(ctx, SecureBootDBPath)
	if err != nil {
		return SecureBootDB{}, fmt.Errorf("the Secure Boot db of %s: %w", node, err)
	}

	defer stream.Close() //nolint:errcheck // a stream that was read to its end, or is abandoned

	variable, err := io.ReadAll(io.LimitReader(stream, maxSecureBootDB+1))
	if err != nil {
		return SecureBootDB{}, fmt.Errorf("the Secure Boot db of %s: %w", node, err)
	}

	if len(variable) > maxSecureBootDB {
		return SecureBootDB{}, fmt.Errorf("the Secure Boot db of %s: larger than %d bytes", node, maxSecureBootDB)
	}

	db, err := ParseSecureBootDB(variable)
	if err != nil {
		return SecureBootDB{}, fmt.Errorf("%s: %w", node, err)
	}

	return db, nil
}
