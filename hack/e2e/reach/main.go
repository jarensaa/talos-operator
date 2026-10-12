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

// Command reach says, from the machine it runs on, whether the hosts the e2e nodes pull their
// images from complete a TLS handshake (docs/dev/e2e.md). It asks each host twice: with the
// handshake a Talos node sends, whose first message carries a post-quantum key share and does not
// fit one packet, and with one that does. A host that answers the second and not the first is
// behind a path that loses full-sized packets, which is what a node that never installs looks
// like from the outside: it connects, and its handshake times out.
package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"time"
)

// timeout is the one containerd gives a handshake.
const timeout = 10 * time.Second

func handshake(host string, curves []tls.CurveID) string {
	start := time.Now()

	conn, err := net.DialTimeout("tcp4", net.JoinHostPort(host, "443"), timeout)
	if err != nil {
		return "no connection: " + err.Error()
	}

	defer conn.Close() //nolint:errcheck // a connection that is given up

	connected := time.Since(start)

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err.Error()
	}

	client := tls.Client(conn, &tls.Config{ServerName: host, CurvePreferences: curves, MinVersion: tls.VersionTLS12})
	if err := client.Handshake(); err != nil {
		return fmt.Sprintf("connected to %s in %s, no handshake after %s: %v", conn.RemoteAddr(), connected.Round(time.Millisecond),
			(time.Since(start) - connected).Round(time.Millisecond), err)
	}

	return fmt.Sprintf("connected to %s in %s, handshake in %s (key exchange %s)", conn.RemoteAddr(), connected.Round(time.Millisecond),
		(time.Since(start) - connected).Round(time.Millisecond), client.ConnectionState().CurveID)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <host>...\n", os.Args[0])
		os.Exit(2)
	}

	for _, host := range os.Args[1:] {
		// Go's own choice of key shares is the node's: containerd is built with it.
		fmt.Printf("%s, as a node asks:      %s\n", host, handshake(host, nil))
		fmt.Printf("%s, in a single packet:  %s\n", host, handshake(host, []tls.CurveID{tls.X25519}))
	}
}
