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
// images from complete a TLS handshake (docs/dev/e2e.md). A node that never installs looks the
// same from the outside whatever is in its way: it connects, and its handshake times out. So each
// host is asked in several ways that differ in one thing each, and what the ways that fail have
// in common is what is in the way:
//
//   - as a node asks: Go's own handshake, as containerd sends it. Its first message carries a
//     post-quantum key share and does not fit one packet;
//   - the same in smaller segments, which tells a path that loses full-sized packets from a host
//     that cannot take a first message in two parts;
//   - without that key share, in one small packet, in one packet that is nearly full, and in two.
package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"time"
)

// timeout is the one containerd gives a handshake.
const timeout = 10 * time.Second

// way is one way of asking.
type way struct {
	name string
	// classic leaves the post-quantum key share out.
	classic bool
	// padding is added to the first message, as protocols the host does not know.
	padding int
	// segment bounds the size of a TCP segment; zero leaves it to the path.
	segment int
}

var ways = []way{
	{name: "as a node asks"},
	{name: "the same, in segments of 1300 bytes", segment: 1300},
	{name: "the same, in segments of 1000 bytes", segment: 1000},
	{name: "without the post-quantum key share", classic: true},
	{name: "without it, in one packet that is nearly full", classic: true, padding: 1100},
	{name: "without it, in two packets", classic: true, padding: 2200},
}

func (w way) config(host string) *tls.Config {
	cfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}}
	if w.classic {
		cfg.CurvePreferences = []tls.CurveID{tls.X25519}
	}

	for left := w.padding; left > 0; left -= 101 {
		cfg.NextProtos = append(cfg.NextProtos, fmt.Sprintf("x-%03d-%s", len(cfg.NextProtos), strings.Repeat("p", 94)))
	}

	return cfg
}

func (w way) dial(address string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}

	if w.segment > 0 {
		dialer.Control = func(_, _ string, raw syscall.RawConn) error {
			var failed error

			if err := raw.Control(func(fd uintptr) {
				failed = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, w.segment)
			}); err != nil {
				return err
			}

			return failed
		}
	}

	return dialer.Dial("tcp4", address)
}

// size is how many bytes the first message of this way of asking has: what a listener on this
// machine is sent before it answers anything.
func (w way) size() int {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0
	}

	defer listener.Close() //nolint:errcheck // a listener that is done

	sent := make(chan int, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			sent <- 0

			return
		}

		defer conn.Close() //nolint:errcheck // a connection that is given up

		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)) //nolint:errcheck // a loopback connection

		n, _ := io.Copy(io.Discard, conn) //nolint:errcheck // the deadline ends it
		sent <- int(n)
	}()

	conn, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		return 0
	}

	defer conn.Close() //nolint:errcheck // a connection that is given up

	_ = conn.SetDeadline(time.Now().Add(time.Second))          //nolint:errcheck // a loopback connection
	_ = tls.Client(conn, w.config("size.invalid")).Handshake() //nolint:errcheck // nobody answers; only what was sent counts

	return <-sent
}

func (w way) ask(host string) string {
	start := time.Now()

	conn, err := w.dial(net.JoinHostPort(host, "443"))
	if errors.Is(err, syscall.EINVAL) && w.segment > 0 {
		return "not asked: this system does not bound a segment before it connects"
	}

	if err != nil {
		return "no connection: " + err.Error()
	}

	defer conn.Close() //nolint:errcheck // a connection that is given up

	connected := time.Since(start)

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err.Error()
	}

	client := tls.Client(conn, w.config(host))
	if err := client.Handshake(); err != nil {
		return fmt.Sprintf("NO HANDSHAKE after %s: %v", (time.Since(start) - connected).Round(time.Millisecond), err)
	}

	return fmt.Sprintf("handshake in %s, key exchange %s", (time.Since(start) - connected).Round(time.Millisecond),
		client.ConnectionState().CurveID)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <host>...\n", os.Args[0])
		os.Exit(2)
	}

	for _, host := range os.Args[1:] {
		fmt.Println(host)

		for _, w := range ways {
			fmt.Printf("  %-46s %5d bytes first: %s\n", w.name, w.size(), w.ask(host))
		}
	}
}
