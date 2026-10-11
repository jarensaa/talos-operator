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

package catalog

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	schematic = "376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba"
	missing   = "0000000000000000000000000000000000000000000000000000000000000000"
)

// upstream is Image Factory and the Kubernetes release site in one server, answering as the
// real ones do: 200 for what exists, 404 for what does not.
type upstream struct {
	mu     sync.Mutex
	asked  []string
	status int
}

func (u *upstream) serve(t *testing.T) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.asked = append(u.asked, r.Method+" "+r.URL.Path)
		status := u.status
		u.mu.Unlock()

		if status != 0 {
			w.WriteHeader(status)

			return
		}

		answers := map[string]string{
			"/versions":                `["v1.13.0","v1.13.9","v1.14.0","v1.14.2","v1.15.0-alpha.1","latest"]`,
			"/schematics/" + schematic: "customization: {}\n",
			"/v2/metal-installer/" + schematic + "/manifests/v1.14.2": "",
			"/release/stable-1.36.txt":                                "v1.36.5\n",
			"/release/stable-1.35.txt":                                "v1.37.0",
			"/release/v1.37.1/bin/linux/amd64/kubelet.sha256":         "abc",
		}

		body, ok := answers[r.URL.Path]
		if !ok {
			http.NotFound(w, r)

			return
		}

		if strings.HasPrefix(r.URL.Path, "/v2/") && (r.Method != http.MethodHead || !strings.Contains(r.Header.Get("Accept"), "image.index")) {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		_, _ = w.Write([]byte(body)) //nolint:errcheck // a test server
	}))
	t.Cleanup(srv.Close)

	return srv.URL
}

func TestHTTP(t *testing.T) {
	u := &upstream{}
	base := u.serve(t)
	h := &HTTP{KubernetesReleases: base}
	ctx := context.Background()

	versions, err := h.TalosVersions(ctx, base)
	if err != nil || !slices.Equal(versions, []string{"v1.13.0", "v1.13.9", "v1.14.0", "v1.14.2"}) {
		t.Fatalf("releases only, no pre-release and no alias: got %v, %v", versions, err)
	}

	if err := h.Installer(ctx, base, "metal", schematic, "v1.14.2"); err != nil {
		t.Fatal(err)
	}

	// What does not exist is a definite no, and says which part is missing.
	err = h.Installer(ctx, base, "metal", schematic, "v1.14.99")
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "no metal installer of Talos v1.14.99") {
		t.Fatalf("got %v", err)
	}

	err = h.Installer(ctx, base, "metal", missing, "v1.14.2")
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "no schematic "+missing) {
		t.Fatalf("got %v", err)
	}

	if latest, err := h.KubernetesLatest(ctx, "1.36"); err != nil || latest != "1.36.5" {
		t.Fatalf("got %q, %v", latest, err)
	}

	if _, err := h.KubernetesLatest(ctx, "1.99"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}

	// An answer that is not a release of the minor asked about is no answer.
	if _, err := h.KubernetesLatest(ctx, "1.35"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}

	if err := h.Kubernetes(ctx, "1.37.1"); err != nil {
		t.Fatal(err)
	}

	if err := h.Kubernetes(ctx, "1.37.99"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}

	if err := h.Kubernetes(ctx, "newest"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}

	// Nothing but questions was sent.
	for _, asked := range u.asked {
		if !strings.HasPrefix(asked, "GET ") && !strings.HasPrefix(asked, "HEAD ") {
			t.Fatalf("sent %s", asked)
		}
	}
}

// A source that is there and unwell says nothing about what exists: not yes, and above all not no.
func TestHTTPTellsAnOutageFromAnAbsence(t *testing.T) {
	u := &upstream{status: http.StatusBadGateway}
	base := u.serve(t)
	h := &HTTP{KubernetesReleases: base}
	ctx := context.Background()

	for name, err := range map[string]error{
		"installer":  h.Installer(ctx, base, "metal", schematic, "v1.14.2"),
		"kubernetes": h.Kubernetes(ctx, "1.37.1"),
	} {
		if err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("%s: got %v", name, err)
		}
	}

	if _, err := h.TalosVersions(ctx, base); err == nil {
		t.Error("versions: an error was expected")
	}

	if _, err := h.KubernetesLatest(ctx, "1.36"); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("latest: got %v", err)
	}

	gone := "http://127.0.0.1:1"
	if err := (&HTTP{}).Installer(ctx, gone, "metal", schematic, "v1.14.2"); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("unreachable: got %v", err)
	}
}

// What is asked decides what a cluster is upgraded to, so it is asked over HTTPS or not at all.
func TestHTTPAsksOnlyOverHTTPS(t *testing.T) {
	ctx := context.Background()

	for _, base := range []string{"http://factory.example.com", "ftp://factory.example.com", "factory.example.com", ""} {
		if _, err := (&HTTP{}).TalosVersions(ctx, base); err == nil || !strings.Contains(err.Error(), "URL") {
			t.Errorf("%q: got %v", base, err)
		}

		if err := (&HTTP{KubernetesReleases: base + "/x"}).Kubernetes(ctx, "1.37.1"); base != "" && (err == nil || !strings.Contains(err.Error(), "URL")) {
			t.Errorf("%q: got %v", base, err)
		}
	}

	// A redirect away from HTTPS is not followed.
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`["v9.9.9"]`)) //nolint:errcheck // a test server
	}))
	defer plain.Close()

	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+"/versions", http.StatusFound)
	}))
	defer redirecting.Close()

	if versions, err := (&HTTP{}).TalosVersions(ctx, redirecting.URL); err == nil {
		t.Fatalf("a redirect to plain HTTP was followed: %v", versions)
	}
}

func TestCached(t *testing.T) {
	fake := &Fake{
		Talos: []string{"v1.14.2"}, Installers: map[string]bool{schematic + " v1.14.2": true},
		Latest: map[string]string{"1.37": "1.37.1"}, Releases: map[string]bool{"1.37.0": true},
	}
	clock := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	c := &Cached{Source: fake, Now: func() time.Time { return clock }}
	ctx := context.Background()

	everything := func() (yes, no error) {
		t.Helper()

		if v, err := c.TalosVersions(ctx, "https://factory.example.com"); err != nil || len(v) != 1 {
			t.Fatalf("got %v, %v", v, err)
		}

		if latest, err := c.KubernetesLatest(ctx, "1.37"); err != nil || latest != "1.37.1" {
			t.Fatalf("got %q, %v", latest, err)
		}

		if err := c.Kubernetes(ctx, "1.37.0"); err != nil {
			t.Fatal(err)
		}

		return c.Installer(ctx, "https://factory.example.com", "metal", schematic, "v1.14.2"),
			c.Installer(ctx, "https://factory.example.com", "metal", schematic, "v1.14.3")
	}

	yes, no := everything()
	if yes != nil || !errors.Is(no, ErrNotFound) || fake.Asked != 5 {
		t.Fatalf("got %v, %v after %d questions", yes, no, fake.Asked)
	}

	// Asked again a moment later, nothing is asked upstream.
	clock = clock.Add(time.Minute)

	if yes, no = everything(); yes != nil || !errors.Is(no, ErrNotFound) || fake.Asked != 5 {
		t.Fatalf("got %v, %v after %d questions", yes, no, fake.Asked)
	}

	// A "no" is asked again sooner than a "yes": the release may have come out meanwhile.
	clock = clock.Add(DefaultKeepAbsent)
	fake.Installers[schematic+" v1.14.3"] = true

	if yes, no = everything(); yes != nil || no != nil || fake.Asked != 6 {
		t.Fatalf("got %v, %v after %d questions", yes, no, fake.Asked)
	}

	clock = clock.Add(DefaultKeep)
	everything()

	if fake.Asked != 11 {
		t.Fatalf("after an hour everything is asked again: %d questions", fake.Asked)
	}

	// A source that does not answer is not asked on every pass, and asked again soon.
	clock = clock.Add(DefaultKeep)
	fake.Err = errors.New("no route to host")

	for range 3 {
		if _, err := c.TalosVersions(ctx, "https://factory.example.com"); err == nil {
			t.Fatal("an error was expected")
		}
	}

	if fake.Asked != 12 {
		t.Fatalf("%d questions", fake.Asked)
	}

	clock = clock.Add(DefaultRetry)
	fake.Err = nil

	if _, err := c.TalosVersions(ctx, "https://factory.example.com"); err != nil || fake.Asked != 13 {
		t.Fatalf("got %v after %d questions", err, fake.Asked)
	}
}
