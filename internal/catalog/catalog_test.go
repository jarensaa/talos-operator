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
	// extended is a schematic that names an extension Talos v1.14.2 has and v1.14.0 does not.
	extended = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	missing  = "0000000000000000000000000000000000000000000000000000000000000000"
	// private is a repository the registry hands no anonymous token for.
	private = "/v2/private/kubelet/manifests/v1.37.1"
	// extendedSchematic is what Image Factory answers for extended.
	extendedSchematic = "customization:\n  systemExtensions:\n    officialExtensions:\n      - siderolabs/intel-hd-audio\n"
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
		// Whoever is asked can tell who is asking.
		if r.Header.Get("User-Agent") != UserAgent {
			w.WriteHeader(http.StatusForbidden)

			return
		}

		u.mu.Lock()
		u.asked = append(u.asked, r.Method+" "+r.URL.Path)
		status := u.status
		u.mu.Unlock()

		if status != 0 {
			w.WriteHeader(status)

			return
		}

		answers := map[string]string{
			"/versions":                                               `["v1.13.0","v1.13.9","v1.14.0","v1.14.2","v1.15.0-alpha.1","latest"]`,
			"/schematics/" + schematic:                                "customization: {}\n",
			"/schematics/" + extended:                                 extendedSchematic,
			"/version/v1.14.2/extensions/official":                    `[{"name":"siderolabs/amazon-ena"},{"name":"siderolabs/intel-hd-audio"}]`,
			"/version/v1.14.0/extensions/official":                    `[{"name":"siderolabs/amazon-ena"}]`,
			"/v2/metal-installer/" + schematic + "/manifests/v1.14.2": "",
			"/v2/metal-installer/" + schematic + "/manifests/v1.14.0": "",
			"/v2/metal-installer/" + extended + "/manifests/v1.14.2":  "",
			"/v2/siderolabs/kubelet/manifests/v1.37.1":                "",
			"/v2/library/busybox/manifests/latest":                    "",
			"/v2/kube-apiserver/manifests/sha256:0123":                "",
			"/token":                   `{"token":"anonymous"}`,
			"/release/stable-1.36.txt": "v1.36.5\n",
			"/release/stable-1.35.txt": "v1.37.0",
		}

		// A registry that hands out anonymous tokens: the kubelet's repository wants one.
		if strings.HasPrefix(r.URL.Path, "/v2/siderolabs/") && r.Header.Get("Authorization") != "Bearer anonymous" {
			w.Header().Set("WWW-Authenticate",
				`Bearer realm="http://`+r.Host+`/token",service="registry.example.com",scope="repository:siderolabs/kubelet:pull"`)
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		// And one that wants credentials nobody has.
		if r.URL.Path == private {
			w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		query := r.URL.Query()
		if r.URL.Path == "/token" && (query.Get("service") != "registry.example.com" || !strings.Contains(query.Get("scope"), "kubelet")) {
			w.WriteHeader(http.StatusBadRequest)

			return
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

	// What does not exist is a definite no, and says which part is missing: the release, the
	// schematic, or an extension the schematic names that this release does not have.
	for _, tt := range []struct {
		schematic, version, want string
	}{
		{schematic, "v1.14.99", "Image Factory has no Talos v1.14.99"},
		{missing, "v1.14.2", "no schematic " + missing},
		{extended, "v1.14.0", "names the extension siderolabs/intel-hd-audio, which Talos v1.14.0 does not have"},
	} {
		err := h.Installer(ctx, base, "metal", tt.schematic, tt.version)
		if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s %s: got %v, want %q", tt.schematic[:8], tt.version, err, tt.want)
		}
	}

	if err := h.Installer(ctx, base, "metal", extended, "v1.14.2"); err != nil {
		t.Fatalf("an extension the release has is no problem: %v", err)
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

	// Images, as a node finds them: with the anonymous token a public registry hands out.
	registry := strings.TrimPrefix(base, "http://")

	for _, image := range []string{registry + "/siderolabs/kubelet:v1.37.1", registry + "/kube-apiserver@sha256:0123"} {
		if err := h.image(ctx, "http", image, requestTimeout); err != nil {
			t.Errorf("%s: %v", image, err)
		}
	}

	if err := h.image(ctx, "http", registry+"/siderolabs/kubelet:v1.37.99", requestTimeout); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}

	// A registry that wants credentials says nothing about what it holds.
	err = h.image(ctx, "http", registry+"/private/kubelet:v1.37.1", requestTimeout)
	if err == nil || errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "wants credentials") {
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
		"installer": h.Installer(ctx, base, "metal", schematic, "v1.14.2"),
		"image":     h.image(ctx, "http", strings.TrimPrefix(base, "http://")+"/siderolabs/kubelet:v1.37.1", requestTimeout),
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

		_, err := (&HTTP{KubernetesReleases: base + "/x"}).KubernetesLatest(ctx, "1.37")
		if base != "" && (err == nil || !strings.Contains(err.Error(), "URL")) {
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
		Latest: map[string]string{"1.37": "1.37.1"}, Tags: map[string]bool{"v1.37.0": true},
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

		if err := c.Image(ctx, "ghcr.io/siderolabs/kubelet:v1.37.0"); err != nil {
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

	if yes, no = everything(); yes != nil || no != nil {
		t.Fatalf("got %v, %v", yes, no)
	}

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

func TestSplitImage(t *testing.T) {
	for image, want := range map[string][3]string{
		"ghcr.io/siderolabs/kubelet:v1.37.1":                            {"ghcr.io", "siderolabs/kubelet", "v1.37.1"},
		"registry.k8s.io/kube-apiserver:v1.37.1":                        {"registry.k8s.io", "kube-apiserver", "v1.37.1"},
		"registry.example.com:5000/mirror/kube-scheduler:v1.37.1":       {"registry.example.com:5000", "mirror/kube-scheduler", "v1.37.1"},
		"registry.k8s.io/kube-apiserver:v1.37.1@sha256:0123":            {"registry.k8s.io", "kube-apiserver", "sha256:0123"},
		"registry.k8s.io/pause":                                         {"registry.k8s.io", "pause", "latest"},
		"docker.io/library/busybox:1.36":                                {"registry-1.docker.io", "library/busybox", "1.36"},
		"busybox":                                                       {"registry-1.docker.io", "library/busybox", "latest"},
		"siderolabs/kubelet:v1.37.1":                                    {"registry-1.docker.io", "siderolabs/kubelet", "v1.37.1"},
		"factory.example.com/metal-installer/" + schematic + ":v1.14.2": {"factory.example.com", "metal-installer/" + schematic, "v1.14.2"},
	} {
		host, repository, reference, err := splitImage(image)
		if err != nil || [3]string{host, repository, reference} != want {
			t.Errorf("%s: got %s %s %s (%v), want %v", image, host, repository, reference, err, want)
		}
	}

	if _, _, _, err := splitImage("registry.example.com/"); err == nil {
		t.Error("a registry alone is no image")
	}
}
