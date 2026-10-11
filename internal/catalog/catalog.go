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

// Package catalog asks what exists upstream: which Talos releases Image Factory offers, whether
// an installer image exists there, and which Kubernetes releases there are (docs/scope.md §7.5).
//
// It is the one place the operator talks to anything but the cluster it manages, and it only
// asks. Every answer has three values: yes, no, and not known. A source that cannot be reached
// makes nothing true and nothing false: what the nodes can pull is for the nodes to find out,
// through their own registry configuration.
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/version"
)

// DefaultKubernetesReleases is where Kubernetes publishes its releases.
const DefaultKubernetesReleases = "https://dl.k8s.io"

const (
	// requestTimeout bounds one request.
	requestTimeout = 15 * time.Second
	// maxBody bounds what is read of an answer. The longest, Image Factory's list of every Talos
	// release there ever was, is a few kilobytes.
	maxBody = 1 << 20
)

// ErrNotFound is the answer of a source that was reached and has no such thing.
var ErrNotFound = errors.New("not found")

// Source answers what exists upstream.
type Source interface {
	// TalosVersions lists the Talos releases the Image Factory at factoryURL builds, as tags
	// (v1.14.2), oldest first.
	TalosVersions(ctx context.Context, factoryURL string) ([]string, error)
	// Installer checks that Image Factory has a schematic and builds its installer for a Talos
	// version: the image an upgrade pulls. ErrNotFound means it certainly does not; any other
	// error, that it could not be told.
	Installer(ctx context.Context, factoryURL, platform, schematic, talosVersion string) error
	// KubernetesLatest returns the newest patch release of a Kubernetes minor ("1.37"), as
	// "1.37.1". ErrNotFound means the minor has no release.
	KubernetesLatest(ctx context.Context, minor string) (string, error)
	// Kubernetes checks that a Kubernetes release exists. ErrNotFound means it certainly does not.
	Kubernetes(ctx context.Context, release string) error
}

// HTTP is a Source that asks over HTTPS.
type HTTP struct {
	// Client is the HTTP client; nil means one with a timeout, and that follows no redirect to
	// anything but HTTPS.
	Client *http.Client
	// KubernetesReleases is the base URL of the Kubernetes release site; empty means the default.
	KubernetesReleases string
}

var _ Source = (*HTTP)(nil)

func (h *HTTP) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}

	return &http.Client{
		Timeout: requestTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if req.URL.Scheme != "https" || len(via) > 5 {
				return fmt.Errorf("refusing a redirect to %s", req.URL.Redacted())
			}

			return nil
		},
	}
}

// endpoint joins a base URL and a path. The base must be HTTPS: what is asked about decides what
// a cluster is upgraded to.
func endpoint(base string, parts ...string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("%q is not a URL", base)
	}

	if u.Scheme != "https" && !isLoopback(u.Hostname()) {
		return "", fmt.Errorf("%q is not an HTTPS URL", base)
	}

	return u.JoinPath(parts...).String(), nil
}

// isLoopback lets a test's server be asked over plain HTTP.
func isLoopback(host string) bool { return host == "127.0.0.1" || host == "::1" || host == "localhost" }

// ask sends one request and returns the answer's body, or ErrNotFound for a 404.
func (h *HTTP) ask(ctx context.Context, method, target string, header http.Header) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return nil, err
	}

	req.Header = header.Clone()
	if req.Header == nil {
		req.Header = http.Header{}
	}

	req.Header.Set("User-Agent", "talos-operator")

	resp, err := h.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, target, err)
	}

	defer resp.Body.Close() //nolint:errcheck // a read-only response body

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("%s: %w", target, ErrNotFound)
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("%s %s: %s", method, target, resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, target, err)
	}

	return body, nil
}

// TalosVersions implements Source (Image Factory: GET /versions).
func (h *HTTP) TalosVersions(ctx context.Context, factoryURL string) ([]string, error) {
	target, err := endpoint(factoryURL, "versions")
	if err != nil {
		return nil, err
	}

	body, err := h.ask(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}

	var listed []string
	if err := json.Unmarshal(body, &listed); err != nil {
		return nil, fmt.Errorf("%s: the answer is not a list of versions", target)
	}

	// Only what is a release: no "latest", and no pre-releases, which nobody is upgraded to
	// without asking for them by name.
	releases := make([]string, 0, len(listed))

	for _, tag := range listed {
		if v, err := version.ParseSemantic(tag); err == nil && v.PreRelease() == "" && strings.HasPrefix(tag, "v") {
			releases = append(releases, tag)
		}
	}

	return releases, nil
}

// manifestTypes are the image manifests a registry may answer with.
const manifestTypes = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, " +
	"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

// Installer implements Source. Image Factory is a registry as well: the installer exists if its
// manifest does, which is what a node asks for when it pulls
// <factory>/<platform>-installer/<schematic>:<version>. The schematic is asked about first, so
// that the answer can say which of the two is missing.
func (h *HTTP) Installer(ctx context.Context, factoryURL, platform, schematic, talosVersion string) error {
	target, err := endpoint(factoryURL, "schematics", schematic)
	if err != nil {
		return err
	}

	if _, err := h.ask(ctx, http.MethodGet, target, nil); err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("Image Factory has no schematic %s: %w", schematic, ErrNotFound)
		}

		return err
	}

	if target, err = endpoint(factoryURL, "v2", platform+"-installer", schematic, "manifests", talosVersion); err != nil {
		return err
	}

	if _, err := h.ask(ctx, http.MethodHead, target, http.Header{"Accept": {manifestTypes}}); err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("Image Factory builds no %s installer of Talos %s for schematic %s: %w", platform, talosVersion, schematic, ErrNotFound)
		}

		return err
	}

	return nil
}

func (h *HTTP) kubernetesReleases() string {
	if h.KubernetesReleases != "" {
		return h.KubernetesReleases
	}

	return DefaultKubernetesReleases
}

// KubernetesLatest implements Source (release/stable-<minor>.txt holds the newest patch release).
func (h *HTTP) KubernetesLatest(ctx context.Context, minor string) (string, error) {
	target, err := endpoint(h.kubernetesReleases(), "release", "stable-"+minor+".txt")
	if err != nil {
		return "", err
	}

	body, err := h.ask(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", err
	}

	tag := strings.TrimSpace(string(body))

	v, err := version.ParseSemantic(tag)
	if err != nil || fmt.Sprintf("%d.%d", v.Major(), v.Minor()) != minor {
		return "", fmt.Errorf("%s: the answer is not a release of Kubernetes %s", target, minor)
	}

	return strings.TrimPrefix(tag, "v"), nil
}

// Kubernetes implements Source. A release exists if its kubelet was published.
func (h *HTTP) Kubernetes(ctx context.Context, release string) error {
	v, err := version.ParseSemantic(release)
	if err != nil {
		return fmt.Errorf("%q is no Kubernetes release", release)
	}

	target, err := endpoint(h.kubernetesReleases(), "release", "v"+v.String(), "bin", "linux", "amd64", "kubelet.sha256")
	if err != nil {
		return err
	}

	if _, err := h.ask(ctx, http.MethodHead, target, nil); err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("there is no Kubernetes release %s: %w", v, ErrNotFound)
		}

		return err
	}

	return nil
}
