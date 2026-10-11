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
	"sigs.k8s.io/yaml"
)

// DefaultKubernetesReleases is where Kubernetes publishes its releases.
const DefaultKubernetesReleases = "https://dl.k8s.io"

const (
	// requestTimeout bounds one request.
	requestTimeout = 15 * time.Second
	// buildTimeout bounds the request for an installer's manifest: Image Factory builds an
	// installer when it is first asked for, and answers when it is built.
	buildTimeout = 3 * time.Minute
	// attempts is how often a request is sent before its failure is the answer. A connection
	// that is not made and a server that is unwell are usually over a moment later, and an answer
	// that is missing is "not known": a failure of the validate check.
	attempts = 3
	// retryWait is the pause before the second attempt; the one before the third is twice that.
	retryWait = time.Second
	// maxBody bounds what is read of an answer. The longest, Image Factory's list of every Talos
	// release there ever was, is a few kilobytes.
	maxBody = 1 << 20
)

// UserAgent is how every request says who is asking, and where to find out more: whoever runs a
// service that is asked should be able to tell, and to say no.
const UserAgent = "talos-operator (+https://github.com/jarensaa/talos-operator)"

// ErrNotFound is the answer of a source that was reached and has no such thing.
var ErrNotFound = errors.New("not found")

// errRedirect is a redirect that is not followed. Asking again would only meet it again.
var errRedirect = errors.New("refusing a redirect")

// Source answers what exists upstream.
type Source interface {
	// TalosVersions lists the Talos releases the Image Factory at factoryURL builds, as tags
	// (v1.14.2), oldest first.
	TalosVersions(ctx context.Context, factoryURL string) ([]string, error)
	// Installer checks that Image Factory has a schematic, has every extension the schematic
	// names for a Talos version, and builds the installer: the image an upgrade pulls.
	// ErrNotFound means it certainly does not; any other error, that it could not be told.
	Installer(ctx context.Context, factoryURL, platform, schematic, talosVersion string) error
	// KubernetesLatest returns the newest patch release of a Kubernetes minor ("1.37"), as
	// "1.37.1". ErrNotFound means the minor has no release.
	KubernetesLatest(ctx context.Context, minor string) (string, error)
	// Image checks that an image exists in its registry, as a node that pulls it without
	// credentials would find it. ErrNotFound means it certainly does not; a registry that wants
	// credentials, like one that does not answer, is an error of another kind.
	Image(ctx context.Context, image string) error
}

// HTTP is a Source that asks over HTTPS.
type HTTP struct {
	// Client is the HTTP client; nil means one with a timeout, and that follows no redirect to
	// anything but HTTPS.
	Client *http.Client
	// KubernetesReleases is the base URL of the Kubernetes release site; empty means the default.
	KubernetesReleases string

	// pause waits between two attempts of a request; nil means for as long as it is told, or
	// until the context ends.
	pause func(ctx context.Context, d time.Duration) error
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
				return fmt.Errorf("%w to %s", errRedirect, req.URL.Redacted())
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

// get asks for a document and returns it, or ErrNotFound for a 404.
func (h *HTTP) get(ctx context.Context, target string) ([]byte, error) {
	body, _, err := h.send(ctx, http.MethodGet, target, nil, requestTimeout)

	return body, err
}

// send asks, and asks again when the request failed on the way or the server said it is unwell:
// every request here only reads. An answer about what was asked for is final, whatever it is,
// and so is the end of the caller's context. With a 401 it also returns what the answer says
// about how to authenticate.
func (h *HTTP) send(ctx context.Context, method, target string, header http.Header, timeout time.Duration) ([]byte, string, error) {
	for attempt := 1; ; attempt++ {
		body, challenge, again, err := h.once(ctx, method, target, header, timeout)
		if !again || attempt == attempts || ctx.Err() != nil {
			return body, challenge, err
		}

		if h.wait(ctx, time.Duration(attempt)*retryWait) != nil {
			return nil, "", err
		}
	}
}

func (h *HTTP) wait(ctx context.Context, d time.Duration) error {
	if h.pause != nil {
		return h.pause(ctx, d)
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// once sends one request, bounded by timeout. again says that the failure is one of the moment.
func (h *HTTP) once(
	ctx context.Context, method, target string, header http.Header, timeout time.Duration,
) (body []byte, challenge string, again bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return nil, "", false, err
	}

	req.Header = header.Clone()
	if req.Header == nil {
		req.Header = http.Header{}
	}

	req.Header.Set("User-Agent", UserAgent)

	client := *h.client()
	client.Timeout = timeout

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", !errors.Is(err, errRedirect), fmt.Errorf("%s %s: %w", method, target, err)
	}

	defer resp.Body.Close() //nolint:errcheck // a read-only response body

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, "", false, fmt.Errorf("%s: %w", target, ErrNotFound)
	case resp.StatusCode == http.StatusUnauthorized:
		return nil, resp.Header.Get("WWW-Authenticate"), false, fmt.Errorf("%s %s: %s", method, target, resp.Status)
	case resp.StatusCode != http.StatusOK:
		// A server that is unwell may be well again; one that says no, or says to ask less
		// often, is not asked again.
		return nil, "", resp.StatusCode >= http.StatusInternalServerError, fmt.Errorf("%s %s: %s", method, target, resp.Status)
	}

	body, err = io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, "", true, fmt.Errorf("%s %s: %w", method, target, err)
	}

	return body, "", false, nil
}

// TalosVersions implements Source (Image Factory: GET /versions).
func (h *HTTP) TalosVersions(ctx context.Context, factoryURL string) ([]string, error) {
	target, err := endpoint(factoryURL, "versions")
	if err != nil {
		return nil, err
	}

	body, err := h.get(ctx, target)
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

// Installer implements Source. The schematic is asked about first, then the extensions it names,
// then the installer itself, so that the answer says which of them is missing. Extensions come
// and go between Talos releases: one that a schematic names may not exist for the next.
func (h *HTTP) Installer(ctx context.Context, factoryURL, platform, schematic, talosVersion string) error {
	target, err := endpoint(factoryURL, "schematics", schematic)
	if err != nil {
		return err
	}

	body, err := h.get(ctx, target)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("Image Factory has no schematic %s: %w", schematic, ErrNotFound)
		}

		return err
	}

	var wanted struct {
		Customization struct {
			SystemExtensions struct {
				OfficialExtensions []string `json:"officialExtensions"`
			} `json:"systemExtensions"`
		} `json:"customization"`
	}
	if err := yaml.Unmarshal(body, &wanted); err != nil {
		return fmt.Errorf("%s: the answer is not a schematic", target)
	}

	if target, err = endpoint(factoryURL, "version", talosVersion, "extensions", "official"); err != nil {
		return err
	}

	if body, err = h.get(ctx, target); err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("Image Factory has no Talos %s: %w", talosVersion, ErrNotFound)
		}

		return err
	}

	var offered []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &offered); err != nil {
		return fmt.Errorf("%s: the answer is not a list of extensions", target)
	}

	for _, extension := range wanted.Customization.SystemExtensions.OfficialExtensions {
		found := false
		for _, has := range offered {
			found = found || has.Name == extension
		}

		if !found {
			return fmt.Errorf("schematic %s names the extension %s, which Talos %s does not have: %w",
				schematic, extension, talosVersion, ErrNotFound)
		}
	}

	u, err := url.Parse(factoryURL)
	if err != nil {
		return fmt.Errorf("%q is not a URL", factoryURL)
	}

	image := fmt.Sprintf("%s/%s-installer/%s:%s", u.Host, platform, schematic, talosVersion)

	// A plain-HTTP factory is a test's; a registry is asked over the scheme of the factory.
	return h.image(ctx, u.Scheme, image, buildTimeout)
}

// Image implements Source.
func (h *HTTP) Image(ctx context.Context, image string) error {
	return h.image(ctx, "https", image, requestTimeout)
}

// image asks a registry for an image's manifest, the way a node does when it pulls: without
// credentials, and with the anonymous token a public registry hands out to whoever asks.
func (h *HTTP) image(ctx context.Context, scheme, image string, timeout time.Duration) error {
	host, repository, reference, err := splitImage(image)
	if err != nil {
		return err
	}

	target, err := endpoint(scheme+"://"+host, "v2", repository, "manifests", reference)
	if err != nil {
		return err
	}

	header := http.Header{"Accept": {manifestTypes}}

	_, challenge, err := h.send(ctx, http.MethodHead, target, header, timeout)
	if err != nil && challenge != "" {
		token, tokenErr := h.token(ctx, challenge)
		if tokenErr != nil {
			return fmt.Errorf("%s: the registry wants credentials: %w", image, tokenErr)
		}

		header.Set("Authorization", "Bearer "+token)

		_, _, err = h.send(ctx, http.MethodHead, target, header, timeout)
	}

	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("the image %s does not exist: %w", image, ErrNotFound)
	}

	return err
}

// splitImage splits an image reference into the registry's host, the repository and the tag or
// digest to ask for.
func splitImage(image string) (host, repository, reference string, err error) {
	name, digest, _ := strings.Cut(image, "@")

	first, rest, ok := strings.Cut(name, "/")
	if !ok || !strings.ContainsAny(first, ".:") && first != "localhost" {
		// No registry named: Docker Hub, where an image without an owner is in "library".
		first, rest = "registry-1.docker.io", name
		if !strings.Contains(rest, "/") {
			rest = "library/" + rest
		}
	}

	if first == "docker.io" {
		first = "registry-1.docker.io"
	}

	repository, tag, tagged := strings.Cut(rest, ":")

	switch {
	case digest != "":
		reference = digest
	case tagged:
		reference = tag
	default:
		reference = "latest"
	}

	if repository == "" || reference == "" {
		return "", "", "", fmt.Errorf("%q is not an image", image)
	}

	return first, repository, reference, nil
}

// token gets the anonymous token a registry's 401 points to
// (WWW-Authenticate: Bearer realm="…",service="…",scope="…").
func (h *HTTP) token(ctx context.Context, challenge string) (string, error) {
	scheme, params, _ := strings.Cut(challenge, " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return "", fmt.Errorf("it asks for %q credentials", scheme)
	}

	fields := map[string]string{}

	for _, field := range strings.Split(params, ",") {
		if key, value, ok := strings.Cut(strings.TrimSpace(field), "="); ok {
			fields[key] = strings.Trim(value, `"`)
		}
	}

	realm, err := endpoint(fields["realm"])
	if err != nil {
		return "", fmt.Errorf("its token service: %w", err)
	}

	query := url.Values{}
	for _, key := range []string{"service", "scope"} {
		if fields[key] != "" {
			query.Set(key, fields[key])
		}
	}

	body, err := h.get(ctx, realm+"?"+query.Encode())
	if err != nil {
		return "", err
	}

	var answer struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &answer); err != nil || answer.Token+answer.AccessToken == "" {
		return "", errors.New("its token service gave no token")
	}

	if answer.Token != "" {
		return answer.Token, nil
	}

	return answer.AccessToken, nil
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

	body, err := h.get(ctx, target)
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
