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
	"fmt"
	"sync"
	"time"
)

// How long an answer is kept. What exists upstream changes with a release, a few times a month.
// A "no" is asked again sooner, since the release someone waits for may be minutes away, and a
// source that did not answer sooner still, without being asked on every pass of a controller.
const (
	DefaultKeep       = time.Hour
	DefaultKeepAbsent = 10 * time.Minute
	DefaultRetry      = 2 * time.Minute
)

// Cached is a Source that remembers the answers of another. It is safe for concurrent use.
type Cached struct {
	Source Source
	// Now is the clock; nil means time.Now.
	Now func() time.Time

	mu      sync.Mutex
	answers map[string]answer
}

var _ Source = (*Cached)(nil)

type answer struct {
	value any
	err   error
	until time.Time
}

func (c *Cached) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}

	return time.Now()
}

// remember returns the answer kept under key, or asks for it and keeps it.
func remember[T any](c *Cached, key string, ask func() (T, error)) (T, error) {
	c.mu.Lock()
	kept, ok := c.answers[key]
	c.mu.Unlock()

	if ok && c.now().Before(kept.until) {
		value, _ := kept.value.(T)

		return value, kept.err
	}

	value, err := ask()

	keep := DefaultKeep

	switch {
	case errors.Is(err, ErrNotFound):
		keep = DefaultKeepAbsent
	case err != nil:
		keep = DefaultRetry
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.answers == nil {
		c.answers = map[string]answer{}
	}

	c.answers[key] = answer{value: value, err: err, until: c.now().Add(keep)}

	return value, err
}

// TalosVersions implements Source.
func (c *Cached) TalosVersions(ctx context.Context, factoryURL string) ([]string, error) {
	return remember(c, "talos "+factoryURL, func() ([]string, error) { return c.Source.TalosVersions(ctx, factoryURL) })
}

// Installer implements Source.
func (c *Cached) Installer(ctx context.Context, factoryURL, platform, schematic, talosVersion string) error {
	_, err := remember(c, fmt.Sprintf("installer %s %s %s %s", factoryURL, platform, schematic, talosVersion), func() (struct{}, error) {
		return struct{}{}, c.Source.Installer(ctx, factoryURL, platform, schematic, talosVersion)
	})

	return err
}

// KubernetesLatest implements Source.
func (c *Cached) KubernetesLatest(ctx context.Context, minor string) (string, error) {
	return remember(c, "latest "+minor, func() (string, error) { return c.Source.KubernetesLatest(ctx, minor) })
}

// Kubernetes implements Source.
func (c *Cached) Kubernetes(ctx context.Context, release string) error {
	_, err := remember(c, "kubernetes "+release, func() (struct{}, error) {
		return struct{}{}, c.Source.Kubernetes(ctx, release)
	})

	return err
}

// Fake is a Source for tests: what it holds exists, nothing else does, and Err, if set, is the
// answer to everything.
type Fake struct {
	mu sync.Mutex
	// Talos is what TalosVersions lists.
	Talos []string
	// Installers holds the installers that exist, as "<schematic> <version>".
	Installers map[string]bool
	// Latest maps a Kubernetes minor to its newest release; Releases holds further releases
	// that exist.
	Latest   map[string]string
	Releases map[string]bool
	// Err makes every question fail: the source cannot be reached.
	Err error
	// Asked counts the questions.
	Asked int
}

var _ Source = (*Fake)(nil)

func (f *Fake) asked() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.Asked++

	return f.Err
}

// TalosVersions implements Source.
func (f *Fake) TalosVersions(context.Context, string) ([]string, error) {
	if err := f.asked(); err != nil {
		return nil, err
	}

	return f.Talos, nil
}

// Installer implements Source.
func (f *Fake) Installer(_ context.Context, _, _, schematic, talosVersion string) error {
	if err := f.asked(); err != nil {
		return err
	}

	if !f.Installers[schematic+" "+talosVersion] {
		return fmt.Errorf("Image Factory builds no installer of Talos %s for schematic %s: %w", talosVersion, schematic, ErrNotFound)
	}

	return nil
}

// KubernetesLatest implements Source.
func (f *Fake) KubernetesLatest(_ context.Context, minor string) (string, error) {
	if err := f.asked(); err != nil {
		return "", err
	}

	if latest, ok := f.Latest[minor]; ok {
		return latest, nil
	}

	return "", fmt.Errorf("Kubernetes %s: %w", minor, ErrNotFound)
}

// Kubernetes implements Source.
func (f *Fake) Kubernetes(_ context.Context, release string) error {
	if err := f.asked(); err != nil {
		return err
	}

	for _, latest := range f.Latest {
		if latest == release {
			return nil
		}
	}

	if !f.Releases[release] {
		return fmt.Errorf("there is no Kubernetes release %s: %w", release, ErrNotFound)
	}

	return nil
}
