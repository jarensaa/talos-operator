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

package controller

import (
	"context"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// What status keeps of a node's health is bounded: a node with hundreds of links or volumes
// says so in Talos, and status holds the first of them by name.
const (
	maxHealthEntries = 64
	maxHealthMessage = 512
)

// maxHaltedMessage is how much of a failure's message a Halted Event quotes: an Event's message
// holds 1 KiB, and the rest of it names where the operation halted and the token.
const maxHaltedMessage = 640

// observeHealth reads what Talos itself says about the node (docs/scope.md §14.2). Each part
// describes a moment, so it is there only when it was read in this pass: nil when nothing was.
func observeHealth(tgt talos.Reader, reads *nodeReads) *talosv1alpha1.NodeHealth {
	health := &talosv1alpha1.NodeHealth{}
	node := reads.node
	read := false

	if reads.read("services", func(ctx context.Context) error {
		services, err := tgt.Services(ctx, node)

		for _, s := range bounded(services) {
			health.Services = append(health.Services, talosv1alpha1.ServiceHealth{
				Name:    s.ID,
				Running: s.Running,
				// A service without a health check is as healthy as running makes it.
				Healthy: s.Healthy || (s.Unknown && s.Running),
			})
		}

		return err
	}) {
		read = true
	} else {
		health.Services = nil
	}

	if reads.read("diagnostics", func(ctx context.Context) error {
		diagnostics, err := tgt.Diagnostics(ctx, node)

		for _, d := range bounded(diagnostics) {
			health.Diagnostics = append(health.Diagnostics, talosv1alpha1.Diagnostic{ID: d.ID, Message: clip(d.Message, maxHealthMessage)})
		}

		return err
	}) {
		read = true
	} else {
		health.Diagnostics = nil
	}

	if reads.read("time status", func(ctx context.Context) error {
		synced, err := tgt.TimeSynced(ctx, node)
		if err == nil {
			health.TimeSynced = &synced
		}

		return err
	}) {
		read = true
	}

	if reads.read("links", func(ctx context.Context) error {
		links, err := tgt.Links(ctx, node)

		for _, l := range bounded(links) {
			health.Links = append(health.Links, talosv1alpha1.LinkHealth{Name: l.Name, Up: l.Up, SpeedMbps: l.SpeedMbps, Bond: l.Bond})
		}

		return err
	}) {
		read = true
	} else {
		health.Links = nil
	}

	if reads.read("volumes", func(ctx context.Context) error {
		volumes, err := tgt.Volumes(ctx, node)

		for _, v := range bounded(volumes) {
			health.Volumes = append(health.Volumes, talosv1alpha1.VolumeHealth{Name: v.Name, Phase: v.Phase, Error: clip(v.Error, maxHealthMessage)})
		}

		return err
	}) {
		read = true
	} else {
		health.Volumes = nil
	}

	if !read {
		return nil
	}

	return health
}

func bounded[T any](list []T) []T {
	if len(list) > maxHealthEntries {
		return list[:maxHealthEntries]
	}

	return list
}

// clip cuts a message to what a status field holds, on a rune boundary.
func clip(s string, limit int) string {
	if len(s) <= limit {
		return s
	}

	cut := limit
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}

	return s[:cut]
}

// followedSize is the size status reports for an etcd database. A database changes with every
// write to the cluster, and status that followed it would be written in every pass: it follows
// only once the size moved by a twentieth of the database, which is precise enough for a
// dashboard and an alert. What decides a defragmentation is read where it is decided.
func followedSize(reported, now, total int64) int64 {
	if reported == 0 || now == 0 {
		return now
	}

	moved := now - reported
	if moved < 0 {
		moved = -moved
	}

	if moved*20 < total {
		return reported
	}

	return now
}
