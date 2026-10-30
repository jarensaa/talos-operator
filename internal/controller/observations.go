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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Observations remembers when this replica last finished observing each cluster.
//
// Status cannot say it: a cluster at rest is not written to, so status.observed.observedAt
// moves only when what is observed changes, and a quiet cluster's is days old while the operator
// reads it every minute. How old the last observation is matters all the same, for the alert
// that the operator has stopped observing, so it is kept here, by the replica that observes, and
// exported from here (docs/scope.md §14.2). Only the leader observes, so only its record moves.
type Observations struct {
	mu   sync.Mutex
	last map[types.NamespacedName]time.Time
}

// LastObserved returns when the cluster was last observed by this replica, if it was.
func (o *Observations) LastObserved(cluster types.NamespacedName) (time.Time, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	at, ok := o.last[cluster]

	return at, ok
}

// observed records that an observation of the cluster finished at, and that status says what it
// found.
func (o *Observations) observed(cluster types.NamespacedName, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.last == nil {
		o.last = map[types.NamespacedName]time.Time{}
	}

	o.last[cluster] = at
}

// forget drops a cluster that is gone, so one of the same name starts afresh.
func (o *Observations) forget(cluster types.NamespacedName) {
	o.mu.Lock()
	defer o.mu.Unlock()

	delete(o.last, cluster)
}
