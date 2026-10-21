/*
Copyright 2026 The llm-d Authors.

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

package disaggregatedsetrollout

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/labels"

	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// resolveOverflowGuard looks up the overflow guard and rejects one that fails open.
func (c *Screener) resolveOverflowGuard() error {
	if c.config.RevisionGating == nil || c.config.RevisionGating.Overflow == nil {
		return nil
	}
	name := c.config.RevisionGating.Overflow.GuardPluginName
	plugin := c.handle.Plugin(name)
	if plugin == nil {
		return fmt.Errorf("revisionGating.overflow.guardPluginName: plugin %q not found", name)
	}
	guard, ok := plugin.(fwksched.Filter)
	if !ok {
		return fmt.Errorf("revisionGating.overflow.guardPluginName: plugin %q is not a scheduling filter", name)
	}
	if failOpen, ok := plugin.(interface{ FailsOpen() bool }); ok && failOpen.FailsOpen() {
		return fmt.Errorf("revisionGating.overflow.guardPluginName: plugin %q fails open; set failOpen: false", name)
	}
	c.overflowGuard = guard
	return nil
}

// overflowRevision returns chosen, or, when overflow is configured and the
// guard admits no endpoint of the overflow role in chosen, a revision where it
// admits one. When no revision has room, chosen is kept: moving would not help.
func (c *Screener) overflowRevision(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint,
	shares map[string]float64, chosen string, draw float64) string {
	if c.overflowGuard == nil || chosen == "" {
		return chosen
	}
	// The common case: the chosen revision has room. A revision with no
	// endpoint of the role in the pool has none, for example while its Pods
	// are still counted Ready but already gone from the pool.
	var chosenCandidates []fwksched.Endpoint
	for _, endpoint := range endpoints {
		if c.overflowRevisionOf(endpoint) == chosen {
			chosenCandidates = append(chosenCandidates, endpoint)
		}
	}
	if len(chosenCandidates) > 0 && c.hasRoom(ctx, request, chosenCandidates) {
		return chosen
	}
	byRevision := map[string][]fwksched.Endpoint{}
	for _, endpoint := range endpoints {
		if revision := c.overflowRevisionOf(endpoint); revision != "" && revision != chosen {
			byRevision[revision] = append(byRevision[revision], endpoint)
		}
	}
	withRoom := make(map[string]float64, len(shares))
	for revision, candidates := range byRevision {
		if share, ok := shares[revision]; ok && c.hasRoom(ctx, request, candidates) {
			withRoom[revision] = share
		}
	}
	if next := pickWeightedRevision(withRoom, drawWithin(shares, chosen, draw)); next != "" {
		return next
	}
	return chosen
}

// overflowRevisionOf returns the revision of an in-scope endpoint of the
// overflow role, or "" for any other endpoint. Out-of-scope endpoints are
// skipped: they are not counted in the shares and must not count as room.
func (c *Screener) overflowRevisionOf(endpoint fwksched.Endpoint) string {
	if endpoint == nil {
		return ""
	}
	metadata := endpoint.GetMetadata()
	if metadata == nil || metadata.Labels[c.roleLabelKey] != c.config.RevisionGating.Overflow.Role {
		return ""
	}
	if c.scope != nil && !c.scope.Matches(labels.Set(metadata.Labels)) {
		return ""
	}
	return metadata.Labels[c.revisionLabelKey]
}

// hasRoom reports whether the guard admits any of the endpoints.
func (c *Screener) hasRoom(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) bool {
	return len(c.overflowGuard.Filter(ctx, request, endpoints)) > 0
}

// drawWithin rescales draw from revision's slice of the pickWeightedRevision
// layout back to [0, 1), so the overflow pick spreads over the remaining
// revisions by their shares instead of landing at the start of the layout.
func drawWithin(shares map[string]float64, revision string, draw float64) float64 {
	revisions := make([]string, 0, len(shares))
	total := 0.0
	for r, share := range shares {
		revisions = append(revisions, r)
		total += share
	}
	sort.Strings(revisions)
	low := 0.0
	for _, r := range revisions {
		if r == revision {
			if shares[r] <= 0 || total <= 0 {
				return draw
			}
			rescaled := (draw*total - low) / shares[r]
			return min(max(rescaled, 0), 1-1e-12)
		}
		low += shares[r]
	}
	return draw
}
