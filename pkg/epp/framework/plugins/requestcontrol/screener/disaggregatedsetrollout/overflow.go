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

	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// resolveOverflowGuard looks up the overflow guard by name. The guard must fail
// closed: a filter that returns every endpoint when all are full never shows
// that a revision has no room.
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
	byRevision := c.overflowEndpointsByRevision(endpoints)
	// The common case: the chosen revision has room.
	if candidates := byRevision[chosen]; len(candidates) == 0 || c.hasRoom(ctx, request, candidates) {
		return chosen
	}
	withRoom := make(map[string]float64, len(shares))
	for revision, candidates := range byRevision {
		if revision == chosen {
			continue
		}
		if share, ok := shares[revision]; ok && c.hasRoom(ctx, request, candidates) {
			withRoom[revision] = share
		}
	}
	if next := pickWeightedRevision(withRoom, drawWithin(shares, chosen, draw)); next != "" {
		return next
	}
	return chosen
}

// overflowEndpointsByRevision groups the endpoints of the overflow role by
// revision.
func (c *Screener) overflowEndpointsByRevision(endpoints []fwksched.Endpoint) map[string][]fwksched.Endpoint {
	role := c.config.RevisionGating.Overflow.Role
	byRevision := map[string][]fwksched.Endpoint{}
	for _, endpoint := range endpoints {
		if endpoint == nil {
			continue
		}
		metadata := endpoint.GetMetadata()
		if metadata == nil || metadata.Labels[c.roleLabelKey] != role {
			continue
		}
		if revision := metadata.Labels[c.revisionLabelKey]; revision != "" {
			byRevision[revision] = append(byRevision[revision], endpoint)
		}
	}
	return byRevision
}

// hasRoom reports whether the guard admits any of the endpoints. The guard
// runs before the data producers, so a token limit sees the in-flight tokens
// but not the uncached tokens of this request.
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
