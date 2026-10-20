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
	"sort"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

// overflowRevision returns chosen, or, when overflow is configured and no
// endpoint of the overflow role in chosen has room, a revision that has one.
// When no revision has room, chosen is kept: moving would not help.
func (c *Screener) overflowRevision(endpoints []fwksched.Endpoint, shares map[string]float64, chosen string, draw float64) string {
	overflow := c.config.RevisionGating.Overflow
	if overflow == nil || chosen == "" {
		return chosen
	}
	// The common case: the chosen revision has room.
	seenChosen := false
	for _, endpoint := range endpoints {
		if c.overflowRevisionOf(endpoint) != chosen {
			continue
		}
		seenChosen = true
		if c.hasRoom(endpoint) {
			return chosen
		}
	}
	if !seenChosen {
		return chosen
	}
	withRoom := make(map[string]float64, len(shares))
	for _, endpoint := range endpoints {
		revision := c.overflowRevisionOf(endpoint)
		if revision == "" || revision == chosen {
			continue
		}
		if share, ok := shares[revision]; ok && c.hasRoom(endpoint) {
			withRoom[revision] = share
		}
	}
	if next := pickWeightedRevision(withRoom, drawWithin(shares, chosen, draw)); next != "" {
		return next
	}
	return chosen
}

// overflowRevisionOf returns the revision of an endpoint of the overflow role,
// or "" for any other endpoint.
func (c *Screener) overflowRevisionOf(endpoint fwksched.Endpoint) string {
	if endpoint == nil {
		return ""
	}
	metadata := endpoint.GetMetadata()
	if metadata == nil || metadata.Labels[c.roleLabelKey] != c.config.RevisionGating.Overflow.Role {
		return ""
	}
	return metadata.Labels[c.revisionLabelKey]
}

func (c *Screener) hasRoom(endpoint fwksched.Endpoint) bool {
	load, ok := fwkdl.ReadAttribute[*attrconcurrency.InFlightLoad](endpoint, c.inFlightLoadKey)
	return !ok || load.Requests < c.config.RevisionGating.Overflow.MaxInFlightRequests
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
