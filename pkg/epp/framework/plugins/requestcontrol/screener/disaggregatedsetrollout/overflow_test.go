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
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/types"

	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

const testOverflowLimit = 12

func overflowConfig() Config {
	config := prefixAffinityConfig("")
	config.RevisionGating.Overflow = &RevisionOverflow{MaxInFlightRequests: testOverflowLimit}
	return config
}

// loadedEndpoint is an endpoint of revision and role carrying requests in flight.
func loadedEndpoint(name, revision, role string, requests int64) fwksched.Endpoint {
	e := endpoint(name, map[string]string{testRevLabel: revision, testRoleLabel: role})
	e.Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{Requests: requests})
	return e
}

// overflowPool has two prefill and two decode endpoints per revision, with the
// given in-flight requests on every prefill endpoint of each revision.
func overflowPool(v1Prefill, v2Prefill int64) []fwksched.Endpoint {
	pool := make([]fwksched.Endpoint, 0, 8)
	for revision, requests := range map[string]int64{"v1": v1Prefill, "v2": v2Prefill} {
		for i := range 2 {
			pool = append(pool,
				loadedEndpoint(fmt.Sprintf("%s-prefill-%d", revision, i), revision, "prefill", requests),
				loadedEndpoint(fmt.Sprintf("%s-decode-%d", revision, i), revision, "decode", 0))
		}
	}
	return pool
}

func seededOverflowScreener(t *testing.T, config Config) *Screener {
	t.Helper()
	screener := newTestScreener(config)
	seedCounts(t, screener, map[string]map[string]int{
		"v1": {"prefill": 2, "decode": 2},
		"v2": {"prefill": 2, "decode": 2},
	})
	return screener
}

func screenedRevision(t *testing.T, screener *Screener, request *fwksched.InferenceRequest, pool []fwksched.Endpoint) string {
	t.Helper()
	got := screenCandidates(t, screener, request, pool)
	if len(got) == 0 {
		t.Fatal("empty survivor set")
	}
	return got[0].GetMetadata().Labels[testRevLabel]
}

// sessionOnV1 returns the first session whose first turn draws v1 on an idle pool.
func sessionOnV1(t *testing.T, screener *Screener) *fwksched.InferenceRequest {
	t.Helper()
	for session := range 1000 {
		request := sessionTurns(session)[0]
		if screenedRevision(t, screener, request, overflowPool(0, 0)) == "v1" {
			return request
		}
	}
	t.Fatal("no session draws v1")
	return nil
}

func TestOverflowMovesARequestOffAFullRevisionForThatRequestOnly(t *testing.T) {
	screener := seededOverflowScreener(t, overflowConfig())
	request := sessionOnV1(t, screener)
	before := testutil.ToFloat64(revisionOverflowTotal.WithLabelValues(PluginType, "test-screener", "v1", "v2"))

	if got := screenedRevision(t, screener, request, overflowPool(testOverflowLimit, 0)); got != "v2" {
		t.Fatalf("v1 prefill full: served by %s, want v2", got)
	}
	if after := testutil.ToFloat64(revisionOverflowTotal.WithLabelValues(PluginType, "test-screener", "v1", "v2")); after != before+1 {
		t.Fatalf("overflow metric = %v, want %v", after, before+1)
	}
	// Nothing is stored: once v1 has room, the session draws v1 again.
	if got := screenedRevision(t, screener, request, overflowPool(testOverflowLimit-1, 0)); got != "v1" {
		t.Fatalf("v1 has room again: served by %s, want v1", got)
	}
}

func TestOverflowKeepsTheRevisionWhenAnyEndpointHasRoom(t *testing.T) {
	screener := seededOverflowScreener(t, overflowConfig())
	request := sessionOnV1(t, screener)
	pool := overflowPool(testOverflowLimit, 0)
	// One v1 prefill endpoint frees a slot.
	for _, e := range pool {
		if e.GetMetadata().Name == "v1-prefill-0" {
			e.Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{Requests: testOverflowLimit - 1})
		}
	}
	if got := screenedRevision(t, screener, request, pool); got != "v1" {
		t.Fatalf("a v1 prefill endpoint has room: served by %s, want v1", got)
	}
}

func TestOverflowKeepsTheRevisionWhenEveryRevisionIsFull(t *testing.T) {
	screener := seededOverflowScreener(t, overflowConfig())
	request := sessionOnV1(t, screener)
	if got := screenedRevision(t, screener, request, overflowPool(testOverflowLimit, testOverflowLimit)); got != "v1" {
		t.Fatalf("every revision full: served by %s, want v1", got)
	}
}

func TestOverflowChecksOnlyTheConfiguredRole(t *testing.T) {
	screener := seededOverflowScreener(t, overflowConfig())
	request := sessionOnV1(t, screener)
	pool := overflowPool(0, 0)
	for _, e := range pool {
		if e.GetMetadata().Labels[testRevLabel] == "v1" && e.GetMetadata().Labels[testRoleLabel] == "decode" {
			e.Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{Requests: testOverflowLimit})
		}
	}
	if got := screenedRevision(t, screener, request, pool); got != "v1" {
		t.Fatalf("only v1 decode is full: served by %s, want v1", got)
	}
}

func TestOverflowNeverOverridesAStrictRevisionHeader(t *testing.T) {
	screener := seededOverflowScreener(t, overflowConfig())
	request := &fwksched.InferenceRequest{Headers: map[string]string{"x-llm-d-disagg-revision": "v1"}}
	if got := screenedRevision(t, screener, request, overflowPool(testOverflowLimit, 0)); got != "v1" {
		t.Fatalf("pinned to v1: served by %s", got)
	}
}

func TestOverflowOffKeepsTheDrawnRevision(t *testing.T) {
	screener := seededOverflowScreener(t, prefixAffinityConfig(""))
	request := sessionOnV1(t, screener)
	if got := screenedRevision(t, screener, request, overflowPool(testOverflowLimit, 0)); got != "v1" {
		t.Fatalf("overflow off: served by %s, want v1", got)
	}
}

func TestOverflowReadsTheNamedProducer(t *testing.T) {
	config := overflowConfig()
	config.RevisionGating.Overflow.InFlightLoadProducerName = "prefill-load"
	screener := seededOverflowScreener(t, config)
	request := sessionOnV1(t, screener)
	pool := overflowPool(0, 0)
	named := attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName("prefill-load")
	for _, e := range pool {
		if e.GetMetadata().Labels[testRevLabel] == "v1" && e.GetMetadata().Labels[testRoleLabel] == "prefill" {
			e.Put(named, &attrconcurrency.InFlightLoad{Requests: testOverflowLimit})
		}
	}
	if got := screenedRevision(t, screener, request, pool); got != "v2" {
		t.Fatalf("named producer shows v1 full: served by %s, want v2", got)
	}
	if _, ok := screener.Consumes().Required[named]; !ok {
		t.Fatal("overflow must require the named producer's in-flight load")
	}
}

// A shrinking old revision holding long sessions is the GLM failure mode: flow
// control admits on the fleet average, then a fail-closed prefill filter finds
// the old revision full. With overflow, no request is sent to a full revision
// while the other one has room.
func TestOverflowSendsNoRequestToAFullRevisionWhileAnotherHasRoom(t *testing.T) {
	for _, tc := range []struct {
		name     string
		config   Config
		wantFull bool
	}{
		{"overflow off", prefixAffinityConfig(""), true},
		{"overflow on", overflowConfig(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			screener := seededOverflowScreener(t, tc.config)
			pool := overflowPool(testOverflowLimit, 2)
			toFull := 0
			for session := range 2000 {
				if screenedRevision(t, screener, sessionTurns(session)[0], pool) == "v1" {
					toFull++
				}
			}
			if (toFull > 0) != tc.wantFull {
				t.Fatalf("%d of 2000 requests sent to the full revision", toFull)
			}
		})
	}
}

func TestOverflowSpreadsOverTheRemainingRevisionsByShare(t *testing.T) {
	config := overflowConfig()
	config.RevisionGating.Mode = GatingModeMaxRole
	screener := newTestScreener(config)
	// a (full) holds 1 of 9 decode Pods; b and c hold 4 each.
	seedCounts(t, screener, map[string]map[string]int{
		"a": {"prefill": 1, "decode": 1},
		"b": {"prefill": 1, "decode": 4},
		"c": {"prefill": 1, "decode": 4},
	})
	pool := []fwksched.Endpoint{
		loadedEndpoint("a-prefill", "a", "prefill", testOverflowLimit),
		loadedEndpoint("b-prefill", "b", "prefill", 0),
		loadedEndpoint("c-prefill", "c", "prefill", 0),
	}
	counts := map[string]int{}
	for session := range 4000 {
		counts[screenedRevision(t, screener, sessionTurns(session)[0], pool)]++
	}
	if counts["a"] != 0 {
		t.Fatalf("%d requests sent to the full revision", counts["a"])
	}
	if b, c := float64(counts["b"]), float64(counts["c"]); b/(b+c) < 0.45 || b/(b+c) > 0.55 {
		t.Fatalf("overflow split b=%v c=%v, want about even", b, c)
	}
}

func TestDrawWithinRescalesTheChosenSlice(t *testing.T) {
	shares := map[string]float64{"a": 0.1, "b": 0.45, "c": 0.45}
	for _, tc := range []struct{ draw, want float64 }{{0, 0}, {0.05, 0.5}, {0.0999, 0.999}} {
		if got := drawWithin(shares, "a", tc.draw); math.Abs(got-tc.want) > 1e-6 {
			t.Fatalf("drawWithin(a, %v) = %v, want %v", tc.draw, got, tc.want)
		}
	}
	if got := drawWithin(shares, "b", 0.1); got != 0 {
		t.Fatalf("the start of b's slice must map to 0, got %v", got)
	}
}

func TestOverflowCountsOnlyWhenItServes(t *testing.T) {
	screener := seededOverflowScreener(t, overflowConfig())
	request := sessionOnV1(t, screener)
	request.Headers["x-llm-d-revision-decision-id"] = "parallel-1"
	// The first request of the decision ID stores v1 while v1 has room.
	if got := screenedRevision(t, screener, request, overflowPool(0, 0)); got != "v1" {
		t.Fatalf("first request served by %s, want v1", got)
	}
	before := testutil.ToFloat64(revisionOverflowTotal.WithLabelValues(PluginType, "test-screener", "v1", "v2"))
	// A sibling sees v1 full, but coordination keeps the stored v1.
	if got := screenedRevision(t, screener, request, overflowPool(testOverflowLimit, 0)); got != "v1" {
		t.Fatalf("coordinated sibling served by %s, want v1", got)
	}
	if after := testutil.ToFloat64(revisionOverflowTotal.WithLabelValues(PluginType, "test-screener", "v1", "v2")); after != before {
		t.Fatalf("an overflow overridden by coordination was counted: %v -> %v", before, after)
	}
}

func TestOverflowSkipsNilEndpoints(t *testing.T) {
	screener := seededOverflowScreener(t, overflowConfig())
	request := sessionOnV1(t, screener)
	pool := append([]fwksched.Endpoint{nil}, overflowPool(testOverflowLimit, 0)...)
	if got := screenedRevision(t, screener, request, pool); got != "v2" {
		t.Fatalf("served by %s, want v2", got)
	}
}

func TestOverflowMetricIsDeletedWithItsRevision(t *testing.T) {
	screener := seededOverflowScreener(t, overflowConfig())
	request := sessionOnV1(t, screener)
	revisionOverflowTotal.DeletePartialMatch(prometheus.Labels{"plugin_name": "test-screener"})
	screenedRevision(t, screener, request, overflowPool(testOverflowLimit, 0))
	if n := testutil.CollectAndCount(revisionOverflowTotal); n != 1 {
		t.Fatalf("want 1 overflow series, got %d", n)
	}
	screener.mu.RLock()
	var v1 []types.NamespacedName
	for key, pod := range screener.pods {
		if pod.revision == "v1" {
			v1 = append(v1, key)
		}
	}
	screener.mu.RUnlock()
	for _, key := range v1 {
		screener.removePod(key)
	}
	if n := testutil.CollectAndCount(revisionOverflowTotal); n != 0 {
		t.Fatalf("overflow series of a removed revision must be deleted, %d left", n)
	}
}
