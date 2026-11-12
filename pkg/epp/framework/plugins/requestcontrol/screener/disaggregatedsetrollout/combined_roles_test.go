package disaggregatedsetrollout

import "testing"

func TestCombinedRolesCoverAndWeigh(t *testing.T) {
	combined := map[string][]string{"prefill-decode": {"prefill", "decode"}}
	counts := map[string]map[string]int{}
	for range 12 {
		incrementRoleCount(counts, podInfo{revision: "aggregated", role: "prefill-decode"}, combined)
	}
	incrementRoleCount(counts, podInfo{revision: "pd", role: "prefill"}, combined)
	incrementRoleCount(counts, podInfo{revision: "pd", role: "decode"}, combined)
	incrementRoleCount(counts, podInfo{revision: "pd", role: "decode"}, combined)

	dist := newRevisionDistribution(counts, []string{"prefill", "decode"}, GatingModeMaxRole)
	if got, want := dist.shares["pd"], 2.0/14.0; got < want-1e-9 || got > want+1e-9 {
		t.Fatalf("pd share = %v, want %v (decode pods 2 of 14)", got, want)
	}
	if got, want := dist.shares["aggregated"], 12.0/14.0; got < want-1e-9 || got > want+1e-9 {
		t.Fatalf("aggregated share = %v, want %v", got, want)
	}
}
