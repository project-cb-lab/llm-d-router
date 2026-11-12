package disagg

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

func TestAlwaysDisaggSelfPrefillSelector(t *testing.T) {
	const params = `{"selfPrefillSelector":"example.com/revision=aggregated"}`
	endpoint := func(revision string) scheduling.Endpoint {
		return scheduling.NewEndpoint(&fwkdl.EndpointMetadata{
			ID:     k8stypes.NamespacedName{Namespace: "default", Name: "pod"},
			Labels: map[string]string{bylabel.RoleLabel: bylabel.RoleDecode, "example.com/revision": revision},
		}, nil, fwkdl.NewAttributes())
	}
	tests := []struct {
		name     string
		params   string
		revision string
		want     bool
	}{
		{"no parameters", "", "aggregated", true},
		{"disaggregated decode pick", params, "abc123", true},
		{"aggregated pick", params, "aggregated", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var raw *json.Decoder
			if test.params != "" {
				raw = json.NewDecoder(strings.NewReader(test.params))
			}
			p, err := AlwaysDisaggPDDeciderPluginFactory("d", raw, nil)
			if err != nil {
				t.Fatal(err)
			}
			got := p.(*AlwaysDisaggPDDecider).disaggregate(context.Background(), nil, endpoint(test.revision))
			if got != test.want {
				t.Fatalf("disaggregate = %v, want %v", got, test.want)
			}
		})
	}

	if _, err := AlwaysDisaggPDDeciderPluginFactory("d", json.NewDecoder(strings.NewReader(`{"selfPrefillSelector":"=="}`)), nil); err == nil {
		t.Fatal("want an error for an invalid selector")
	}
}
