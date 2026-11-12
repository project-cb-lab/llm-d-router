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

package disagg

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const (
	// AlwaysDisaggPDDeciderPluginType is the type-name of the alwaysDisaggPDDecider plugin.
	AlwaysDisaggPDDeciderPluginType = "always-disagg-pd-decider"
)

// compile-time type assertion
var _ deciderPlugin = &AlwaysDisaggPDDecider{}

// AlwaysDisaggPDDecider is a PD decider plugin which always decide to disaggregate PD,
// except when the picked decode endpoint matches SelfPrefillSelector: such an endpoint
// runs its own prefill (an aggregated engine), so the request is served decode-only.
type AlwaysDisaggPDDecider struct {
	typedName   plugin.TypedName
	selfPrefill labels.Selector
}

type alwaysDisaggPDDeciderParameters struct {
	// SelfPrefillSelector is a Kubernetes label selector over the decode endpoint's labels.
	SelfPrefillSelector string `json:"selfPrefillSelector,omitempty"`
}

// AlwaysDisaggPDDeciderPluginFactory defines the factory function for creating
// a new instance of the AlwaysDisaggPDDecider.
func AlwaysDisaggPDDeciderPluginFactory(name string, rawParameters *json.Decoder,
	_ plugin.Handle) (plugin.Plugin, error) {
	parameters := alwaysDisaggPDDeciderParameters{}
	if rawParameters != nil {
		if err := rawParameters.Decode(&parameters); err != nil {
			return nil, fmt.Errorf("failed to parse %s plugin config: %w", AlwaysDisaggPDDeciderPluginType, err)
		}
	}
	d := newAlwaysDisaggPDDecider().WithName(name)
	if parameters.SelfPrefillSelector != "" {
		selector, err := labels.Parse(parameters.SelfPrefillSelector)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid selfPrefillSelector: %w", AlwaysDisaggPDDeciderPluginType, err)
		}
		d.selfPrefill = selector
	}
	return d, nil
}

func newAlwaysDisaggPDDecider() *AlwaysDisaggPDDecider {
	return &AlwaysDisaggPDDecider{
		typedName: plugin.TypedName{Type: AlwaysDisaggPDDeciderPluginType},
	}
}

// TypedName returns the typed name of the plugin.
func (d *AlwaysDisaggPDDecider) TypedName() plugin.TypedName {
	return d.typedName
}

// WithName sets the name of the plugin.
func (d *AlwaysDisaggPDDecider) WithName(name string) *AlwaysDisaggPDDecider {
	d.typedName.Name = name
	return d
}

func (d *AlwaysDisaggPDDecider) disaggregate(ctx context.Context, request *scheduling.InferenceRequest, endpoint scheduling.Endpoint) bool {
	if d.selfPrefill == nil || endpoint == nil || endpoint.GetMetadata() == nil {
		return true
	}
	return !d.selfPrefill.Matches(labels.Set(endpoint.GetMetadata().Labels))
}
