package integrations

import (
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
)

// TestPydanticResponsesStreamSkipsFilteredEvents verifies an event that WithDefaults
// filters from the OpenAI-format stream yields a nil response, which the router skips,
// rather than a typed nil pointer that serializes as "null".
func TestPydanticResponsesStreamSkipsFilteredEvents(t *testing.T) {
	routes := withPydanticResponsesNullNormalization([]RouteConfig{{
		Path: "/v1/responses",
		StreamConfig: &StreamConfig{
			ResponsesStreamResponseConverter: func(*schemas.BifrostContext, *schemas.BifrostResponsesStreamResponse) (string, interface{}, error) {
				return "", nil, nil
			},
		},
	}})
	convert := routes[0].StreamConfig.ResponsesStreamResponseConverter

	_, converted, err := convert(nil, &schemas.BifrostResponsesStreamResponse{Type: schemas.ResponsesStreamResponseTypePing})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if converted != nil {
		t.Errorf("converted = %#v, want nil so the router skips the event", converted)
	}
}
