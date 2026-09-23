package integrations

import (
	"context"
	"errors"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenAIResponsesStreamErrorConverter verifies a BifrostError becomes the flat OpenAI
// Responses error stream event, with code and param present as null when unset.
func TestOpenAIResponsesStreamErrorConverter(t *testing.T) {
	cases := []struct {
		name string
		err  *schemas.BifrostError
		want string
	}{
		{
			name: "code and message",
			err: &schemas.BifrostError{Error: &schemas.ErrorField{
				Type:    schemas.Ptr("invalid_request_error"),
				Code:    schemas.Ptr("rate_limit_exceeded"),
				Message: "We're currently processing too many requests",
			}},
			want: `{"type":"error","code":"rate_limit_exceeded","message":"We're currently processing too many requests","param":null}`,
		},
		{
			name: "string param",
			err:  &schemas.BifrostError{Error: &schemas.ErrorField{Message: "bad input", Param: "input"}},
			want: `{"type":"error","code":null,"message":"bad input","param":"input"}`,
		},
		{
			name: "non-string param and message from error",
			err:  &schemas.BifrostError{Error: &schemas.ErrorField{Error: errors.New("upstream closed"), Param: 3}},
			want: `{"type":"error","code":null,"message":"upstream closed","param":null}`,
		},
		{
			name: "no error field",
			err:  &schemas.BifrostError{},
			want: `{"type":"error","code":null,"message":"An error occurred while processing your request","param":null}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := sonic.Marshal(openAIResponsesStreamErrorConverter(nil, tc.err))
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, string(encoded))
		})
	}
}

// TestOpenAIResponsesRoutesStreamErrorEvent verifies every OpenAI route that streams
// Responses sends the OpenAI error event for a Responses request, while the deployments
// route keeps the BifrostError for its other request types.
func TestOpenAIResponsesRoutesStreamErrorEvent(t *testing.T) {
	bifrostErr := &schemas.BifrostError{Error: &schemas.ErrorField{Code: schemas.Ptr("rate_limit_exceeded"), Message: "slow down"}}
	responsesCtx := schemas.NewBifrostContext(context.WithValue(context.Background(), schemas.BifrostContextKeyHTTPRequestType, schemas.ResponsesRequest), schemas.NoDeadline)
	chatCtx := schemas.NewBifrostContext(context.WithValue(context.Background(), schemas.BifrostContextKeyHTTPRequestType, schemas.ChatCompletionRequest), schemas.NoDeadline)

	checked := 0
	for _, route := range CreateOpenAIRouteConfigs("/openai", &mockHandlerStore{}) {
		if route.StreamConfig == nil || route.StreamConfig.ResponsesStreamResponseConverter == nil {
			continue
		}
		checked++
		require.NotNil(t, route.StreamConfig.ErrorConverter, "%s %s", route.Method, route.Path)
		event, ok := route.StreamConfig.ErrorConverter(responsesCtx, bifrostErr).(*openAIResponsesStreamErrorEvent)
		require.True(t, ok, "%s %s must send the OpenAI Responses error event", route.Method, route.Path)
		assert.Equal(t, "error", event.Type)
		assert.Equal(t, "rate_limit_exceeded", *event.Code)
		assert.Equal(t, "slow down", event.Message)

		if route.StreamConfig.ChatStreamResponseConverter != nil {
			assert.Same(t, bifrostErr, route.StreamConfig.ErrorConverter(chatCtx, bifrostErr),
				"%s %s must keep chat stream errors unchanged", route.Method, route.Path)
		}
	}
	assert.GreaterOrEqual(t, checked, 7, "expected the deployments, responses and responses retrieve routes")
}
