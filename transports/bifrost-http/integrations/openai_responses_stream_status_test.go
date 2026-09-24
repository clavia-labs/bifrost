package integrations

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// streamStatusAccount serves OpenAI from one key at baseURL, without retries.
type streamStatusAccount struct{ baseURL string }

func (streamStatusAccount) GetConfiguredProviders() ([]schemas.ModelProvider, error) {
	return []schemas.ModelProvider{schemas.OpenAI}, nil
}

func (streamStatusAccount) GetKeysForProvider(_ context.Context, _ schemas.ModelProvider) ([]schemas.Key, error) {
	return []schemas.Key{{ID: "openai-key", Value: *schemas.NewSecretVar("sk-test"), Models: schemas.WhiteList{"*"}, Weight: 1}}, nil
}

func (a streamStatusAccount) GetConfigForProvider(_ schemas.ModelProvider) (*schemas.ProviderConfig, error) {
	network := schemas.DefaultNetworkConfig
	network.BaseURL = a.baseURL
	network.MaxRetries = 0
	return &schemas.ProviderConfig{NetworkConfig: network, ConcurrencyAndBufferSize: schemas.DefaultConcurrencyAndBufferSize}, nil
}

// A provider that refuses a streamed Responses request answers HTTP 200 and sends the
// refusal as the stream's error event before any output. The OpenAI Responses route
// returns that refusal as the HTTP response, which must carry the status the provider
// gives the same refusal on a non-stream request, with the provider's error intact.
func TestOpenAIResponsesStreamRefusalStatus(t *testing.T) {
	preamble := []string{
		`{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}`,
		`{"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}`,
	}
	cases := []struct {
		name       string
		events     []string
		wantStatus int
		wantType   string
		wantCode   string
		wantMsg    string
	}{
		{
			name:       "flex capacity refusal",
			events:     []string{`{"type":"error","sequence_number":0,"error":{"type":"invalid_request_error","code":"rate_limit_exceeded","message":"We're currently processing too many requests — please try again later.","param":null}}`},
			wantStatus: fasthttp.StatusTooManyRequests,
			wantType:   "invalid_request_error",
			wantCode:   "rate_limit_exceeded",
			wantMsg:    "We're currently processing too many requests — please try again later.",
		},
		{
			name:       "capacity refusal after startup events",
			events:     append(append([]string{}, preamble...), `{"type":"error","sequence_number":2,"error":{"type":"invalid_request_error","code":"rate_limit_exceeded","message":"We're currently processing too many requests — please try again later.","param":null}}`),
			wantStatus: fasthttp.StatusTooManyRequests,
			wantType:   "invalid_request_error",
			wantCode:   "rate_limit_exceeded",
			wantMsg:    "We're currently processing too many requests — please try again later.",
		},
		{
			name:       "server failure",
			events:     append(append([]string{}, preamble...), `{"type":"response.failed","sequence_number":2,"response":{"id":"resp_1","object":"response","status":"failed","error":{"code":"server_error","message":"The server had an error while processing your request."}}}`),
			wantStatus: fasthttp.StatusInternalServerError,
			wantCode:   "server_error",
			wantMsg:    "The server had an error while processing your request.",
		},
		{
			name:       "request rejection",
			events:     []string{`{"type":"error","sequence_number":0,"error":{"type":"invalid_request_error","code":"invalid_prompt","message":"Invalid prompt.","param":null}}`},
			wantStatus: fasthttp.StatusBadRequest,
			wantType:   "invalid_request_error",
			wantCode:   "invalid_prompt",
			wantMsg:    "Invalid prompt.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				for _, event := range tc.events {
					fmt.Fprintf(w, "data: %s\n\n", event)
				}
			}))
			defer upstream.Close()

			logger := bifrost.NewNoOpLogger()
			client, err := bifrost.Init(context.Background(), schemas.BifrostConfig{Account: streamStatusAccount{baseURL: upstream.URL}, Logger: logger})
			require.NoError(t, err)
			defer client.Shutdown()

			var route RouteConfig
			for _, candidate := range CreateOpenAIRouteConfigs("/openai", &mockHandlerStore{}) {
				if candidate.Path == "/openai/v1/responses" {
					route = candidate
				}
			}
			require.NotNil(t, route.StreamConfig, "OpenAI Responses route")

			parent, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			bifrostCtx := schemas.NewBifrostContext(parent, schemas.NoDeadline)
			request := &schemas.BifrostRequest{ResponsesRequest: &schemas.BifrostResponsesRequest{
				Provider: schemas.OpenAI,
				Model:    "gpt-6-luna",
				Input: []schemas.ResponsesMessage{{
					Role:    schemas.Ptr(schemas.ResponsesInputMessageRoleUser),
					Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr("hi")},
				}},
			}}

			httpCtx := &fasthttp.RequestCtx{}
			router := NewGenericRouter(client, &mockHandlerStore{}, nil, nil, nil, logger)
			router.handleStreamingRequest(httpCtx, route, request, bifrostCtx, cancel)

			require.Equal(t, tc.wantStatus, httpCtx.Response.StatusCode(), "body: %s", httpCtx.Response.Body())
			var body struct {
				Error struct {
					Type    *string `json:"type"`
					Code    *string `json:"code"`
					Message string  `json:"message"`
				} `json:"error"`
				ExtraFields struct {
					Provider schemas.ModelProvider `json:"provider"`
				} `json:"extra_fields"`
			}
			require.NoError(t, sonic.Unmarshal(httpCtx.Response.Body(), &body))
			if tc.wantType != "" {
				require.Equal(t, tc.wantType, *body.Error.Type)
			}
			require.Equal(t, tc.wantCode, *body.Error.Code)
			require.Equal(t, tc.wantMsg, body.Error.Message)
			require.Equal(t, schemas.OpenAI, body.ExtraFields.Provider)
		})
	}
}
