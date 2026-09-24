package bifrost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

// End-to-end tests for the fallback eligibility policy: a request moves to the next
// provider only after a capacity or availability failure, a provider's own 4xx ends the
// chain with that provider's error, and an exhausted chain returns the last error.
//
// The topology mirrors a routing rule that sends OpenAI models to an OpenAI-compatible
// custom provider first and falls back to OpenAI: "openai-compatible" is the primary, OpenAI the
// first fallback, and a second OpenAI-compatible provider the last.

const (
	policyPrimary    = schemas.ModelProvider("openai-compatible")
	policyFallback   = schemas.OpenAI
	policyLastResort = schemas.ModelProvider("openai-backup")
)

// policyUpstream answers every request with status (an OpenAI error body) or, when status
// is 200, with a chat completion in the shape the request asked for.
type policyUpstream struct {
	status int
	hits   atomic.Int32
	server *httptest.Server
}

func newPolicyUpstream(t *testing.T, status int) *policyUpstream {
	t.Helper()
	u := &policyUpstream{status: status}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		if u.status != http.StatusOK {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(u.status)
			fmt.Fprintf(w, `{"error":{"message":"upstream answered %d","type":"upstream_%d","code":null}}`, u.status, u.status)
			return
		}
		var req struct {
			Stream bool `json:"stream"`
		}
		_ = json.Unmarshal(body, &req)
		if req.Stream {
			sseHandler(
				`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}`,
				`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
			)(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"c1","object":"chat.completion","model":"gpt-6-sol","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	t.Cleanup(u.server.Close)
	return u
}

type policyCase struct {
	name string
	// Upstream statuses for primary, fallback, last resort. 0 means the primary's only
	// key is disabled, so Bifrost refuses the attempt before dispatch.
	statuses [3]int
	// wantStatus 0 means the request succeeds.
	wantStatus   int
	wantProvider schemas.ModelProvider
	wantHits     [3]int32
}

var policyCases = []policyCase{
	{
		name:         "primary 4xx is returned without trying fallbacks",
		statuses:     [3]int{http.StatusBadRequest, http.StatusOK, http.StatusOK},
		wantStatus:   http.StatusBadRequest,
		wantProvider: policyPrimary,
		wantHits:     [3]int32{1, 0, 0},
	},
	{
		name:         "disabled primary key falls back and the fallback's 4xx reaches the caller",
		statuses:     [3]int{0, http.StatusBadRequest, http.StatusOK},
		wantStatus:   http.StatusBadRequest,
		wantProvider: policyFallback,
		wantHits:     [3]int32{0, 1, 0},
	},
	{
		name:         "disabled primary key falls back to a serving fallback",
		statuses:     [3]int{0, http.StatusOK, http.StatusOK},
		wantProvider: policyFallback,
		wantHits:     [3]int32{0, 1, 0},
	},
	{
		name:         "request timeout falls back",
		statuses:     [3]int{http.StatusRequestTimeout, http.StatusOK, http.StatusOK},
		wantProvider: policyFallback,
		wantHits:     [3]int32{1, 1, 0},
	},
	{
		name:         "capacity failures walk the chain and return the last error",
		statuses:     [3]int{http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusInternalServerError},
		wantStatus:   http.StatusInternalServerError,
		wantProvider: policyLastResort,
		wantHits:     [3]int32{1, 1, 1},
	},
	{
		name:         "fallback 4xx after a capacity failure ends the chain",
		statuses:     [3]int{http.StatusTooManyRequests, http.StatusBadRequest, http.StatusOK},
		wantStatus:   http.StatusBadRequest,
		wantProvider: policyFallback,
		wantHits:     [3]int32{1, 1, 0},
	},
}

func TestFallbackPolicy(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range policyCases {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, tc.name), func(t *testing.T) {
				runPolicyCase(t, tc, stream)
			})
		}
	}
}

func runPolicyCase(t *testing.T, tc policyCase, stream bool) {
	providers := [3]schemas.ModelProvider{policyPrimary, policyFallback, policyLastResort}
	upstreams := [3]*policyUpstream{}
	account := NewMockAccount()
	for i, provider := range providers {
		upstreams[i] = newPolicyUpstream(t, tc.statuses[i])
		account.AddProviderWithBaseURL(provider, 1, 1, upstreams[i].server.URL)
		account.configs[provider].NetworkConfig.MaxRetries = 0
		if provider != schemas.OpenAI {
			account.SetCustomProviderConfig(provider, &schemas.CustomProviderConfig{BaseProviderType: schemas.OpenAI})
		}
		key := schemas.Key{ID: string(provider) + "-key", Value: *schemas.NewSecretVar("sk-" + string(provider)), Models: schemas.WhiteList{"*"}, Weight: 100}
		if tc.statuses[i] == 0 {
			key.Enabled = Ptr(false)
		}
		account.SetKeysForProvider(provider, []schemas.Key{key})
	}
	client := newStreamTestClient(t, account)

	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(30*time.Second))
	req := &schemas.BifrostChatRequest{
		Provider: policyPrimary,
		Model:    "gpt-6-sol",
		Input: []schemas.ChatMessage{
			{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: Ptr("hi")}},
		},
		Fallbacks: []schemas.Fallback{
			{Provider: policyFallback, Model: "gpt-6-sol"},
			{Provider: policyLastResort, Model: "gpt-6-sol"},
		},
	}

	var bifrostErr *schemas.BifrostError
	var servedBy schemas.ModelProvider
	if stream {
		var ch chan *schemas.BifrostStreamChunk
		ch, bifrostErr = client.ChatCompletionStreamRequest(ctx, req)
		if bifrostErr == nil {
			for chunk := range ch {
				if chunk.BifrostError != nil {
					t.Fatalf("stream carried an error chunk: %s", chunk.BifrostError.GetErrorString())
				}
				if chunk.BifrostChatResponse != nil {
					servedBy = chunk.BifrostChatResponse.ExtraFields.Provider
				}
			}
		}
	} else {
		var resp *schemas.BifrostChatResponse
		resp, bifrostErr = client.ChatCompletionRequest(ctx, req)
		if resp != nil {
			servedBy = resp.ExtraFields.Provider
		}
	}

	for i, upstream := range upstreams {
		if got := upstream.hits.Load(); got != tc.wantHits[i] {
			t.Errorf("%s upstream hits = %d, want %d", providers[i], got, tc.wantHits[i])
		}
	}

	if tc.wantStatus == 0 {
		if bifrostErr != nil {
			t.Fatalf("request failed: %s", bifrostErr.GetErrorString())
		}
		if servedBy != tc.wantProvider {
			t.Fatalf("served by %q, want %q", servedBy, tc.wantProvider)
		}
		return
	}
	if bifrostErr == nil {
		t.Fatalf("request succeeded (served by %q), want HTTP %d from %s", servedBy, tc.wantStatus, tc.wantProvider)
	}
	gotStatus := 0
	if bifrostErr.StatusCode != nil {
		gotStatus = *bifrostErr.StatusCode
	}
	if gotStatus != tc.wantStatus {
		t.Errorf("status = %d, want %d (error: %s)", gotStatus, tc.wantStatus, bifrostErr.GetErrorString())
	}
	if bifrostErr.ExtraFields.Provider != tc.wantProvider {
		t.Errorf("error from %q, want %q (error: %s)", bifrostErr.ExtraFields.Provider, tc.wantProvider, bifrostErr.GetErrorString())
	}
}
