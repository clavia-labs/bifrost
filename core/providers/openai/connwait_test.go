package openai

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

// heldResponsesSSEServer serves a Responses stream that emits response.created
// and then holds the connection open until release is closed, when it emits
// response.completed.
func heldResponsesSSEServer(t *testing.T, release <-chan struct{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server ResponseWriter is not an http.Flusher")
			return
		}
		_, _ = w.Write([]byte(`data: {"type":"response.created","sequence_number":0,"response":{"id":"r1","object":"response","created_at":1,"model":"held-model","status":"in_progress"}}` + "\n\n"))
		flusher.Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`data: {"type":"response.completed","sequence_number":1,"response":{"id":"r1","object":"response","created_at":1,"model":"held-model","status":"completed","output":[]}}` + "\n\n"))
		flusher.Flush()
	}))
}

func heldResponsesRequest() *schemas.BifrostResponsesRequest {
	return &schemas.BifrostResponsesRequest{
		Provider: schemas.OpenAI,
		Model:    "held-model",
		Input: []schemas.ResponsesMessage{{
			Type:    schemas.Ptr(schemas.ResponsesMessageTypeMessage),
			Role:    schemas.Ptr(schemas.ResponsesInputMessageRoleUser),
			Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr("hi")},
		}},
	}
}

// TestResponsesStreamFailsFastWhenPoolExhausted verifies that with
// max_conns_per_host=1 and max_conn_wait_timeout_in_seconds=0 a second
// concurrent stream fails immediately with a fallback-eligible error while the
// first stream holds the only connection, and that the connection is usable
// again once the first stream completes.
func TestResponsesStreamFailsFastWhenPoolExhausted(t *testing.T) {
	release := make(chan struct{})
	server := heldResponsesSSEServer(t, release)
	defer server.Close()
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	provider := NewOpenAIProvider(&schemas.ProviderConfig{
		NetworkConfig: schemas.NetworkConfig{
			BaseURL:                        server.URL,
			DefaultRequestTimeoutInSeconds: 10,
			MaxConnsPerHost:                1,
			MaxConnWaitTimeoutInSeconds:    schemas.Ptr(0),
		},
	}, testNoopLogger{})

	first, bifrostErr := provider.ResponsesStream(newStreamTestContext(), passthroughPostHook, nil, testKey(), heldResponsesRequest())
	if bifrostErr != nil {
		t.Fatalf("first stream setup failed: %v", bifrostErr.GetErrorString())
	}
	select {
	case chunk := <-first:
		if chunk == nil || chunk.BifrostError != nil {
			t.Fatalf("first stream: expected response.created, got %+v", chunk)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first stream: timed out waiting for response.created")
	}

	start := time.Now()
	second, secondErr := provider.ResponsesStream(newStreamTestContext(), passthroughPostHook, nil, testKey(), heldResponsesRequest())
	elapsed := time.Since(start)
	if secondErr == nil {
		close(release)
		released = true
		collectChunks(t, second)
		t.Fatal("second stream: expected a pool exhaustion error while the first stream holds the only connection")
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("second stream waited %v for a connection; want an immediate failure", elapsed)
	}
	if secondErr.Error == nil || secondErr.Error.Message != schemas.ErrProviderNoFreeConns {
		t.Errorf("second stream error message = %q, want %q", secondErr.GetErrorString(), schemas.ErrProviderNoFreeConns)
	}
	if secondErr.AllowFallbacks != nil && !*secondErr.AllowFallbacks {
		t.Error("pool exhaustion must allow fallbacks")
	}
	if secondErr.IsBifrostError {
		t.Error("pool exhaustion must have IsBifrostError=false so the retry loop classifies it")
	}

	close(release)
	released = true
	collectChunks(t, first)

	third, thirdErr := provider.ResponsesStream(newStreamTestContext(), passthroughPostHook, nil, testKey(), heldResponsesRequest())
	if thirdErr != nil {
		t.Fatalf("stream after the first completed failed: %v", thirdErr.GetErrorString())
	}
	for _, chunk := range collectChunks(t, third) {
		if chunk.BifrostError != nil {
			t.Fatalf("stream after the first completed returned an error chunk: %v", chunk.BifrostError.GetErrorString())
		}
	}
}

// TestMaxConnWaitTimeoutDefaultsToRequestTimeout verifies that an unset
// max_conn_wait_timeout_in_seconds keeps the request timeout as the pool wait on
// both the unary and the streaming client, and that 0 reaches both clients.
func TestMaxConnWaitTimeoutDefaultsToRequestTimeout(t *testing.T) {
	unset := NewOpenAIProvider(&schemas.ProviderConfig{
		NetworkConfig: schemas.NetworkConfig{DefaultRequestTimeoutInSeconds: 42},
	}, testNoopLogger{})
	if unset.client.MaxConnWaitTimeout != 42*time.Second || unset.streamingClient.MaxConnWaitTimeout != 42*time.Second {
		t.Errorf("unset: client=%v streaming=%v, want 42s on both", unset.client.MaxConnWaitTimeout, unset.streamingClient.MaxConnWaitTimeout)
	}

	zero := NewOpenAIProvider(&schemas.ProviderConfig{
		NetworkConfig: schemas.NetworkConfig{DefaultRequestTimeoutInSeconds: 42, MaxConnWaitTimeoutInSeconds: schemas.Ptr(0)},
	}, testNoopLogger{})
	if zero.client.MaxConnWaitTimeout != 0 || zero.streamingClient.MaxConnWaitTimeout != 0 {
		t.Errorf("zero: client=%v streaming=%v, want 0 on both", zero.client.MaxConnWaitTimeout, zero.streamingClient.MaxConnWaitTimeout)
	}
}
