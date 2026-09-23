package utils

import (
	"fmt"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

func TestMaxConnWaitTimeout(t *testing.T) {
	requestTimeout := 30 * time.Second
	cases := []struct {
		name    string
		seconds *int
		want    time.Duration
	}{
		{"unset keeps the request timeout", nil, requestTimeout},
		{"zero fails immediately", schemas.Ptr(0), 0},
		{"positive waits that long", schemas.Ptr(5), 5 * time.Second},
		{"negative fails immediately", schemas.Ptr(-1), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MaxConnWaitTimeout(schemas.NetworkConfig{MaxConnWaitTimeoutInSeconds: tc.seconds}, requestTimeout)
			if got != tc.want {
				t.Fatalf("MaxConnWaitTimeout = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUpstreamConnectionErrorNoFreeConns pins the pool-exhaustion error shape:
// a specific message, IsBifrostError=false so the retry loop classifies it, and
// AllowFallbacks unset so fallbacks apply.
func TestUpstreamConnectionErrorNoFreeConns(t *testing.T) {
	err := NewBifrostUpstreamConnectionError(schemas.ErrProviderDoRequest, fmt.Errorf("wrapped: %w", fasthttp.ErrNoFreeConns))
	if err.Error == nil || err.Error.Message != schemas.ErrProviderNoFreeConns {
		t.Fatalf("message = %v, want %q", err.Error, schemas.ErrProviderNoFreeConns)
	}
	if err.IsBifrostError {
		t.Error("IsBifrostError must be false")
	}
	if err.AllowFallbacks != nil && !*err.AllowFallbacks {
		t.Error("AllowFallbacks must not be false")
	}

	other := NewBifrostUpstreamConnectionError(schemas.ErrProviderDoRequest, fasthttp.ErrConnectionClosed)
	if other.Error.Message != schemas.ErrProviderDoRequest {
		t.Fatalf("unrelated errors keep their message, got %q", other.Error.Message)
	}
}
