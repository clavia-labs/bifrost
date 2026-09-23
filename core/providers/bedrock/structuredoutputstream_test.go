package bedrock

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectStructuredOutputResponsesStream serves the given Converse events and returns the
// Responses stream events of a json_schema request whose structured-output tool is bf_so_answer.
func collectStructuredOutputResponsesStream(t *testing.T, events [][2]string) []*schemas.BifrostResponsesStreamResponse {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		w.WriteHeader(http.StatusOK)
		for _, event := range events {
			writeEventStreamEvent(t, w, event[0], []byte(event[1]))
		}
	}))
	defer ts.Close()

	provider := newTestProviderWithServer(t, ts)
	ctx := testBedrockCtx()
	ctx.SetValue(schemas.BifrostContextKeyStructuredOutputToolName, "bf_so_answer")
	req := testResponsesRequest()
	req.Model = testConverseStreamModel
	streamChan, bifrostErr := provider.ResponsesStream(ctx, noopPostHookRunner, nil, testBedrockKey(), req)
	require.Nil(t, bifrostErr)

	var out []*schemas.BifrostResponsesStreamResponse
	for chunk := range streamChan {
		require.Nil(t, chunk.BifrostError)
		require.NotNil(t, chunk.BifrostResponsesStreamResponse)
		out = append(out, chunk.BifrostResponsesStreamResponse)
	}
	return out
}

// TestResponsesStreamStructuredOutputIsMessageItem verifies a structured-output tool block
// streams as a complete assistant message item, with suppressed reasoning taking no output
// index, whether the item closes on contentBlockStop or at the end of the stream.
func TestResponsesStreamStructuredOutputIsMessageItem(t *testing.T) {
	head := [][2]string{
		{"messageStart", `{"role":"assistant"}`},
		{"contentBlockDelta", `{"contentBlockIndex":0,"delta":{"reasoningContent":{"text":"thinking"}}}`},
		{"contentBlockDelta", `{"contentBlockIndex":0,"delta":{"reasoningContent":{"signature":"sig"}}}`},
		{"contentBlockStop", `{"contentBlockIndex":0}`},
		{"contentBlockStart", `{"contentBlockIndex":1,"start":{"toolUse":{"toolUseId":"tooluse_1","name":"bf_so_answer"}}}`},
		{"contentBlockDelta", `{"contentBlockIndex":1,"delta":{"toolUse":{"input":"{\"answer\""}}}`},
		{"contentBlockDelta", `{"contentBlockIndex":1,"delta":{"toolUse":{"input":":42}"}}}`},
	}
	tail := [][2]string{
		{"messageStop", `{"stopReason":"tool_use"}`},
		{"metadata", `{"usage":{"inputTokens":10,"outputTokens":5,"totalTokens":15},"metrics":{"latencyMs":1}}`},
	}
	withStop := append(append(append([][2]string{}, head...), [2]string{"contentBlockStop", `{"contentBlockIndex":1}`}), tail...)
	withoutStop := append(append([][2]string{}, head...), tail...)

	for name, events := range map[string][][2]string{"contentBlockStop": withStop, "endOfStream": withoutStop} {
		t.Run(name, func(t *testing.T) {
			responses := collectStructuredOutputResponsesStream(t, events)

			var types []schemas.ResponsesStreamResponseType
			for i, r := range responses {
				types = append(types, r.Type)
				assert.Equal(t, i, r.SequenceNumber, "sequence numbers must be contiguous")
			}
			require.Equal(t, []schemas.ResponsesStreamResponseType{
				schemas.ResponsesStreamResponseTypeCreated,
				schemas.ResponsesStreamResponseTypeInProgress,
				schemas.ResponsesStreamResponseTypeOutputItemAdded,
				schemas.ResponsesStreamResponseTypeContentPartAdded,
				schemas.ResponsesStreamResponseTypeOutputTextDelta,
				schemas.ResponsesStreamResponseTypeOutputTextDelta,
				schemas.ResponsesStreamResponseTypeOutputTextDone,
				schemas.ResponsesStreamResponseTypeContentPartDone,
				schemas.ResponsesStreamResponseTypeOutputItemDone,
				schemas.ResponsesStreamResponseTypeCompleted,
			}, types)

			added := responses[2]
			require.NotNil(t, added.Item)
			require.NotNil(t, added.Item.ID)
			itemID := *added.Item.ID
			assert.Equal(t, schemas.ResponsesMessageTypeMessage, *added.Item.Type)
			assert.Equal(t, schemas.ResponsesInputMessageRoleAssistant, *added.Item.Role)
			assert.Equal(t, "in_progress", *added.Item.Status)

			for _, r := range responses[2:9] {
				require.NotNil(t, r.OutputIndex, "%s must carry output_index", r.Type)
				assert.Equal(t, 0, *r.OutputIndex, "%s output_index", r.Type)
				if r.Type != schemas.ResponsesStreamResponseTypeOutputItemAdded && r.Type != schemas.ResponsesStreamResponseTypeOutputItemDone {
					require.NotNil(t, r.ItemID, "%s must carry item_id", r.Type)
					assert.Equal(t, itemID, *r.ItemID, "%s item_id", r.Type)
					require.NotNil(t, r.ContentIndex, "%s must carry content_index", r.Type)
					assert.Equal(t, 0, *r.ContentIndex, "%s content_index", r.Type)
				}
			}
			assert.Equal(t, `{"answer"`, *responses[4].Delta)
			assert.Equal(t, `:42}`, *responses[5].Delta)
			assert.Equal(t, `{"answer":42}`, *responses[6].Text)
			assert.Equal(t, `{"answer":42}`, *responses[7].Part.Text)

			done := responses[8].Item
			require.NotNil(t, done)
			assert.Equal(t, itemID, *done.ID)
			assert.Equal(t, "completed", *done.Status)
			require.Len(t, done.Content.ContentBlocks, 1)
			assert.Equal(t, schemas.ResponsesOutputMessageContentTypeText, done.Content.ContentBlocks[0].Type)
			assert.Equal(t, `{"answer":42}`, *done.Content.ContentBlocks[0].Text)

			completed := responses[9].Response
			require.NotNil(t, completed)
			require.Len(t, completed.Output, 1, "response.completed must carry the structured-output message")
			assert.Equal(t, itemID, *completed.Output[0].ID)
			assert.Equal(t, `{"answer":42}`, *completed.Output[0].Content.ContentBlocks[0].Text)
			require.NotNil(t, completed.StopReason)
			assert.Equal(t, string(schemas.BifrostFinishReasonStop), *completed.StopReason)
		})
	}
}
