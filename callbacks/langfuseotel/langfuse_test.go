package langfuseotel

import (
	"context"
	"errors"
	"testing"
	"time"

	aclotel "github.com/hungryTechBoy/eino-ext/libs/acl/opentelemetry"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestParseLangfuseHost(t *testing.T) {
	endpoint, urlPath, insecure, err := parseLangfuseHost("https://jp.cloud.langfuse.com")
	assert.NoError(t, err)
	assert.Equal(t, "jp.cloud.langfuse.com", endpoint)
	assert.Equal(t, "/api/public/otel/v1/traces", urlPath)
	assert.False(t, insecure)
}

func TestLangfuseOTELCallbackGraph(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	originalFactory := newOpenTelemetryProvider
	defer func() { newOpenTelemetryProvider = originalFactory }()
	newOpenTelemetryProvider = func(opts ...aclotel.Option) (*aclotel.OtelProvider, error) {
		return &aclotel.OtelProvider{TracerProvider: provider}, nil
	}

	cbh, shutdown, err := NewLangfuseOTELHandler(&Config{Name: "insurance-miniprogram", Release: "test"})
	if err != nil {
		t.Fatalf("NewLangfuseOTELHandler() err = %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	callbacks.InitCallbackHandlers([]callbacks.Handler{cbh})
	ctx := context.Background()

	g := compose.NewGraph[string, string]()
	assert.NoError(t, g.AddLambdaNode("node1", compose.InvokableLambda(func(ctx context.Context, input string) (string, error) {
		return input, nil
	}), compose.WithNodeName("node1")))
	assert.NoError(t, g.AddLambdaNode("node2", compose.InvokableLambda(func(ctx context.Context, input string) (string, error) {
		return input + input, nil
	}), compose.WithNodeName("node2")))
	assert.NoError(t, g.AddEdge(compose.START, "node1"))
	assert.NoError(t, g.AddEdge("node1", "node2"))
	assert.NoError(t, g.AddEdge("node2", compose.END))

	runner, err := g.Compile(ctx)
	if err != nil {
		t.Fatalf("Compile() err = %v", err)
	}
	result, err := runner.Invoke(ctx, "input")
	if err != nil {
		t.Fatalf("Invoke() err = %v", err)
	}
	assert.Equal(t, "inputinput", result)

	spans := recorder.Ended()
	assert.Len(t, spans, 3)

	rootCount := 0
	childCount := 0
	names := make([]string, 0, len(spans))
	for _, span := range spans {
		names = append(names, span.Name())
		if !span.Parent().IsValid() {
			rootCount++
		} else {
			childCount++
		}
	}
	assert.Equal(t, 1, rootCount)
	assert.Equal(t, 2, childCount)
	assert.Contains(t, names, "node1")
	assert.Contains(t, names, "node2")
}

func TestLangfuseOTELCallbackError(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	handler := &CallbackHandler{tracer: provider.Tracer(scopeName), release: "test"}

	ctx := handler.OnStart(context.Background(), &callbacks.RunInfo{Name: "failing-node"}, map[string]string{"q": "err"})
	handler.OnError(ctx, &callbacks.RunInfo{Name: "failing-node"}, errors.New("boom"))

	spans := recorder.Ended()
	assert.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	assert.NotEmpty(t, spans[0].Events())
}

func TestLangfuseOTELCallbackStream(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	handler := &CallbackHandler{tracer: provider.Tracer(scopeName), release: "test"}

	insr, insw := schema.Pipe[callbacks.CallbackInput](2)
	insw.Send(&model.CallbackInput{Messages: []*schema.Message{{Role: schema.User, Content: "hello"}}}, nil)
	insw.Close()
	outsr, outsw := schema.Pipe[callbacks.CallbackOutput](2)
	outsw.Send(&model.CallbackOutput{Message: &schema.Message{Role: schema.Assistant, Content: "world"}}, nil)
	outsw.Close()

	runInfo := &callbacks.RunInfo{Name: "stream-node", Component: components.ComponentOfChatModel}
	ctx := handler.OnStartWithStreamInput(context.Background(), runInfo, insr)
	handler.OnEndWithStreamOutput(ctx, runInfo, outsr)

	waitForEndedSpans(t, recorder, 1)
	spans := recorder.Ended()
	assert.Len(t, spans, 1)
	assert.Equal(t, "stream-node", spans[0].Name())
	assert.True(t, hasAttributeKey(spans[0].Attributes(), "langfuse.trace.input"))
	assert.True(t, hasAttributeKey(spans[0].Attributes(), "langfuse.trace.output"))
	assert.False(t, hasAttributeKey(spans[0].Attributes(), "gen_ai.output"))
}

func TestExtractMessageOutput_PrefersGroundedAnswer(t *testing.T) {
	message := &schema.Message{
		Role:    schema.Assistant,
		Content: "",
		ToolCalls: []schema.ToolCall{{
			ID:   "call-1",
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "submit_grounded_answer",
				Arguments: `{"answer":"final answer","selected_chunk_ids":["a","b"]}`,
			},
		}},
	}

	got := extractOutputText(message)

	assert.Equal(t, "final answer", got)
}

func TestExtractObservationMetadata_StripsReasoningAndRawExtra(t *testing.T) {
	extra := map[string]any{
		"openai-request-id": "req-1",
		"reasoning-content": "hidden reasoning",
		"other":             map[string]any{"nested": true},
	}

	message := &schema.Message{Extra: extra}
	got := extractObservationMetadata(message)

	assert.Equal(t, "req-1", got["openai_request_id"])
	_, hasReasoning := got["reasoning_content"]
	assert.False(t, hasReasoning)
	_, hasOther := got["other"]
	assert.False(t, hasOther)
}

func TestExtractObservationOutput_SerializesToolCalls(t *testing.T) {
	message := &schema.Message{
		Role:             schema.Assistant,
		Content:          "partial answer",
		ReasoningContent: "think step",
		ResponseMeta:     &schema.ResponseMeta{FinishReason: "tool_calls"},
		ToolCalls: []schema.ToolCall{{
			ID:   "call-1",
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "retrieve_knowledge",
				Arguments: `{"queries":["BCIM3等候期多久"]}`,
			},
		}},
	}

	raw := extractObservationOutput(message)

	assert.NotEmpty(t, raw)
	assert.Contains(t, raw, `"content":"partial answer"`)
	assert.Contains(t, raw, `"reasoning_content":"think step"`)
	assert.Contains(t, raw, `"tool_calls"`)
	assert.Contains(t, raw, `"retrieve_knowledge"`)
	assert.Contains(t, raw, `"queries"`)
	assert.Contains(t, raw, `"finish_reason":"tool_calls"`)
}

func waitForEndedSpans(t *testing.T, recorder *tracetest.SpanRecorder, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(recorder.Ended()) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("ended spans = %d, want at least %d", len(recorder.Ended()), want)
}

func hasAttributeKey(attrs []attribute.KeyValue, key string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return true
		}
	}
	return false
}
