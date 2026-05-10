package langfuseotel

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"runtime/debug"
	"sort"
	"time"

	"github.com/bytedance/sonic"
	aclotel "github.com/hungryTechBoy/eino-ext/libs/acl/opentelemetry"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/schema"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const scopeName = "github.com/hungryTechBoy/eino-ext/callbacks/langfuseotel"

type Config struct {
	Host      string
	PublicKey string
	SecretKey string
	Name      string
	UserID    string
	SessionID string
	Release   string
	Tags      []string
	Public    bool
}

type CallbackHandler struct {
	provider  *aclotel.OtelProvider
	tracer    trace.Tracer
	name      string
	userID    string
	sessionID string
	release   string
	tags      []string
	public    bool
}

type stateKey struct{}

type callbackState struct {
	span      trace.Span
	inputDone chan struct{}
}

var newOpenTelemetryProvider = aclotel.NewOpenTelemetryProvider

func NewLangfuseOTELHandler(cfg *Config) (callbacks.Handler, func(ctx context.Context) error, error) {
	if cfg == nil {
		return nil, nil, errors.New("langfuse otel config is nil")
	}
	endpoint, urlPath, insecure, err := parseLangfuseHost(cfg.Host)
	if err != nil {
		return nil, nil, err
	}

	headers := map[string]string{
		"Authorization":                "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", cfg.PublicKey, cfg.SecretKey))),
		"x-langfuse-ingestion-version": "4",
	}

	providerOpts := []aclotel.Option{
		aclotel.WithEnableMetrics(false),
		aclotel.WithExportProtocolHTTP(),
		aclotel.WithExportEndpoint(endpoint),
		aclotel.WithTraceExportURLPath(urlPath),
		aclotel.WithHeaders(headers),
		aclotel.WithServiceName(nonEmpty(cfg.Name, "langfuse-otel")),
	}
	if insecure {
		providerOpts = append(providerOpts, aclotel.WithInsecure())
	}
	if cfg.Release != "" {
		providerOpts = append(providerOpts, aclotel.WithResourceAttribute(semconv.ServiceVersionKey.String(cfg.Release)))
	}

	p, err := newOpenTelemetryProvider(providerOpts...)
	if err != nil {
		return nil, nil, err
	}
	if p == nil || p.TracerProvider == nil {
		return nil, nil, errors.New("tracer provider is nil")
	}

	handler := &CallbackHandler{
		provider:  p,
		tracer:    p.TracerProvider.Tracer(scopeName),
		name:      cfg.Name,
		userID:    cfg.UserID,
		sessionID: cfg.SessionID,
		release:   cfg.Release,
		tags:      cfg.Tags,
		public:    cfg.Public,
	}

	return handler, p.Shutdown, nil
}

func (c *CallbackHandler) OnStart(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
	if info == nil {
		return ctx
	}

	ctx, state := c.startSpan(ctx, info)
	if state == nil {
		return ctx
	}
	c.setInputAttributes(state.span, info, input, ctx.Value(stateKey{}) == nil)
	return ctx
}

func (c *CallbackHandler) OnEnd(ctx context.Context, info *callbacks.RunInfo, output callbacks.CallbackOutput) context.Context {
	if info == nil {
		return ctx
	}
	state, ok := ctx.Value(stateKey{}).(*callbackState)
	if !ok || state == nil {
		return ctx
	}
	c.setOutputAttributes(state.span, info, output, ctx.Value(stateKey{}) == nil)
	state.span.End(trace.WithTimestamp(time.Now()))
	return ctx
}

func (c *CallbackHandler) OnError(ctx context.Context, info *callbacks.RunInfo, err error) context.Context {
	if info == nil {
		return ctx
	}
	state, ok := ctx.Value(stateKey{}).(*callbackState)
	if !ok || state == nil {
		return ctx
	}
	state.span.RecordError(err)
	state.span.SetStatus(codes.Error, err.Error())
	state.span.SetAttributes(attribute.String("eino.error", err.Error()))
	state.span.End(trace.WithTimestamp(time.Now()))
	return ctx
}

func (c *CallbackHandler) OnStartWithStreamInput(ctx context.Context, info *callbacks.RunInfo, input *schema.StreamReader[callbacks.CallbackInput]) context.Context {
	if info == nil {
		return ctx
	}

	ctx, state := c.startSpan(ctx, info)
	if state == nil {
		return ctx
	}
	state.inputDone = make(chan struct{})
	go func() {
		defer func() {
			if e := recover(); e != nil {
				state.span.RecordError(fmt.Errorf("recover stream input panic: %v", e))
			}
			input.Close()
			close(state.inputDone)
		}()

		var inputs []callbacks.CallbackInput
		for {
			chunk, err := input.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				state.span.RecordError(err)
				state.span.SetStatus(codes.Error, err.Error())
				return
			}
			inputs = append(inputs, chunk)
		}
		c.setStreamInputAttributes(state.span, info, inputs)
	}()

	return ctx
}

func (c *CallbackHandler) OnEndWithStreamOutput(ctx context.Context, info *callbacks.RunInfo, output *schema.StreamReader[callbacks.CallbackOutput]) context.Context {
	if info == nil {
		return ctx
	}
	state, ok := ctx.Value(stateKey{}).(*callbackState)
	if !ok || state == nil {
		output.Close()
		return ctx
	}

	go func() {
		defer func() {
			if e := recover(); e != nil {
				state.span.RecordError(fmt.Errorf("recover stream output panic: %v, stack: %s", e, string(debug.Stack())))
				state.span.SetStatus(codes.Error, fmt.Sprintf("panic: %v", e))
			}
			output.Close()
			state.span.End(trace.WithTimestamp(time.Now()))
		}()

		if state.inputDone != nil {
			<-state.inputDone
		}

		var outputs []callbacks.CallbackOutput
		for {
			chunk, err := output.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				state.span.RecordError(err)
				state.span.SetStatus(codes.Error, err.Error())
				return
			}
			outputs = append(outputs, chunk)
		}
		c.setStreamOutputAttributes(state.span, info, outputs)
	}()

	return ctx
}

func (c *CallbackHandler) startSpan(ctx context.Context, info *callbacks.RunInfo) (context.Context, *callbackState) {
	spanName := getName(info)
	startTime := time.Now()
	isRoot := ctx.Value(stateKey{}) == nil
	ctx, span := c.tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindInternal), trace.WithTimestamp(startTime))
	span.SetAttributes(
		attribute.String("runinfo.name", info.Name),
		attribute.String("runinfo.type", info.Type),
		attribute.String("runinfo.component", string(info.Component)),
	)
	c.setTraceAttributes(ctx, span, spanName, isRoot)
	state := &callbackState{span: span}
	return context.WithValue(ctx, stateKey{}, state), state
}

func (c *CallbackHandler) setTraceAttributes(ctx context.Context, span trace.Span, curName string, isRoot bool) {
	if !isRoot {
		return
	}
	options := c.resolveTraceOptions(ctx, curName)
	if options.Name != "" {
		span.SetAttributes(attribute.String("langfuse.trace.name", options.Name))
	}
	if options.UserID != "" {
		span.SetAttributes(attribute.String("langfuse.user.id", options.UserID))
	}
	if options.SessionID != "" {
		span.SetAttributes(attribute.String("langfuse.session.id", options.SessionID))
	}
	if options.Release != "" {
		span.SetAttributes(attribute.String("langfuse.release", options.Release))
	}
	if len(options.Tags) > 0 {
		span.SetAttributes(attribute.StringSlice("langfuse.tags", options.Tags))
	}
	span.SetAttributes(attribute.Bool("langfuse.public", options.Public))
	if len(options.Metadata) > 0 {
		keys := make([]string, 0, len(options.Metadata))
		for key := range options.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			span.SetAttributes(attribute.String("langfuse.metadata."+key, options.Metadata[key]))
		}
	}
}

func (c *CallbackHandler) resolveTraceOptions(ctx context.Context, curName string) *traceOptions {
	options := &traceOptions{
		Name:      c.name,
		UserID:    c.userID,
		SessionID: c.sessionID,
		Release:   c.release,
		Tags:      c.tags,
		Public:    c.public,
	}
	if options.Name == "" {
		options.Name = curName
	}
	if traceOpts, ok := ctx.Value(traceOptionsKey{}).(*traceOptions); ok && traceOpts != nil {
		if traceOpts.Name != "" {
			options.Name = traceOpts.Name
		}
		if traceOpts.UserID != "" {
			options.UserID = traceOpts.UserID
		}
		if traceOpts.SessionID != "" {
			options.SessionID = traceOpts.SessionID
		}
		if traceOpts.Release != "" {
			options.Release = traceOpts.Release
		}
		if len(traceOpts.Tags) > 0 {
			options.Tags = traceOpts.Tags
		}
		if traceOpts.Public {
			options.Public = true
		}
		if len(traceOpts.Metadata) > 0 {
			options.Metadata = traceOpts.Metadata
		}
	}
	return options
}

func (c *CallbackHandler) setInputAttributes(span trace.Span, info *callbacks.RunInfo, input callbacks.CallbackInput, isRoot bool) {
	if info.Component == components.ComponentOfChatModel {
		config, messages, extra, err := extractModelInput(convModelCallbackInput([]callbacks.CallbackInput{input}))
		if err == nil {
			if config != nil {
				span.SetAttributes(attribute.String("gen_ai.request.model", config.Model))
			}
			c.setMessageAttributes(span, messages, isRoot)
			_ = extra
			return
		}
	}
	if raw, ok := marshalToString(input); ok {
		span.SetAttributes(attribute.String("langfuse.observation.input", raw))
		if isRoot {
			span.SetAttributes(attribute.String("langfuse.trace.input", raw))
		}
	}
}

func (c *CallbackHandler) setOutputAttributes(span trace.Span, info *callbacks.RunInfo, output callbacks.CallbackOutput, isRoot bool) {
	if info.Component == components.ComponentOfChatModel {
		usage, message, extra, err := extractModelOutput(convModelCallbackOutput([]callbacks.CallbackOutput{output}))
		if err == nil {
			c.setOutputMessageAttributes(span, message, isRoot)
			if usage != nil {
				span.SetAttributes(
					attribute.Int("gen_ai.usage.input_tokens", usage.PromptTokens),
					attribute.Int("gen_ai.usage.output_tokens", usage.CompletionTokens),
					attribute.Int("gen_ai.usage.total_tokens", usage.TotalTokens),
				)
			}
			_ = extra
			return
		}
	}
	if raw, ok := marshalToString(output); ok {
		span.SetAttributes(attribute.String("langfuse.observation.output", raw))
		if isRoot {
			span.SetAttributes(attribute.String("langfuse.trace.output", raw))
		}
	}
}

func (c *CallbackHandler) setStreamInputAttributes(span trace.Span, info *callbacks.RunInfo, inputs []callbacks.CallbackInput) {
	if info.Component == components.ComponentOfChatModel {
		config, messages, extra, err := extractModelInput(convModelCallbackInput(inputs))
		if err == nil {
			if config != nil {
				span.SetAttributes(attribute.String("gen_ai.request.model", config.Model))
			}
			c.setMessageAttributes(span, messages, true)
			_ = extra
			return
		}
	}
	if raw, ok := marshalToString(inputs); ok {
		span.SetAttributes(attribute.String("langfuse.observation.input", raw))
	}
}

func (c *CallbackHandler) setStreamOutputAttributes(span trace.Span, info *callbacks.RunInfo, outputs []callbacks.CallbackOutput) {
	if info.Component == components.ComponentOfChatModel {
		usage, message, extra, err := extractModelOutput(convModelCallbackOutput(outputs))
		if err == nil {
			c.setOutputMessageAttributes(span, message, true)
			if usage != nil {
				span.SetAttributes(
					attribute.Int("gen_ai.usage.input_tokens", usage.PromptTokens),
					attribute.Int("gen_ai.usage.output_tokens", usage.CompletionTokens),
					attribute.Int("gen_ai.usage.total_tokens", usage.TotalTokens),
				)
			}
			_ = extra
			return
		}
	}
	if raw, ok := marshalToString(outputs); ok {
		span.SetAttributes(attribute.String("langfuse.observation.output", raw))
	}
}

func (c *CallbackHandler) setMessageAttributes(span trace.Span, messages []*schema.Message, isRoot bool) {
	for i, message := range messages {
		if message == nil {
			continue
		}
		span.SetAttributes(
			attribute.String(fmt.Sprintf("gen_ai.prompt.%d.role", i), string(message.Role)),
			attribute.String(fmt.Sprintf("gen_ai.prompt.%d.content", i), message.Content),
		)
	}
	if raw, ok := marshalToString(messages); ok {
		span.SetAttributes(attribute.String("langfuse.observation.input", raw))
	}
	if isRoot {
		if raw, ok := marshalToString(messages); ok {
			span.SetAttributes(attribute.String("langfuse.trace.input", raw))
		}
	}
}

func (c *CallbackHandler) setOutputMessageAttributes(span trace.Span, message *schema.Message, isRoot bool) {
	if message == nil {
		return
	}
	observationOutput := extractObservationOutput(message)
	if observationOutput != "" {
		span.SetAttributes(attribute.String("langfuse.observation.output", observationOutput))
	}
	outputText := extractOutputText(message)
	if outputText != "" {
		if isRoot {
			span.SetAttributes(attribute.String("langfuse.trace.output", outputText))
		}
	}
	for key, value := range extractObservationMetadata(message) {
		span.SetAttributes(attribute.String("langfuse.observation.metadata."+key, value))
	}
	if isRoot {
		for key, value := range extractObservationMetadata(message) {
			span.SetAttributes(attribute.String("langfuse.trace.metadata."+key, value))
		}
	}
}

func marshalToString(v any) (string, bool) {
	raw, err := sonic.MarshalString(v)
	if err != nil {
		return "", false
	}
	return raw, true
}

func parseLangfuseHost(rawHost string) (endpoint string, urlPath string, insecure bool, err error) {
	host := rawHost
	if host == "" {
		host = "https://cloud.langfuse.com"
	}
	parsed, err := url.Parse(host)
	if err != nil {
		return "", "", false, fmt.Errorf("parse langfuse host failed: %w", err)
	}
	if parsed.Scheme == "" {
		parsed, err = url.Parse("https://" + host)
		if err != nil {
			return "", "", false, fmt.Errorf("parse langfuse host failed: %w", err)
		}
	}
	endpoint = parsed.Host
	insecure = parsed.Scheme == "http"
	urlPath = path.Join(parsed.Path, "/api/public/otel/v1/traces")
	if urlPath == "" {
		urlPath = "/api/public/otel/v1/traces"
	}
	return endpoint, urlPath, insecure, nil
}

func nonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
