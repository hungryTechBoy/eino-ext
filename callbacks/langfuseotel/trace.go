package langfuseotel

import "context"

type traceOptionsKey struct{}

func SetTrace(ctx context.Context, opts ...TraceOption) context.Context {
	options := &traceOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return context.WithValue(ctx, traceOptionsKey{}, options)
}

type TraceOption func(*traceOptions)

func WithName(name string) TraceOption {
	return func(o *traceOptions) { o.Name = name }
}

func WithUserID(userID string) TraceOption {
	return func(o *traceOptions) { o.UserID = userID }
}

func WithSessionID(sessionID string) TraceOption {
	return func(o *traceOptions) { o.SessionID = sessionID }
}

func WithRelease(release string) TraceOption {
	return func(o *traceOptions) { o.Release = release }
}

func WithTags(tags ...string) TraceOption {
	return func(o *traceOptions) { o.Tags = tags }
}

func WithPublic(public bool) TraceOption {
	return func(o *traceOptions) { o.Public = public }
}

func WithMetadata(metadata map[string]string) TraceOption {
	return func(o *traceOptions) { o.Metadata = metadata }
}

type traceOptions struct {
	Name      string
	UserID    string
	SessionID string
	Release   string
	Tags      []string
	Public    bool
	Metadata  map[string]string
}
