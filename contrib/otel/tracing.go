package otel

import (
	"github.com/flohansen/goqueue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// middleware implements OpenTelemetry tracing for goqueue.
type middleware struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// middlewareConfig holds configuration options for the middleware.
type middlewareConfig struct {
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
}

// NewMiddleware creates a new OpenTelemetry tracing middleware for goqueue.
// Options can be provided to customize the tracer provider and propagator.
func NewMiddleware(opts ...Option) *middleware {
	cfg := &middlewareConfig{
		provider:   otel.GetTracerProvider(),
		propagator: otel.GetTextMapPropagator(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	return &middleware{
		tracer:     cfg.provider.Tracer("goqueue"),
		propagator: cfg.propagator,
	}
}

// Enqueue wraps the enqueue handler to start a new span and inject tracing
// context into the job metadata.
func (m *middleware) Enqueue(next goqueue.EnqueueHandler) goqueue.EnqueueHandler {
	return func(ectx goqueue.JobEnqueueContext) error {
		ctx, span := m.tracer.Start(ectx, "goqueue.enqueue")
		defer span.End()

		carrier := &metadataCarrier{metadata: make(map[string]string)}
		m.propagator.Inject(ctx, carrier)

		for k, v := range carrier.metadata {
			ectx.Metadata[k] = v
		}

		ectx = ectx.WithContext(ctx)
		return next(ectx)
	}
}

// Process wraps the process handler to extract tracing context from the job
// metadata and continue the trace.
func (m *middleware) Process(next goqueue.ProcessHandler) goqueue.ProcessHandler {
	return func(pctx goqueue.JobProcessContext) error {
		metadata := make(map[string]string)
		for k, v := range pctx.Metadata {
			if s, ok := v.(string); ok {
				metadata[k] = s
			}
		}

		carrier := &metadataCarrier{metadata: metadata}
		ctx := m.propagator.Extract(pctx, carrier)

		ctx, span := m.tracer.Start(ctx, "goqueue.process")
		defer span.End()

		pctx = pctx.WithContext(ctx)
		return next(pctx)
	}
}

// Option defines a configuration option for the middleware.
type Option func(*middlewareConfig)

// WithTracerProvider sets a custom TracerProvider for the middleware.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(cfg *middlewareConfig) {
		cfg.provider = provider
	}
}

// WithTextMapPropagator sets a custom TextMapPropagator for the middleware.
func WithTextMapPropagator(propagator propagation.TextMapPropagator) Option {
	return func(cfg *middlewareConfig) {
		cfg.propagator = propagator
	}
}

// metadataCarrier implements the TextMapCarrier interface for job metadata.
type metadataCarrier struct {
	metadata map[string]string
}

// Get retrieves a value from the metadata by key.
func (m *metadataCarrier) Get(key string) string {
	return m.metadata[key]
}

// Keys returns all keys in the metadata.
func (m *metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m.metadata))
	for k := range m.metadata {
		keys = append(keys, k)
	}
	return keys
}

// Set sets a key-value pair in the metadata.
func (m *metadataCarrier) Set(key string, value string) {
	m.metadata[key] = value
}
