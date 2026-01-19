package otel

import (
	"github.com/flohansen/goqueue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type middleware struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

type middlewareConfig struct {
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
}

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

type Option func(*middlewareConfig)

func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(cfg *middlewareConfig) {
		cfg.provider = provider
	}
}

func WithTextMapPropagator(propagator propagation.TextMapPropagator) Option {
	return func(cfg *middlewareConfig) {
		cfg.propagator = propagator
	}
}

type metadataCarrier struct {
	metadata map[string]string
}

func (m *metadataCarrier) Get(key string) string {
	return m.metadata[key]
}

func (m *metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m.metadata))
	for k := range m.metadata {
		keys = append(keys, k)
	}
	return keys
}

func (m *metadataCarrier) Set(key string, value string) {
	m.metadata[key] = value
}
