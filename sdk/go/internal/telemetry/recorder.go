package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type contextKey string

const correlationIDKey contextKey = "resilix-correlation-id"

type Recorder struct {
	logger             *slog.Logger
	tracer             trace.Tracer
	counter            *prometheus.CounterVec
	duration           *prometheus.HistogramVec
	inflight           prometheus.Gauge
	slowQueries        *prometheus.CounterVec
	slowQueryThreshold time.Duration
}

func New(serviceName string, logger *slog.Logger, tracer trace.Tracer, registry prometheus.Registerer, slowQueryThreshold time.Duration) *Recorder {
	if logger == nil {
		logger = slog.Default()
	}
	if tracer == nil {
		tracer = otel.Tracer(serviceName)
	}
	if registry == nil {
		registry = prometheus.DefaultRegisterer
	}
	if slowQueryThreshold <= 0 {
		slowQueryThreshold = 150 * time.Millisecond
	}

	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "resilix",
			Subsystem: "redis",
			Name:      "operations_total",
			Help:      "Total Redis operations executed by Resilix.",
		},
		[]string{"operation", "status"},
	)

	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "resilix",
			Subsystem: "redis",
			Name:      "operation_duration_seconds",
			Help:      "Redis operation duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	inflight := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "resilix",
			Subsystem: "redis",
			Name:      "inflight_operations",
			Help:      "Current number of inflight Redis operations.",
		},
	)

	slowQueries := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "resilix",
			Subsystem: "redis",
			Name:      "slow_queries_total",
			Help:      "Number of operations above slow-query threshold.",
		},
		[]string{"operation"},
	)

	counter = registerCounterVec(registry, counter)
	duration = registerHistogramVec(registry, duration)
	inflight = registerGauge(registry, inflight)
	slowQueries = registerCounterVec(registry, slowQueries)

	return &Recorder{
		logger:             logger,
		tracer:             tracer,
		counter:            counter,
		duration:           duration,
		inflight:           inflight,
		slowQueries:        slowQueries,
		slowQueryThreshold: slowQueryThreshold,
	}
}

func registerCounterVec(registry prometheus.Registerer, collector *prometheus.CounterVec) *prometheus.CounterVec {
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			if existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.CounterVec); ok {
				return existing
			}
		}
	}
	return collector
}

func registerHistogramVec(registry prometheus.Registerer, collector *prometheus.HistogramVec) *prometheus.HistogramVec {
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			if existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.HistogramVec); ok {
				return existing
			}
		}
	}
	return collector
}

func registerGauge(registry prometheus.Registerer, collector prometheus.Gauge) prometheus.Gauge {
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			if existing, ok := alreadyRegistered.ExistingCollector.(prometheus.Gauge); ok {
				return existing
			}
		}
	}
	return collector
}

func (r *Recorder) Begin(ctx context.Context, operation, kind, correlationID string) (context.Context, func(error)) {
	spanName := "redis." + strings.ToLower(operation)
	ctx, span := r.tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(
		attribute.String("db.system", "redis"),
		attribute.String("resilix.operation", operation),
		attribute.String("resilix.kind", kind),
	)
	if correlationID != "" {
		span.SetAttributes(attribute.String("resilix.correlation_id", correlationID))
	}

	start := time.Now()
	r.inflight.Inc()

	return ctx, func(err error) {
		elapsed := time.Since(start)
		status := "ok"
		if err != nil {
			status = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "ok")
		}

		r.counter.WithLabelValues(operation, status).Inc()
		r.duration.WithLabelValues(operation).Observe(elapsed.Seconds())
		r.inflight.Dec()

		if elapsed >= r.slowQueryThreshold {
			r.slowQueries.WithLabelValues(operation).Inc()
			r.logger.WarnContext(ctx, "slow redis operation",
				"operation", operation,
				"kind", kind,
				"duration", elapsed.String(),
				"correlation_id", correlationID,
			)
		}

		span.End()
	}
}

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey, correlationID)
}

func CorrelationIDFromContext(ctx context.Context) string {
	value, ok := ctx.Value(correlationIDKey).(string)
	if !ok {
		return ""
	}
	return value
}
