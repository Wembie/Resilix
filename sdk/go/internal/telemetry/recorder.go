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
	breakerState       *prometheus.GaugeVec
	poolHits           prometheus.Gauge
	poolMisses         prometheus.Gauge
	poolTimeouts       prometheus.Gauge
	poolConns          prometheus.Gauge
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
		[]string{"operation", "status", "error_type"},
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

	breakerState := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "resilix",
			Subsystem: "redis",
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (1 = active state, 0 = inactive). Labels: closed, open, half_open.",
		},
		[]string{"state"},
	)

	poolHits := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "resilix", Subsystem: "redis",
		Name: "pool_hits_total", Help: "Connection pool hits.",
	})
	poolMisses := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "resilix", Subsystem: "redis",
		Name: "pool_misses_total", Help: "Connection pool misses.",
	})
	poolTimeouts := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "resilix", Subsystem: "redis",
		Name: "pool_timeouts_total", Help: "Connection pool timeouts.",
	})
	poolConns := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "resilix", Subsystem: "redis",
		Name: "pool_total_conns", Help: "Total open connections in pool.",
	})

	counter = registerCounterVec(registry, counter)
	duration = registerHistogramVec(registry, duration)
	inflight = registerGauge(registry, inflight)
	slowQueries = registerCounterVec(registry, slowQueries)
	breakerState = registerGaugeVec(registry, breakerState)
	poolHits = registerGauge(registry, poolHits)
	poolMisses = registerGauge(registry, poolMisses)
	poolTimeouts = registerGauge(registry, poolTimeouts)
	poolConns = registerGauge(registry, poolConns)

	// Initialise breaker state labels so they appear in /metrics immediately.
	breakerState.WithLabelValues("closed").Set(1)
	breakerState.WithLabelValues("open").Set(0)
	breakerState.WithLabelValues("half_open").Set(0)

	return &Recorder{
		logger:             logger,
		tracer:             tracer,
		counter:            counter,
		duration:           duration,
		inflight:           inflight,
		slowQueries:        slowQueries,
		breakerState:       breakerState,
		poolHits:           poolHits,
		poolMisses:         poolMisses,
		poolTimeouts:       poolTimeouts,
		poolConns:          poolConns,
		slowQueryThreshold: slowQueryThreshold,
	}
}

// RecordBreakerState updates the circuit_breaker_state gauge for the new state.
func (r *Recorder) RecordBreakerState(state string) {
	r.breakerState.WithLabelValues("closed").Set(0)
	r.breakerState.WithLabelValues("open").Set(0)
	r.breakerState.WithLabelValues("half_open").Set(0)
	r.breakerState.WithLabelValues(state).Set(1)
}

// RecordPoolStats updates pool connection gauges from a PoolStats snapshot.
func (r *Recorder) RecordPoolStats(hits, misses, timeouts, total uint32) {
	r.poolHits.Set(float64(hits))
	r.poolMisses.Set(float64(misses))
	r.poolTimeouts.Set(float64(timeouts))
	r.poolConns.Set(float64(total))
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

func registerGaugeVec(registry prometheus.Registerer, collector *prometheus.GaugeVec) *prometheus.GaugeVec {
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			if existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.GaugeVec); ok {
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
		errorType := ""
		if err != nil {
			status = "error"
			errorType = classifyError(err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "ok")
		}

		r.counter.WithLabelValues(operation, status, errorType).Inc()
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

func classifyError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "connection"):
		return "connection"
	case strings.Contains(msg, "circuit"):
		return "circuit_open"
	case strings.Contains(msg, "key not found"):
		return "not_found"
	default:
		return "other"
	}
}
