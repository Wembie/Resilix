package resilix

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModeCluster    Mode = "cluster"
	ModeSentinel   Mode = "sentinel"
)

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
	Jitter      float64
}

type CircuitBreakerConfig struct {
	FailureThreshold    int64
	OpenTimeout         time.Duration
	HalfOpenMaxRequests int64
}

type ObservabilityOptions struct {
	Logger             *slog.Logger
	Tracer             trace.Tracer
	Meter              metric.Meter
	Registry           *prometheus.Registry
	SlowQueryThreshold time.Duration
}

type Options struct {
	Name            string
	Mode            Mode
	Addrs           []string
	MasterName      string
	DB              int
	Username        string
	Password        string
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolSize        int
	MinIdleConns    int
	MaxConnAge      time.Duration
	PoolTimeout     time.Duration
	ConnMaxIdleTime time.Duration
	TLSConfig       *tls.Config
	Retry           RetryPolicy
	CircuitBreaker  CircuitBreakerConfig
	Observability   ObservabilityOptions
	Middlewares     []Middleware
	Hooks           HookSet
	Plugins         []Plugin
	MaxInflight     int
	RateLimiter     AdmissionController
}

func DefaultOptions() Options {
	return Options{
		Name:            "resilix-go",
		Mode:            ModeStandalone,
		Addrs:           []string{"127.0.0.1:6379"},
		DialTimeout:     2 * time.Second,
		ReadTimeout:     500 * time.Millisecond,
		WriteTimeout:    500 * time.Millisecond,
		PoolSize:        64,
		MinIdleConns:    8,
		MaxConnAge:      10 * time.Minute,
		PoolTimeout:     time.Second,
		ConnMaxIdleTime: 5 * time.Minute,
		Retry: RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   25 * time.Millisecond,
			MaxDelay:    750 * time.Millisecond,
			Multiplier:  2,
			Jitter:      0.2,
		},
		CircuitBreaker: CircuitBreakerConfig{
			FailureThreshold:    5,
			OpenTimeout:         5 * time.Second,
			HalfOpenMaxRequests: 1,
		},
		Observability: ObservabilityOptions{
			Logger:             slog.Default(),
			Registry:           prometheus.NewRegistry(),
			SlowQueryThreshold: 150 * time.Millisecond,
		},
		MaxInflight: 1024,
	}
}

func (o *Options) Normalize() error {
	defaults := DefaultOptions()

	if o.Name == "" {
		o.Name = defaults.Name
	}
	if o.Mode == "" {
		o.Mode = defaults.Mode
	}
	if len(o.Addrs) == 0 {
		o.Addrs = defaults.Addrs
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = defaults.DialTimeout
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = defaults.ReadTimeout
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = defaults.WriteTimeout
	}
	if o.PoolSize <= 0 {
		o.PoolSize = defaults.PoolSize
	}
	if o.MinIdleConns < 0 {
		o.MinIdleConns = defaults.MinIdleConns
	}
	if o.MaxConnAge <= 0 {
		o.MaxConnAge = defaults.MaxConnAge
	}
	if o.PoolTimeout <= 0 {
		o.PoolTimeout = defaults.PoolTimeout
	}
	if o.ConnMaxIdleTime <= 0 {
		o.ConnMaxIdleTime = defaults.ConnMaxIdleTime
	}
	if o.MaxInflight <= 0 {
		o.MaxInflight = defaults.MaxInflight
	}
	if o.Observability.Logger == nil {
		o.Observability.Logger = defaults.Observability.Logger
	}
	if o.Observability.Registry == nil {
		o.Observability.Registry = defaults.Observability.Registry
	}
	if o.Observability.SlowQueryThreshold <= 0 {
		o.Observability.SlowQueryThreshold = defaults.Observability.SlowQueryThreshold
	}
	if o.Retry.MaxAttempts <= 0 {
		o.Retry.MaxAttempts = defaults.Retry.MaxAttempts
	}
	if o.Retry.BaseDelay <= 0 {
		o.Retry.BaseDelay = defaults.Retry.BaseDelay
	}
	if o.Retry.MaxDelay <= 0 {
		o.Retry.MaxDelay = defaults.Retry.MaxDelay
	}
	if o.Retry.Multiplier < 1 {
		o.Retry.Multiplier = defaults.Retry.Multiplier
	}
	if o.Retry.Jitter < 0 {
		o.Retry.Jitter = defaults.Retry.Jitter
	}
	if o.CircuitBreaker.FailureThreshold <= 0 {
		o.CircuitBreaker.FailureThreshold = defaults.CircuitBreaker.FailureThreshold
	}
	if o.CircuitBreaker.OpenTimeout <= 0 {
		o.CircuitBreaker.OpenTimeout = defaults.CircuitBreaker.OpenTimeout
	}
	if o.CircuitBreaker.HalfOpenMaxRequests <= 0 {
		o.CircuitBreaker.HalfOpenMaxRequests = defaults.CircuitBreaker.HalfOpenMaxRequests
	}

	if err := o.validate(); err != nil {
		return err
	}
	return nil
}

func (o Options) validate() error {
	if len(o.Addrs) == 0 {
		return fmt.Errorf("%w: redis address is required", ErrInvalidConfiguration)
	}
	switch o.Mode {
	case ModeStandalone, ModeCluster, ModeSentinel:
	default:
		return ValidationError{Field: "mode", Message: "unsupported redis mode"}
	}
	if o.Mode == ModeSentinel && o.MasterName == "" {
		return ValidationError{Field: "master_name", Message: "master name is required for sentinel mode"}
	}
	return nil
}

func (o Options) universalOptions() *redis.UniversalOptions {
	return &redis.UniversalOptions{
		Addrs:           o.Addrs,
		MasterName:      o.MasterName,
		DB:              o.DB,
		Username:        o.Username,
		Password:        o.Password,
		DialTimeout:     o.DialTimeout,
		ReadTimeout:     o.ReadTimeout,
		WriteTimeout:    o.WriteTimeout,
		PoolSize:        o.PoolSize,
		MinIdleConns:    o.MinIdleConns,
		ConnMaxLifetime: o.MaxConnAge,
		PoolTimeout:     o.PoolTimeout,
		ConnMaxIdleTime: o.ConnMaxIdleTime,
		TLSConfig:       o.TLSConfig,
	}
}
