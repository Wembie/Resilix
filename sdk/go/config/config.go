package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	resilix "github.com/Wembie/Resilix/sdk/go"
	"gopkg.in/yaml.v3"
)

type File struct {
	App           AppConfig           `json:"app" yaml:"app"`
	Redis         RedisConfig         `json:"redis" yaml:"redis"`
	Retry         RetryConfig         `json:"retry" yaml:"retry"`
	Breaker       BreakerConfig       `json:"breaker" yaml:"breaker"`
	Observability ObservabilityConfig `json:"observability" yaml:"observability"`
	Runtime       RuntimeConfig       `json:"runtime" yaml:"runtime"`
}

type AppConfig struct {
	Name string `json:"name" yaml:"name"`
}

type RedisConfig struct {
	Mode            string   `json:"mode" yaml:"mode"`
	Addrs           []string `json:"addrs" yaml:"addrs"`
	MasterName      string   `json:"master_name" yaml:"master_name"`
	DB              int      `json:"db" yaml:"db"`
	Username        string   `json:"username" yaml:"username"`
	Password        string   `json:"password" yaml:"password"`
	DialTimeout     string   `json:"dial_timeout" yaml:"dial_timeout"`
	ReadTimeout     string   `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout    string   `json:"write_timeout" yaml:"write_timeout"`
	PoolSize        int      `json:"pool_size" yaml:"pool_size"`
	MinIdleConns    int      `json:"min_idle_conns" yaml:"min_idle_conns"`
	MaxConnAge      string   `json:"max_conn_age" yaml:"max_conn_age"`
	PoolTimeout     string   `json:"pool_timeout" yaml:"pool_timeout"`
	ConnMaxIdleTime string   `json:"conn_max_idle_time" yaml:"conn_max_idle_time"`
}

type RetryConfig struct {
	MaxAttempts int     `json:"max_attempts" yaml:"max_attempts"`
	BaseDelay   string  `json:"base_delay" yaml:"base_delay"`
	MaxDelay    string  `json:"max_delay" yaml:"max_delay"`
	Multiplier  float64 `json:"multiplier" yaml:"multiplier"`
	Jitter      float64 `json:"jitter" yaml:"jitter"`
}

type BreakerConfig struct {
	FailureThreshold    int64  `json:"failure_threshold" yaml:"failure_threshold"`
	OpenTimeout         string `json:"open_timeout" yaml:"open_timeout"`
	HalfOpenMaxRequests int64  `json:"half_open_max_requests" yaml:"half_open_max_requests"`
}

type ObservabilityConfig struct {
	SlowQueryThreshold string `json:"slow_query_threshold" yaml:"slow_query_threshold"`
}

type RuntimeConfig struct {
	MaxInflight int `json:"max_inflight" yaml:"max_inflight"`
}

func Load(path string) (File, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}

	var cfg File
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		err = yaml.Unmarshal(payload, &cfg)
	case ".json":
		err = json.Unmarshal(payload, &cfg)
	default:
		return File{}, fmt.Errorf("resilix/config: unsupported config extension %s", filepath.Ext(path))
	}
	if err != nil {
		return File{}, err
	}

	return cfg, nil
}

func LoadOptions(path, envPrefix string, overrides map[string]any) (resilix.Options, error) {
	cfg, err := Load(path)
	if err != nil {
		return resilix.Options{}, err
	}

	cfg.ApplyEnv(envPrefix)
	cfg.ApplyOverrides(overrides)

	return cfg.BuildOptions()
}

func (f *File) ApplyEnv(prefix string) {
	if prefix == "" {
		prefix = "RESILIX"
	}

	if value := os.Getenv(prefix + "_APP_NAME"); value != "" {
		f.App.Name = value
	}
	if value := os.Getenv(prefix + "_REDIS_MODE"); value != "" {
		f.Redis.Mode = value
	}
	if value := os.Getenv(prefix + "_REDIS_ADDRS"); value != "" {
		f.Redis.Addrs = strings.Split(value, ",")
	}
	if value := os.Getenv(prefix + "_REDIS_MASTER_NAME"); value != "" {
		f.Redis.MasterName = value
	}
	if value := os.Getenv(prefix + "_REDIS_DB"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			f.Redis.DB = parsed
		}
	}
	if value := os.Getenv(prefix + "_REDIS_USERNAME"); value != "" {
		f.Redis.Username = value
	}
	if value := os.Getenv(prefix + "_REDIS_PASSWORD"); value != "" {
		f.Redis.Password = value
	}
	if value := os.Getenv(prefix + "_REDIS_POOL_SIZE"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			f.Redis.PoolSize = parsed
		}
	}
	if value := os.Getenv(prefix + "_RUNTIME_MAX_INFLIGHT"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			f.Runtime.MaxInflight = parsed
		}
	}
}

func (f *File) ApplyOverrides(overrides map[string]any) {
	for key, value := range overrides {
		switch strings.ToLower(key) {
		case "app.name":
			if cast, ok := value.(string); ok {
				f.App.Name = cast
			}
		case "redis.mode":
			if cast, ok := value.(string); ok {
				f.Redis.Mode = cast
			}
		case "redis.addrs":
			if cast, ok := value.([]string); ok {
				f.Redis.Addrs = cast
			}
		case "redis.password":
			if cast, ok := value.(string); ok {
				f.Redis.Password = cast
			}
		case "runtime.max_inflight":
			switch cast := value.(type) {
			case int:
				f.Runtime.MaxInflight = cast
			case int64:
				f.Runtime.MaxInflight = int(cast)
			}
		}
	}
}

func (f File) BuildOptions() (resilix.Options, error) {
	options := resilix.DefaultOptions()
	if f.App.Name != "" {
		options.Name = f.App.Name
	}
	if f.Redis.Mode != "" {
		options.Mode = resilix.Mode(f.Redis.Mode)
	}
	if len(f.Redis.Addrs) > 0 {
		options.Addrs = f.Redis.Addrs
	}
	options.MasterName = f.Redis.MasterName
	options.DB = f.Redis.DB
	options.Username = f.Redis.Username
	options.Password = f.Redis.Password
	options.PoolSize = f.Redis.PoolSize
	options.MinIdleConns = f.Redis.MinIdleConns
	options.MaxInflight = f.Runtime.MaxInflight
	options.Retry.MaxAttempts = f.Retry.MaxAttempts
	options.Retry.Multiplier = f.Retry.Multiplier
	options.Retry.Jitter = f.Retry.Jitter
	options.CircuitBreaker.FailureThreshold = f.Breaker.FailureThreshold
	options.CircuitBreaker.HalfOpenMaxRequests = f.Breaker.HalfOpenMaxRequests

	var err error
	if options.DialTimeout, err = parseDurationOrDefault(f.Redis.DialTimeout, options.DialTimeout); err != nil {
		return resilix.Options{}, err
	}
	if options.ReadTimeout, err = parseDurationOrDefault(f.Redis.ReadTimeout, options.ReadTimeout); err != nil {
		return resilix.Options{}, err
	}
	if options.WriteTimeout, err = parseDurationOrDefault(f.Redis.WriteTimeout, options.WriteTimeout); err != nil {
		return resilix.Options{}, err
	}
	if options.MaxConnAge, err = parseDurationOrDefault(f.Redis.MaxConnAge, options.MaxConnAge); err != nil {
		return resilix.Options{}, err
	}
	if options.PoolTimeout, err = parseDurationOrDefault(f.Redis.PoolTimeout, options.PoolTimeout); err != nil {
		return resilix.Options{}, err
	}
	if options.ConnMaxIdleTime, err = parseDurationOrDefault(f.Redis.ConnMaxIdleTime, options.ConnMaxIdleTime); err != nil {
		return resilix.Options{}, err
	}
	if options.Retry.BaseDelay, err = parseDurationOrDefault(f.Retry.BaseDelay, options.Retry.BaseDelay); err != nil {
		return resilix.Options{}, err
	}
	if options.Retry.MaxDelay, err = parseDurationOrDefault(f.Retry.MaxDelay, options.Retry.MaxDelay); err != nil {
		return resilix.Options{}, err
	}
	if options.CircuitBreaker.OpenTimeout, err = parseDurationOrDefault(f.Breaker.OpenTimeout, options.CircuitBreaker.OpenTimeout); err != nil {
		return resilix.Options{}, err
	}
	if options.Observability.SlowQueryThreshold, err = parseDurationOrDefault(f.Observability.SlowQueryThreshold, options.Observability.SlowQueryThreshold); err != nil {
		return resilix.Options{}, err
	}

	if err := options.Normalize(); err != nil {
		return resilix.Options{}, err
	}
	return options, nil
}

func parseDurationOrDefault(raw string, fallback time.Duration) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	return time.ParseDuration(raw)
}
