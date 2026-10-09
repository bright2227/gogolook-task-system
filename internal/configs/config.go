package configs

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/delivery/rabbitmq"
)

type Config struct {
	DatabaseURL    string
	HTTPAddr       string
	Rabbit         rabbitmq.Config
	Workers        int
	RelayBatch     int
	RelayInterval  time.Duration
	ProcessTimeout time.Duration
	Lease          time.Duration
	RetryDelays    []time.Duration
	WorkerID       string
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL: env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/gogolook_tasks?sslmode=disable"),
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		Rabbit: rabbitmq.Config{
			URL:        env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			Queue:      env("TASK_QUEUE", rabbitmq.DefaultQueue),
			RetryQueue: env("RETRY_QUEUE", rabbitmq.DefaultRetryQueue),
			DeadQueue:  env("DLQ_QUEUE", rabbitmq.DefaultDeadQueue),
		},
		Workers: 8, RelayBatch: 32, RelayInterval: time.Second, ProcessTimeout: 10 * time.Second, Lease: 30 * time.Second,
	}
	var err error
	if c.RetryDelays, err = retryDelays(env("TASK_RETRY_DELAYS", "1s,10s,60s")); err != nil {
		return c, err
	}
	c.WorkerID = os.Getenv("POD_NAME")
	if c.WorkerID == "" {
		c.WorkerID, _ = os.Hostname()
	}
	if c.Workers, err = intEnv("WORKERS", c.Workers); err != nil {
		return c, err
	}
	if c.RelayBatch, err = intEnv("RELAY_BATCH", c.RelayBatch); err != nil {
		return c, err
	}
	if c.RelayInterval, err = durationEnv("RELAY_INTERVAL", c.RelayInterval); err != nil {
		return c, err
	}
	if c.ProcessTimeout, err = durationEnv("PROCESS_TIMEOUT", c.ProcessTimeout); err != nil {
		return c, err
	}
	if c.Lease, err = durationEnv("TASK_LEASE", c.Lease); err != nil {
		return c, err
	}
	if c.Rabbit.RetryDelay, err = durationEnv("RETRY_DELAY", 2*time.Second); err != nil {
		return c, err
	}
	if c.Workers < 1 || c.RelayBatch < 1 || c.RelayBatch > 1024 {
		return c, fmt.Errorf("WORKERS must be positive and RELAY_BATCH must be between 1 and 1024")
	}
	if c.ProcessTimeout <= 0 || c.Lease <= c.ProcessTimeout {
		return c, fmt.Errorf("TASK_LEASE must exceed positive PROCESS_TIMEOUT")
	}
	if c.RelayInterval <= 0 {
		return c, fmt.Errorf("RELAY_INTERVAL must be positive")
	}
	if c.Rabbit.RetryDelay < time.Millisecond || c.Rabbit.RetryDelay.Milliseconds() > 2147483647 {
		return c, fmt.Errorf("RETRY_DELAY must be at least 1ms and fit in 32-bit milliseconds")
	}
	c.Rabbit.DialTimeout = 3 * time.Second
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func intEnv(k string, def int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, e := strconv.Atoi(v)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return n, nil
}
func durationEnv(k string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	d, e := time.ParseDuration(v)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return d, nil
}

// retryDelays defines execution retries; legacy RETRY_DELAY is configuration compatibility only.
func retryDelays(raw string) ([]time.Duration, error) {
	if strings.TrimSpace(raw) == "none" {
		return []time.Duration{}, nil
	}
	parts := strings.Split(raw, ",")
	if len(parts) > 10 {
		return nil, fmt.Errorf("TASK_RETRY_DELAYS must contain at most 10 delays")
	}
	delays := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		delay, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || delay < time.Millisecond || delay > time.Hour {
			return nil, fmt.Errorf("TASK_RETRY_DELAYS values must be durations from 1ms to 1h")
		}
		if len(delays) > 0 && delay < delays[len(delays)-1] {
			return nil, fmt.Errorf("TASK_RETRY_DELAYS must be in non-decreasing order")
		}
		delays = append(delays, delay)
	}
	return delays, nil
}
