package configs

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
)

type Config struct {
	DatabaseURL     string
	DBMaxOpenConns  int
	DBMaxIdleConns  int
	HTTPAddr        string
	Rabbit          rabbitmq.Config
	Consumers       int
	SweeperBatch    int
	SweeperInterval time.Duration
	ProcessTimeout  time.Duration
	// RetryDelays is the retry budget shared by network failures, per-task
	// deadlines and stale claims released by the sweeper.
	RetryDelays []time.Duration
	// QueueChannels sizes the server's pool of publication channels.
	QueueChannels int
	// RepublishAfter republishes submitted tasks still pending after this
	// long, a safety net for messages lost by the broker; 0 disables it.
	RepublishAfter time.Duration
	// StaleAfter releases running claims not updated for this long; it must be
	// at least 2*ProcessTimeout+10s.
	StaleAfter time.Duration
}

func LoadConfig() (Config, error) {
	c := Config{
		DatabaseURL:    env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/gogolook_tasks?sslmode=disable"),
		DBMaxOpenConns: 4,
		DBMaxIdleConns: 1,
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		Rabbit: rabbitmq.Config{
			URL:   env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			Queue: env("TASK_QUEUE", rabbitmq.DefaultQueue),
		},
		Consumers: 4, SweeperBatch: 8, SweeperInterval: time.Second, ProcessTimeout: 10 * time.Second, QueueChannels: rabbitmq.DefaultQueueChannels, StaleAfter: 5 * time.Minute,
	}
	var err error
	if c.RetryDelays, err = retryDelays(env("TASK_RETRY_DELAYS", "1s,10s,60s")); err != nil {
		return c, err
	}
	if c.Consumers, err = intEnv("CONSUMERS", c.Consumers); err != nil {
		return c, err
	}
	if c.DBMaxOpenConns, err = intEnv("DB_MAX_OPEN_CONNS", c.DBMaxOpenConns); err != nil {
		return c, err
	}
	if c.DBMaxIdleConns, err = intEnv("DB_MAX_IDLE_CONNS", c.DBMaxIdleConns); err != nil {
		return c, err
	}
	if c.SweeperBatch, err = intEnv("SWEEPER_BATCH", c.SweeperBatch); err != nil {
		return c, err
	}
	if c.SweeperInterval, err = durationEnv("SWEEPER_INTERVAL", c.SweeperInterval); err != nil {
		return c, err
	}
	if c.ProcessTimeout, err = durationEnv("PROCESS_TIMEOUT", c.ProcessTimeout); err != nil {
		return c, err
	}
	if c.QueueChannels, err = intEnv("TASK_QUEUE_CHANNELS", c.QueueChannels); err != nil {
		return c, err
	}
	if c.RepublishAfter, err = durationEnv("TASK_REPUBLISH_AFTER", c.RepublishAfter); err != nil {
		return c, err
	}
	if c.StaleAfter, err = durationEnv("TASK_STALE_AFTER", c.StaleAfter); err != nil {
		return c, err
	}
	if c.QueueChannels < 1 {
		return c, fmt.Errorf("TASK_QUEUE_CHANNELS must be positive")
	}
	if c.Consumers < 1 || c.SweeperBatch < 1 || c.SweeperBatch > 1024 {
		return c, fmt.Errorf("CONSUMERS must be positive and SWEEPER_BATCH must be between 1 and 1024")
	}
	if c.DBMaxOpenConns < 1 || c.DBMaxIdleConns < 0 || c.DBMaxIdleConns > c.DBMaxOpenConns {
		return c, fmt.Errorf("DB_MAX_OPEN_CONNS must be positive and DB_MAX_IDLE_CONNS must be between 0 and DB_MAX_OPEN_CONNS")
	}
	if c.ProcessTimeout <= 0 {
		return c, fmt.Errorf("PROCESS_TIMEOUT must be positive")
	}
	if c.SweeperInterval <= 0 {
		return c, fmt.Errorf("SWEEPER_INTERVAL must be positive")
	}
	if c.RepublishAfter < 0 {
		return c, fmt.Errorf("TASK_REPUBLISH_AFTER must not be negative")
	}
	// A live claim is updated when claimed and when its outcome commits: in
	// between it spends up to one PROCESS_TIMEOUT executing plus the outcome
	// commit, and claims are stamped with consumer clocks, not the database's.
	if c.StaleAfter < 2*c.ProcessTimeout+10*time.Second {
		return c, fmt.Errorf("TASK_STALE_AFTER must be at least 2*PROCESS_TIMEOUT+10s: a live claim spends up to one PROCESS_TIMEOUT executing plus its outcome commit without an update, and claims are stamped with consumer clocks, so a shorter threshold releases live claims")
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

// retryDelays bounds executor retries for network failures.
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
