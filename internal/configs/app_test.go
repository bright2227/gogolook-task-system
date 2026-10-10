package configs

import (
	"testing"
	"time"
)

func TestExecutionRetryPolicyValidation(t *testing.T) {
	for _, raw := range []string{"", "0s", "1ns", "2h", "10s,1s", "1s,", "bad"} {
		if _, err := retryDelays(raw); err == nil {
			t.Errorf("accepted invalid execution retry policy %q", raw)
		}
	}
	delays, err := retryDelays("1s,10s,60s")
	if err != nil || len(delays) != 3 || delays[0] != time.Second || delays[1] != 10*time.Second || delays[2] != time.Minute {
		t.Fatalf("unexpected default policy %v: %v", delays, err)
	}
	disabled, err := retryDelays("none")
	if err != nil || disabled == nil || len(disabled) != 0 {
		t.Fatalf("disabled retries must be an explicit nonnil empty policy: %v, %v", disabled, err)
	}
}

func TestConnectionPoolDefaultsAndValidation(t *testing.T) {
	keys := []string{"DATABASE_URL", "DB_MAX_OPEN_CONNS", "DB_MAX_IDLE_CONNS", "CONSUMERS", "SWEEPER_BATCH", "SWEEPER_INTERVAL", "PROCESS_TIMEOUT", "TASK_LEASE", "TASK_RETRY_DELAYS", "TASK_QUEUE_CHANNELS", "TASK_REPUBLISH_AFTER", "TASK_STALE_AFTER"}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DBMaxOpenConns != 4 || cfg.DBMaxIdleConns != 1 || cfg.QueueChannels != 8 || cfg.RepublishAfter != 0 || cfg.StaleAfter != 5*time.Minute {
		t.Fatalf("defaults: %+v", cfg)
	}
	t.Setenv("TASK_REPUBLISH_AFTER", "10m")
	// The smallest stale threshold for the default 10s PROCESS_TIMEOUT.
	t.Setenv("TASK_STALE_AFTER", "30s")
	if cfg, err := LoadConfig(); err != nil || cfg.RepublishAfter != 10*time.Minute || cfg.StaleAfter != 30*time.Second {
		t.Fatalf("recovery overrides=%+v,%v", cfg, err)
	}
	t.Setenv("PROCESS_TIMEOUT", "1m")
	t.Setenv("TASK_STALE_AFTER", "130s")
	if cfg, err := LoadConfig(); err != nil || cfg.StaleAfter != 130*time.Second {
		t.Fatalf("stale threshold for a longer timeout=%+v,%v", cfg, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"DB_MAX_OPEN_CONNS", "0"}, {"DB_MAX_OPEN_CONNS", "bad"},
		{"DB_MAX_IDLE_CONNS", "-1"}, {"DB_MAX_IDLE_CONNS", "5"},
		{"TASK_QUEUE_CHANNELS", "0"}, {"TASK_QUEUE_CHANNELS", "bad"},
		{"TASK_REPUBLISH_AFTER", "-1s"}, {"TASK_REPUBLISH_AFTER", "bad"},
		{"TASK_STALE_AFTER", "29s"}, {"TASK_STALE_AFTER", "10s"}, {"TASK_STALE_AFTER", "1s"}, {"TASK_STALE_AFTER", "-1m"}, {"TASK_STALE_AFTER", "bad"}, {"PROCESS_TIMEOUT", "5m"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			for _, key := range keys {
				t.Setenv(key, "")
			}
			t.Setenv(tc.key, tc.value)
			if _, err := LoadConfig(); err == nil {
				t.Fatalf("accepted %s=%s", tc.key, tc.value)
			}
		})
	}
}
