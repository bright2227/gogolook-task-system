package configs

import (
	"testing"
	"time"
)

func TestLoadRejectsRetryDelayOutsideMillisecondRange(t *testing.T) {
	for key, value := range map[string]string{
		"WORKERS": "", "RELAY_BATCH": "", "RELAY_INTERVAL": "",
		"PROCESS_TIMEOUT": "", "TASK_LEASE": "",
	} {
		t.Setenv(key, value)
	}
	for _, delay := range []string{"0s", "1ns", "2147483648ms"} {
		t.Run(delay, func(t *testing.T) {
			t.Setenv("RETRY_DELAY", delay)
			if _, err := Load(); err == nil {
				t.Fatalf("Load accepted RETRY_DELAY=%s", delay)
			}
		})
	}
}

func TestLoadAcceptsOneMillisecondRetryDelay(t *testing.T) {
	for key, value := range map[string]string{
		"WORKERS": "", "RELAY_BATCH": "", "RELAY_INTERVAL": "",
		"PROCESS_TIMEOUT": "", "TASK_LEASE": "",
	} {
		t.Setenv(key, value)
	}
	t.Setenv("RETRY_DELAY", "1ms")
	if _, err := Load(); err != nil {
		t.Fatalf("Load rejected minimum RETRY_DELAY: %v", err)
	}
}

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
