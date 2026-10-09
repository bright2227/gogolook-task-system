package runtime

import "testing"

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
