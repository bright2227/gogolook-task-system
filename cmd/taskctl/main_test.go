package main

import (
	"io"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
)

func TestParseInspectCommand(t *testing.T) {
	got, err := parseCommand([]string{"dlq", "inspect", "--limit", "17", "--timeout", "45s"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if got.replay || got.inspectLimit != 17 || got.scanLimit != 17 || got.timeout != 45*time.Second {
		t.Fatalf("inspect command = %#v", got)
	}
}

func TestParseReplayRequiresAuditAndBoundedScan(t *testing.T) {
	messageID := string(domain.NewID())
	args := []string{
		"dlq", "replay", "--message-id", messageID, "--operator", "on-call",
		"--reason", "fixed upstream dependency", "--scan-limit", "1000", "--timeout", "30s",
	}
	got, err := parseCommand(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !got.replay || got.messageID != messageID || got.operator != "on-call" || got.reason == "" || got.scanLimit != 1000 {
		t.Fatalf("replay command = %#v", got)
	}
	if _, err := parseCommand([]string{"dlq", "replay", "--message-id", messageID, "--operator", "x", "--reason", "y", "--scan-limit", "1001"}, io.Discard); err == nil {
		t.Fatal("accepted a scan limit above the hard bound")
	}
	if _, err := parseCommand([]string{"dlq", "replay", "--message-id", messageID, "--operator", " ", "--reason", "y"}, io.Discard); err == nil {
		t.Fatal("accepted an empty operator")
	}
}
