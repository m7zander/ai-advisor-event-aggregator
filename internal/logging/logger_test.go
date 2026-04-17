package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestLoggerWritesInfoToStdoutAndErrorToStderr(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	logger := NewWithWriters(&stdout, &stderr)

	ctx := WithRequestID(context.Background(), "req-123")
	logger.Info(ctx, "http.events.completed", "http/events", "operation completed", Field{Key: "limit", Value: 10})
	logger.Error(ctx, "http.events.failed", "http/events", "operation failed", assertErr("boom"), Field{Key: "limit", Value: 10})

	if strings.TrimSpace(stdout.String()) == "" {
		t.Fatal("expected info log in stdout")
	}
	if strings.TrimSpace(stderr.String()) == "" {
		t.Fatal("expected error log in stderr")
	}

	var info map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &info); err != nil {
		t.Fatalf("decode info log: %v", err)
	}
	if info["level"] != "info" {
		t.Fatalf("unexpected info level: %v", info["level"])
	}
	if info["event"] != "http.events.completed" {
		t.Fatalf("unexpected info event: %v", info["event"])
	}
	if info["request_id"] != "req-123" {
		t.Fatalf("unexpected request_id: %v", info["request_id"])
	}

	var errorEntry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &errorEntry); err != nil {
		t.Fatalf("decode error log: %v", err)
	}
	if errorEntry["level"] != "error" {
		t.Fatalf("unexpected error level: %v", errorEntry["level"])
	}
	if errorEntry["event"] != "http.events.failed" {
		t.Fatalf("unexpected error event: %v", errorEntry["event"])
	}
	if errorEntry["error"] != "boom" {
		t.Fatalf("unexpected error text: %v", errorEntry["error"])
	}
}

func TestLoggerErrorWithContractIncludesMandatoryFields(t *testing.T) {
	var stderr bytes.Buffer
	logger := NewWithWriters(&bytes.Buffer{}, &stderr)

	logger.ErrorWithContract(
		context.Background(),
		"http.events.failed",
		"http/events",
		"operation failed",
		assertErr("boom"),
		ErrorContract{
			Failure:        "query_failed",
			Cause:          "database timeout",
			SanitizedInput: `{"limit":50}`,
			Reaction:       "returned 500",
		},
		Field{Key: "limit", Value: 50},
	)

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &entry); err != nil {
		t.Fatalf("decode error log: %v", err)
	}
	if entry["failure"] != "query_failed" {
		t.Fatalf("unexpected failure: %v", entry["failure"])
	}
	if entry["cause"] != "database timeout" {
		t.Fatalf("unexpected cause: %v", entry["cause"])
	}
	if entry["sanitized_input"] != `{"limit":50}` {
		t.Fatalf("unexpected sanitized_input: %v", entry["sanitized_input"])
	}
	if entry["reaction"] != "returned 500" {
		t.Fatalf("unexpected reaction: %v", entry["reaction"])
	}
}

type assertErr string

func (e assertErr) Error() string {
	return string(e)
}
