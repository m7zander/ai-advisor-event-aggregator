package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
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

	infoRaw := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(infoRaw, `{"message":`) {
		t.Fatalf("expected message to be first field in info log, got: %s", infoRaw)
	}

	var info map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &info); err != nil {
		t.Fatalf("decode info log: %v", err)
	}
	if info["message"] != "operation completed" {
		t.Fatalf("unexpected info message: %v", info["message"])
	}
	if _, ok := info["timestamp"]; !ok {
		t.Fatal("expected timestamp in info log")
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
	errorRaw := strings.TrimSpace(stderr.String())
	if !strings.HasPrefix(errorRaw, `{"message":`) {
		t.Fatalf("expected message to be first field in error log, got: %s", errorRaw)
	}
	if errorEntry["message"] != "operation failed" {
		t.Fatalf("unexpected error message: %v", errorEntry["message"])
	}
	if _, ok := errorEntry["timestamp"]; !ok {
		t.Fatal("expected timestamp in error log")
	}
	if _, ok := errorEntry["request_id"]; !ok {
		t.Fatal("expected request_id key in error log")
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

func TestLoggerIncludesTraceIDWhenSpanContextIsValid(t *testing.T) {
	var stdout bytes.Buffer
	logger := NewWithWriters(&stdout, &bytes.Buffer{})

	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		SpanID:     trace.SpanID{2, 2, 2, 2, 2, 2, 2, 2},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(WithRequestID(context.Background(), "req-456"), spanCtx)

	logger.Info(ctx, "http.events.completed", "http/events", "operation completed")

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &entry); err != nil {
		t.Fatalf("decode info log: %v", err)
	}
	if entry["request_id"] != "req-456" {
		t.Fatalf("unexpected request_id: %v", entry["request_id"])
	}
	if entry["trace_id"] == "" {
		t.Fatalf("expected trace_id in info log: %v", entry["trace_id"])
	}
	if entry["span_id"] == "" {
		t.Fatalf("expected span_id in info log: %v", entry["span_id"])
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

func TestLoggerMarshalFailureFallbackPreservesRequestID(t *testing.T) {
	var stderr bytes.Buffer
	logger := NewWithWriters(&bytes.Buffer{}, &stderr)

	ctx := WithRequestID(context.Background(), "req-marshal-fail")
	logger.Error(ctx, "http.events.failed", "http/events", "operation failed", nil, Field{Key: "invalid", Value: math.NaN()})

	raw := strings.TrimSpace(stderr.String())
	if !strings.HasPrefix(raw, `{"message":"failed to marshal log entry"`) {
		t.Fatalf("unexpected fallback log output: %s", raw)
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		t.Fatalf("decode fallback error log: %v", err)
	}
	if entry["request_id"] != "req-marshal-fail" {
		t.Fatalf("expected request_id to be preserved, got: %v", entry["request_id"])
	}
	if entry["level"] != "error" {
		t.Fatalf("unexpected level: %v", entry["level"])
	}
}

type assertErr string

func (e assertErr) Error() string {
	return string(e)
}
