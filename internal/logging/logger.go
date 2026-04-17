package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type ctxKey string

const requestIDKey ctxKey = "request_id"

type Field struct {
	Key   string
	Value any
}

// ErrorContract defines mandatory structured fields for every error log entry.
type ErrorContract struct {
	Failure        string
	Cause          string
	SanitizedInput string
	Reaction       string
}

type Logger struct {
	info  *log.Logger
	error *log.Logger
}

func New() *Logger {
	return NewWithWriters(os.Stdout, os.Stderr)
}

func NewWithWriters(infoWriter io.Writer, errorWriter io.Writer) *Logger {
	return &Logger{
		info:  log.New(infoWriter, "", 0),
		error: log.New(errorWriter, "", 0),
	}
}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	trimmed := strings.TrimSpace(requestID)
	if trimmed == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey, trimmed)
}

func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	val, _ := ctx.Value(requestIDKey).(string)
	return strings.TrimSpace(val)
}

func (l *Logger) Info(ctx context.Context, event string, component string, msg string, fields ...Field) {
	l.emit(l.info, "info", event, msg, component, "", ctx, fields...)
}

func (l *Logger) Error(ctx context.Context, event string, component string, msg string, err error, fields ...Field) {
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	l.emit(l.error, "error", event, msg, component, errText, ctx, fields...)
}

// ErrorWithContract logs an error entry including mandatory error contract fields.
func (l *Logger) ErrorWithContract(ctx context.Context, event string, component string, msg string, err error, contract ErrorContract, fields ...Field) {
	merged := append([]Field{}, fields...)
	merged = append(merged,
		Field{Key: "failure", Value: strings.TrimSpace(contract.Failure)},
		Field{Key: "cause", Value: strings.TrimSpace(contract.Cause)},
		Field{Key: "sanitized_input", Value: strings.TrimSpace(contract.SanitizedInput)},
		Field{Key: "reaction", Value: strings.TrimSpace(contract.Reaction)},
	)
	l.Error(ctx, event, component, msg, err, merged...)
}

func (l *Logger) emit(out *log.Logger, level string, event string, msg string, component string, errText string, ctx context.Context, fields ...Field) {
	if l == nil || out == nil {
		return
	}
	entry := map[string]any{
		"ts":         time.Now().UTC().Format(time.RFC3339Nano),
		"level":      level,
		"event":      strings.TrimSpace(event),
		"msg":        msg,
		"request_id": RequestIDFromContext(ctx),
		"component":  component,
	}
	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		entry["trace_id"] = spanCtx.TraceID().String()
		entry["span_id"] = spanCtx.SpanID().String()
	}
	if errText != "" {
		entry["error"] = errText
	}
	for _, field := range fields {
		key := strings.TrimSpace(field.Key)
		if key == "" {
			continue
		}
		entry[key] = field.Value
	}

	keys := make([]string, 0, len(entry))
	for k := range entry {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]any, len(entry))
	for _, key := range keys {
		ordered[key] = entry[key]
	}

	encoded, err := json.Marshal(ordered)
	if err != nil {
		out.Printf(`{"level":"error","msg":"failed to marshal log entry","component":"internal/logging","error":%q}`,
			fmt.Sprintf("%v", err))
		return
	}
	out.Println(string(encoded))
}
