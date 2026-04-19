package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"reflect"
	"strings"
	"time"

	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
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
	info       *stdlog.Logger
	error      *stdlog.Logger
	otelLogger otellog.Logger
}

func New() *Logger {
	return NewWithWriters(os.Stdout, os.Stderr)
}

func NewWithWriters(infoWriter io.Writer, errorWriter io.Writer) *Logger {
	return &Logger{
		info:       stdlog.New(infoWriter, "", 0),
		error:      stdlog.New(errorWriter, "", 0),
		otelLogger: global.GetLoggerProvider().Logger("ai-advisor-event-aggregator/logging"),
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

func (l *Logger) emit(out *stdlog.Logger, level string, event string, msg string, component string, errText string, ctx context.Context, fields ...Field) {
	if l == nil || out == nil {
		return
	}
	entry := newOrderedJSON()
	entry.add("message", msg)
	entry.add("timestamp", time.Now().UTC().Format(time.RFC3339Nano))
	entry.add("level", level)
	entry.add("request_id", RequestIDFromContext(ctx))

	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		entry.add("trace_id", spanCtx.TraceID().String())
		entry.add("span_id", spanCtx.SpanID().String())
	}
	entry.add("event", strings.TrimSpace(event))
	entry.add("component", component)
	if errText != "" {
		entry.add("error", errText)
	}
	for _, field := range fields {
		key := strings.TrimSpace(field.Key)
		if key == "" {
			continue
		}
		entry.add(key, field.Value)
	}

	encoded, err := json.Marshal(entry)
	if err != nil {
		out.Printf(`{"message":"failed to marshal log entry","timestamp":"%s","level":"error","request_id":%q,"component":"internal/logging","error":%q}`,
			time.Now().UTC().Format(time.RFC3339Nano),
			RequestIDFromContext(ctx),
			fmt.Sprintf("%v", err))
		return
	}
	out.Println(string(encoded))
	l.emitOTel(ctx, entry)
}

func (l *Logger) emitOTel(ctx context.Context, entry *orderedJSON) {
	if l == nil {
		return
	}
	record := otellog.Record{}
	record.SetTimestamp(time.Now().UTC())
	record.SetObservedTimestamp(time.Now().UTC())

	level, _ := entry.value("level").(string)
	switch strings.ToLower(level) {
	case "error":
		record.SetSeverity(otellog.SeverityError)
	default:
		record.SetSeverity(otellog.SeverityInfo)
	}
	if message, ok := entry.value("message").(string); ok {
		record.SetBody(otellog.StringValue(message))
	}

	attrs := make([]otellog.KeyValue, 0, len(entry.pairs))
	for _, pair := range entry.pairs {
		attrs = append(attrs, otellog.KeyValue{
			Key:   pair.Key,
			Value: anyToValue(pair.Value),
		})
	}
	record.AddAttributes(attrs...)
	l.otelLogger.Emit(ctx, record)
}

func anyToValue(v any) otellog.Value {
	switch value := v.(type) {
	case string:
		return otellog.StringValue(value)
	case bool:
		return otellog.BoolValue(value)
	case int:
		return otellog.Int64Value(int64(value))
	case int64:
		return otellog.Int64Value(value)
	case int32:
		return otellog.Int64Value(int64(value))
	case uint:
		return otellog.Int64Value(int64(value))
	case uint64:
		return otellog.Int64Value(int64(value))
	case uint32:
		return otellog.Int64Value(int64(value))
	case float64:
		return otellog.Float64Value(value)
	case float32:
		return otellog.Float64Value(float64(value))
	case []string:
		items := make([]otellog.Value, 0, len(value))
		for _, item := range value {
			items = append(items, otellog.StringValue(item))
		}
		return otellog.SliceValue(items...)
	case []int64:
		items := make([]otellog.Value, 0, len(value))
		for _, item := range value {
			items = append(items, otellog.Int64Value(item))
		}
		return otellog.SliceValue(items...)
	case []int:
		items := make([]otellog.Value, 0, len(value))
		for _, item := range value {
			items = append(items, otellog.Int64Value(int64(item)))
		}
		return otellog.SliceValue(items...)
	case []any:
		items := make([]otellog.Value, 0, len(value))
		for _, item := range value {
			items = append(items, anyToValue(item))
		}
		return otellog.SliceValue(items...)
	case map[string]string:
		kvs := make([]otellog.KeyValue, 0, len(value))
		for key, item := range value {
			kvs = append(kvs, otellog.String(key, item))
		}
		return otellog.MapValue(kvs...)
	case map[string]any:
		kvs := make([]otellog.KeyValue, 0, len(value))
		for key, item := range value {
			kvs = append(kvs, otellog.KeyValue{Key: key, Value: anyToValue(item)})
		}
		return otellog.MapValue(kvs...)
	default:
		reflected := reflect.ValueOf(v)
		switch reflected.Kind() {
		case reflect.Slice, reflect.Array:
			items := make([]otellog.Value, 0, reflected.Len())
			for i := 0; i < reflected.Len(); i++ {
				items = append(items, anyToValue(reflected.Index(i).Interface()))
			}
			return otellog.SliceValue(items...)
		case reflect.Map:
			if reflected.Type().Key().Kind() == reflect.String {
				kvs := make([]otellog.KeyValue, 0, reflected.Len())
				for _, key := range reflected.MapKeys() {
					kvs = append(kvs, otellog.KeyValue{
						Key:   key.String(),
						Value: anyToValue(reflected.MapIndex(key).Interface()),
					})
				}
				return otellog.MapValue(kvs...)
			}
		}
		return otellog.StringValue(fmt.Sprintf("%v", value))
	}
}

type orderedJSON struct {
	pairs []Field
	index map[string]int
}

func newOrderedJSON() *orderedJSON {
	return &orderedJSON{
		pairs: make([]Field, 0, 8),
		index: make(map[string]int),
	}
}

func (o *orderedJSON) add(key string, value any) {
	if idx, exists := o.index[key]; exists {
		o.pairs[idx].Value = value
		return
	}
	o.index[key] = len(o.pairs)
	o.pairs = append(o.pairs, Field{Key: key, Value: value})
}

func (o *orderedJSON) MarshalJSON() ([]byte, error) {
	var builder strings.Builder
	builder.WriteByte('{')
	for i, pair := range o.pairs {
		if i > 0 {
			builder.WriteByte(',')
		}
		keyEncoded, err := json.Marshal(pair.Key)
		if err != nil {
			return nil, err
		}
		valueEncoded, err := json.Marshal(pair.Value)
		if err != nil {
			return nil, err
		}
		builder.Write(keyEncoded)
		builder.WriteByte(':')
		builder.Write(valueEncoded)
	}
	builder.WriteByte('}')
	return []byte(builder.String()), nil
}

func (o *orderedJSON) value(key string) any {
	idx, exists := o.index[key]
	if !exists {
		return nil
	}
	return o.pairs[idx].Value
}
