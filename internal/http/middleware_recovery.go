package httpapi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"ai-advisor-event-aggregator/internal/logging"
)

// RecoveryMiddleware recovers panics from downstream handlers, emits one structured error log,
// and enforces failure-safe response behavior.
func RecoveryMiddleware(logger *logging.Logger, next http.Handler) http.Handler {
	if next == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "handler not configured", http.StatusInternalServerError)
		})
	}
	if logger == nil {
		logger = logging.New()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trackedWriter := newRecoveryResponseWriter(w)
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			reaction := "returned safe 500 response"
			if trackedWriter.ResponseStarted() {
				reaction = "aborted request because response already started"
			}
			logger.ErrorWithContract(r.Context(), "http.panic.recovered", "http/recovery", "recovered panic while handling HTTP request", fmt.Errorf("%s", safePanicValue(recovered)), logging.ErrorContract{
				Failure:        "handler_panic",
				Cause:          safePanicValue(recovered),
				SanitizedInput: sanitizedRequestInput(r),
				Reaction:       reaction,
			},
				logging.Field{Key: "method", Value: r.Method},
				logging.Field{Key: "path", Value: r.URL.Path},
				logging.Field{Key: "response_started", Value: trackedWriter.ResponseStarted()},
			)

			if trackedWriter.ResponseStarted() {
				panic(http.ErrAbortHandler)
			}

			http.Error(trackedWriter, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}()

		next.ServeHTTP(trackedWriter, r)
	})
}

func sanitizedRequestInput(r *http.Request) string {
	if r == nil {
		return `{}`
	}
	payload, err := json.Marshal(map[string]string{
		"method": r.Method,
		"path":   r.URL.Path,
	})
	if err != nil {
		return `{"method":"unknown","path":"unknown"}`
	}
	return string(payload)
}

func safePanicValue(recovered any) string {
	switch v := recovered.(type) {
	case nil:
		return "unknown"
	case error:
		return strings.TrimSpace(v.Error())
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprintf("panic_value_type=%T", recovered))
	}
}

type recoveryResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
	wroteBody   bool
}

func newRecoveryResponseWriter(w http.ResponseWriter) *recoveryResponseWriter {
	return &recoveryResponseWriter{ResponseWriter: w}
}

func (w *recoveryResponseWriter) ResponseStarted() bool {
	return w.wroteHeader || w.wroteBody
}

func (w *recoveryResponseWriter) WriteHeader(statusCode int) {
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *recoveryResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	w.wroteBody = true
	return w.ResponseWriter.Write(p)
}

func (w *recoveryResponseWriter) Flush() {
	flusher, ok := w.ResponseWriter.(http.Flusher)
	if !ok {
		return
	}
	flusher.Flush()
}

func (w *recoveryResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not implement http.Hijacker")
	}
	return hijacker.Hijack()
}

func (w *recoveryResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *recoveryResponseWriter) ReadFrom(src io.Reader) (int64, error) {
	readerFrom, ok := w.ResponseWriter.(io.ReaderFrom)
	if !ok {
		return io.Copy(w.ResponseWriter, src)
	}
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	w.wroteBody = true
	return readerFrom.ReadFrom(src)
}
