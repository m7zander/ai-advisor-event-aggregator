package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/netip"
	"strings"

	"ai-advisor-event-aggregator/internal/logging"
)

const (
	requestIDHeaderName        = "X-Request-ID"
	forwardedForHeaderName     = "X-Forwarded-For"
	forwardedProtoHeaderName   = "X-Forwarded-Proto"
	effectiveClientIPHeader    = "X-Effective-Client-IP"
	effectiveClientProtoHeader = "X-Effective-Proto"
	requestIDLength            = 32
	maxRequestIDLength         = 128
)

// ForwardedHeaderMiddleware normalizes trusted reverse-proxy metadata into internal-only headers.
// Trust boundary: only X-Forwarded-* values are trusted for external client metadata because Railway
// terminates TLS at the edge and forwards traffic to this service over internal HTTP.
// Sanitization rules: only first-hop values are considered; proto is allow-listed to http/https and
// client IP must parse as a valid IP address, otherwise deterministic fallbacks are used.
func ForwardedHeaderMiddleware(next http.Handler) http.Handler {
	if next == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "handler not configured", http.StatusInternalServerError)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		updated := r.Clone(r.Context())
		updated.Header.Set(effectiveClientIPHeader, trustedForwardedClientIP(r))
		updated.Header.Set(effectiveClientProtoHeader, trustedForwardedProto(r))
		next.ServeHTTP(w, updated)
	})
}

// RequestIDMiddleware ensures each request has a trusted request ID.
// It accepts a valid X-Request-ID from the client, otherwise generates one.
// The request ID is stored in request context and echoed in response headers.
func RequestIDMiddleware(next http.Handler) http.Handler {
	if next == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "handler not configured", http.StatusInternalServerError)
		})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := validatedRequestID(r.Header.Get(requestIDHeaderName))
		if requestID == "" {
			requestID = generateRequestID()
		}

		ctx := logging.WithRequestID(r.Context(), requestID)
		w.Header().Set(requestIDHeaderName, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func validatedRequestID(raw string) string {
	requestID := strings.TrimSpace(raw)
	if requestID == "" || len(requestID) > maxRequestIDLength {
		return ""
	}
	for _, ch := range requestID {
		if ch < 33 || ch > 126 {
			return ""
		}
	}
	return requestID
}

func generateRequestID() string {
	token := make([]byte, requestIDLength/2)
	if _, err := rand.Read(token); err != nil {
		return "reqid_unavailable"
	}
	return hex.EncodeToString(token)
}

func trustedForwardedClientIP(r *http.Request) string {
	firstHop := firstForwardedValue(r.Header.Get(forwardedForHeaderName))
	if firstHop == "" {
		return "unknown"
	}
	if _, err := netip.ParseAddr(firstHop); err != nil {
		return "unknown"
	}
	return firstHop
}

func trustedForwardedProto(r *http.Request) string {
	switch strings.ToLower(firstForwardedValue(r.Header.Get(forwardedProtoHeaderName))) {
	case "https":
		return "https"
	case "http":
		return "http"
	default:
		return "http"
	}
}

func firstForwardedValue(headerValue string) string {
	first := headerValue
	if idx := strings.Index(first, ","); idx >= 0 {
		first = first[:idx]
	}
	return strings.TrimSpace(first)
}
