// Package llm provides an OpenAI-backed implementation of the extractor contract.
// It performs one deterministic chat-completions request and returns validated extraction results.
// Trust boundary: model output is untrusted until strict JSON/schema validation succeeds.
package llm

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"ai-advisor-event-aggregator/internal/extract"
	"ai-advisor-event-aggregator/internal/logging"
)

const defaultBaseURL = "https://api.openai.com/v1"

var llmLogger = logging.New()
var retryAfterSecondsPattern = regexp.MustCompile(`(?i)\b(?:retry|try again)\D+(\d+(?:\.\d+)?)\s*(ms|millisecond|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes)?\b`)

// Client is an infrastructure extractor that calls OpenAI Chat Completions.
type Client struct {
	apiKey     string
	model      string
	baseURL    string
	httpClient *http.Client

	retryInitialDelay time.Duration
	retryMaxDelay     time.Duration
	retryMaxAttempts  int
	sleepFn           func(context.Context, time.Duration) error
	jitterFn          func(time.Duration) time.Duration
}

// NewClient constructs a new OpenAI-backed extractor client.
// Parameters: apiKey is the OpenAI API key; model is the model name; baseURL is optional and defaults when empty;
// timeout controls the HTTP client timeout.
// It returns a configured client or an error when required parameters are missing or invalid.
func NewClient(apiKey, model, baseURL string, timeout time.Duration) (*Client, error) {
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("api key is required")
	}
	if strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("model is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be > 0")
	}

	normalizedBaseURL := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if normalizedBaseURL == "" {
		normalizedBaseURL = defaultBaseURL
	}

	return &Client{
		apiKey:  apiKey,
		model:   model,
		baseURL: normalizedBaseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		retryInitialDelay: 250 * time.Millisecond,
		retryMaxDelay:     5 * time.Second,
		retryMaxAttempts:  3,
		sleepFn:           waitWithContext,
		jitterFn:          boundedJitter,
	}, nil
}

// Extract calls OpenAI once to extract structured event data for one article.
// Parameters: ctx controls request cancellation/deadline; in is the extractor domain input contract.
// It returns a validated ExtractResult or an error for request, HTTP, parse, or validation failures.
// Sanitization rules: input contract is validated before transmission; output keys/required fields are allow-listed.
func (c *Client) Extract(ctx context.Context, in extract.ExtractInput) (extract.ExtractResult, error) {
	if err := in.Validate(); err != nil {
		return extract.ExtractResult{}, fmt.Errorf("validate extract input: %w", err)
	}

	userPrompt, err := buildUserPrompt(in)
	if err != nil {
		return extract.ExtractResult{}, fmt.Errorf("request build failure: %w", err)
	}

	payload := chatCompletionRequest{
		Model:       c.model,
		Temperature: 0,
		ResponseFormat: chatResponseFormat{
			Type: "json_object",
		},
		Messages: []chatMessage{
			{Role: "system", Content: extractorSystemPrompt},
			{Role: "user", Content: userPrompt},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return extract.ExtractResult{}, fmt.Errorf("request build failure: %w", err)
	}

	responseBody, err := c.doWithRetry(ctx, body)
	if err != nil {
		return extract.ExtractResult{}, err
	}

	var decoded chatCompletionResponse
	if err := decodeJSONAllowUnknownFields(responseBody, &decoded); err != nil {
		logJSONDecodeFailure(ctx, "chat_completions_envelope", responseBody, err)
		return extract.ExtractResult{}, fmt.Errorf("json decode failure: %w", err)
	}

	content := strings.TrimSpace(firstContent(decoded))
	if content == "" {
		return extract.ExtractResult{}, fmt.Errorf("empty model response")
	}

	var out extract.ExtractResult
	if err := decodeExtractResultJSON([]byte(content), &out); err != nil {
		logJSONDecodeFailure(ctx, "assistant_content", []byte(content), err)
		return extract.ExtractResult{}, fmt.Errorf("json decode failure: %w", err)
	}

	if err := out.Validate(); err != nil {
		return extract.ExtractResult{}, fmt.Errorf("validation failure: %w", err)
	}
	if out.ArticleID != in.ArticleID {
		return extract.ExtractResult{}, fmt.Errorf("validation failure: article id mismatch input=%d result=%d", in.ArticleID, out.ArticleID)
	}

	return out, nil
}

func (c *Client) doWithRetry(ctx context.Context, body []byte) ([]byte, error) {
	attempts := c.retryMaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	lastErr := error(nil)

	for attempt := 1; attempt <= attempts; attempt++ {
		responseBody, retryDelay, err := c.doOnce(ctx, body, attempt)
		if err == nil {
			return responseBody, nil
		}
		lastErr = err
		if retryDelay <= 0 || attempt == attempts {
			break
		}
		if waitErr := c.sleepFn(ctx, retryDelay); waitErr != nil {
			return nil, fmt.Errorf("http failure: retry wait canceled: %w", waitErr)
		}
	}
	return nil, lastErr
}

func (c *Client) doOnce(ctx context.Context, body []byte, attempt int) ([]byte, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, 0, fmt.Errorf("request build failure: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if shouldRetryTransportError(ctx, err) {
			return nil, c.nextBackoffDelay(attempt), fmt.Errorf("http failure: %w", err)
		}
		return nil, 0, fmt.Errorf("http failure: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			llmLogger.ErrorWithContract(ctx, "llm.http_response_body_close_failed", "llm/client", "failed to close http response body", cerr, logging.ErrorContract{
				Failure:        "llm_response_body_close_failed",
				Cause:          cerr.Error(),
				SanitizedInput: sanitizeJSONLogInput("", nil),
				Reaction:       "connection cleanup failed; request already completed",
			},
				logging.Field{Key: "operation", Value: "extract_chat_completions"},
			)
		}
	}()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("http failure: read response: %w", err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return responseBody, 0, nil
	}
	if !shouldRetryHTTPStatus(resp.StatusCode) {
		return nil, 0, fmt.Errorf("non-2xx response: status=%d body=%s", resp.StatusCode, sanitizeBodyForError(responseBody))
	}

	delay := c.computeRetryDelay(resp, responseBody, attempt)
	return nil, delay, fmt.Errorf("transient non-2xx response: status=%d body=%s", resp.StatusCode, sanitizeBodyForError(responseBody))
}

func shouldRetryHTTPStatus(status int) bool {
	return status == http.StatusTooManyRequests ||
		status == http.StatusInternalServerError ||
		status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func shouldRetryTransportError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if ue, ok := err.(*url.Error); ok && ue.Timeout() {
		return true
	}
	type timeoutErr interface{ Timeout() bool }
	if te, ok := err.(timeoutErr); ok && te.Timeout() {
		return true
	}
	return false
}

func (c *Client) computeRetryDelay(resp *http.Response, responseBody []byte, attempt int) time.Duration {
	if resp.StatusCode == http.StatusTooManyRequests {
		if d, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			return d
		}
		if d, ok := parseOpenAIRetryDelay(responseBody); ok {
			return d
		}
	}
	return c.nextBackoffDelay(attempt)
}

func (c *Client) nextBackoffDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := c.retryInitialDelay
	for i := 1; i < attempt; i++ {
		base *= 2
		if base >= c.retryMaxDelay {
			base = c.retryMaxDelay
			break
		}
	}
	if base > c.retryMaxDelay {
		base = c.retryMaxDelay
	}
	jitter := c.jitterFn(base)
	return minDuration(c.retryMaxDelay, base+jitter)
}

func parseRetryAfter(value string) (time.Duration, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}
	if sec, err := strconv.Atoi(trimmed); err == nil {
		if sec <= 0 {
			return 0, false
		}
		return time.Duration(sec) * time.Second, true
	}
	if ts, err := http.ParseTime(trimmed); err == nil {
		wait := time.Until(ts)
		if wait <= 0 {
			return 0, false
		}
		return wait, true
	}
	return 0, false
}

func parseOpenAIRetryDelay(body []byte) (time.Duration, bool) {
	match := retryAfterSecondsPattern.FindStringSubmatch(strings.TrimSpace(string(body)))
	if len(match) < 2 {
		return 0, false
	}
	amount, err := strconv.ParseFloat(match[1], 64)
	if err != nil || amount <= 0 {
		return 0, false
	}
	unit := "s"
	if len(match) >= 3 && strings.TrimSpace(match[2]) != "" {
		unit = strings.ToLower(strings.TrimSpace(match[2]))
	}
	switch unit {
	case "ms", "millisecond", "milliseconds":
		return time.Duration(amount * float64(time.Millisecond)), true
	case "m", "min", "mins", "minute", "minutes":
		return time.Duration(amount * float64(time.Minute)), true
	default:
		return time.Duration(amount * float64(time.Second)), true
	}
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func boundedJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	maxJitter := base / 5
	if maxJitter <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(maxJitter.Nanoseconds()+1))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}

func sanitizeBodyForError(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if len(trimmed) > 256 {
		trimmed = trimmed[:256]
	}
	return trimmed
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// firstContent extracts the first assistant content string from a chat-completions response.
// The resp parameter is the parsed chat-completions response payload.
// It returns the first content value, or empty string when unavailable.
func firstContent(resp chatCompletionResponse) string {
	if len(resp.Choices) == 0 {
		return ""
	}
	return resp.Choices[0].Message.Content
}

// decodeStrictJSON decodes JSON into dst with unknown-field rejection and trailing-data rejection.
// The body parameter is the raw JSON bytes; dst is the decode target pointer.
// It returns a decode error if input is malformed, contains unknown fields, or has extra tokens.
func decodeStrictJSON(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("extra tokens after json object")
	}
	return nil
}

// decodeJSONAllowUnknownFields decodes JSON into dst while allowing additional object keys.
// Parameters: body contains raw JSON bytes; dst is the decode target pointer.
// It returns a decode error when JSON is malformed or has trailing non-whitespace tokens.
func decodeJSONAllowUnknownFields(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("extra tokens after json object")
	}
	return nil
}

// logJSONDecodeFailure writes a structured diagnostic log for JSON decode failures.
// Parameters: stage identifies where decoding failed; payload is the raw JSON bytes that failed parsing;
// decodeErr is the original decode error.
// It returns no value. Side effect: emits one log line through the process logger for platform log aggregation.
func logJSONDecodeFailure(ctx context.Context, stage string, payload []byte, decodeErr error) {
	llmLogger.ErrorWithContract(ctx, "llm.decode_json_failed", "llm/client", "llm json decode failure", decodeErr, logging.ErrorContract{
		Failure:        "llm_decode_failed",
		Cause:          decodeErr.Error(),
		SanitizedInput: sanitizeJSONLogInput(stage, payload),
		Reaction:       "request failed and extraction result rejected",
	},
		logging.Field{Key: "stage", Value: strings.TrimSpace(stage)},
	)
}

func sanitizeJSONLogInput(stage string, payload []byte) string {
	trimmed := strings.TrimSpace(string(payload))
	if len(trimmed) > 256 {
		trimmed = trimmed[:256]
	}
	body, err := json.Marshal(map[string]any{
		"stage":           strings.TrimSpace(stage),
		"payload_preview": trimmed,
		"payload_bytes":   len(payload),
	})
	if err != nil {
		return `{"error":"sanitize_failed"}`
	}
	return string(body)
}

// decodeExtractResultJSON decodes one extractor output JSON object into dst while enforcing strict key validation.
// Parameters: body contains the model JSON output bytes; dst is the destination *extract.ExtractResult pointer.
// It returns an error when JSON is malformed, unknown keys are present, required fields are missing, or trailing tokens exist.
// Security note: this rejects prompt-injected extra keys by allow-listing schema fields before decode.
func decodeExtractResultJSON(body []byte, dst *extract.ExtractResult) error {
	var raw map[string]json.RawMessage
	if err := decodeStrictJSON(body, &raw); err != nil {
		return err
	}

	allowed := map[string]struct{}{
		"article_id":       {},
		"event_type":       {},
		"geo_cluster":      {},
		"countries":        {},
		"companies":        {},
		"sectors":          {},
		"industries":       {},
		"impact_direction": {},
		"impact_strength":  {},
		"channels":         {},
		"time_horizon":     {},
		"confidence":       {},
	}
	for key := range raw {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("json: unknown field %q", key)
		}
	}

	requiredFields := []string{
		"article_id",
		"event_type",
		"geo_cluster",
		"countries",
		"companies",
		"sectors",
		"industries",
		"impact_direction",
		"impact_strength",
		"channels",
		"time_horizon",
		"confidence",
	}
	for _, field := range requiredFields {
		if _, ok := raw[field]; !ok {
			return fmt.Errorf("missing required field %s", field)
		}
	}

	normalized, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("normalize output json: %w", err)
	}
	return decodeStrictJSON(normalized, dst)
}

// chatCompletionRequest models the subset of request fields needed for deterministic extraction calls.
type chatCompletionRequest struct {
	Model          string             `json:"model"`
	Messages       []chatMessage      `json:"messages"`
	Temperature    float64            `json:"temperature"`
	ResponseFormat chatResponseFormat `json:"response_format"`
}

// chatMessage models one chat message entry.
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatResponseFormat models the JSON-only response format request.
type chatResponseFormat struct {
	Type string `json:"type"`
}

// chatCompletionResponse models the subset of chat-completions response needed for parsing content.
type chatCompletionResponse struct {
	Choices []chatChoice `json:"choices"`
}

// chatChoice models one choice item from chat-completions response.
type chatChoice struct {
	Message chatMessage `json:"message"`
}
