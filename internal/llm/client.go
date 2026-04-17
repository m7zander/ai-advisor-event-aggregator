// Package llm provides an OpenAI-backed implementation of the extractor contract.
// It performs one deterministic chat-completions request and returns validated extraction results.
// Trust boundary: model output is untrusted until strict JSON/schema validation succeeds.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"ai-advisor-impact-service/internal/extract"
	"ai-advisor-impact-service/internal/logging"
)

const defaultBaseURL = "https://api.openai.com/v1"

var llmLogger = logging.New()

// Client is an infrastructure extractor that calls OpenAI Chat Completions.
type Client struct {
	apiKey     string
	model      string
	baseURL    string
	httpClient *http.Client
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return extract.ExtractResult{}, fmt.Errorf("request build failure: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return extract.ExtractResult{}, fmt.Errorf("http failure: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			llmLogger.Error(ctx, "llm.http_response_body_close_failed", "llm/client", "failed to close http response body", cerr,
				logging.Field{Key: "operation", Value: "extract_chat_completions"},
			)
		}
	}()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return extract.ExtractResult{}, fmt.Errorf("http failure: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return extract.ExtractResult{}, fmt.Errorf("non-2xx response: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
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
	llmLogger.Error(ctx, "llm.decode_json_failed", "llm/client", "llm json decode failure", decodeErr,
		logging.Field{Key: "stage", Value: strings.TrimSpace(stage)},
		logging.Field{Key: "payload", Value: string(payload)},
	)
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
