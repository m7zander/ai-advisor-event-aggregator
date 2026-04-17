// Package llm tests OpenAI-backed extractor client behavior and error handling.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ai-advisor-impact-service/internal/extract"
	"ai-advisor-impact-service/internal/logging"
)

// validInput returns a contract-valid extractor input used by llm client tests.
// The testing parameter is unused and included for consistency in test helper signatures.
// It returns a fully populated extract.ExtractInput.
func validInput(_ *testing.T) extract.ExtractInput {
	return extract.ExtractInput{
		ArticleID:   99,
		Title:       "Title",
		Link:        "https://example.com/news",
		Source:      "example",
		PublishedAt: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
		Text:        "Processed text with sufficient detail.",
	}
}

// validResult returns a contract-valid extractor output bound to the supplied article ID.
// The articleID parameter is assigned to the output ArticleID.
// It returns a valid extract.ExtractResult.
func validResult(articleID int64) extract.ExtractResult {
	return extract.ExtractResult{
		ArticleID:       articleID,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"US"},
		Companies:       []string{"ACME"},
		Sectors:         []string{"industrials"},
		Industries:      []string{"Software - Application"},
		ImpactDirection: extract.ImpactDirectionNeutral,
		ImpactStrength:  40,
		Channels:        []extract.Channel{extract.ChannelRiskSentiment},
		TimeHorizon:     extract.TimeHorizonShort,
		Confidence:      0.8,
	}
}

// newTestClient creates a client pointed to a local httptest server.
// The serverURL parameter is used as base URL for the client under test.
// It returns a configured client and fails the test on constructor errors.
func newTestClient(t *testing.T, serverURL string) *Client {
	t.Helper()
	c, err := NewClient("test-key", "gpt-4o-mini", serverURL, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to build test client: %v", err)
	}
	return c
}

// writeChatResponse writes a minimal valid chat-completions JSON envelope to the response writer.
// The w parameter is the HTTP response writer and content is embedded as assistant message content.
// It fails the test if JSON encoding fails.
func writeChatResponse(t *testing.T, w http.ResponseWriter, content string) {
	t.Helper()
	payload := map[string]any{
		"choices": []map[string]any{
			{"message": map[string]any{"content": content}},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

// writeOpenAIStyleChatResponse writes a realistic OpenAI-like envelope including metadata fields.
// The w parameter is the HTTP response writer and content is embedded as assistant message content.
// It fails the test if JSON encoding fails.
func writeOpenAIStyleChatResponse(t *testing.T, w http.ResponseWriter, content string) {
	t.Helper()
	payload := map[string]any{
		"id":      "chatcmpl-test",
		"object":  "chat.completion",
		"created": 1760000000,
		"model":   "gpt-4o-mini",
		"choices": []map[string]any{
			{"index": 0, "message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

// TestClientExtract_SuccessfulResponseParsing verifies successful JSON extraction and validation path.
// It serves a valid OpenAI envelope containing contract-valid result JSON.
// It fails if parsing, validation, or article-id matching does not succeed.
func TestClientExtract_SuccessfulResponseParsing(t *testing.T) {
	in := validInput(t)
	resultJSON, err := json.Marshal(validResult(in.ArticleID))
	if err != nil {
		t.Fatalf("marshal result json: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		writeChatResponse(t, w, string(resultJSON))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	out, err := client.Extract(context.Background(), in)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if out.ArticleID != in.ArticleID {
		t.Fatalf("unexpected article id: got %d want %d", out.ArticleID, in.ArticleID)
	}
}

// TestClientExtract_SuccessfulResponseParsing_OpenAIEnvelope verifies extra top-level OpenAI metadata fields are tolerated.
// It serves a realistic chat-completions envelope containing id/object/created/model plus valid choices content.
// It fails if metadata fields cause response decoding to fail.
func TestClientExtract_SuccessfulResponseParsing_OpenAIEnvelope(t *testing.T) {
	in := validInput(t)
	resultJSON, err := json.Marshal(validResult(in.ArticleID))
	if err != nil {
		t.Fatalf("marshal result json: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		writeOpenAIStyleChatResponse(t, w, string(resultJSON))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	out, err := client.Extract(context.Background(), in)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if out.ArticleID != in.ArticleID {
		t.Fatalf("unexpected article id: got %d want %d", out.ArticleID, in.ArticleID)
	}
}

// TestClientExtract_InvalidJSONResponse verifies non-JSON assistant content is rejected.
// It serves assistant content that is not valid JSON.
// It fails if invalid JSON is accepted.
func TestClientExtract_InvalidJSONResponse(t *testing.T) {
	in := validInput(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, "not-json")
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected json decode failure")
	}
}

// TestClientExtract_InvalidJSONResponse_LogsPayload verifies decode failures log stage and full payload.
// It serves invalid assistant content and captures logger output during extraction.
// It fails if log output does not include decode stage and offending payload text.
func TestClientExtract_InvalidJSONResponse_LogsPayload(t *testing.T) {
	in := validInput(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, "not-json")
	}))
	defer srv.Close()

	var buf bytes.Buffer
	originalLogger := llmLogger
	llmLogger = logging.NewWithWriters(&buf, &buf)
	defer func() {
		llmLogger = originalLogger
	}()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected json decode failure")
	}

	logged := buf.String()
	if !strings.Contains(logged, `"stage":"assistant_content"`) {
		t.Fatalf("expected assistant_content stage in log, got: %s", logged)
	}
	if !strings.Contains(logged, `"payload":"not-json"`) {
		t.Fatalf("expected payload value in log, got: %s", logged)
	}
}

// TestClientExtract_EmptyResponse verifies empty assistant content is rejected.
// It serves a chat-completions payload with empty content.
// It fails if empty output is accepted.
func TestClientExtract_EmptyResponse(t *testing.T) {
	in := validInput(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, "")
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected empty response error")
	}
}

// TestClientExtract_Non2xxResponse verifies upstream non-2xx HTTP responses are rejected.
// It serves a non-2xx status with body content.
// It fails if non-2xx responses are accepted.
func TestClientExtract_Non2xxResponse(t *testing.T) {
	in := validInput(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected non-2xx error")
	}
}

// TestClientExtract_ValidationFailure verifies parsed JSON that violates domain constraints is rejected.
// It serves a JSON object with an invalid enum value.
// It fails if validation errors are not propagated.
func TestClientExtract_ValidationFailure(t *testing.T) {
	in := validInput(t)
	invalid := validResult(in.ArticleID)
	invalid.EventType = extract.EventType("invalid")
	resultJSON, err := json.Marshal(invalid)
	if err != nil {
		t.Fatalf("marshal invalid result json: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, string(resultJSON))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected validation failure")
	}
}

// TestPromptContainsRequiredConstraints verifies the system prompt includes strict response and behavior rules.
// It checks required constraints for one-article extraction, json-only output, and non-goal restrictions.
// It fails if any required instruction is missing from the prompt text.
func TestPromptContainsRequiredConstraints(t *testing.T) {
	required := []string{
		"EXACTLY ONE preprocessed news article",
		"Return EXACTLY ONE JSON object",
		"Return NO markdown",
		"Return NO code fences",
		"Return NO prose",
		"Return NO explanation",
		"Use ONLY the allowed enum values",
		`Never use "id"; use "article_id" exactly.`,
		"Do not infer facts that are not supported by the input text",
		"extract ONLY the primary market-relevant event",
		"Never return null. Use empty arrays instead.",
		"labor_dispute",
	}
	for _, needle := range required {
		if !strings.Contains(extractorSystemPrompt, needle) {
			t.Fatalf("prompt missing required constraint: %q", needle)
		}
	}
}

// TestPromptContainsAllowedChannelList verifies the prompt contains the full allowed channel enum list.
// It checks each expected channel token so prompt constraints stay aligned with backend validation.
// It fails if any expected channel token is missing from the prompt text.
func TestPromptContainsAllowedChannelList(t *testing.T) {
	allowedChannels := []string{
		"oil_supply_risk",
		"gas_supply_risk",
		"shipping_disruption",
		"risk_sentiment",
		"defense_spending",
		"rates_fx",
		"inflation",
		"regulation_policy",
		"earnings_guidance",
		"analyst_revision",
		"supply_chain_disruption",
		"demand_shift",
		"labor_dispute",
	}

	for _, channel := range allowedChannels {
		if !strings.Contains(extractorSystemPrompt, channel) {
			t.Fatalf("prompt missing channel value: %q", channel)
		}
	}
}

// TestClientExtract_RejectsMarkdownCodeFence verifies code-fenced output is rejected as non-JSON.
// It serves model content wrapped in markdown fences.
// It fails if markdown-formatted payload is accepted.
func TestClientExtract_RejectsMarkdownCodeFence(t *testing.T) {
	in := validInput(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, "```json\n{\"article_id\":99}\n```")
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected markdown/code-fenced response to be rejected")
	}
}

// TestClientExtract_RejectsExtraNarrativeText verifies narrative text outside JSON is rejected.
// It serves content with prose before and after an otherwise valid JSON object.
// It fails if non-JSON wrapper text is silently accepted.
func TestClientExtract_RejectsExtraNarrativeText(t *testing.T) {
	in := validInput(t)
	validJSON, err := json.Marshal(validResult(in.ArticleID))
	if err != nil {
		t.Fatalf("marshal valid result json: %v", err)
	}
	content := "Here is the result: " + string(validJSON) + " done."

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, content)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected narrative-wrapped response to be rejected")
	}
}

// TestClientExtract_RejectsUnknownFields verifies strict JSON decode rejects unknown output fields.
// It serves a valid result plus an extra unsupported field.
// It fails if unknown fields are silently ignored.
func TestClientExtract_RejectsUnknownFields(t *testing.T) {
	in := validInput(t)
	content := `{"article_id":99,"event_type":"macro","geo_cluster":"global","countries":["US"],"companies":["ACME"],"sectors":["industrials"],"industries":["Software - Application"],"impact_direction":"neutral","impact_strength":40,"channels":["risk_sentiment"],"time_horizon":"short","confidence":0.8,"summary":"not allowed"}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, content)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected unknown field rejection")
	}
}

// TestClientExtract_RejectsOutputUsingIDInsteadOfArticleID verifies legacy `id` key is rejected.
// It serves a full otherwise-valid result object that uses `id` instead of `article_id`.
// It fails if extraction accepts the schema violation.
func TestClientExtract_RejectsOutputUsingIDInsteadOfArticleID(t *testing.T) {
	in := validInput(t)
	content := `{"id":99,"event_type":"macro","geo_cluster":"global","countries":["US"],"companies":["ACME"],"sectors":["industrials"],"industries":["Software - Application"],"impact_direction":"neutral","impact_strength":40,"channels":["risk_sentiment"],"time_horizon":"short","confidence":0.8}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, content)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected id alias to be rejected")
	}
}

// TestClientExtract_RejectsConflictingIDAndArticleID verifies `id` is rejected even when `article_id` is present.
// It serves an otherwise-valid result that sets both `article_id` and legacy `id`.
// It fails if any payload containing `id` is accepted.
func TestClientExtract_RejectsConflictingIDAndArticleID(t *testing.T) {
	in := validInput(t)
	content := `{"article_id":99,"id":100,"event_type":"macro","geo_cluster":"global","countries":["US"],"companies":["ACME"],"sectors":["industrials"],"industries":["Software - Application"],"impact_direction":"neutral","impact_strength":40,"channels":["risk_sentiment"],"time_horizon":"short","confidence":0.8}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, content)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected conflicting id/article_id rejection")
	}
}

// TestClientExtract_RejectsMissingIndustries verifies strict schema enforcement rejects outputs without industries.
// It serves a payload that omits industries while keeping all other fields valid.
// It fails if extraction accepts a response missing required industries.
func TestClientExtract_RejectsMissingIndustries(t *testing.T) {
	in := validInput(t)
	content := `{"article_id":99,"event_type":"macro","geo_cluster":"global","countries":["US"],"companies":["ACME"],"sectors":["industrials"],"impact_direction":"neutral","impact_strength":40,"channels":["risk_sentiment"],"time_horizon":"short","confidence":0.8}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeChatResponse(t, w, content)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err == nil {
		t.Fatal("expected missing industries rejection")
	}
}

// TestPromptSchemaExcludesDuplicateCandidate verifies the strict prompt schema omits duplicate_candidate.
// It inspects the system prompt text directly.
// It fails if duplicate_candidate is still present in the schema contract.
func TestPromptSchemaExcludesDuplicateCandidate(t *testing.T) {
	if strings.Contains(extractorSystemPrompt, "duplicate_candidate") {
		t.Fatal("did not expect duplicate_candidate in extractor prompt schema")
	}
}

// TestClientExtract_RequestPayloadUsesArticleID verifies outgoing request asks for strict JSON-object output and article_id key.
// It captures and decodes the outgoing chat-completions request and then validates response_format and user payload keys.
// It fails if response_format type is not json_object, if article_id is missing, or if user payload uses top-level id.
func TestClientExtract_RequestPayloadUsesArticleID(t *testing.T) {
	in := validInput(t)
	resultJSON, err := json.Marshal(validResult(in.ArticleID))
	if err != nil {
		t.Fatalf("marshal result json: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Fatalf("read request body: %v", readErr)
		}

		var req chatCompletionRequest
		if decodeErr := decodeStrictJSON(body, &req); decodeErr != nil {
			t.Fatalf("decode request body: %v", decodeErr)
		}

		if req.ResponseFormat.Type != "json_object" {
			t.Fatalf("unexpected response_format.type: %q", req.ResponseFormat.Type)
		}
		if len(req.Messages) < 2 {
			t.Fatalf("expected at least 2 messages, got %d", len(req.Messages))
		}
		if req.Messages[1].Role != "user" {
			t.Fatalf("expected second message role user, got %q", req.Messages[1].Role)
		}

		var userPayload map[string]any
		if decodeErr := decodeStrictJSON([]byte(req.Messages[1].Content), &userPayload); decodeErr != nil {
			t.Fatalf("decode user payload: %v", decodeErr)
		}
		if _, ok := userPayload["article_id"]; !ok {
			t.Fatal("expected article_id in user payload")
		}
		if _, ok := userPayload["id"]; ok {
			t.Fatal("did not expect top-level id key in user payload")
		}
		if articleIDRaw, ok := userPayload["article_id"]; !ok || articleIDRaw == nil {
			t.Fatal("expected non-nil article_id in user payload")
		}

		writeChatResponse(t, w, string(resultJSON))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	if _, err := client.Extract(context.Background(), in); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}
