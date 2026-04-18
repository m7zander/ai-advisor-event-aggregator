// Package httpapi tests extraction HTTP endpoints that delegate to app-layer extraction flows.
package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appextraction "ai-advisor-event-aggregator/internal/app/extraction"
	"ai-advisor-event-aggregator/internal/extract"
	"ai-advisor-event-aggregator/internal/logging"
	repopkg "ai-advisor-event-aggregator/internal/repository/extraction"
	"ai-advisor-event-aggregator/internal/upstream"
)

// fakeExtractor is a deterministic test extractor for HTTP endpoint tests.
type fakeExtractor struct {
	errs map[int64]error
}

// Extract returns a valid result by default or a configured per-article error.
// The ctx parameter is accepted to satisfy interface compatibility.
// It returns deterministic values for endpoint assertions.
func (f *fakeExtractor) Extract(_ context.Context, in extract.ExtractInput) (extract.ExtractResult, error) {
	if err, ok := f.errs[in.ArticleID]; ok {
		return extract.ExtractResult{}, err
	}
	return extract.ExtractResult{
		ArticleID:       in.ArticleID,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"US"},
		Companies:       []string{"ACME"},
		Sectors:         []string{"industrials"},
		Industries:      []string{"Software - Application"},
		ImpactDirection: extract.ImpactDirectionNeutral,
		ImpactStrength:  20,
		Channels:        []extract.Channel{extract.ChannelRiskSentiment},
		TimeHorizon:     extract.TimeHorizonShort,
		Confidence:      0.7,
	}, nil
}

// fakeRepo captures writes and serves get responses for endpoint tests.
type fakeRepo struct {
	records map[int64]repopkg.Record
	calls   []string
}

// ClaimPending grants first claim and returns existing-status outcomes on subsequent attempts.
// Parameters identify claim target and model metadata.
// It returns deterministic claim outcomes for endpoint tests.
func (f *fakeRepo) ClaimPending(_ context.Context, articleID int64, model string, startedAt time.Time) (repopkg.ClaimResult, error) {
	if f.records == nil {
		f.records = map[int64]repopkg.Record{}
	}
	rec, ok := f.records[articleID]
	if !ok {
		rec = repopkg.Record{ArticleID: articleID, ExtractionStatus: repopkg.StatusPending, ExtractionModel: model, ExtractionStartedAt: &startedAt}
		f.records[articleID] = rec
		f.calls = append(f.calls, "pending")
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeGranted}, nil
	}
	switch rec.ExtractionStatus {
	case repopkg.StatusDone:
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeAlreadyDone, Existing: &rec}, nil
	case repopkg.StatusPending:
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeAlreadyPending, Existing: &rec}, nil
	case repopkg.StatusFailed:
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeAlreadyFailed, Existing: &rec}, nil
	default:
		return repopkg.ClaimResult{}, sql.ErrConnDone
	}
}

// UpsertPending stores pending status for one article.
// Parameters are used to build a persisted record stub.
// It returns nil for successful test persistence.
// UpsertSuccess stores done status and extracted fields for one article.
// Parameters are used to update persisted record stub.
// It returns nil for successful test persistence.
func (f *fakeRepo) UpsertSuccess(_ context.Context, articleID int64, model string, _ time.Time, result extract.ExtractResult) error {
	if f.records == nil {
		f.records = map[int64]repopkg.Record{}
	}
	rec := f.records[articleID]
	rec.ArticleID = articleID
	rec.ExtractionStatus = repopkg.StatusDone
	rec.ExtractionModel = model
	rec.ExtractedEventType = &result.EventType
	rec.ExtractedGeoCluster = &result.GeoCluster
	rec.ExtractedCountries = result.Countries
	rec.ExtractedCompanies = result.Companies
	rec.ExtractedSectors = result.Sectors
	rec.ExtractedIndustries = result.Industries
	rec.ExtractedImpactDirection = &result.ImpactDirection
	rec.ExtractedImpactStrength = &result.ImpactStrength
	rec.ExtractedChannels = result.Channels
	rec.ExtractedTimeHorizon = &result.TimeHorizon
	rec.ExtractedConfidence = &result.Confidence
	f.records[articleID] = rec
	f.calls = append(f.calls, "done")
	return nil
}

// UpsertFailure stores failed status and error for one article.
// Parameters are used to update persisted record stub.
// It returns nil for successful test persistence.
func (f *fakeRepo) UpsertFailure(_ context.Context, articleID int64, model string, _ time.Time, errMsg string) error {
	if f.records == nil {
		f.records = map[int64]repopkg.Record{}
	}
	rec := f.records[articleID]
	rec.ArticleID = articleID
	rec.ExtractionStatus = repopkg.StatusFailed
	rec.ExtractionModel = model
	rec.ExtractionError = &errMsg
	f.records[articleID] = rec
	f.calls = append(f.calls, "failed")
	return nil
}

// GetByArticleID returns stored record or sql.ErrNoRows-like error behavior.
// The articleID parameter selects one record.
// It returns not-found when absent.
func (f *fakeRepo) GetByArticleID(_ context.Context, articleID int64) (repopkg.Record, error) {
	rec, ok := f.records[articleID]
	if !ok {
		return repopkg.Record{}, sql.ErrNoRows
	}
	return rec, nil
}

// newUpstreamClientWithArticles builds a test upstream client serving provided articles payload.
// The body parameter must be a full JSON object string with data array.
// It returns client and cleanup function.
func newUpstreamClientWithArticles(t *testing.T, body string) (*upstream.Client, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	hostPort := strings.TrimPrefix(srv.URL, "http://")
	parts := strings.Split(hostPort, ":")
	client := upstream.NewClient(parts[0], parts[1])
	return client, srv.Close
}

// TestExtractRun_ValidRequest verifies successful single extraction endpoint behavior.
// It posts a valid article_id and expects done status with result.
// It fails if endpoint does not return successful extraction payload.
func TestExtractRun_ValidRequest(t *testing.T) {
	upstreamBody := `{"data":[{"id":1,"title":"t","link":"https://x","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."}]}`
	client, cleanup := newUpstreamClientWithArticles(t, upstreamBody)
	defer cleanup()

	repo := &fakeRepo{records: map[int64]repopkg.Record{}}
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, repo, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run", bytes.NewBufferString(`{"article_id":1}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var resp runResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != string(appextraction.ExecutionOutcomeNewlyExtracted) || resp.Result == nil || resp.Result.ArticleID != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if len(repo.calls) != 2 || repo.calls[0] != "pending" || repo.calls[1] != "done" {
		t.Fatalf("expected pending->done persistence sequence, got %+v", repo.calls)
	}
}

// TestExtractRun_InvalidRequest verifies request validation for single extraction endpoint.
// It posts an invalid body and expects 400.
// It fails if invalid request is accepted.
func TestExtractRun_InvalidRequest(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run", bytes.NewBufferString(`{"article_id":0}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

func TestExtractRun_RejectsUnknownFields(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run", bytes.NewBufferString(`{"article_id":1,"unexpected":"x"}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
	if !strings.Contains(rw.Body.String(), "invalid request body") {
		t.Fatalf("expected parse error response, got %q", rw.Body.String())
	}
}

func TestExtractRun_RejectsTrailingJSON(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run", bytes.NewBufferString(`{"article_id":1}{"article_id":2}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
	if !strings.Contains(rw.Body.String(), "trailing JSON") {
		t.Fatalf("expected trailing JSON error, got %q", rw.Body.String())
	}
}

// TestExtractRun_ArticleNotFound verifies single extraction returns 404 for unknown article IDs.
// It posts an article_id not present in upstream response.
// It fails if unknown articles do not map to not-found response.
func TestExtractRun_ArticleNotFound(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[{"id":1,"title":"t","link":"https://x","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."}]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run", bytes.NewBufferString(`{"article_id":999}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rw.Code)
	}
}

// TestExtractRun_InternalErrorSanitized verifies app/internal extraction errors are not exposed in responses.
func TestExtractRun_InternalErrorSanitized(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[{"id":1,"title":"t","link":"https://x","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."}]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{errs: map[int64]error{1: errors.New("openai upstream 500: connection reset")}}, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run", bytes.NewBufferString(`{"article_id":1}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var resp runResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Error != "extraction run failed" {
		t.Fatalf("expected sanitized error message, got %q", resp.Error)
	}
	if strings.Contains(resp.Error, "openai") || strings.Contains(resp.Error, "connection reset") {
		t.Fatalf("response leaked internal details: %+v", resp)
	}
}

func TestExtractRun_InternalErrorLogContainsMandatoryContractFields(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[{"id":1,"title":"t","link":"https://x","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."}]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{errs: map[int64]error{1: errors.New("openai upstream 500: connection reset")}}, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	h.logger = logging.NewWithWriters(&stdout, &stderr)

	mux := http.NewServeMux()
	h.Register(mux)
	req := httptest.NewRequest(http.MethodPost, "/api/extract/run", bytes.NewBufferString(`{"article_id":1}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}

	var logEntry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &logEntry); err != nil {
		t.Fatalf("decode error log: %v", err)
	}
	for _, key := range []string{"failure", "cause", "sanitized_input", "reaction"} {
		if _, ok := logEntry[key]; !ok {
			t.Fatalf("expected mandatory error field %q in log entry: %+v", key, logEntry)
		}
	}
}

// TestExtractRunBatch_ValidRequest verifies successful batch extraction endpoint behavior.
// It posts multiple valid article IDs and expects done results for all items.
// It fails if batch success response is not returned.
func TestExtractRunBatch_ValidRequest(t *testing.T) {
	upstreamBody := `{"data":[{"id":1,"title":"t1","link":"https://x/1","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."},{"id":2,"title":"t2","link":"https://x/2","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."}]}`
	client, cleanup := newUpstreamClientWithArticles(t, upstreamBody)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run-batch", bytes.NewBufferString(`{"article_ids":[1,2],"concurrency":2}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var resp batchRunResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Total != 2 || resp.Succeeded != 2 || resp.Failed != 0 {
		t.Fatalf("unexpected batch response: %+v", resp)
	}
}

// TestExtractRunBatch_InvalidRequest verifies batch endpoint request validation.
// It posts invalid payload with empty article_ids.
// It fails if invalid batch requests are accepted.
func TestExtractRunBatch_InvalidRequest(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run-batch", bytes.NewBufferString(`{"article_ids":[]}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

func TestExtractRunBatch_RejectsUnknownFields(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run-batch", bytes.NewBufferString(`{"article_ids":[1],"unknown":true}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

func TestExtractRunBatch_RejectsTrailingJSON(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[]}`)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run-batch", bytes.NewBufferString(`{"article_ids":[1]}[]`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
	if !strings.Contains(rw.Body.String(), "trailing JSON") {
		t.Fatalf("expected trailing JSON error, got %q", rw.Body.String())
	}
}

// TestExtractRunBatch_PartialFailure verifies per-item failure isolation in batch endpoint.
// It posts one valid and one missing article ID and expects mixed item statuses with HTTP 200.
// It fails if partial failures are not returned in payload.
func TestExtractRunBatch_PartialFailure(t *testing.T) {
	upstreamBody := `{"data":[{"id":1,"title":"t1","link":"https://x/1","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."}]}`
	client, cleanup := newUpstreamClientWithArticles(t, upstreamBody)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run-batch", bytes.NewBufferString(`{"article_ids":[1,999]}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var resp batchRunResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Total != 2 || resp.Succeeded != 1 || resp.Failed != 1 {
		t.Fatalf("unexpected mixed batch response: %+v", resp)
	}
	if resp.Items[1].Status != "failed" || resp.Items[1].Error == "" {
		t.Fatalf("expected per-item failure details, got %+v", resp.Items[1])
	}
}

// TestExtractRunBatch_DuplicateIDs verifies duplicate article IDs are fully resolved and not left pending.
// It posts duplicated article IDs and expects both items to receive terminal statuses.
// It fails if duplicate ID mapping drops one item update.
func TestExtractRunBatch_DuplicateIDs(t *testing.T) {
	upstreamBody := `{"data":[{"id":1,"title":"t1","link":"https://x/1","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."}]}`
	client, cleanup := newUpstreamClientWithArticles(t, upstreamBody)
	defer cleanup()
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/extract/run-batch", bytes.NewBufferString(`{"article_ids":[1,1],"concurrency":2}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}

	var resp batchRunResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected two items, got %d", len(resp.Items))
	}
	if resp.Items[0].Status == "pending" || resp.Items[1].Status == "pending" {
		t.Fatalf("expected duplicate items to be resolved, got %+v", resp.Items)
	}
}

// TestExtractRun_AlreadyDoneReusesStoredResult verifies idempotent reuse response when status is already done.
// It seeds repository with done record and calls run endpoint again for same article.
// It fails if endpoint triggers new extraction instead of reusing stored result outcome.
func TestExtractRun_AlreadyDoneReusesStoredResult(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[{"id":11,"title":"t","link":"https://x","source":"s","published_at":"2026-03-28T08:35:02Z","content":"This content has enough words to pass preprocessing checks."}]}`)
	defer cleanup()
	eventType := extract.EventTypeMacro
	geo := extract.GeoClusterGlobal
	direction := extract.ImpactDirectionNeutral
	strength := 20
	horizon := extract.TimeHorizonShort
	confidence := 0.7
	repo := &fakeRepo{records: map[int64]repopkg.Record{
		11: {
			ArticleID:                11,
			ExtractionStatus:         repopkg.StatusDone,
			ExtractionModel:          "gpt-4o-mini",
			ExtractedEventType:       &eventType,
			ExtractedGeoCluster:      &geo,
			ExtractedCountries:       []string{"US"},
			ExtractedCompanies:       []string{"ACME"},
			ExtractedSectors:         []string{"industrials"},
			ExtractedImpactDirection: &direction,
			ExtractedImpactStrength:  &strength,
			ExtractedChannels:        []extract.Channel{extract.ChannelRiskSentiment},
			ExtractedTimeHorizon:     &horizon,
			ExtractedConfidence:      &confidence,
		},
	}}
	h := NewHandlerWithExtraction(client, &fakeExtractor{}, repo, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)
	req := httptest.NewRequest(http.MethodPost, "/api/extract/run", bytes.NewBufferString(`{"article_id":11}`))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var resp runResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != string(appextraction.ExecutionOutcomeAlreadyDone) || resp.Result == nil {
		t.Fatalf("expected already_done with result, got %+v", resp)
	}
}

// TestExtractResult_ValidRequest verifies persisted extraction read endpoint behavior.
// It seeds repository state and requests article result by query param.
// It fails if endpoint does not return expected persisted record fields.
func TestExtractResult_ValidRequest(t *testing.T) {
	client, cleanup := newUpstreamClientWithArticles(t, `{"data":[]}`)
	defer cleanup()
	repo := &fakeRepo{records: map[int64]repopkg.Record{}}
	eventType := extract.EventTypeMacro
	repo.records[5] = repopkg.Record{ArticleID: 5, ExtractionStatus: repopkg.StatusDone, ExtractionModel: "gpt-4o-mini", ExtractedEventType: &eventType}

	h := NewHandlerWithExtraction(client, &fakeExtractor{}, repo, "gpt-4o-mini")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/extract/result?article_id=5", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var resp readResultResponse
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ArticleID != 5 || resp.ExtractionStatus != repopkg.StatusDone {
		t.Fatalf("unexpected read response: %+v", resp)
	}
}
