// Package httpapi exposes HTTP endpoints for health checks, preprocessing, and extraction transport.
// Trust boundary: every inbound HTTP field (headers, query strings, JSON bodies) is untrusted until
// explicit validation in handlers. Sanitized values only are passed into app/repository layers.
package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	appextraction "ai-advisor-event-aggregator/internal/app/extraction"
	"ai-advisor-event-aggregator/internal/event"
	"ai-advisor-event-aggregator/internal/extract"
	"ai-advisor-event-aggregator/internal/logging"
	"ai-advisor-event-aggregator/internal/model"
	"ai-advisor-event-aggregator/internal/observability"
	"ai-advisor-event-aggregator/internal/preprocess"
	repopkg "ai-advisor-event-aggregator/internal/repository/extraction"
	"ai-advisor-event-aggregator/internal/upstream"
)

// extractionRepository describes repository methods needed by extraction HTTP endpoints.
type extractionRepository interface {
	ClaimPending(ctx context.Context, articleID int64, model string, startedAt time.Time) (repopkg.ClaimResult, error)
	UpsertSuccess(ctx context.Context, articleID int64, model string, finishedAt time.Time, result extract.ExtractResult) error
	UpsertFailure(ctx context.Context, articleID int64, model string, finishedAt time.Time, errMsg string) error
	GetByArticleID(ctx context.Context, articleID int64) (repopkg.Record, error)
}

// Handler serves HTTP endpoints and delegates extraction logic to app-layer orchestration.
type Handler struct {
	upstreamClient *upstream.Client
	extractor      appextraction.Extractor
	repo           extractionRepository
	clusterService clusterService
	extractorModel string
	logger         *logging.Logger
}

// clusterService describes the application service used by clustering run endpoint.
type clusterService interface {
	ListEvents(ctx context.Context, limit int, since *time.Time, until *time.Time) ([]event.Event, error)
}

// NewHandler constructs an HTTP handler with preprocess-only dependencies.
// The upstreamClient parameter provides upstream article access.
// It returns a handler supporting health and preprocess endpoints.
func NewHandler(upstreamClient *upstream.Client) *Handler {
	return &Handler{upstreamClient: upstreamClient, logger: logging.New()}
}

// NewHandlerWithExtraction constructs an HTTP handler with extraction dependencies.
// Parameters include upstream client, app-layer extractor, persistence repository, and extractor model identifier.
// It returns a handler supporting health, preprocess, and extraction endpoints.
func NewHandlerWithExtraction(upstreamClient *upstream.Client, extractor appextraction.Extractor, repo extractionRepository, extractorModel string) *Handler {
	return &Handler{upstreamClient: upstreamClient, extractor: extractor, repo: repo, extractorModel: extractorModel, logger: logging.New()}
}

// NewHandlerWithExtractionAndClustering constructs an HTTP handler with extraction and clustering dependencies.
// Parameters include upstream client, extraction app dependencies, and clustering app service.
// It returns a handler supporting health, preprocess, extraction, and clustering endpoints.
func NewHandlerWithExtractionAndClustering(upstreamClient *upstream.Client, extractor appextraction.Extractor, repo extractionRepository, extractorModel string, clustering clusterService) *Handler {
	return &Handler{
		upstreamClient: upstreamClient,
		extractor:      extractor,
		repo:           repo,
		clusterService: clustering,
		extractorModel: extractorModel,
		logger:         logging.New(),
	}
}

// Register wires all HTTP routes for this handler on the provided mux.
// The mux parameter is mutated with route registrations.
// It has no return value.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.health)
	mux.HandleFunc("/api/health", h.health)
	mux.HandleFunc("/api/preprocess", h.preprocess)
	mux.HandleFunc("/api/extract/run", h.extractRun)
	mux.HandleFunc("/api/extract/run-batch", h.extractRunBatch)
	mux.HandleFunc("/api/extract/result", h.extractResult)
	mux.HandleFunc("/api/events", h.listEvents)
}

// health handles service liveness requests.
// Request must be GET and response is JSON with status field.
// It writes status directly to the response writer.
func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		h.logger.ErrorWithContract(
			r.Context(),
			"http.health.encode_failed",
			"http/health",
			"failed to encode health response payload",
			err,
			logging.ErrorContract{
				Failure:        "health_response_encode_failed",
				Cause:          err.Error(),
				SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
				Reaction:       "attempted to return internal server error response",
			},
			logging.Field{Key: "method", Value: r.Method},
			logging.Field{Key: "path", Value: r.URL.Path},
		)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

// preprocess handles article preprocess transport by delegating to preprocess package flow.
// Request must be GET and upstream articles are fetched internally.
// It writes JSON preprocess output or HTTP error status.
// Trust boundary: upstream payload is treated as external input and normalized by preprocess.Process.
func (h *Handler) preprocess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	articles, err := h.upstreamClient.FetchArticles(r.Context())
	if err != nil {
		http.Error(w, "failed to fetch upstream articles", http.StatusBadGateway)
		return
	}

	out := model.PreprocessResponse{Data: make([]model.CleanText, 0, len(articles))}
	for _, article := range articles {
		source := preprocess.BuildInputText(
			article.Title,
			article.Description,
			article.Content,
			article.FulltextExcerpt,
			article.FulltextContentText,
		)
		cleaned := preprocess.Process(source)
		out.Data = append(out.Data, model.CleanText{
			ArticleID:   article.ID,
			Title:       article.Title,
			Link:        article.Link,
			Source:      article.Source,
			PublishedAt: article.PublishedAt,
			Text:        cleaned,
		})
	}

	writeJSON(w, http.StatusOK, out)
}

type runRequest struct {
	ArticleID int64 `json:"article_id"`
}

type runResponse struct {
	ArticleID int64                  `json:"article_id"`
	Status    string                 `json:"status"`
	Result    *extract.ExtractResult `json:"result,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

// extractRun executes extraction for one article by delegating to app-layer RunAndPersist flow.
// Request must be POST with JSON body containing article_id.
// It writes JSON response with per-article status/result or error.
// Sanitization: article_id must be a positive integer before lookup and persistence calls.
func (h *Handler) extractRun(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.extract_run")
	defer span.End()
	r = r.WithContext(ctx)

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.extractor == nil || h.repo == nil || h.extractorModel == "" {
		http.Error(w, "extraction dependencies not configured", http.StatusInternalServerError)
		return
	}

	var req runRequest
	if err := parseJSONBody(r, &req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.ArticleID <= 0 {
		http.Error(w, "invalid request body: article_id must be greater than 0", http.StatusBadRequest)
		return
	}

	article, ok, err := h.findArticleByID(r.Context(), req.ArticleID)
	if err != nil {
		observability.RecordError(span, err)
		http.Error(w, "failed to fetch upstream articles", http.StatusBadGateway)
		return
	}
	if !ok {
		http.Error(w, "article not found", http.StatusNotFound)
		return
	}

	appArticle, err := appextraction.FromModelArticle(article)
	if err != nil {
		observability.RecordError(span, err)
		http.Error(w, "invalid article data", http.StatusInternalServerError)
		return
	}

	outcome, err := appextraction.RunAndPersistWithOutcome(r.Context(), appArticle, h.extractor, h.repo, h.extractorModel)
	if err != nil {
		observability.RecordError(span, err)
		h.logger.ErrorWithContract(
			r.Context(),
			"http.extraction.run_failed",
			"http/extract/run",
			"single extraction run failed",
			err,
			logging.ErrorContract{
				Failure:        "single_extraction_run_failed",
				Cause:          err.Error(),
				SanitizedInput: fmt.Sprintf(`{"article_id":%d}`, req.ArticleID),
				Reaction:       "returned failed extraction status",
			},
			logging.Field{Key: "article_id", Value: req.ArticleID},
		)
		writeJSON(w, http.StatusOK, runResponse{ArticleID: req.ArticleID, Status: "failed", Error: "extraction run failed"})
		return
	}
	status := string(outcome.Outcome)
	if status == "" {
		status = "failed"
	}
	writeJSON(w, http.StatusOK, runResponse{ArticleID: req.ArticleID, Status: status, Result: outcome.Result, Error: outcome.Error})
}

type batchRunRequest struct {
	ArticleIDs  []int64 `json:"article_ids"`
	Concurrency int     `json:"concurrency,omitempty"`
}

type batchRunResponse struct {
	Items     []runResponse `json:"items"`
	Total     int           `json:"total"`
	Succeeded int           `json:"succeeded"`
	Failed    int           `json:"failed"`
}

// extractRunBatch executes extraction for multiple requested article IDs using app-layer batch flow.
// Request must be POST with JSON body containing article_ids and optional concurrency.
// It writes a JSON batch result with per-item success/failure isolation.
// Sanitization: invalid IDs/concurrency are rejected before any upstream fetch or extractor call.
func (h *Handler) extractRunBatch(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.extract_run_batch")
	defer span.End()
	r = r.WithContext(ctx)

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.extractor == nil || h.repo == nil || h.extractorModel == "" {
		http.Error(w, "extraction dependencies not configured", http.StatusInternalServerError)
		return
	}

	var req batchRunRequest
	if err := parseJSONBody(r, &req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.ArticleIDs) == 0 {
		http.Error(w, "invalid request body: article_ids must not be empty", http.StatusBadRequest)
		return
	}
	if req.Concurrency < 0 {
		http.Error(w, "invalid concurrency", http.StatusBadRequest)
		return
	}

	articles, err := h.upstreamClient.FetchArticles(r.Context())
	if err != nil {
		observability.RecordError(span, err)
		http.Error(w, "failed to fetch upstream articles", http.StatusBadGateway)
		return
	}
	articleByID := make(map[int64]model.Article, len(articles))
	for _, article := range articles {
		articleByID[int64(article.ID)] = article
	}

	validAppArticles := make([]appextraction.Article, 0, len(req.ArticleIDs))
	itemByArticleID := make(map[int64][]*runResponse, len(req.ArticleIDs))
	items := make([]runResponse, 0, len(req.ArticleIDs))
	for _, id := range req.ArticleIDs {
		item := runResponse{ArticleID: id}
		if id <= 0 {
			item.Status = "failed"
			item.Error = "invalid article_id"
			items = append(items, item)
			continue
		}
		article, ok := articleByID[id]
		if !ok {
			item.Status = "failed"
			item.Error = "article not found"
			items = append(items, item)
			continue
		}
		appArticle, convErr := appextraction.FromModelArticle(article)
		if convErr != nil {
			item.Status = "failed"
			item.Error = "invalid article data"
			items = append(items, item)
			continue
		}
		item.Status = "pending"
		items = append(items, item)
		itemByArticleID[id] = append(itemByArticleID[id], &items[len(items)-1])
		validAppArticles = append(validAppArticles, appArticle)
	}

	if len(validAppArticles) > 0 {
		batchResult, batchErr := appextraction.RunBatchAndPersistWithOutcome(
			r.Context(),
			validAppArticles,
			h.extractor,
			h.repo,
			h.extractorModel,
			appextraction.BatchOptions{Concurrency: req.Concurrency},
		)
		if batchErr != nil {
			observability.RecordError(span, batchErr)
			http.Error(w, "failed to run extraction batch", http.StatusInternalServerError)
			return
		}
		for _, item := range batchResult.Items {
			respQueue := itemByArticleID[item.ArticleID]
			if len(respQueue) == 0 {
				continue
			}
			respItem := respQueue[0]
			itemByArticleID[item.ArticleID] = respQueue[1:]
			if item.Error != "" {
				respItem.Status = "failed"
				respItem.Error = item.Error
				continue
			}
			respItem.Status = string(item.Outcome)
			respItem.Result = item.Result
			respItem.Error = item.Error
		}
	}

	resp := batchRunResponse{Items: items, Total: len(items)}
	for _, item := range items {
		switch item.Status {
		case string(appextraction.ExecutionOutcomeNewlyExtracted), string(appextraction.ExecutionOutcomeAlreadyDone), string(appextraction.ExecutionOutcomeAlreadyPending):
			resp.Succeeded++
		default:
			resp.Failed++
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

type readResultResponse struct {
	ArticleID                int64                    `json:"article_id"`
	ExtractionStatus         string                   `json:"extraction_status"`
	ExtractionModel          string                   `json:"extraction_model"`
	ExtractionError          *string                  `json:"extraction_error,omitempty"`
	ExtractedEventType       *extract.EventType       `json:"extracted_event_type,omitempty"`
	ExtractedGeoCluster      *extract.GeoCluster      `json:"extracted_geo_cluster,omitempty"`
	ExtractedCountries       []string                 `json:"extracted_countries"`
	ExtractedCompanies       []string                 `json:"extracted_companies"`
	ExtractedSectors         []string                 `json:"extracted_sectors"`
	ExtractedIndustries      []string                 `json:"extracted_industries"`
	ExtractedImpactDirection *extract.ImpactDirection `json:"extracted_impact_direction,omitempty"`
	ExtractedImpactStrength  *int                     `json:"extracted_impact_strength,omitempty"`
	ExtractedChannels        []extract.Channel        `json:"extracted_channels"`
	ExtractedTimeHorizon     *extract.TimeHorizon     `json:"extracted_time_horizon,omitempty"`
	ExtractedConfidence      *float64                 `json:"extracted_confidence,omitempty"`
}

// extractResult reads persisted extraction result and status by article_id.
// Request must be GET with article_id query parameter.
// It writes JSON record data or not-found/internal errors.
// Sanitization: query article_id is strictly parsed as positive base-10 int64.
func (h *Handler) extractResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.repo == nil {
		http.Error(w, "extraction repository not configured", http.StatusInternalServerError)
		return
	}
	id, err := strconv.ParseInt(r.URL.Query().Get("article_id"), 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "invalid article_id", http.StatusBadRequest)
		return
	}

	rec, err := h.repo.GetByArticleID(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "article result not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to load extraction result", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, readResultResponse{
		ArticleID:                rec.ArticleID,
		ExtractionStatus:         rec.ExtractionStatus,
		ExtractionModel:          rec.ExtractionModel,
		ExtractionError:          rec.ExtractionError,
		ExtractedEventType:       rec.ExtractedEventType,
		ExtractedGeoCluster:      rec.ExtractedGeoCluster,
		ExtractedCountries:       rec.ExtractedCountries,
		ExtractedCompanies:       rec.ExtractedCompanies,
		ExtractedSectors:         rec.ExtractedSectors,
		ExtractedIndustries:      rec.ExtractedIndustries,
		ExtractedImpactDirection: rec.ExtractedImpactDirection,
		ExtractedImpactStrength:  rec.ExtractedImpactStrength,
		ExtractedChannels:        rec.ExtractedChannels,
		ExtractedTimeHorizon:     rec.ExtractedTimeHorizon,
		ExtractedConfidence:      rec.ExtractedConfidence,
	})
}

// listEvents returns aggregated persistent events sorted by recency.
// Request must be GET and accepts optional query parameters limit, since, and until (RFC3339 timestamps).
// It writes a JSON payload containing the effective filters and matching events, serialized directly from event.Event (including industries).
// Sanitization: limit must be >0 and since/until must parse as RFC3339 before query execution.
func (h *Handler) listEvents(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.list_events")
	defer span.End()
	r = r.WithContext(ctx)

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.clusterService == nil {
		http.Error(w, "clustering service not configured", http.StatusInternalServerError)
		return
	}
	limit := 20
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = parsed
	}

	var sincePtr *time.Time
	if rawSince := r.URL.Query().Get("since"); rawSince != "" {
		since, err := time.Parse(time.RFC3339, rawSince)
		if err != nil {
			http.Error(w, "invalid since", http.StatusBadRequest)
			return
		}
		since = since.UTC()
		sincePtr = &since
	}
	var untilPtr *time.Time
	if rawUntil := r.URL.Query().Get("until"); rawUntil != "" {
		until, err := time.Parse(time.RFC3339, rawUntil)
		if err != nil {
			http.Error(w, "invalid until", http.StatusBadRequest)
			return
		}
		until = until.UTC()
		untilPtr = &until
	}

	events, err := h.clusterService.ListEvents(r.Context(), limit, sincePtr, untilPtr)
	if err != nil {
		observability.RecordError(span, err)
		h.logger.ErrorWithContract(r.Context(), "http.events.query_failed", "http/events", "events query failed", err, logging.ErrorContract{
			Failure:        "events_query_failed",
			Cause:          err.Error(),
			SanitizedInput: fmt.Sprintf(`{"limit":%d}`, limit),
			Reaction:       "returned http 500",
		}, logging.Field{Key: "limit", Value: limit})
		http.Error(w, "failed to load events", http.StatusInternalServerError)
		return
	}
	h.logger.Info(r.Context(), "http.events.query_completed", "http/events", "events query completed",
		logging.Field{Key: "limit", Value: limit},
		logging.Field{Key: "returned", Value: len(events)},
	)
	writeJSON(w, http.StatusOK, map[string]any{"limit": limit, "since": sincePtr, "until": untilPtr, "events": events})
}

// findArticleByID fetches upstream articles and returns one matching ID.
// Parameters: ctx controls fetch lifecycle and articleID is the sought identifier.
// It returns the article, whether found, and an upstream fetch error when present.
func (h *Handler) findArticleByID(ctx context.Context, articleID int64) (model.Article, bool, error) {
	articles, err := h.upstreamClient.FetchArticles(ctx)
	if err != nil {
		return model.Article{}, false, err
	}
	for _, article := range articles {
		if int64(article.ID) == articleID {
			return article, true, nil
		}
	}
	return model.Article{}, false, nil
}

// parseJSONBody strictly decodes one JSON object from request body into target.
// It rejects unknown fields and trailing non-whitespace JSON tokens.
func parseJSONBody(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON content")
		}
		return fmt.Errorf("unexpected trailing JSON content: %w", err)
	}
	return nil
}

// writeJSON writes a JSON response with the provided HTTP status code and payload.
// Parameters: w is the target response writer, code is HTTP status, payload is JSON-encoded body value.
// It writes an internal error status if encoding fails.
func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}
