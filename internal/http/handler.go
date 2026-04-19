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
		h.respondError(r, w, "http.health.method_not_allowed", "http/health", "health check rejected request because HTTP method is not allowed", nil, logging.ErrorContract{
			Failure:        "health_method_not_allowed",
			Cause:          "method must be GET",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 405 method not allowed",
		}, "method not allowed", http.StatusMethodNotAllowed, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
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
		h.respondError(r, w, "http.preprocess.method_not_allowed", "http/preprocess", "preprocess endpoint rejected request because HTTP method is not allowed", nil, logging.ErrorContract{
			Failure:        "preprocess_method_not_allowed",
			Cause:          "method must be GET",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 405 method not allowed",
		}, "method not allowed", http.StatusMethodNotAllowed, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	articles, err := h.upstreamClient.FetchArticles(r.Context())
	if err != nil {
		h.respondError(r, w, "http.preprocess.fetch_articles_failed", "http/preprocess", "preprocess failed to fetch upstream articles", err, logging.ErrorContract{
			Failure:        "preprocess_fetch_articles_failed",
			Cause:          err.Error(),
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 502 bad gateway",
		}, "failed to fetch upstream articles", http.StatusBadGateway, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
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

	writeJSON(r.Context(), h.logger, w, http.StatusOK, out, fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path))
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
		h.respondError(r, w, "http.extract_run.method_not_allowed", "http/extract/run", "single extraction run rejected request because HTTP method is not allowed", nil, logging.ErrorContract{
			Failure:        "extract_run_method_not_allowed",
			Cause:          "method must be POST",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 405 method not allowed",
		}, "method not allowed", http.StatusMethodNotAllowed, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	if h.extractor == nil || h.repo == nil || h.extractorModel == "" {
		h.respondError(r, w, "http.extract_run.dependencies_missing", "http/extract/run", "single extraction run failed because extraction dependencies are not configured", nil, logging.ErrorContract{
			Failure:        "extract_run_dependencies_missing",
			Cause:          "extractor, repository, and model must be configured",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 500 internal server error",
		}, "extraction dependencies not configured", http.StatusInternalServerError, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}

	var req runRequest
	if err := parseJSONBody(r, &req); err != nil {
		h.respondError(r, w, "http.extract_run.invalid_request_body", "http/extract/run", "single extraction run rejected invalid JSON request body", err, logging.ErrorContract{
			Failure:        "extract_run_invalid_request_body",
			Cause:          err.Error(),
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 400 bad request",
		}, "invalid request body: "+err.Error(), http.StatusBadRequest, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	if req.ArticleID <= 0 {
		h.respondError(r, w, "http.extract_run.invalid_article_id", "http/extract/run", "single extraction run rejected request because article_id is not greater than zero", nil, logging.ErrorContract{
			Failure:        "extract_run_invalid_article_id",
			Cause:          "article_id must be greater than 0",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, req.ArticleID),
			Reaction:       "returned http 400 bad request",
		}, "invalid request body: article_id must be greater than 0", http.StatusBadRequest, logging.Field{Key: "article_id", Value: req.ArticleID})
		return
	}

	article, ok, err := h.findArticleByID(r.Context(), req.ArticleID)
	if err != nil {
		observability.RecordError(span, err)
		h.respondError(r, w, "http.extract_run.fetch_articles_failed", "http/extract/run", "single extraction run failed to fetch upstream articles", err, logging.ErrorContract{
			Failure:        "extract_run_fetch_articles_failed",
			Cause:          err.Error(),
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, req.ArticleID),
			Reaction:       "returned http 502 bad gateway",
		}, "failed to fetch upstream articles", http.StatusBadGateway, logging.Field{Key: "article_id", Value: req.ArticleID})
		return
	}
	if !ok {
		h.respondError(r, w, "http.extract_run.article_not_found", "http/extract/run", "single extraction run failed because requested article was not found", nil, logging.ErrorContract{
			Failure:        "extract_run_article_not_found",
			Cause:          "article_id was not present in upstream articles",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, req.ArticleID),
			Reaction:       "returned http 404 not found",
		}, "article not found", http.StatusNotFound, logging.Field{Key: "article_id", Value: req.ArticleID})
		return
	}

	appArticle, err := appextraction.FromModelArticle(article)
	if err != nil {
		observability.RecordError(span, err)
		h.respondError(r, w, "http.extract_run.article_conversion_failed", "http/extract/run", "single extraction run failed to convert upstream article into extraction input", err, logging.ErrorContract{
			Failure:        "extract_run_article_conversion_failed",
			Cause:          err.Error(),
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, req.ArticleID),
			Reaction:       "returned http 500 internal server error",
		}, "invalid article data", http.StatusInternalServerError, logging.Field{Key: "article_id", Value: req.ArticleID})
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
		writeJSON(r.Context(), h.logger, w, http.StatusOK, runResponse{ArticleID: req.ArticleID, Status: "failed", Error: "extraction run failed"}, fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, req.ArticleID))
		return
	}
	status := string(outcome.Outcome)
	if status == "" {
		status = "failed"
	}
	writeJSON(r.Context(), h.logger, w, http.StatusOK, runResponse{ArticleID: req.ArticleID, Status: status, Result: outcome.Result, Error: outcome.Error}, fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, req.ArticleID))
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
		h.respondError(r, w, "http.extract_run_batch.method_not_allowed", "http/extract/run-batch", "batch extraction run rejected request because HTTP method is not allowed", nil, logging.ErrorContract{
			Failure:        "extract_run_batch_method_not_allowed",
			Cause:          "method must be POST",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 405 method not allowed",
		}, "method not allowed", http.StatusMethodNotAllowed, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	if h.extractor == nil || h.repo == nil || h.extractorModel == "" {
		h.respondError(r, w, "http.extract_run_batch.dependencies_missing", "http/extract/run-batch", "batch extraction run failed because extraction dependencies are not configured", nil, logging.ErrorContract{
			Failure:        "extract_run_batch_dependencies_missing",
			Cause:          "extractor, repository, and model must be configured",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 500 internal server error",
		}, "extraction dependencies not configured", http.StatusInternalServerError, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}

	var req batchRunRequest
	if err := parseJSONBody(r, &req); err != nil {
		h.respondError(r, w, "http.extract_run_batch.invalid_request_body", "http/extract/run-batch", "batch extraction run rejected invalid JSON request body", err, logging.ErrorContract{
			Failure:        "extract_run_batch_invalid_request_body",
			Cause:          err.Error(),
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 400 bad request",
		}, "invalid request body: "+err.Error(), http.StatusBadRequest, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	if len(req.ArticleIDs) == 0 {
		h.respondError(r, w, "http.extract_run_batch.empty_article_ids", "http/extract/run-batch", "batch extraction run rejected request because article_ids is empty", nil, logging.ErrorContract{
			Failure:        "extract_run_batch_empty_article_ids",
			Cause:          "article_ids must not be empty",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_ids_count":%d}`, r.Method, r.URL.Path, len(req.ArticleIDs)),
			Reaction:       "returned http 400 bad request",
		}, "invalid request body: article_ids must not be empty", http.StatusBadRequest, logging.Field{Key: "article_ids_count", Value: len(req.ArticleIDs)})
		return
	}
	if req.Concurrency < 0 {
		h.respondError(r, w, "http.extract_run_batch.invalid_concurrency", "http/extract/run-batch", "batch extraction run rejected request because concurrency is negative", nil, logging.ErrorContract{
			Failure:        "extract_run_batch_invalid_concurrency",
			Cause:          "concurrency must be greater than or equal to 0",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"concurrency":%d}`, r.Method, r.URL.Path, req.Concurrency),
			Reaction:       "returned http 400 bad request",
		}, "invalid concurrency", http.StatusBadRequest, logging.Field{Key: "concurrency", Value: req.Concurrency})
		return
	}

	articles, err := h.upstreamClient.FetchArticles(r.Context())
	if err != nil {
		observability.RecordError(span, err)
		h.respondError(r, w, "http.extract_run_batch.fetch_articles_failed", "http/extract/run-batch", "batch extraction run failed to fetch upstream articles", err, logging.ErrorContract{
			Failure:        "extract_run_batch_fetch_articles_failed",
			Cause:          err.Error(),
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_ids_count":%d,"concurrency":%d}`, r.Method, r.URL.Path, len(req.ArticleIDs), req.Concurrency),
			Reaction:       "returned http 502 bad gateway",
		}, "failed to fetch upstream articles", http.StatusBadGateway, logging.Field{Key: "article_ids_count", Value: len(req.ArticleIDs)}, logging.Field{Key: "concurrency", Value: req.Concurrency})
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
			h.respondError(r, w, "http.extract_run_batch.execution_failed", "http/extract/run-batch", "batch extraction run failed during execution", batchErr, logging.ErrorContract{
				Failure:        "extract_run_batch_execution_failed",
				Cause:          batchErr.Error(),
				SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_ids_count":%d,"concurrency":%d}`, r.Method, r.URL.Path, len(req.ArticleIDs), req.Concurrency),
				Reaction:       "returned http 500 internal server error",
			}, "failed to run extraction batch", http.StatusInternalServerError, logging.Field{Key: "article_ids_count", Value: len(req.ArticleIDs)}, logging.Field{Key: "concurrency", Value: req.Concurrency})
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
	writeJSON(r.Context(), h.logger, w, http.StatusOK, resp, fmt.Sprintf(`{"method":%q,"path":%q,"article_ids_count":%d,"concurrency":%d}`, r.Method, r.URL.Path, len(req.ArticleIDs), req.Concurrency))
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
		h.respondError(r, w, "http.extract_result.method_not_allowed", "http/extract/result", "extract result endpoint rejected request because HTTP method is not allowed", nil, logging.ErrorContract{
			Failure:        "extract_result_method_not_allowed",
			Cause:          "method must be GET",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 405 method not allowed",
		}, "method not allowed", http.StatusMethodNotAllowed, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	if h.repo == nil {
		h.respondError(r, w, "http.extract_result.repository_missing", "http/extract/result", "extract result endpoint failed because repository dependency is not configured", nil, logging.ErrorContract{
			Failure:        "extract_result_repository_missing",
			Cause:          "repository must be configured",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 500 internal server error",
		}, "extraction repository not configured", http.StatusInternalServerError, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	rawArticleID := r.URL.Query().Get("article_id")
	id, err := strconv.ParseInt(rawArticleID, 10, 64)
	if err != nil || id <= 0 {
		cause := "article_id must be a positive base-10 integer"
		if err != nil {
			cause = err.Error()
		}
		h.respondError(r, w, "http.extract_result.invalid_article_id", "http/extract/result", "extract result endpoint rejected request because article_id is invalid", err, logging.ErrorContract{
			Failure:        "extract_result_invalid_article_id",
			Cause:          cause,
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%q}`, r.Method, r.URL.Path, rawArticleID),
			Reaction:       "returned http 400 bad request",
		}, "invalid article_id", http.StatusBadRequest, logging.Field{Key: "article_id", Value: rawArticleID})
		return
	}

	rec, err := h.repo.GetByArticleID(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			h.respondError(r, w, "http.extract_result.not_found", "http/extract/result", "extract result endpoint did not find persisted article result", err, logging.ErrorContract{
				Failure:        "extract_result_not_found",
				Cause:          err.Error(),
				SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, id),
				Reaction:       "returned http 404 not found",
			}, "article result not found", http.StatusNotFound, logging.Field{Key: "article_id", Value: id})
			return
		}
		h.respondError(r, w, "http.extract_result.load_failed", "http/extract/result", "extract result endpoint failed while loading persisted article result", err, logging.ErrorContract{
			Failure:        "extract_result_load_failed",
			Cause:          err.Error(),
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, id),
			Reaction:       "returned http 500 internal server error",
		}, "failed to load extraction result", http.StatusInternalServerError, logging.Field{Key: "article_id", Value: id})
		return
	}

	writeJSON(r.Context(), h.logger, w, http.StatusOK, readResultResponse{
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
	}, fmt.Sprintf(`{"method":%q,"path":%q,"article_id":%d}`, r.Method, r.URL.Path, id))
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
		h.respondError(r, w, "http.events.method_not_allowed", "http/events", "events endpoint rejected request because HTTP method is not allowed", nil, logging.ErrorContract{
			Failure:        "events_method_not_allowed",
			Cause:          "method must be GET",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 405 method not allowed",
		}, "method not allowed", http.StatusMethodNotAllowed, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	if h.clusterService == nil {
		h.respondError(r, w, "http.events.service_missing", "http/events", "events endpoint failed because clustering service dependency is not configured", nil, logging.ErrorContract{
			Failure:        "events_service_missing",
			Cause:          "clustering service must be configured",
			SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q}`, r.Method, r.URL.Path),
			Reaction:       "returned http 500 internal server error",
		}, "clustering service not configured", http.StatusInternalServerError, logging.Field{Key: "method", Value: r.Method}, logging.Field{Key: "path", Value: r.URL.Path})
		return
	}
	limit := 20
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			cause := "limit must be a positive integer"
			if err != nil {
				cause = err.Error()
			}
			h.respondError(r, w, "http.events.invalid_limit", "http/events", "events endpoint rejected request because limit query parameter is invalid", err, logging.ErrorContract{
				Failure:        "events_invalid_limit",
				Cause:          cause,
				SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"limit":%q}`, r.Method, r.URL.Path, rawLimit),
				Reaction:       "returned http 400 bad request",
			}, "invalid limit", http.StatusBadRequest, logging.Field{Key: "limit", Value: rawLimit})
			return
		}
		limit = parsed
	}

	var sincePtr *time.Time
	if rawSince := r.URL.Query().Get("since"); rawSince != "" {
		since, err := time.Parse(time.RFC3339, rawSince)
		if err != nil {
			h.respondError(r, w, "http.events.invalid_since", "http/events", "events endpoint rejected request because since query parameter is not valid RFC3339", err, logging.ErrorContract{
				Failure:        "events_invalid_since",
				Cause:          err.Error(),
				SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"limit":%d,"since":%q}`, r.Method, r.URL.Path, limit, rawSince),
				Reaction:       "returned http 400 bad request",
			}, "invalid since", http.StatusBadRequest, logging.Field{Key: "since", Value: rawSince}, logging.Field{Key: "limit", Value: limit})
			return
		}
		since = since.UTC()
		sincePtr = &since
	}
	var untilPtr *time.Time
	if rawUntil := r.URL.Query().Get("until"); rawUntil != "" {
		until, err := time.Parse(time.RFC3339, rawUntil)
		if err != nil {
			h.respondError(r, w, "http.events.invalid_until", "http/events", "events endpoint rejected request because until query parameter is not valid RFC3339", err, logging.ErrorContract{
				Failure:        "events_invalid_until",
				Cause:          err.Error(),
				SanitizedInput: fmt.Sprintf(`{"method":%q,"path":%q,"limit":%d,"until":%q}`, r.Method, r.URL.Path, limit, rawUntil),
				Reaction:       "returned http 400 bad request",
			}, "invalid until", http.StatusBadRequest, logging.Field{Key: "until", Value: rawUntil}, logging.Field{Key: "limit", Value: limit})
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
	writeJSON(r.Context(), h.logger, w, http.StatusOK, map[string]any{"limit": limit, "since": sincePtr, "until": untilPtr, "events": events}, fmt.Sprintf(`{"method":%q,"path":%q,"limit":%d}`, r.Method, r.URL.Path, limit))
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
func writeJSON(ctx context.Context, logger *logging.Logger, w http.ResponseWriter, code int, payload any, sanitizedInput string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		if logger != nil {
			logger.ErrorWithContract(ctx, "http.response.write_json_failed", "http/response", "failed to encode HTTP JSON response payload", err, logging.ErrorContract{
				Failure:        "write_json_encode_failed",
				Cause:          err.Error(),
				SanitizedInput: sanitizedInput,
				Reaction:       "attempted to return internal server error response",
			}, logging.Field{Key: "status_code", Value: code})
		}
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (h *Handler) respondError(r *http.Request, w http.ResponseWriter, event string, component string, message string, err error, contract logging.ErrorContract, clientMessage string, status int, fields ...logging.Field) {
	if h != nil && h.logger != nil {
		h.logger.ErrorWithContract(r.Context(), event, component, message, err, contract, fields...)
	}
	http.Error(w, clientMessage, status)
}
