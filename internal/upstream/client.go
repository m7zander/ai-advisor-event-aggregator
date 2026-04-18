// Package upstream provides a typed HTTP client for the News Intake Service.
// Trust boundary: upstream responses are external/untrusted and must be validated by consuming layers.
package upstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"ai-advisor-event-aggregator/internal/logging"
	"ai-advisor-event-aggregator/internal/model"
	"ai-advisor-event-aggregator/internal/observability"
)

const (
	defaultTimeout       = 10 * time.Second
	eligibleArticlesPath = "/articles"
)

var upstreamLogger = logging.New()

// ArticlesResponse represents the upstream list response used by scheduler polling.
type ArticlesResponse struct {
	Data []model.Article `json:"data"`
}

// Client calls the upstream News Intake Service over HTTP.
type Client struct {
	httpClient *http.Client
	host       string
	port       string
}

// NewClient constructs a new upstream client.
// The host and port parameters identify the News Intake Service network location.
// It returns a ready-to-use client with a fixed request timeout.
func NewClient(host, port string) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: defaultTimeout},
		host:       host,
		port:       port,
	}
}

// FetchArticles loads one default page of eligible articles for existing preprocess/extract handlers.
// The ctx parameter controls request cancellation and timeout propagation.
// It returns decoded article data or an error when request execution/decoding fails.
func (c *Client) FetchArticles(ctx context.Context) ([]model.Article, error) {
	resp, err := c.ListEligibleArticles(ctx, 50, 0)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// ListEligibleArticles loads one page of eligible upstream articles with explicit pagination controls.
// The ctx parameter controls lifecycle, while limit and offset define page size and start position.
// It returns a decoded upstream response or an error for non-200 status, request failure, or malformed payload.
// Sanitization rules: limit and offset are validated before URL construction to avoid unsafe query values.
func (c *Client) ListEligibleArticles(ctx context.Context, limit int, offset int) (ArticlesResponse, error) {
	ctx, span := observability.StartSpan(ctx, "upstream.fetch_eligible_articles")
	defer span.End()

	if limit <= 0 {
		err := fmt.Errorf("limit must be > 0")
		observability.RecordError(span, err)
		return ArticlesResponse{}, err
	}
	if offset < 0 {
		err := fmt.Errorf("offset must be >= 0")
		observability.RecordError(span, err)
		return ArticlesResponse{}, err
	}

	requestURL, err := c.buildEligibleArticlesURL(limit, offset)
	if err != nil {
		observability.RecordError(span, err)
		return ArticlesResponse{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		observability.RecordError(span, err)
		return ArticlesResponse{}, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		observability.RecordError(span, err)
		return ArticlesResponse{}, fmt.Errorf("call upstream: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			upstreamLogger.ErrorWithContract(ctx, "upstream.http_response_body_close_failed", "upstream/client", "failed to close http response body", cerr, logging.ErrorContract{
				Failure:        "upstream_response_body_close_failed",
				Cause:          cerr.Error(),
				SanitizedInput: fmt.Sprintf(`{"operation":"list_eligible_articles","limit":%d,"offset":%d}`, limit, offset),
				Reaction:       "connection close failure logged",
			},
				logging.Field{Key: "operation", Value: "list_eligible_articles"},
				logging.Field{Key: "url", Value: requestURL},
				logging.Field{Key: "limit", Value: limit},
				logging.Field{Key: "offset", Value: offset},
			)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("upstream non-200 status: %d", resp.StatusCode)
		observability.RecordError(span, err)
		return ArticlesResponse{}, err
	}

	var decoded ArticlesResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		observability.RecordError(span, err)
		return ArticlesResponse{}, fmt.Errorf("decode upstream response: %w", err)
	}
	return decoded, nil
}

// buildEligibleArticlesURL builds the request URL for eligible article polling.
// The limit and offset parameters are encoded as query values alongside fixed eligibility filters.
// It returns an absolute URL string or an error when URL assembly fails.
func (c *Client) buildEligibleArticlesURL(limit int, offset int) (string, error) {
	if c.host == "" || c.port == "" {
		return "", fmt.Errorf("host and port are required")
	}
	return fmt.Sprintf(
		"http://%s:%s%s?status=done&decision=relevant&fulltext_status=done&limit=%d&offset=%d",
		c.host,
		c.port,
		eligibleArticlesPath,
		limit,
		offset,
	), nil
}
