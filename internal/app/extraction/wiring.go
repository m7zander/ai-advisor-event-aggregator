// Package extraction provides internal wiring helpers for the single-article extraction flow.
// This file connects app orchestration contracts with the concrete LLM extractor implementation.
package extraction

import (
	"fmt"
	"strings"
	"time"

	"ai-advisor-impact-service/internal/llm"
	"ai-advisor-impact-service/internal/model"
)

// LLMConfig contains minimal constructor config for the real LLM-backed extractor.
type LLMConfig struct {
	APIKey  string
	Model   string
	BaseURL string
	Timeout time.Duration
}

// NewLLMExtractor constructs the real LLM-backed extractor dependency from configuration.
// The cfg parameter supplies API key, model, timeout, and optional base URL.
// It returns an Extractor-compatible implementation or an error when configuration is invalid.
func NewLLMExtractor(cfg LLMConfig) (Extractor, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("api key is required")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, fmt.Errorf("model is required")
	}
	if cfg.Timeout <= 0 {
		return nil, fmt.Errorf("timeout must be > 0")
	}

	client, err := llm.NewClient(cfg.APIKey, cfg.Model, cfg.BaseURL, cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("build llm extractor: %w", err)
	}
	return client, nil
}

// FromModelArticle adapts the existing model.Article into the app-layer Article contract.
// The in parameter is an upstream/model article with PublishedAt as RFC3339 string.
// It returns an app-layer Article or an error when PublishedAt parsing fails.
func FromModelArticle(in model.Article) (Article, error) {
	publishedAt, err := time.Parse(time.RFC3339, in.PublishedAt)
	if err != nil {
		return Article{}, fmt.Errorf("parse published_at: %w", err)
	}

	return Article{
		ID:                  int64(in.ID),
		Title:               in.Title,
		Link:                in.Link,
		Source:              in.Source,
		PublishedAt:         publishedAt,
		Description:         in.Description,
		Content:             in.Content,
		FulltextExcerpt:     in.FulltextExcerpt,
		FulltextContentText: in.FulltextContentText,
	}, nil
}
