// Package extraction provides the internal single-article orchestration flow for extraction.
// It coordinates preprocessing, contract construction/validation, and a pluggable extractor dependency.
package extraction

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ai-advisor-impact-service/internal/extract"
	"ai-advisor-impact-service/internal/observability"
	"ai-advisor-impact-service/internal/preprocess"
)

// Extractor defines the narrow dependency required by the extraction orchestration.
// Implementations execute extraction using any backend and must return a contract-compliant result.
type Extractor interface {
	Extract(ctx context.Context, in extract.ExtractInput) (extract.ExtractResult, error)
}

// Article is the minimal app-layer article contract consumed by single-article extraction orchestration.
type Article struct {
	ID                  int64
	Title               string
	Link                string
	Source              string
	PublishedAt         time.Time
	Description         *string
	Content             *string
	FulltextExcerpt     *string
	FulltextContentText *string
}

// Run executes deterministic orchestration for extracting one article.
// It validates the incoming article, builds and preprocesses source text, validates extractor input,
// invokes the injected extractor, validates the extractor result, and enforces article ID consistency.
// Parameters: ctx is the request lifecycle context; article is the single input record;
// extractor is the injected extraction dependency.
// Returns a validated extract result or an error with contextual wrapping.
func Run(ctx context.Context, article Article, extractor Extractor) (extract.ExtractResult, error) {
	ctx, span := observability.StartSpan(ctx, "extraction.run")
	defer span.End()

	if err := validateArticle(article); err != nil {
		observability.RecordError(span, err)
		return extract.ExtractResult{}, fmt.Errorf("validate article: %w", err)
	}
	if extractor == nil {
		err := fmt.Errorf("extractor is nil")
		observability.RecordError(span, err)
		return extract.ExtractResult{}, err
	}

	source := preprocess.BuildInputText(
		article.Title,
		article.Description,
		article.Content,
		article.FulltextExcerpt,
		article.FulltextContentText,
	)
	cleanText := preprocess.Process(source)

	in := extract.ExtractInput{
		ArticleID:   article.ID,
		Title:       article.Title,
		Link:        article.Link,
		Source:      article.Source,
		PublishedAt: article.PublishedAt,
		Text:        cleanText,
	}

	if err := in.Validate(); err != nil {
		observability.RecordError(span, err)
		return extract.ExtractResult{}, fmt.Errorf("validate extract input: %w", err)
	}

	out, err := extractor.Extract(ctx, in)
	if err != nil {
		observability.RecordError(span, err)
		return extract.ExtractResult{}, fmt.Errorf("extractor call failed: %w", err)
	}

	if err := out.Validate(); err != nil {
		observability.RecordError(span, err)
		return extract.ExtractResult{}, fmt.Errorf("validate extract result: %w", err)
	}
	if out.ArticleID != article.ID {
		err := fmt.Errorf("article id mismatch: input=%d result=%d", article.ID, out.ArticleID)
		observability.RecordError(span, err)
		return extract.ExtractResult{}, err
	}

	return out, nil
}

// validateArticle enforces required article fields needed by orchestration.
// The article parameter is the app-layer single-article input contract.
// It returns an error describing the first invalid required field or nil when valid.
func validateArticle(article Article) error {
	if article.ID <= 0 {
		return fmt.Errorf("id must be > 0")
	}
	if strings.TrimSpace(article.Title) == "" {
		return fmt.Errorf("title must not be empty")
	}
	if strings.TrimSpace(article.Link) == "" {
		return fmt.Errorf("link must not be empty")
	}
	if strings.TrimSpace(article.Source) == "" {
		return fmt.Errorf("source must not be empty")
	}
	if article.PublishedAt.IsZero() {
		return fmt.Errorf("published_at must not be zero")
	}
	return nil
}
