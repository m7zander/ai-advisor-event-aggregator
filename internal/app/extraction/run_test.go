// Package extraction tests the single-article extraction orchestration behavior.
package extraction

import (
	"context"
	"errors"
	"testing"
	"time"

	"ai-advisor-event-aggregator/internal/extract"
	"ai-advisor-event-aggregator/internal/preprocess"
)

// fakeExtractor is a controllable test double for the Extractor dependency.
type fakeExtractor struct {
	called bool
	lastIn extract.ExtractInput
	out    extract.ExtractResult
	err    error
}

// Extract stores call input and returns configured output/error.
// The context parameter is accepted to satisfy the Extractor interface.
// It returns deterministic preconfigured values for test assertions.
func (f *fakeExtractor) Extract(_ context.Context, in extract.ExtractInput) (extract.ExtractResult, error) {
	f.called = true
	f.lastIn = in
	if f.err != nil {
		return extract.ExtractResult{}, f.err
	}
	return f.out, nil
}

// validArticle returns a baseline valid article for orchestration tests.
// The testing parameter is unused and retained for test helper consistency.
// It returns an Article that passes article-level validation.
func validArticle(_ *testing.T) Article {
	description := "description"
	content := "Content sentence with enough terms to survive filtering."
	return Article{
		ID:                  101,
		Title:               "Article title",
		Link:                "https://example.com/a",
		Source:              "example",
		PublishedAt:         time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC),
		Description:         &description,
		Content:             &content,
		FulltextExcerpt:     nil,
		FulltextContentText: nil,
	}
}

// validResultForArticle returns a baseline valid extract result for the provided article ID.
// The articleID parameter is propagated to the result ArticleID field for matching assertions.
// It returns a contract-valid result object.
func validResultForArticle(articleID int64) extract.ExtractResult {
	return extract.ExtractResult{
		ArticleID:       articleID,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"US"},
		Companies:       []string{"ACME"},
		Sectors:         []string{"industrials"},
		ImpactDirection: extract.ImpactDirectionNeutral,
		ImpactStrength:  25,
		Channels:        []extract.Channel{extract.ChannelRiskSentiment},
		TimeHorizon:     extract.TimeHorizonShort,
		Confidence:      0.6,
	}
}

// TestRun_Success verifies full happy-path orchestration for one article.
// It asserts preprocessing, extractor invocation, and validated result return.
// It fails if orchestration deviates from the required pipeline.
func TestRun_Success(t *testing.T) {
	article := validArticle(t)
	fx := &fakeExtractor{out: validResultForArticle(article.ID)}

	got, err := Run(context.Background(), article, fx)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if !fx.called {
		t.Fatal("expected extractor to be called")
	}
	if got.ArticleID != article.ID {
		t.Fatalf("unexpected article id in output: got %d want %d", got.ArticleID, article.ID)
	}
}

// TestRun_PreprocessFlowsIntoExtractInputText verifies cleaned preprocess text is passed to extractor input.
// It computes expected text using preprocess helpers and compares with captured extractor input text.
// It fails if orchestration bypasses or alters required preprocess flow.
func TestRun_PreprocessFlowsIntoExtractInputText(t *testing.T) {
	article := validArticle(t)
	fx := &fakeExtractor{out: validResultForArticle(article.ID)}

	_, err := Run(context.Background(), article, fx)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	source := preprocess.BuildInputText(
		article.Title,
		article.Description,
		article.Content,
		article.FulltextExcerpt,
		article.FulltextContentText,
	)
	want := preprocess.Process(source)

	if fx.lastIn.Text != want {
		t.Fatalf("unexpected extract input text\nwant: %q\ngot:  %q", want, fx.lastIn.Text)
	}
}

// TestRun_InvalidArticle verifies invalid article inputs fail before extractor invocation.
// It mutates required article fields into invalid states.
// It fails if invalid article data is accepted.
func TestRun_InvalidArticle(t *testing.T) {
	base := validArticle(t)
	tests := []struct {
		name string
		mut  func(a *Article)
	}{
		{name: "id", mut: func(a *Article) { a.ID = 0 }},
		{name: "title", mut: func(a *Article) { a.Title = "" }},
		{name: "link", mut: func(a *Article) { a.Link = "" }},
		{name: "source", mut: func(a *Article) { a.Source = "" }},
		{name: "published_at", mut: func(a *Article) { a.PublishedAt = time.Time{} }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			article := base
			tc.mut(&article)
			fx := &fakeExtractor{out: validResultForArticle(base.ID)}

			_, err := Run(context.Background(), article, fx)
			if err == nil {
				t.Fatalf("expected error for invalid article %s", tc.name)
			}
			if fx.called {
				t.Fatalf("extractor must not be called for invalid article %s", tc.name)
			}
		})
	}
}

// TestRun_InvalidExtractInput verifies invalid extract input is rejected before extractor invocation.
// It uses a valid article with content fields that preprocess into empty text.
// It fails if invalid extracted input passes validation.
func TestRun_InvalidExtractInput(t *testing.T) {
	article := validArticle(t)
	empty := ""
	article.Description = &empty
	article.Content = &empty
	article.FulltextExcerpt = &empty
	article.FulltextContentText = &empty

	fx := &fakeExtractor{out: validResultForArticle(article.ID)}
	_, err := Run(context.Background(), article, fx)
	if err == nil {
		t.Fatal("expected invalid extract input error")
	}
	if fx.called {
		t.Fatal("extractor must not be called when extract input is invalid")
	}
}

// TestRun_ExtractorError verifies extractor call errors are wrapped and returned.
// It configures the fake extractor to fail.
// It fails if the error is swallowed or extractor is skipped.
func TestRun_ExtractorError(t *testing.T) {
	article := validArticle(t)
	fx := &fakeExtractor{err: errors.New("boom")}

	_, err := Run(context.Background(), article, fx)
	if err == nil {
		t.Fatal("expected extractor error")
	}
	if !fx.called {
		t.Fatal("expected extractor to be called")
	}
}

// TestRun_InvalidExtractResult verifies extractor outputs are revalidated before return.
// It returns an invalid result enum from the fake extractor.
// It fails if invalid results are accepted.
func TestRun_InvalidExtractResult(t *testing.T) {
	article := validArticle(t)
	res := validResultForArticle(article.ID)
	res.EventType = extract.EventType("invalid")
	fx := &fakeExtractor{out: res}

	_, err := Run(context.Background(), article, fx)
	if err == nil {
		t.Fatal("expected invalid extract result error")
	}
}

// TestRun_ArticleIDMismatch verifies article ID consistency enforcement across orchestration boundary.
// It returns a valid result with mismatched article ID from the extractor.
// It fails if mismatch is not rejected.
func TestRun_ArticleIDMismatch(t *testing.T) {
	article := validArticle(t)
	fx := &fakeExtractor{out: validResultForArticle(article.ID + 1)}

	_, err := Run(context.Background(), article, fx)
	if err == nil {
		t.Fatal("expected article id mismatch error")
	}
}
