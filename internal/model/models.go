// Package model defines API and domain data structures shared across layers.
package model

type UpstreamResponse struct {
	Data []Article `json:"data"`
}

type Article struct {
	ID                  int     `json:"id"`
	Title               string  `json:"title"`
	Link                string  `json:"link"`
	Source              string  `json:"source"`
	Description         *string `json:"description"`
	Content             *string `json:"content"`
	PublishedAt         string  `json:"published_at"`
	FulltextExcerpt     *string `json:"fulltext_excerpt"`
	FulltextContentText *string `json:"fulltext_content_text"`
}

type CleanText struct {
	ArticleID   int    `json:"article_id"`
	Title       string `json:"title"`
	Link        string `json:"link"`
	Source      string `json:"source"`
	PublishedAt string `json:"published_at"`
	Text        string `json:"text"`
}

type PreprocessResponse struct {
	Data []CleanText `json:"data"`
}
