// Package preprocess provides deterministic text input construction and cleanup.
package preprocess

import (
	"math"
	"strings"
	"unicode"
)

const (
	MaxTotalChars        = 15000
	HeadRatio            = 0.75
	SentenceCutTolerance = 200
	HeadTailJoiner       = " ... "
)

var boilerplateMarkers = []string{
	"Hinweis:",
	"Quelle:",
	"Weitere Artikel",
	"Themen im Trend",
	"Newsletter",
	"Feedback",
	"Kopieren",
	"Anhören",
	"Aufhören",
	"Schrift vergrößern",
	"Schrift verkleinern",
	"Für dich zusammengefasst:",
}

var uiControlMarkers = []string{
	"click",
	"klicken",
	"menu",
	"menü",
	"teilen",
	"share",
	"anmelden",
	"login",
	"registrieren",
	"cookie",
	"datenschutz",
	"impressum",
	"subscribe",
	"abo",
}

func Process(source string) string {
	normalized := Normalize(source)
	fragments := SplitFragments(normalized)
	fragments = RemoveBoilerplateFragments(fragments)
	fragments = FilterNoisyFragments(fragments)
	rebuilt := RebuildText(fragments)
	return HeadTailExtract(rebuilt)
}

// BuildInputText concatenates configured article fields into one labeled block.
//
// Parameters:
//   - title: article title field.
//   - description: article description field.
//   - content: article content field.
//   - fulltextExcerpt: article fulltext excerpt field.
//   - fulltextContentText: article fulltext plain content field.
//
// Returns:
//   - A single text block with sections in fixed order and labels.
//   - Missing or empty fields are skipped.
//
// Side effects/failure behavior:
//   - No side effects.
//   - Never returns an error.
func BuildInputText(title string, description *string, content *string, fulltextExcerpt *string, fulltextContentText *string) string {
	sections := make([]string, 0, 5)
	if trimmed := strings.TrimSpace(title); trimmed != "" {
		sections = append(sections, "TITLE:\n"+trimmed)
	}
	if description != nil {
		if trimmed := strings.TrimSpace(*description); trimmed != "" {
			sections = append(sections, "DESCRIPTION:\n"+trimmed)
		}
	}
	if content != nil {
		if trimmed := strings.TrimSpace(*content); trimmed != "" {
			sections = append(sections, "CONTENT:\n"+trimmed)
		}
	}
	if fulltextExcerpt != nil {
		if trimmed := strings.TrimSpace(*fulltextExcerpt); trimmed != "" {
			sections = append(sections, "FULLTEXT_EXCERPT:\n"+trimmed)
		}
	}
	if fulltextContentText != nil {
		if trimmed := strings.TrimSpace(*fulltextContentText); trimmed != "" {
			sections = append(sections, "FULLTEXT_CONTENT_TEXT:\n"+trimmed)
		}
	}
	return strings.Join(sections, "\n\n")
}

func Normalize(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	fields := strings.Fields(text)
	text = strings.Join(fields, " ")
	replacer := strings.NewReplacer(" ,", ",", " .", ".", " !", "!", " ?", "?", " ;", ";", " :", ":")
	text = replacer.Replace(text)
	text = strings.TrimSpace(text)
	return text
}

func SplitFragments(text string) []string {
	if text == "" {
		return nil
	}

	var fragments []string
	var current []rune
	for _, r := range text {
		current = append(current, r)
		if isSeparator(r) {
			fragment := strings.TrimSpace(string(current))
			if fragment != "" {
				fragments = append(fragments, fragment)
			}
			current = current[:0]
		}
	}
	if len(current) > 0 {
		fragment := strings.TrimSpace(string(current))
		if fragment != "" {
			fragments = append(fragments, fragment)
		}
	}
	return fragments
}

func RemoveBoilerplateFragments(fragments []string) []string {
	out := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		remove := false
		for _, marker := range boilerplateMarkers {
			if strings.Contains(fragment, marker) {
				remove = true
				break
			}
		}
		if !remove {
			out = append(out, fragment)
		}
	}
	return out
}

func FilterNoisyFragments(fragments []string) []string {
	out := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		if wordCount(fragment) < 5 {
			continue
		}
		if isUIControlText(fragment) {
			continue
		}
		if hasExcessiveSymbolDensity(fragment, 0.30) {
			continue
		}
		out = append(out, fragment)
	}
	return out
}

func RebuildText(fragments []string) string {
	if len(fragments) == 0 {
		return ""
	}
	text := strings.Join(fragments, " ")
	return Normalize(text)
}

func HeadTailExtract(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}

	runes := []rune(text)
	if len(runes) <= MaxTotalChars {
		return text
	}

	joinerRunes := []rune(HeadTailJoiner)
	available := MaxTotalChars - len(joinerRunes)
	if available <= 0 {
		return string(runes[:MaxTotalChars])
	}

	headTarget := int(math.Round(float64(available) * HeadRatio))
	tailTarget := available - headTarget

	headCut := findBestSentenceBoundary(runes, headTarget, SentenceCutTolerance)
	tailStartTarget := len(runes) - tailTarget
	tailStart := findBestSentenceBoundary(runes, tailStartTarget, SentenceCutTolerance)

	if tailStart < headCut {
		tailStart = headCut
	}

	head := strings.TrimRightFunc(string(runes[:headCut]), unicode.IsSpace)
	tail := strings.TrimLeftFunc(string(runes[tailStart:]), unicode.IsSpace)

	result := head + HeadTailJoiner + tail
	resultRunes := []rune(result)
	if len(resultRunes) > MaxTotalChars {
		headRunes := []rune(head)
		tailRunes := []rune(tail)

		allowedHead := MaxTotalChars - len(joinerRunes) - len(tailRunes)
		if allowedHead < 0 {
			allowedTail := MaxTotalChars
			if len(joinerRunes) < MaxTotalChars {
				allowedTail = MaxTotalChars - len(joinerRunes)
			}
			if allowedTail < len(tailRunes) {
				tailRunes = tailRunes[len(tailRunes)-allowedTail:]
			}

			if len(tailRunes) == MaxTotalChars {
				return string(tailRunes)
			}
			return HeadTailJoiner + string(tailRunes)
		}

		if len(headRunes) > allowedHead {
			headRunes = headRunes[:allowedHead]
		}
		head = strings.TrimRightFunc(string(headRunes), unicode.IsSpace)
		return head + HeadTailJoiner + tail
	}
	return result
}

func findBestSentenceBoundary(runes []rune, target int, tolerance int) int {
	n := len(runes)
	if n == 0 {
		return 0
	}
	if target <= 0 {
		return 0
	}
	if target >= n {
		return n
	}

	start := target - tolerance
	if start < 1 {
		start = 1
	}
	end := target + tolerance
	if end > n-1 {
		end = n - 1
	}

	best := -1
	bestDist := n + 1
	for i := start; i <= end; i++ {
		if isSentenceBoundary(runes, i) {
			dist := absInt(i - target)
			if dist < bestDist {
				best = i
				bestDist = dist
			}
		}
	}

	if best != -1 {
		return best
	}
	return target
}

func isSentenceBoundary(runes []rune, idx int) bool {
	if idx <= 0 || idx >= len(runes) {
		return false
	}
	return isSentenceTerminator(runes[idx-1])
}

func isSentenceTerminator(r rune) bool {
	return r == '.' || r == '!' || r == '?'
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func isSeparator(r rune) bool {
	switch r {
	case '.', '!', '?', ';':
		return true
	default:
		return false
	}
}

func wordCount(text string) int {
	return len(strings.Fields(strings.TrimSpace(text)))
}

func isUIControlText(fragment string) bool {
	lower := strings.ToLower(fragment)
	words := make(map[string]struct{})
	for _, token := range strings.FieldsFunc(lower, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if token == "" {
			continue
		}
		words[token] = struct{}{}
	}

	for _, marker := range uiControlMarkers {
		marker = strings.ToLower(marker)
		if strings.Contains(marker, " ") {
			if strings.Contains(lower, marker) {
				return true
			}
			continue
		}
		if _, ok := words[marker]; ok {
			return true
		}
	}
	return false
}

func hasExcessiveSymbolDensity(fragment string, threshold float64) bool {
	runes := []rune(fragment)
	if len(runes) == 0 {
		return false
	}
	symbols := 0
	for _, r := range runes {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			continue
		}
		symbols++
	}
	ratio := float64(symbols) / float64(len(runes))
	return ratio > threshold
}
