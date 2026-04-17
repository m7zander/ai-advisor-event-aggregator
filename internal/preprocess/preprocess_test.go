// Package preprocess tests deterministic input construction and cleanup behavior.
package preprocess

import (
	"strings"
	"testing"
)

func TestBuildInputText_IncludesAllFieldsInFixedOrder(t *testing.T) {
	description := "Description text"
	content := "Content text"
	excerpt := "Excerpt text"
	fulltext := "Fulltext content text"

	got := BuildInputText("Title text", &description, &content, &excerpt, &fulltext)
	want := "TITLE:\nTitle text\n\nDESCRIPTION:\nDescription text\n\nCONTENT:\nContent text\n\nFULLTEXT_EXCERPT:\nExcerpt text\n\nFULLTEXT_CONTENT_TEXT:\nFulltext content text"
	if got != want {
		t.Fatalf("unexpected composed input\nwant: %q\ngot:  %q", want, got)
	}
}

func TestBuildInputText_SkipsMissingOrEmptyFields(t *testing.T) {
	empty := "   "
	excerpt := "Excerpt text"

	got := BuildInputText(" ", nil, &empty, &excerpt, nil)
	want := "FULLTEXT_EXCERPT:\nExcerpt text"
	if got != want {
		t.Fatalf("unexpected composed input when skipping missing fields\nwant: %q\ngot:  %q", want, got)
	}
}

func TestNormalize(t *testing.T) {
	in := "  Hello   world  !  This   is,a test .  "
	got := Normalize(in)
	want := "Hello world! This is,a test."
	if got != want {
		t.Fatalf("normalize mismatch\nwant: %q\ngot:  %q", want, got)
	}
}

func TestSplitFragments(t *testing.T) {
	in := "One sentence. Two sentence! Three sentence? Four: five; end"
	got := SplitFragments(in)
	wantLen := 5
	if len(got) != wantLen {
		t.Fatalf("expected %d fragments, got %d (%v)", wantLen, len(got), got)
	}
}

func TestSplitFragments_DoesNotSplitOnColon(t *testing.T) {
	in := "Für dich zusammengefasst: Das ist boilerplate text with many words."
	got := SplitFragments(in)
	if len(got) != 1 {
		t.Fatalf("expected colon-delimited text to remain in a single fragment, got %d (%v)", len(got), got)
	}
}

func TestRemoveBoilerplateFragments(t *testing.T) {
	in := []string{
		"This is normal content with enough words to remain.",
		"Hinweis: this should be removed entirely.",
		"Another valid prose sentence with six words.",
	}
	got := RemoveBoilerplateFragments(in)
	if len(got) != 2 {
		t.Fatalf("expected 2 fragments after removal, got %d", len(got))
	}
	if strings.Contains(strings.Join(got, " "), "Hinweis:") {
		t.Fatal("boilerplate marker should be removed")
	}
}

func TestFilterNoisyFragments(t *testing.T) {
	in := []string{
		"Too short now",
		"Click menu share anmelden subscribe now please quickly",
		"%%%% #### !!!! ??? *** &&& $$$ lots symbols words text data",
		"This is a proper fragment that should remain in output.",
	}
	got := FilterNoisyFragments(in)
	if len(got) != 1 {
		t.Fatalf("expected 1 fragment to remain, got %d (%v)", len(got), got)
	}
}

func TestFilterNoisyFragments_MarkerWholeWordMatching(t *testing.T) {
	in := []string{
		"This article talks about market trends and keeps full context.",
		"Please buy an abo now to unlock full article access quickly.",
	}

	got := FilterNoisyFragments(in)
	if len(got) != 1 {
		t.Fatalf("expected only the non-UI prose fragment to remain, got %d (%v)", len(got), got)
	}
	if got[0] != in[0] {
		t.Fatalf("expected valid prose containing 'about' to remain, got %q", got[0])
	}
}

func TestRebuildText(t *testing.T) {
	in := []string{"First fragment.", "Second fragment!"}
	got := RebuildText(in)
	want := "First fragment. Second fragment!"
	if got != want {
		t.Fatalf("expected rebuilt text %q, got %q", want, got)
	}
}

func TestHeadTailExtract_ShortTextUnchanged(t *testing.T) {
	short := strings.Repeat("a", MaxTotalChars)
	if got := HeadTailExtract(short); got != short {
		t.Fatalf("text at max limit should remain unchanged")
	}
}

func TestHeadTailExtract_LongTextMaxLengthAndJoiner(t *testing.T) {
	sentence := "This is a sentence that ends cleanly. "
	long := strings.Repeat(sentence, 700)

	got := HeadTailExtract(long)
	if len([]rune(got)) > MaxTotalChars {
		t.Fatalf("output exceeds max total chars: %d", len([]rune(got)))
	}
	if !strings.Contains(got, HeadTailJoiner) {
		t.Fatalf("expected joiner in long text extraction")
	}
}

func TestHeadTailExtract_RespectsSentenceBoundaryWithinTolerance(t *testing.T) {
	sentence := "Alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu. "
	long := strings.Repeat(sentence, 500)

	got := HeadTailExtract(long)
	parts := strings.Split(got, HeadTailJoiner)
	if len(parts) != 2 {
		t.Fatalf("expected exactly two parts around joiner")
	}

	head := strings.TrimSpace(parts[0])
	tail := strings.TrimSpace(parts[1])
	if head == "" || tail == "" {
		t.Fatalf("head and tail should both be non-empty")
	}
	if !strings.HasSuffix(head, ".") && !strings.HasSuffix(head, "!") && !strings.HasSuffix(head, "?") {
		t.Fatalf("head should end at a sentence boundary when available")
	}
}

func TestHeadTailExtract_PreservesOriginalTailWhenBoundarySnappingOverflows(t *testing.T) {
	joinerRunes := []rune(HeadTailJoiner)
	available := MaxTotalChars - len(joinerRunes)
	headTarget := int(float64(available)*HeadRatio + 0.5)
	tailTarget := available - headTarget

	totalRunes := 15450
	tailStartTarget := totalRunes - tailTarget

	inputRunes := make([]rune, totalRunes)
	for i := range inputRunes {
		inputRunes[i] = 'a'
	}

	// Place sentence terminators so boundary snapping expands both sides:
	// head boundary snaps forward (+tolerance), tail boundary snaps backward (-tolerance).
	headBoundaryIdx := headTarget + SentenceCutTolerance
	tailBoundaryIdx := tailStartTarget - SentenceCutTolerance
	inputRunes[headBoundaryIdx-1] = '.'
	inputRunes[tailBoundaryIdx-1] = '.'

	input := string(inputRunes)
	got := HeadTailExtract(input)
	if len([]rune(got)) > MaxTotalChars {
		t.Fatalf("output exceeds max total chars: %d", len([]rune(got)))
	}

	parts := strings.Split(got, HeadTailJoiner)
	if len(parts) != 2 {
		t.Fatalf("expected exactly two parts around joiner, got %d", len(parts))
	}
	tail := strings.TrimLeftFunc(parts[1], func(r rune) bool { return r == ' ' || r == '\n' || r == '\t' })
	if !strings.HasSuffix(input, tail) {
		t.Fatalf("expected output tail to preserve original ending segment")
	}
}
