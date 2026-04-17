// Package extract tests strict domain contract validation for extractor input and output types.
package extract

import (
	"testing"
	"time"
)

// validInput returns a baseline valid ExtractInput for test setup.
// The testing parameter is unused and included only for consistency in test factories.
// It returns a fully populated, valid contract value.
func validInput(_ *testing.T) ExtractInput {
	return ExtractInput{
		ArticleID:   123,
		Title:       "Test Title",
		Link:        "https://example.com/article",
		Source:      "example",
		PublishedAt: time.Now().UTC(),
		Text:        "preprocessed text",
	}
}

// validResult returns a baseline valid ExtractResult for test setup.
// The testing parameter is unused and included only for consistency in test factories.
// It returns a fully populated, valid contract value.
func validResult(_ *testing.T) ExtractResult {
	return ExtractResult{
		ArticleID:       123,
		EventType:       EventTypeMacro,
		GeoCluster:      GeoClusterGlobal,
		Countries:       []string{"US"},
		Companies:       []string{"ACME"},
		Sectors:         []string{"industrials"},
		Industries:      []string{"Software - Application"},
		ImpactDirection: ImpactDirectionMixed,
		ImpactStrength:  42,
		Channels:        []Channel{ChannelRiskSentiment},
		TimeHorizon:     TimeHorizonShort,
		Confidence:      0.7,
	}
}

// TestExtractInputValidate_Valid verifies that a fully populated input passes validation.
// It uses a baseline valid input and expects no error.
// It fails the test if validation incorrectly rejects valid input.
func TestExtractInputValidate_Valid(t *testing.T) {
	in := validInput(t)
	if err := in.Validate(); err != nil {
		t.Fatalf("expected valid input, got error: %v", err)
	}
}

// TestExtractInputValidate_Invalid verifies required field checks for extractor input.
// It applies table-driven invalid mutations to baseline input.
// It fails the test if any invalid input is accepted.
func TestExtractInputValidate_Invalid(t *testing.T) {
	base := validInput(t)
	tests := []struct {
		name string
		mut  func(in *ExtractInput)
	}{
		{name: "article id", mut: func(in *ExtractInput) { in.ArticleID = 0 }},
		{name: "title", mut: func(in *ExtractInput) { in.Title = "" }},
		{name: "link", mut: func(in *ExtractInput) { in.Link = "" }},
		{name: "source", mut: func(in *ExtractInput) { in.Source = "" }},
		{name: "published at", mut: func(in *ExtractInput) { in.PublishedAt = time.Time{} }},
		{name: "text", mut: func(in *ExtractInput) { in.Text = "" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			tc.mut(&in)
			if err := in.Validate(); err == nil {
				t.Fatalf("expected error for invalid %s", tc.name)
			}
		})
	}
}

// TestExtractResultValidate_Valid verifies a valid extractor result passes strict validation.
// It uses a baseline valid result contract.
// It fails the test if strict validation rejects valid data.
func TestExtractResultValidate_Valid(t *testing.T) {
	res := validResult(t)
	if err := res.Validate(); err != nil {
		t.Fatalf("expected valid result, got error: %v", err)
	}
}

// TestExtractResultValidate_InvalidEnums verifies rejection of invalid enum-like values.
// It mutates each controlled enum field with unknown values.
// It fails the test if invalid enum values are accepted.
func TestExtractResultValidate_InvalidEnums(t *testing.T) {
	tests := []struct {
		name string
		mut  func(r *ExtractResult)
	}{
		{name: "event type", mut: func(r *ExtractResult) { r.EventType = EventType("bad") }},
		{name: "geo cluster", mut: func(r *ExtractResult) { r.GeoCluster = GeoCluster("bad") }},
		{name: "impact direction", mut: func(r *ExtractResult) { r.ImpactDirection = ImpactDirection("bad") }},
		{name: "time horizon", mut: func(r *ExtractResult) { r.TimeHorizon = TimeHorizon("bad") }},
		{name: "channel", mut: func(r *ExtractResult) { r.Channels = []Channel{Channel("bad")} }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := validResult(t)
			tc.mut(&res)
			if err := res.Validate(); err == nil {
				t.Fatalf("expected enum validation error for %s", tc.name)
			}
		})
	}
}

// TestExtractResultValidate_GeoClusterPromptValues verifies all prompt-defined geo_cluster enum values are accepted.
// It validates each allowed geo_cluster from the extractor prompt as a valid ExtractResult value.
// It fails the test if any prompt-defined geo_cluster value is rejected by domain validation.
func TestExtractResultValidate_GeoClusterPromptValues(t *testing.T) {
	tests := []struct {
		name  string
		value GeoCluster
	}{
		{name: "none", value: GeoClusterNone},
		{name: "north_america", value: GeoClusterNorthAmerica},
		{name: "europe", value: GeoClusterEurope},
		{name: "russia_cis", value: GeoClusterRussiaCIS},
		{name: "middle_east", value: GeoClusterMiddleEast},
		{name: "east_asia", value: GeoClusterEastAsia},
		{name: "south_asia", value: GeoClusterSouthAsia},
		{name: "africa", value: GeoClusterAfrica},
		{name: "global_trade_routes", value: GeoClusterGlobalTradeRoutes},
		{name: "global", value: GeoClusterGlobal},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := validResult(t)
			res.GeoCluster = tc.value
			if err := res.Validate(); err != nil {
				t.Fatalf("expected geo_cluster %q to be valid, got error: %v", tc.value, err)
			}
		})
	}
}

// TestExtractResultValidate_GeoClusterLegacyValuesRejected verifies geo_cluster values no longer in the prompt are rejected.
// It checks representative legacy values that were previously accepted by domain validation.
// It fails the test if any legacy geo_cluster value is accepted.
func TestExtractResultValidate_GeoClusterLegacyValuesRejected(t *testing.T) {
	tests := []struct {
		name  string
		value GeoCluster
	}{
		{name: "iran", value: GeoCluster("iran")},
		{name: "ukraine", value: GeoCluster("ukraine")},
		{name: "china", value: GeoCluster("china")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := validResult(t)
			res.GeoCluster = tc.value
			if err := res.Validate(); err == nil {
				t.Fatalf("expected geo_cluster %q to be rejected", tc.value)
			}
		})
	}
}

// TestExtractResultValidate_InvalidRanges verifies numeric range constraints.
// It checks both lower and upper bound violations for strength and confidence.
// It fails the test if out-of-range values are accepted.
func TestExtractResultValidate_InvalidRanges(t *testing.T) {
	tests := []struct {
		name string
		mut  func(r *ExtractResult)
	}{
		{name: "impact strength low", mut: func(r *ExtractResult) { r.ImpactStrength = -1 }},
		{name: "impact strength high", mut: func(r *ExtractResult) { r.ImpactStrength = 101 }},
		{name: "confidence low", mut: func(r *ExtractResult) { r.Confidence = -0.01 }},
		{name: "confidence high", mut: func(r *ExtractResult) { r.Confidence = 1.01 }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := validResult(t)
			tc.mut(&res)
			if err := res.Validate(); err == nil {
				t.Fatalf("expected range validation error for %s", tc.name)
			}
		})
	}
}

// TestExtractResultValidate_DuplicateChannels verifies duplicate channel values are rejected.
// It constructs a result with repeated valid channels.
// It fails the test if duplicates are accepted.
func TestExtractResultValidate_DuplicateChannels(t *testing.T) {
	res := validResult(t)
	res.Channels = []Channel{ChannelRiskSentiment, ChannelRiskSentiment}
	if err := res.Validate(); err == nil {
		t.Fatalf("expected duplicate channel error")
	}
}

// TestExtractResultValidate_ChannelPromptValues verifies all prompt-defined channel enum values are accepted.
// It validates each allowed channel from the extractor prompt as a valid ExtractResult channel value.
// It fails the test if any prompt-defined channel value is rejected by domain validation.
func TestExtractResultValidate_ChannelPromptValues(t *testing.T) {
	tests := []struct {
		name  string
		value Channel
	}{
		{name: "oil_supply_risk", value: ChannelOilSupplyRisk},
		{name: "gas_supply_risk", value: ChannelGasSupplyRisk},
		{name: "shipping_disruption", value: ChannelShippingDisruption},
		{name: "risk_sentiment", value: ChannelRiskSentiment},
		{name: "defense_spending", value: ChannelDefenseSpending},
		{name: "rates_fx", value: ChannelRatesFX},
		{name: "inflation", value: ChannelInflation},
		{name: "regulation_policy", value: ChannelRegulationPolicy},
		{name: "earnings_guidance", value: ChannelEarningsGuidance},
		{name: "analyst_revision", value: ChannelAnalystRevision},
		{name: "supply_chain_disruption", value: ChannelSupplyChainDisruption},
		{name: "demand_shift", value: ChannelDemandShift},
		{name: "labor_dispute", value: ChannelLaborDispute},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := validResult(t)
			res.Channels = []Channel{tc.value}
			if err := res.Validate(); err != nil {
				t.Fatalf("expected channel %q to be valid, got error: %v", tc.value, err)
			}
		})
	}
}

// TestExtractResultValidate_EmptyStringsInArrays verifies array element non-empty constraints.
// It checks countries, companies, sectors, industries, and channels arrays individually.
// It fails the test if empty array elements are accepted.
func TestExtractResultValidate_EmptyStringsInArrays(t *testing.T) {
	tests := []struct {
		name string
		mut  func(r *ExtractResult)
	}{
		{name: "countries", mut: func(r *ExtractResult) { r.Countries = []string{""} }},
		{name: "companies", mut: func(r *ExtractResult) { r.Companies = []string{""} }},
		{name: "sectors", mut: func(r *ExtractResult) { r.Sectors = []string{""} }},
		{name: "industries", mut: func(r *ExtractResult) { r.Industries = []string{""} }},
		{name: "channels", mut: func(r *ExtractResult) { r.Channels = []Channel{""} }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := validResult(t)
			tc.mut(&res)
			if err := res.Validate(); err == nil {
				t.Fatalf("expected empty element error for %s", tc.name)
			}
		})
	}
}

// TestExtractResultValidate_NormalizesNilSlices verifies nil arrays are normalized to empty slices.
// It passes a result with nil arrays into validation.
// It fails the test if validation does not normalize arrays deterministically.
func TestExtractResultValidate_NormalizesNilSlices(t *testing.T) {
	res := validResult(t)
	res.Countries = nil
	res.Companies = nil
	res.Sectors = nil
	res.Industries = nil
	res.Channels = nil

	if err := res.Validate(); err != nil {
		t.Fatalf("expected normalized nil slices to remain valid, got error: %v", err)
	}
	if res.Countries == nil || res.Companies == nil || res.Sectors == nil || res.Industries == nil || res.Channels == nil {
		t.Fatalf("expected all slices to be normalized to empty non-nil slices")
	}
	if len(res.Countries) != 0 || len(res.Companies) != 0 || len(res.Sectors) != 0 || len(res.Industries) != 0 || len(res.Channels) != 0 {
		t.Fatalf("expected normalized slices to be empty")
	}
}
