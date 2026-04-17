// Package extract provides deterministic validation for extractor domain contracts.
// Validation is strict and rejects unknown enum values, out-of-range numeric values, and malformed arrays.
package extract

import (
	"fmt"
)

var (
	validEventTypes = map[EventType]struct{}{
		EventTypeGeopolitical: {},
		EventTypeMacro:        {},
		EventTypeCorporate:    {},
		EventTypeSupplyChain:  {},
		EventTypeRegulation:   {},
		EventTypeMarkets:      {},
	}
	validGeoClusters = map[GeoCluster]struct{}{
		GeoClusterNone:              {},
		GeoClusterNorthAmerica:      {},
		GeoClusterEurope:            {},
		GeoClusterRussiaCIS:         {},
		GeoClusterMiddleEast:        {},
		GeoClusterEastAsia:          {},
		GeoClusterSouthAsia:         {},
		GeoClusterAfrica:            {},
		GeoClusterGlobalTradeRoutes: {},
		GeoClusterGlobal:            {},
	}
	validImpactDirections = map[ImpactDirection]struct{}{
		ImpactDirectionPositive: {},
		ImpactDirectionNegative: {},
		ImpactDirectionMixed:    {},
		ImpactDirectionNeutral:  {},
	}
	validTimeHorizons = map[TimeHorizon]struct{}{
		TimeHorizonIntraday: {},
		TimeHorizonShort:    {},
		TimeHorizonMedium:   {},
	}
	validChannels = map[Channel]struct{}{
		ChannelOilSupplyRisk:         {},
		ChannelGasSupplyRisk:         {},
		ChannelShippingDisruption:    {},
		ChannelRiskSentiment:         {},
		ChannelDefenseSpending:       {},
		ChannelRatesFX:               {},
		ChannelInflation:             {},
		ChannelRegulationPolicy:      {},
		ChannelEarningsGuidance:      {},
		ChannelAnalystRevision:       {},
		ChannelSupplyChainDisruption: {},
		ChannelDemandShift:           {},
		ChannelLaborDispute:          {},
	}
)

// Validate validates the extractor input contract fields.
// It checks required fields, positive identifiers, and non-zero publish time.
// It returns an error describing the first validation failure, or nil if valid.
func (in ExtractInput) Validate() error {
	if in.ArticleID <= 0 {
		return fmt.Errorf("article_id must be > 0")
	}
	if in.Title == "" {
		return fmt.Errorf("title must not be empty")
	}
	if in.Link == "" {
		return fmt.Errorf("link must not be empty")
	}
	if in.Source == "" {
		return fmt.Errorf("source must not be empty")
	}
	if in.PublishedAt.IsZero() {
		return fmt.Errorf("published_at must not be zero")
	}
	if in.Text == "" {
		return fmt.Errorf("text must not be empty")
	}
	return nil
}

// Normalize normalizes array fields on the extract result to empty slices instead of nil.
// It mutates the receiver pointer so downstream JSON serialization remains deterministic.
// It has no failure behavior and performs no validation.
func (r *ExtractResult) Normalize() {
	if r.Countries == nil {
		r.Countries = make([]string, 0)
	}
	if r.Companies == nil {
		r.Companies = make([]string, 0)
	}
	if r.Sectors == nil {
		r.Sectors = make([]string, 0)
	}
	if r.Industries == nil {
		r.Industries = make([]string, 0)
	}
	if r.Channels == nil {
		r.Channels = make([]Channel, 0)
	}
}

// Validate validates the extractor output contract fields.
// It normalizes nil slices, enforces controlled enums, checks numeric ranges, and validates array contents.
// It returns an error describing the first validation failure, or nil if valid.
func (r *ExtractResult) Validate() error {
	r.Normalize()

	if r.ArticleID <= 0 {
		return fmt.Errorf("article_id must be > 0")
	}
	if _, ok := validEventTypes[r.EventType]; !ok {
		return fmt.Errorf("event_type has invalid value %q", r.EventType)
	}
	if _, ok := validGeoClusters[r.GeoCluster]; !ok {
		return fmt.Errorf("geo_cluster has invalid value %q", r.GeoCluster)
	}
	if _, ok := validImpactDirections[r.ImpactDirection]; !ok {
		return fmt.Errorf("impact_direction has invalid value %q", r.ImpactDirection)
	}
	if r.ImpactStrength < 0 || r.ImpactStrength > 100 {
		return fmt.Errorf("impact_strength must be in range 0..100")
	}
	if _, ok := validTimeHorizons[r.TimeHorizon]; !ok {
		return fmt.Errorf("time_horizon has invalid value %q", r.TimeHorizon)
	}
	if r.Confidence < 0.0 || r.Confidence > 1.0 {
		return fmt.Errorf("confidence must be in range 0.0..1.0")
	}

	if err := validateStringSlice("countries", r.Countries); err != nil {
		return err
	}
	if err := validateStringSlice("companies", r.Companies); err != nil {
		return err
	}
	if err := validateStringSlice("sectors", r.Sectors); err != nil {
		return err
	}
	if err := validateStringSlice("industries", r.Industries); err != nil {
		return err
	}
	if err := validateChannels(r.Channels); err != nil {
		return err
	}

	return nil
}

// validateStringSlice validates that no element in a string slice is empty.
// The field parameter is the logical field name used in returned error messages.
// It returns an error when an empty element is present, otherwise nil.
func validateStringSlice(field string, values []string) error {
	for i, v := range values {
		if v == "" {
			return fmt.Errorf("%s[%d] must not be empty", field, i)
		}
	}
	return nil
}

// validateChannels validates allowed channel values, non-empty elements, and duplicate channel rejection.
// The channels parameter is the channel list from an ExtractResult.
// It returns an error for any invalid channel condition, otherwise nil.
func validateChannels(channels []Channel) error {
	seen := make(map[Channel]struct{}, len(channels))
	for i, ch := range channels {
		if ch == "" {
			return fmt.Errorf("channels[%d] must not be empty", i)
		}
		if _, ok := validChannels[ch]; !ok {
			return fmt.Errorf("channels[%d] has invalid value %q", i, ch)
		}
		if _, exists := seen[ch]; exists {
			return fmt.Errorf("channels contains duplicate value %q", ch)
		}
		seen[ch] = struct{}{}
	}
	return nil
}
