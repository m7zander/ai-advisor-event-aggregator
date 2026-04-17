// Package extract defines the strict domain contract for LLM-based extraction input/output.
// It contains only deterministic data types and controlled enum-like values used by the extractor pipeline.
package extract

import "time"

// EventType represents the controlled category for an extracted event.
type EventType string

const (
	// EventTypeGeopolitical represents geopolitical events.
	EventTypeGeopolitical EventType = "geopolitical"
	// EventTypeMacro represents macroeconomic events.
	EventTypeMacro EventType = "macro"
	// EventTypeCorporate represents corporate events.
	EventTypeCorporate EventType = "corporate"
	// EventTypeSupplyChain represents supply chain events.
	EventTypeSupplyChain EventType = "supply_chain"
	// EventTypeRegulation represents regulation events.
	EventTypeRegulation EventType = "regulation"
	// EventTypeMarkets represents markets events.
	EventTypeMarkets EventType = "markets"
)

// GeoCluster represents the controlled geopolitical cluster classification.
type GeoCluster string

const (
	// GeoClusterNone indicates no cluster applies.
	GeoClusterNone GeoCluster = "none"
	// GeoClusterNorthAmerica indicates the North America cluster.
	GeoClusterNorthAmerica GeoCluster = "north_america"
	// GeoClusterEurope indicates the Europe cluster (excluding Russia/CIS).
	GeoClusterEurope GeoCluster = "europe"
	// GeoClusterRussiaCIS indicates the Russia/CIS cluster.
	GeoClusterRussiaCIS GeoCluster = "russia_cis"
	// GeoClusterMiddleEast indicates the Middle East cluster.
	GeoClusterMiddleEast GeoCluster = "middle_east"
	// GeoClusterEastAsia indicates the East Asia cluster.
	GeoClusterEastAsia GeoCluster = "east_asia"
	// GeoClusterSouthAsia indicates the South Asia cluster.
	GeoClusterSouthAsia GeoCluster = "south_asia"
	// GeoClusterAfrica indicates the Africa cluster.
	GeoClusterAfrica GeoCluster = "africa"
	// GeoClusterGlobalTradeRoutes indicates global trade routes cluster.
	GeoClusterGlobalTradeRoutes GeoCluster = "global_trade_routes"
	// GeoClusterGlobal indicates global cluster.
	GeoClusterGlobal GeoCluster = "global"
)

// ImpactDirection represents the controlled direction of estimated impact.
type ImpactDirection string

const (
	// ImpactDirectionPositive indicates positive impact.
	ImpactDirectionPositive ImpactDirection = "positive"
	// ImpactDirectionNegative indicates negative impact.
	ImpactDirectionNegative ImpactDirection = "negative"
	// ImpactDirectionMixed indicates mixed impact.
	ImpactDirectionMixed ImpactDirection = "mixed"
	// ImpactDirectionNeutral indicates neutral impact.
	ImpactDirectionNeutral ImpactDirection = "neutral"
)

// TimeHorizon represents the controlled horizon of expected impact.
type TimeHorizon string

const (
	// TimeHorizonIntraday indicates intraday horizon.
	TimeHorizonIntraday TimeHorizon = "intraday"
	// TimeHorizonShort indicates short horizon.
	TimeHorizonShort TimeHorizon = "short"
	// TimeHorizonMedium indicates medium horizon.
	TimeHorizonMedium TimeHorizon = "medium"
)

// Channel represents a controlled transmission channel for impact.
type Channel string

const (
	// ChannelOilSupplyRisk indicates oil supply risk channel.
	ChannelOilSupplyRisk Channel = "oil_supply_risk"
	// ChannelGasSupplyRisk indicates gas supply risk channel.
	ChannelGasSupplyRisk Channel = "gas_supply_risk"
	// ChannelShippingDisruption indicates shipping disruption channel.
	ChannelShippingDisruption Channel = "shipping_disruption"
	// ChannelRiskSentiment indicates risk sentiment channel.
	ChannelRiskSentiment Channel = "risk_sentiment"
	// ChannelDefenseSpending indicates defense spending channel.
	ChannelDefenseSpending Channel = "defense_spending"
	// ChannelRatesFX indicates rates and foreign exchange channel.
	ChannelRatesFX Channel = "rates_fx"
	// ChannelInflation indicates inflation channel.
	ChannelInflation Channel = "inflation"
	// ChannelRegulationPolicy indicates regulation/policy channel.
	ChannelRegulationPolicy Channel = "regulation_policy"
	// ChannelEarningsGuidance indicates earnings guidance channel.
	ChannelEarningsGuidance Channel = "earnings_guidance"
	// ChannelAnalystRevision indicates analyst revision channel.
	ChannelAnalystRevision Channel = "analyst_revision"
	// ChannelSupplyChainDisruption indicates supply chain disruption channel.
	ChannelSupplyChainDisruption Channel = "supply_chain_disruption"
	// ChannelDemandShift indicates demand shift channel.
	ChannelDemandShift Channel = "demand_shift"
	// ChannelLaborDispute indicates labor dispute channel.
	ChannelLaborDispute Channel = "labor_dispute"
)

// ExtractInput is the strict extractor input contract for one preprocessed article.
type ExtractInput struct {
	ArticleID   int64     `json:"article_id"`
	Title       string    `json:"title"`
	Link        string    `json:"link"`
	Source      string    `json:"source"`
	PublishedAt time.Time `json:"published_at"`
	Text        string    `json:"text"`
}

// ExtractResult is the strict extractor output contract for one article.
type ExtractResult struct {
	ArticleID       int64           `json:"article_id"`
	EventType       EventType       `json:"event_type"`
	GeoCluster      GeoCluster      `json:"geo_cluster"`
	Countries       []string        `json:"countries"`
	Companies       []string        `json:"companies"`
	Sectors         []string        `json:"sectors"`
	Industries      []string        `json:"industries"`
	ImpactDirection ImpactDirection `json:"impact_direction"`
	ImpactStrength  int             `json:"impact_strength"`
	Channels        []Channel       `json:"channels"`
	TimeHorizon     TimeHorizon     `json:"time_horizon"`
	Confidence      float64         `json:"confidence"`
}
