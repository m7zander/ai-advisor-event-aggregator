// Package llm contains OpenAI-backed extractor infrastructure implementation details.
// This file centralizes strict prompt rules and deterministic input payload shaping.
package llm

import (
	"encoding/json"
	"fmt"

	"ai-advisor-impact-service/internal/extract"
)

const extractorSystemPrompt = `You are performing structured event extraction for EXACTLY ONE preprocessed news article.

Return EXACTLY ONE JSON object.
Return NO markdown.
Return NO code fences.
Return NO prose.
Return NO explanation.

Return ONLY the schema fields listed below and no others.
Never use "id"; use "article_id" exactly.

Use ONLY the allowed enum values listed below.
Do not invent new values.

Do not infer facts that are not supported by the input text.

If multiple events are mentioned:

* extract ONLY the primary market-relevant event
* ignore secondary or background information

Prioritize information with direct market impact (energy, rates, supply chains, defense, regulation) over general context.

If information is missing or unclear:

* use empty arrays for countries, companies, sectors, channels
* use "neutral" for impact_direction
* use impact_strength in range 0–30
* use confidence in range 0.3–0.5
* do NOT invent entities or relationships

Never return null. Use empty arrays instead.

---

OUTPUT SCHEMA (exact keys only)

{
"article_id": <int>,

"event_type": "geopolitical|macro|corporate|supply_chain|regulation|markets",

"geo_cluster": "none|north_america|europe|russia_cis|middle_east|east_asia|south_asia|africa|global_trade_routes|global",

"countries": ["string, English country names"],
"companies": ["string, company names"],

"sectors": [
"Basic Materials|Communication Services|Conglomerates|Consumer Cyclical|Consumer Defensive|Consumer Goods|Energy|Financial|Financial Services|Healthcare|Industrial Goods|Industrials|Other|Real Estate|Services|Technology|Utilities"
],

"industries": [
"Agricultural Chemicals|Agricultural Inputs|Aluminum|Building Materials|Chemicals|Chemicals - Major Diversified|Coal|Coking Coal|Copper|Gold|Independent Oil & Gas|Industrial Metals & Minerals|Lumber & Wood Production|Major Integrated Oil & Gas|Nonmetallic Mineral Mining|Oil & Gas Drilling & Exploration|Oil & Gas Equipment & Services|Oil & Gas Pipelines|Oil & Gas Refining & Marketing|Other Industrial Metals & Mining|Other Precious Metals & Mining|Paper & Paper Products|Silver|Specialty Chemicals|Steel|Steel & Iron|Synthetics|Advertising Agencies|Broadcasting|Electronic Gaming & Multimedia|Entertainment|Internet Content & Information|Pay TV|Publishing|Telecom Services|Conglomerates|Apparel Manufacturing|Apparel Retail|Apparel Stores|Auto & Truck Dealerships|Auto Manufacturers|Auto Parts|Broadcasting - Radio|Broadcasting - TV|Department Stores|Footwear & Accessories|Furnishings, Fixtures & Appliances|Gambling|Home Furnishings & Fixtures|Home Improvement Retail|Home Improvement Stores|Internet Retail|Leisure|Lodging|Luxury Goods|Marketing Services|Media - Diversified|Packaging & Containers|Personal Services|Recreational Vehicles|Residential Construction|Resorts & Casinos|Restaurants|Rubber & Plastics|Specialty Retail|Textile Manufacturing|Travel Services|Beverages - Brewers|Beverages - Soft Drinks|Beverages - Wineries & Distilleries|Beverages-Brewers|Beverages-Non-Alcoholic|Beverages-Wineries & Distilleries|Confectioners|Discount Stores|Education & Training Services|Farm Products|Food Distribution|Grocery Stores|Household & Personal Products|Packaged Foods|Pharmaceutical Retailers|Tobacco|Appliances|Auto Manufacturers - Major|Business Equipment|Cigarettes|Cleaning Products|Dairy Products|Electronic Equipment|Food - Major Diversified|Housewares & Accessories|Meat Products|Office Supplies|Personal Products|Photographic Equipment & Supplies|Processed & Packaged Goods|Recreational Goods, Other|REIT - Retail|Sporting Goods|Textile - Apparel Clothing|Textile - Apparel Footwear & Accessories|Tobacco Products, Other|Toys & Games|Trucks & Other Vehicles|Oil & Gas Drilling|Oil & Gas E&P|Oil & Gas Integrated|Oil & Gas Midstream|Thermal Coal|Uranium|Accident & Health Insurance|Asset Management|Closed-End Fund - Debt|Closed-End Fund - Equity|Closed-End Fund - Foreign|Credit Services|Diversified Investments|Foreign Money Center Banks|Foreign Regional Banks|Insurance Brokers|Investment Brokerage - National|Investment Brokerage - Regional|Life Insurance|Money Center Banks|Mortgage Investment|Property & Casualty Insurance|Property Management|Real Estate Development|Regional - Mid-Atlantic Banks|Regional - Midwest Banks|Regional - Northeast Banks|Regional - Pacific Banks|Regional - Southeast Banks|Regional - Southwest  Banks|REIT - Diversified|REIT - Healthcare Facilities|REIT - Hotel/Motel|REIT - Industrial|REIT - Office|REIT - Residential|Savings & Loans|Surety & Title Insurance|Banks - Global|Banks - Regional - Africa|Banks - Regional - Asia|Banks - Regional - Australia|Banks - Regional - Canada|Banks - Regional - Europe|Banks - Regional - Latin America|Banks - Regional - US|Banks-Diversified|Banks-Regional|Capital Markets|Financial Conglomerates|Financial Data & Stock Exchanges|Financial Exchanges|Insurance - Diversified|Insurance - Life|Insurance - Property & Casualty|Insurance - Reinsurance|Insurance - Specialty|Insurance-Diversified|Insurance-Life|Insurance-Property & Casualty|Insurance-Reinsurance|Insurance-Specialty|Mortgage Finance|Savings & Cooperative Banks|Shell Companies|Specialty Finance|Biotechnology|Diagnostic Substances|Diagnostics & Research|Drug Delivery|Drug Manufacturers - Major|Drug Manufacturers - Other|Drug Manufacturers - Specialty & Generic|Drug Manufacturers-General|Drug Manufacturers-Specialty & Generic|Drug Related Products|Drugs - Generic|Health Care Plans|Health Information Services|Healthcare Plans|Home Health Care|Hospitals|Long-Term Care Facilities|Medical Appliances & Equipment|Medical Care|Medical Care Facilities|Medical Devices|Medical Distribution|Medical Instruments & Supplies|Medical Laboratories & Research|Medical Practitioners|Specialized Health Services|Aerospace/Defense - Major Diversified|Aerospace/Defense Products & Services|Cement|Diversified Machinery|Farm & Construction Machinery|General Building Materials|General Contractors|Heavy Construction|Industrial Electrical Equipment|Industrial Equipment & Components|Lumber, Wood Production|Machine Tools & Accessories|Manufactured Housing|Metal Fabrication|Pollution & Treatment Controls|Small Tools & Accessories|Textile Industrial|Waste Management|Aerospace & Defense|Airlines|Airports & Air Services|Building Products & Equipment|Business Equipment & Supplies|Business Services|Consulting Services|Diversified Industrials|Electrical Equipment & Parts|Engineering & Construction|Farm & Construction Equipment|Farm & Heavy Construction Machinery|Industrial Distribution|Infrastructure Operations|Integrated Freight & Logistics|Integrated Shipping & Logistics|Marine Shipping|Railroads|Rental & Leasing Services|Security & Protection Services|Shipping & Ports|Specialty Business Services|Specialty Industrial Machinery|Staffing & Employment Services|Staffing & Outsourcing Services|Tools & Accessories|Truck Manufacturing|Trucking|Real Estate - General|Real Estate Services|Real Estate-Development|Real Estate-Diversified|REIT - Hotel & Motel|REIT-Diversified|REIT-Healthcare Facilities|REIT-Hotel & Motel|REIT-Industrial|REIT-Mortgage|REIT-Office|REIT-Residential|REIT-Retail|REIT-Specialty|Air Delivery & Freight Services|Air Services, Other|Auto Dealerships|Auto Parts Stores|Auto Parts Wholesale|Basic Materials Wholesale|Building Materials Wholesale|Catalog & Mail Order Houses|CATV Systems|Computers Wholesale|Consumer Services|Discount, Variety Stores|Drug Stores|Drugs Wholesale|Electronics Stores|Electronics Wholesale|Entertainment - Diversified|Food Wholesale|Gaming Activities|General Entertainment|Home Furnishing Stores|Industrial Equipment Wholesale|Information Technology Services|Jewelry Stores|Major Airlines|Management Services|Medical Equipment Wholesale|Movie Production, Theaters|Music & Video Stores|Publishing - Books|Publishing - Newspapers|Publishing - Periodicals|Regional Airlines|Research Services|Specialty Eateries|Specialty Retail, Other|Sporting Activities|Sporting Goods Stores|Technical Services|Toy & Hobby Stores|Wholesale, Other|Application Software|Business Software & Services|Communication Equipment|Computer Based Systems|Computer Distribution|Computer Hardware|Computer Peripherals|Computer Systems|Consumer Electronics|Contract Manufacturers|Data Storage|Data Storage Devices|Diversified Communication Services|Diversified Computer Systems|Diversified Electronics|Electronic Components|Electronics & Computer Distribution|Electronics Distribution|Healthcare Information Services|Information & Delivery Services|Internet Information Providers|Internet Service Providers|Internet Software & Services|Long Distance Carriers|Multimedia & Graphics Software|Networking & Communication Devices|Personal Computers|Printed Circuit Boards|Processing Systems & Products|Scientific & Technical Instruments|Security Software & Services|Semiconductor - Broad Line|Semiconductor - Integrated Circuits|Semiconductor - Specialized|Semiconductor Equipment & Materials|Semiconductor Memory|Semiconductor- Memory Chips|Semiconductors|Software - Application|Software - Infrastructure|Software-Application|Software-Infrastructure|Solar|Technical & System Software|Telecom Services - Domestic|Telecom Services - Foreign|Wireless Communications|Diversified Utilities|Electric Utilities|Foreign Utilities|Gas Utilities|Utilities - Diversified|Utilities - Independent Power Producers|Utilities - Regulated Electric|Utilities - Regulated Gas|Utilities - Regulated Water|Utilities-Diversified|Utilities-Independent Power Producers|Utilities-Regulated Electric|Utilities-Regulated Gas|Utilities-Regulated Water|Utilities-Renewable|Water Utilities"
],

"impact_direction": "positive|negative|mixed|neutral",

"impact_strength": <integer 0-100>,

"channels": [
"oil_supply_risk|
gas_supply_risk|
shipping_disruption|
risk_sentiment|
defense_spending|
rates_fx|
inflation|
regulation_policy|
earnings_guidance|
analyst_revision|
supply_chain_disruption|
demand_shift|
labor_dispute"
],

"time_horizon": "intraday|short|medium",

"confidence": <float 0.0-1.0>
}

---

FIELD RULES

* countries, companies, sectors, industries, channels:

  * must be arrays
  * may be empty
  * must not contain empty strings
  * must not contain duplicates

* channels:

  * must contain ONLY allowed values
  * do not derive new channel names

* sectors:

  * must contain ONLY allowed values (from provided sector list)
  * do not invent sector names

* industries:

  * must contain ONLY allowed values (from provided industry list)
  * do not invent industry names
  * must be consistent with selected sector

* event_type:

  * geopolitical: war, conflict, sanctions, geopolitical tension
  * macro: inflation, central banks, macro data
  * corporate: company-specific events (earnings, guidance, M&A)
  * supply_chain: logistics, shipping, production disruption
  * regulation: laws, sanctions, regulatory decisions
  * markets: broad financial market structure events

* geo_cluster:

  * must represent the primary geopolitical or macro-relevant region of the event
  * choose exactly one value
  * use "global" only if no single primary region dominates
  * use "none" only if the article has no meaningful geopolitical or regional anchor
  * north_america: United States, Canada, Mexico
  * europe: European states excluding Russia/CIS
  * russia_cis: Russia, Ukraine, Belarus, Caucasus, Central Asia, CIS-related regional context
  * middle_east: Gulf region, Iran, Iraq, Israel, Levant, Arabian Peninsula
  * east_asia: China, Taiwan, Japan, Koreas
  * south_asia: India, Pakistan, Bangladesh, Sri Lanka, surrounding regional context
  * africa: African states and region-specific African context
  * global_trade_routes: shipping chokepoints and trade corridors such as Suez, Panama, Bab al-Mandab, Strait of Hormuz, Red Sea
  * global: truly cross-regional or globally distributed events

* impact_strength guidelines:

  * 0–30: weak or indirect impact
  * 30–70: moderate impact
  * 70–100: strong, immediate market-relevant impact

* confidence:

  * reflects certainty of extraction (not market impact)
  * low if ambiguous or incomplete

---

STRICT RULES

* Output must be valid JSON
* Output must contain ALL fields
* No additional fields allowed
* No comments
* No trailing text
`

// llmInputPayload is the deterministic user payload shape sent to the model.
type llmInputPayload struct {
	ArticleID   int64  `json:"article_id"`
	Title       string `json:"title"`
	Source      string `json:"source"`
	Link        string `json:"link"`
	PublishedAt string `json:"published_at"`
	Text        string `json:"text"`
}

// buildUserPrompt builds the user payload passed to the model for one extraction call.
// The in parameter is the validated extractor input contract.
// It returns a deterministic JSON string with only the allowed input fields, or an error on marshal failure.
// Sanitization rule: only explicit schema fields are serialized; no raw maps or passthrough metadata.
func buildUserPrompt(in extract.ExtractInput) (string, error) {
	payload := llmInputPayload{
		ArticleID:   in.ArticleID,
		Title:       in.Title,
		Source:      in.Source,
		Link:        in.Link,
		PublishedAt: in.PublishedAt.UTC().Format("2006-01-02T15:04:05Z"),
		Text:        in.Text,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal llm input payload: %w", err)
	}
	return string(b), nil
}
