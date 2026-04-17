package impactapp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"ai-advisor-impact-service/internal/event"
	impactdomain "ai-advisor-impact-service/internal/impact"
	"ai-advisor-impact-service/internal/impact/rules"
	"ai-advisor-impact-service/internal/observability"
	impactrepo "ai-advisor-impact-service/internal/repository/impact"
	universestore "ai-advisor-impact-service/internal/universe/store"
)

type EventLookup interface {
	GetEventByID(ctx context.Context, eventID string) (event.Event, bool, error)
}

type ImpactReader interface {
	ListEventSecurityImpacts(ctx context.Context, eventID string, filter impactrepo.EventImpactFilter) ([]impactdomain.EventSecurityImpact, error)
	ListSecurityImpacts(ctx context.Context, code string, filter impactrepo.SecurityImpactFilter) ([]impactdomain.EventSecurityImpact, error)
	GetImpactsBySecurity(ctx context.Context, code string) ([]impactdomain.EventSecurityImpact, error)
	CountImpactsByEventIDs(ctx context.Context, eventIDs []string) (map[string]int, error)
}

type UniverseLookup interface {
	GetByCode(code string) (universestore.SecurityProfile, bool)
}

type QueryService struct {
	events   EventLookup
	impacts  ImpactReader
	universe UniverseLookup
}

var (
	ErrEventNotFound    = errors.New("event not found")
	ErrSecurityNotFound = errors.New("security not found")
)

func NewQueryService(events EventLookup, impacts ImpactReader, universe UniverseLookup) (*QueryService, error) {
	if events == nil || impacts == nil || universe == nil {
		return nil, fmt.Errorf("events, impacts, and universe are required")
	}
	return &QueryService{events: events, impacts: impacts, universe: universe}, nil
}

type EventSecurityFilters struct {
	Limit     int
	Offset    int
	MinScore  float64
	Direction *impactdomain.ImpactDirection
}

func (s *QueryService) GetEventSecurities(ctx context.Context, eventID string, filters EventSecurityFilters) ([]impactdomain.EventSecurityImpact, error) {
	ctx, span := observability.StartSpan(ctx, "impact.query_event_securities")
	defer span.End()

	id := strings.TrimSpace(eventID)
	if id == "" {
		err := fmt.Errorf("event_id is required")
		observability.RecordError(span, err)
		return nil, err
	}
	if _, found, err := s.events.GetEventByID(ctx, id); err != nil {
		observability.RecordError(span, err)
		return nil, err
	} else if !found {
		err := fmt.Errorf("%w: %s", ErrEventNotFound, id)
		observability.RecordError(span, err)
		return nil, err
	}
	out, err := s.impacts.ListEventSecurityImpacts(ctx, id, impactrepo.EventImpactFilter{
		Limit:     filters.Limit,
		Offset:    filters.Offset,
		MinScore:  filters.MinScore,
		Direction: filters.Direction,
	})
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	return out, nil
}

type SecurityImpactFilters struct {
	Limit    int
	Offset   int
	MinScore float64
}

type SecurityImpactAggregate struct {
	PositiveScore float64
	NegativeScore float64
	NetScore      float64
	Direction     impactdomain.ImpactDirection
	TopEventIDs   []string
	TopDrivers    []string
	ActiveEvents  int
}

func (s *QueryService) GetSecurityImpacts(ctx context.Context, code string, filters SecurityImpactFilters) (universestore.SecurityProfile, []impactdomain.EventSecurityImpact, SecurityImpactAggregate, error) {
	ctx, span := observability.StartSpan(ctx, "impact.query_security_impacts")
	defer span.End()

	normalized := strings.TrimSpace(code)
	if normalized == "" {
		err := fmt.Errorf("security code is required")
		observability.RecordError(span, err)
		return universestore.SecurityProfile{}, nil, SecurityImpactAggregate{}, err
	}
	security, found := s.universe.GetByCode(normalized)
	if !found {
		err := fmt.Errorf("%w: %s", ErrSecurityNotFound, normalized)
		observability.RecordError(span, err)
		return universestore.SecurityProfile{}, nil, SecurityImpactAggregate{}, err
	}
	impacts, err := s.impacts.GetImpactsBySecurity(ctx, normalized)
	if err != nil {
		observability.RecordError(span, err)
		return universestore.SecurityProfile{}, nil, SecurityImpactAggregate{}, err
	}
	activeImpacts, err := s.filterActiveImpacts(ctx, impacts)
	if err != nil {
		observability.RecordError(span, err)
		return universestore.SecurityProfile{}, nil, SecurityImpactAggregate{}, err
	}
	filtered := make([]impactdomain.EventSecurityImpact, 0, len(activeImpacts))
	for _, item := range activeImpacts {
		if item.ImpactScore >= filters.MinScore {
			filtered = append(filtered, item)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		left := math.Abs(filtered[i].ImpactScore)
		right := math.Abs(filtered[j].ImpactScore)
		if left == right {
			return filtered[i].EventID < filtered[j].EventID
		}
		return left > right
	})
	if filters.Limit <= 0 {
		filters.Limit = 50
	}
	if filters.Offset < 0 {
		err := fmt.Errorf("offset must be >= 0")
		observability.RecordError(span, err)
		return universestore.SecurityProfile{}, nil, SecurityImpactAggregate{}, err
	}
	if filters.Offset >= len(filtered) {
		filtered = []impactdomain.EventSecurityImpact{}
	} else {
		end := filters.Offset + filters.Limit
		if end > len(filtered) {
			end = len(filtered)
		}
		filtered = filtered[filters.Offset:end]
	}
	ruleSet := rules.DefaultRuleSet()
	summary := summarizeSecurityImpacts(normalized, activeImpacts, ruleSet.Score.MinScoreThreshold, ruleSet.Score.MaxScoreCap)
	return security, filtered, SecurityImpactAggregate{
		PositiveScore: summary.PositiveScore,
		NegativeScore: summary.NegativeScore,
		NetScore:      summary.NetScore,
		Direction:     summary.Direction,
		TopEventIDs:   summary.TopEventIDs,
		TopDrivers:    summary.TopExplanationCodes,
		ActiveEvents:  summary.ActiveEventCount,
	}, nil
}

func (s *QueryService) CountByEventIDs(ctx context.Context, eventIDs []string) (map[string]int, error) {
	ctx, span := observability.StartSpan(ctx, "impact.query_count_by_event_ids")
	defer span.End()
	out, err := s.impacts.CountImpactsByEventIDs(ctx, eventIDs)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	return out, nil
}

type SecurityImpactSummary struct {
	SecurityCode        string
	PositiveScore       float64
	NegativeScore       float64
	NetScore            float64
	Direction           impactdomain.ImpactDirection
	ActiveEventCount    int
	TopEventIDs         []string
	TopExplanationCodes []string
}

func (s *QueryService) ComputeSecurityImpactSummary(ctx context.Context, securityCode string) (SecurityImpactSummary, error) {
	code := strings.TrimSpace(securityCode)
	if code == "" {
		return SecurityImpactSummary{}, fmt.Errorf("security code is required")
	}
	impacts, err := s.impacts.GetImpactsBySecurity(ctx, code)
	if err != nil {
		return SecurityImpactSummary{}, err
	}
	activeImpacts, err := s.filterActiveImpacts(ctx, impacts)
	if err != nil {
		return SecurityImpactSummary{}, err
	}
	ruleSet := rules.DefaultRuleSet()
	return summarizeSecurityImpacts(code, activeImpacts, ruleSet.Score.MinScoreThreshold, ruleSet.Score.MaxScoreCap), nil
}

func (s *QueryService) filterActiveImpacts(ctx context.Context, impacts []impactdomain.EventSecurityImpact) ([]impactdomain.EventSecurityImpact, error) {
	active := make([]impactdomain.EventSecurityImpact, 0, len(impacts))
	statusByEventID := make(map[string]string, len(impacts))
	for _, item := range impacts {
		if status, seen := statusByEventID[item.EventID]; seen {
			if status == event.StatusActive {
				active = append(active, item)
			}
			continue
		}
		evt, found, err := s.events.GetEventByID(ctx, item.EventID)
		if err != nil {
			return nil, err
		}
		if !found {
			statusByEventID[item.EventID] = ""
			continue
		}
		statusByEventID[item.EventID] = evt.Status
		if evt.Status == event.StatusActive {
			active = append(active, item)
		}
	}
	return active, nil
}

func summarizeSecurityImpacts(code string, impacts []impactdomain.EventSecurityImpact, netDirectionThreshold float64, eventScoreCap float64) SecurityImpactSummary {
	out := SecurityImpactSummary{
		SecurityCode:        code,
		Direction:           impactdomain.ImpactDirectionNeutral,
		TopEventIDs:         []string{},
		TopExplanationCodes: []string{},
	}
	if len(impacts) == 0 {
		return out
	}
	explanationCounts := make(map[string]int)
	uniqueEvents := make(map[string]struct{})
	top := make([]impactdomain.EventSecurityImpact, len(impacts))
	copy(top, impacts)
	sort.SliceStable(top, func(i, j int) bool {
		left := math.Abs(top[i].ImpactScore)
		right := math.Abs(top[j].ImpactScore)
		if left == right {
			return top[i].EventID < top[j].EventID
		}
		return left > right
	})
	for _, item := range impacts {
		uniqueEvents[item.EventID] = struct{}{}
		score := math.Abs(item.ImpactScore)
		if eventScoreCap > 0 && score > eventScoreCap {
			score = eventScoreCap
		}
		switch item.ImpactDirection {
		case impactdomain.ImpactDirectionPositive:
			out.PositiveScore += score
		case impactdomain.ImpactDirectionNegative:
			out.NegativeScore += score
		}
		for _, code := range item.ExplanationCodes {
			normalized := strings.TrimSpace(code)
			if normalized == "" {
				continue
			}
			explanationCounts[normalized]++
		}
	}
	out.ActiveEventCount = len(uniqueEvents)
	out.NetScore = out.PositiveScore - out.NegativeScore
	if out.NetScore > netDirectionThreshold {
		out.Direction = impactdomain.ImpactDirectionPositive
	} else if out.NetScore < (-1 * netDirectionThreshold) {
		out.Direction = impactdomain.ImpactDirectionNegative
	}
	seenEventIDs := map[string]struct{}{}
	for i := 0; i < len(top) && len(out.TopEventIDs) < 3; i++ {
		if _, seen := seenEventIDs[top[i].EventID]; seen {
			continue
		}
		seenEventIDs[top[i].EventID] = struct{}{}
		out.TopEventIDs = append(out.TopEventIDs, top[i].EventID)
	}
	type explanationCount struct {
		code  string
		count int
	}
	ranked := make([]explanationCount, 0, len(explanationCounts))
	for code, count := range explanationCounts {
		ranked = append(ranked, explanationCount{code: code, count: count})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].count == ranked[j].count {
			return ranked[i].code < ranked[j].code
		}
		return ranked[i].count > ranked[j].count
	})
	for i := 0; i < len(ranked) && i < 5; i++ {
		out.TopExplanationCodes = append(out.TopExplanationCodes, ranked[i].code)
	}
	return out
}
