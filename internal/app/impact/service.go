package impactapp

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"ai-advisor-impact-service/internal/event"
	impactdomain "ai-advisor-impact-service/internal/impact"
	"ai-advisor-impact-service/internal/impact/rules"
	"ai-advisor-impact-service/internal/logging"
	impactrepo "ai-advisor-impact-service/internal/repository/impact"
	universestore "ai-advisor-impact-service/internal/universe/store"
)

type EventSource interface {
	ListEvents(ctx context.Context, limit int, since *time.Time, until *time.Time) ([]event.Event, error)
}

type ImpactSink interface {
	ReplaceAllEventSecurityImpacts(ctx context.Context, impacts []impactdomain.EventSecurityImpact) error
}

type UniverseSource interface {
	GetAll() []universestore.SecurityProfile
}

type Config struct {
	EventsLimit int
	MinScore    float64
	RuleSet     rules.RuleSet
}

type Service struct {
	events   EventSource
	universe UniverseSource
	impacts  ImpactSink
	logger   *logging.Logger
	cfg      Config
}

func NewService(events EventSource, universe UniverseSource, impacts ImpactSink, logger *logging.Logger, cfg Config) (*Service, error) {
	if events == nil || universe == nil || impacts == nil || logger == nil {
		return nil, fmt.Errorf("events, universe, impacts, and logger are required")
	}
	if cfg.EventsLimit <= 0 {
		return nil, fmt.Errorf("events limit must be > 0")
	}
	if cfg.MinScore < 0 {
		return nil, fmt.Errorf("min score must be >= 0")
	}
	if err := cfg.RuleSet.Validate(); err != nil {
		return nil, fmt.Errorf("invalid ruleset: %w", err)
	}
	return &Service{events: events, universe: universe, impacts: impacts, logger: logger, cfg: cfg}, nil
}

func (s *Service) RecalculateImpacts(ctx context.Context) error {
	started := time.Now().UTC()
	events, err := s.loadAllEvents(ctx)
	if err != nil {
		return fmt.Errorf("load events: %w", err)
	}
	securities := s.universe.GetAll()

	results := make([]impactdomain.EventSecurityImpact, 0, len(events))
	failures := 0
	for _, evt := range events {
		if evt.Status != event.StatusActive {
			continue
		}
		for _, sec := range securities {
			profile := toImpactProfile(sec)
			impact, keep := impactdomain.ComputeEventSecurityImpact(evt, profile, s.cfg.RuleSet, started)
			if !keep || impact.ImpactScore < s.cfg.MinScore {
				continue
			}
			results = append(results, impact)
		}
	}

	if err := s.impacts.ReplaceAllEventSecurityImpacts(ctx, results); err != nil {
		return fmt.Errorf("persist impacts: %w", err)
	}

	s.logger.Info(ctx, "app.impact.recalc.completed", "app/impact", "impact recalculation completed",
		logging.Field{Key: "events", Value: len(events)},
		logging.Field{Key: "securities", Value: len(securities)},
		logging.Field{Key: "computed_impacts", Value: len(results)},
		logging.Field{Key: "failures", Value: failures},
		logging.Field{Key: "duration_ms", Value: time.Since(started).Milliseconds()},
	)
	return nil
}

func (s *Service) loadAllEvents(ctx context.Context) ([]event.Event, error) {
	all := make([]event.Event, 0, s.cfg.EventsLimit)
	var until *time.Time
	for {
		batch, err := s.fetchPageWithTieExpansion(ctx, until)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		all = append(all, batch...)
		if len(batch) < s.cfg.EventsLimit {
			break
		}
		nextUntil := batch[len(batch)-1].LastSeenAt.UTC().Add(-1 * time.Nanosecond)
		until = &nextUntil
	}
	return all, nil
}

func (s *Service) fetchPageWithTieExpansion(ctx context.Context, until *time.Time) ([]event.Event, error) {
	limit := s.cfg.EventsLimit
	for {
		batch, err := s.events.ListEvents(ctx, limit, nil, until)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 || len(batch) < limit {
			return batch, nil
		}
		boundary := batch[len(batch)-1].LastSeenAt.UTC()
		complete, err := s.isBoundaryTimestampComplete(ctx, batch, boundary)
		if err != nil {
			return nil, err
		}
		if complete {
			return batch, nil
		}
		if limit >= math.MaxInt/2 {
			return nil, fmt.Errorf("cannot page events: too many rows share last_seen_at=%s", boundary.Format(time.RFC3339Nano))
		}
		limit *= 2
	}
}

func (s *Service) isBoundaryTimestampComplete(ctx context.Context, batch []event.Event, boundary time.Time) (bool, error) {
	boundaryRowsInBatch := 0
	for i := len(batch) - 1; i >= 0; i-- {
		if !batch[i].LastSeenAt.UTC().Equal(boundary) {
			break
		}
		boundaryRowsInBatch++
	}
	if boundaryRowsInBatch == 0 {
		return true, nil
	}
	if boundaryRowsInBatch >= math.MaxInt {
		return false, fmt.Errorf("cannot page events: boundary row count overflow at last_seen_at=%s", boundary.Format(time.RFC3339Nano))
	}
	probeLimit := boundaryRowsInBatch + 1
	probeSince := boundary
	probeUntil := boundary
	probeRows, err := s.events.ListEvents(ctx, probeLimit, &probeSince, &probeUntil)
	if err != nil {
		return false, err
	}
	return len(probeRows) <= boundaryRowsInBatch, nil
}

func NewRepositoryAdapter(repo *impactrepo.Repository) ImpactSink {
	return repo
}

func toImpactProfile(in universestore.SecurityProfile) impactdomain.SecurityProfile {
	return impactdomain.SecurityProfile{
		Code:                 in.Code,
		ISIN:                 in.ISIN,
		Name:                 in.Name,
		Exchange:             in.Exchange,
		GICSector:            in.GICSector,
		GICGroup:             in.GICGroup,
		GICIndustry:          in.GICIndustry,
		GICSubIndustry:       in.GICSubIndustry,
		GeoExposure:          cloneNormalizedMap(in.GeoExposure),
		CountryExposure:      cloneNormalizedMap(in.CountryExposure),
		EventTypeSensitivity: cloneNormalizedMap(in.EventTypeSensitivity),
		ProfileConfidence: func() float64 {
			if in.ProfileConfidence == nil {
				return 0
			}
			return *in.ProfileConfidence
		}(),
	}
}

func cloneNormalizedMap(in map[string]float64) map[string]float64 {
	if in == nil {
		return map[string]float64{}
	}
	out := make(map[string]float64, len(in))
	for k, v := range in {
		normalizedKey := strings.ToLower(strings.TrimSpace(k))
		if normalizedKey == "" {
			continue
		}
		out[normalizedKey] += v
	}
	return out
}
