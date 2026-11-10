package logs

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"
)

type cycleBudget struct {
	maxFetches, maxRequests, maxRecords int
	maxBytes                            int64
	deadline                            time.Time
	fetches, requests                   int
	bytesReceived                       int64
}

type AdaptiveFetcher struct{ Service *Service }

func NewAdaptiveFetcher(service *Service) *AdaptiveFetcher { return &AdaptiveFetcher{Service: service} }

func validateRange(start, end float64) error {
	if math.IsNaN(start) || math.IsNaN(end) || math.IsInf(start, 0) || math.IsInf(end, 0) || start < 0 || end <= start {
		return fmt.Errorf("expected finite Unix timestamps with 0 <= start < end")
	}
	return nil
}

func newCycleBudget(cfg *SourceConfig) *cycleBudget {
	return &cycleBudget{maxFetches: cfg.MaxCycleFetches, maxRequests: cfg.MaxCycleRequests, maxBytes: cfg.MaxCycleBytes, maxRecords: cfg.MaxCycleRecords, deadline: time.Now().Add(time.Duration(cfg.MaxCycleSeconds) * time.Second)}
}

func (a *AdaptiveFetcher) fetch(ctx context.Context, source string, start, end float64, limit int, budget *cycleBudget, focus string) (FetchResult, error) {
	remainingRequests := budget.maxRequests - budget.requests
	remainingBytes := budget.maxBytes - budget.bytesReceived
	remainingTime := time.Until(budget.deadline)
	if budget.fetches >= budget.maxFetches {
		return FetchResult{}, fmt.Errorf("adaptive fetch budget exhausted: fetch count")
	}
	if remainingRequests < 1 {
		return FetchResult{}, fmt.Errorf("adaptive fetch budget exhausted: backend requests")
	}
	if remainingBytes < 1024 {
		return FetchResult{}, fmt.Errorf("adaptive fetch budget exhausted: response bytes")
	}
	if remainingTime <= 0 {
		return FetchResult{}, fmt.Errorf("adaptive fetch budget exhausted: elapsed time")
	}
	result, err := a.Service.fetch(ctx, source, start, end, limit, requestOptions{maxPages: remainingRequests, maxBytes: remainingBytes, timeout: remainingTime, focus: focus})
	if err != nil {
		return FetchResult{}, err
	}
	budget.fetches++
	budget.requests += result.Requests
	budget.bytesReceived += result.BytesReceived
	return result, nil
}

func mergeRecords(target map[string]Record, rows []Record, cap int) error {
	for _, row := range rows {
		if _, exists := target[row.ID]; !exists {
			target[row.ID] = row
		}
		if len(target) > cap {
			return fmt.Errorf("adaptive fetch budget exhausted: normalized records")
		}
	}
	return nil
}

func summary(source, provider string, start, end float64, records map[string]Record, attempts []Attempt, budget *cycleBudget) Investigation {
	ordered := make([]Record, 0, len(records))
	for _, record := range records {
		ordered = append(ordered, record)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Timestamp == ordered[j].Timestamp {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].Timestamp < ordered[j].Timestamp
	})
	return Investigation{Source: source, Provider: provider, ScopeStart: iso(start), ScopeEnd: iso(end), Records: ordered, Attempts: attempts, Usage: Usage{Fetches: budget.fetches, BackendRequests: budget.requests, BytesReceived: budget.bytesReceived, Records: len(ordered)}}
}

func (a *AdaptiveFetcher) Investigate(ctx context.Context, source string, start, end float64, targetRecords int, focus string) (Investigation, error) {
	if err := validateRange(start, end); err != nil {
		return Investigation{}, err
	}
	cfg, ok := a.Service.source(source)
	if !ok {
		return Investigation{}, fmt.Errorf("unknown log source")
	}
	if targetRecords == 0 {
		targetRecords = cfg.TargetRecords
	}
	if targetRecords < 1 || targetRecords > cfg.MaxCycleRecords {
		return Investigation{}, fmt.Errorf("target_records exceeds the configured adaptive cycle limit")
	}
	requestedStart := start
	scopeStart := math.Max(start, end-float64(cfg.MaxScopeSeconds))
	scopeClamped := scopeStart > requestedStart
	budget := newCycleBudget(cfg)
	records := map[string]Record{}
	attempts := []Attempt{}
	warningSet := map[string]bool{}
	bandEnd := end
	bandWidth := math.Min(float64(cfg.InitialWindow), bandEnd-scopeStart)
	bandStart := math.Max(scopeStart, bandEnd-bandWidth)
	limit := cfg.InitialRecords
	stopReason := "scope_exhausted"
	backendPartial := false
	for bandEnd > scopeStart {
		result, err := a.fetch(ctx, source, bandStart, bandEnd, limit, budget, focus)
		if err != nil {
			return Investigation{}, err
		}
		if err := mergeRecords(records, result.Records, budget.maxRecords); err != nil {
			return Investigation{}, err
		}
		attempts = append(attempts, Attempt{Start: iso(bandStart), End: iso(bandEnd), Limit: limit, Records: len(result.Records), Truncated: result.Truncated, Warnings: result.Warnings})
		if len(records) >= targetRecords {
			stopReason = "target_reached"
			backendPartial = result.Truncated
			if backendPartial {
				for _, warning := range result.Warnings {
					warningSet[warning] = true
				}
			}
			break
		}
		dense := contains(result.Warnings, "record_limit_reached") || contains(result.Warnings, "more_results")
		if result.Truncated && dense && limit < cfg.MaxRecords {
			limit = min(cfg.MaxRecords, limit*cfg.GrowthFactor)
			continue
		}
		if result.Truncated {
			stopReason = "dense_or_partial_scope_limit"
			backendPartial = true
			for _, warning := range result.Warnings {
				warningSet[warning] = true
			}
			break
		}
		if bandStart <= scopeStart {
			stopReason = "scope_exhausted"
			break
		}
		bandEnd = bandStart
		bandWidth = math.Min(float64(cfg.MaxWindowSeconds), math.Min(bandWidth*float64(cfg.GrowthFactor), bandEnd-scopeStart))
		bandStart = math.Max(scopeStart, bandEnd-bandWidth)
		limit = min(cfg.MaxRecords, limit*cfg.GrowthFactor)
	}
	if scopeClamped {
		warningSet["requested_scope_clamped"] = true
	}
	result := summary(source, cfg.Provider, scopeStart, end, records, attempts, budget)
	result.Strategy = "expand_backwards"
	if focus != "" {
		result.Strategy += "_with_provider_focus_" + focus
	}
	result.RequestedStart = iso(requestedStart)
	result.TargetRecords = targetRecords
	result.TargetReached = len(records) >= targetRecords
	result.CoverageComplete = stopReason == "scope_exhausted" && !backendPartial
	result.BackendPartial = backendPartial
	result.StopReason = stopReason
	result.Warnings = mapKeys(warningSet)
	return result, nil
}

type segment struct {
	start, end float64
	limit      int
}

func (a *AdaptiveFetcher) FetchComplete(ctx context.Context, source string, start, end float64) (Investigation, error) {
	if err := validateRange(start, end); err != nil {
		return Investigation{}, err
	}
	cfg, ok := a.Service.source(source)
	if !ok {
		return Investigation{}, fmt.Errorf("unknown log source")
	}
	if end-start > float64(cfg.MaxScopeSeconds) {
		return Investigation{}, fmt.Errorf("complete fetch exceeds configured maximum scope")
	}
	budget := newCycleBudget(cfg)
	records := map[string]Record{}
	attempts := []Attempt{}
	queue := []segment{}
	for cursor := start; cursor < end; {
		segmentEnd := math.Min(end, cursor+float64(cfg.MaxWindowSeconds))
		queue = append(queue, segment{cursor, segmentEnd, cfg.InitialRecords})
		cursor = segmentEnd
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result, err := a.fetch(ctx, source, current.start, current.end, current.limit, budget, "")
		if err != nil {
			return Investigation{}, err
		}
		attempts = append(attempts, Attempt{Start: iso(current.start), End: iso(current.end), Limit: current.limit, Records: len(result.Records), Truncated: result.Truncated, Warnings: result.Warnings})
		if !result.Truncated {
			if err := mergeRecords(records, result.Records, budget.maxRecords); err != nil {
				return Investigation{}, err
			}
			continue
		}
		dense := contains(result.Warnings, "record_limit_reached") || contains(result.Warnings, "more_results")
		if dense && current.limit < cfg.MaxRecords {
			queue = append([]segment{{current.start, current.end, min(cfg.MaxRecords, current.limit*cfg.GrowthFactor)}}, queue...)
			continue
		}
		width := current.end - current.start
		if width/2 < float64(cfg.MinSplitWindow) {
			return Investigation{}, fmt.Errorf("dense or partial log range cannot be completed within split limits; checkpoint unchanged")
		}
		midpoint := current.start + width/2
		queue = append([]segment{{current.start, midpoint, cfg.InitialRecords}, {midpoint, current.end, cfg.InitialRecords}}, queue...)
	}
	result := summary(source, cfg.Provider, start, end, records, attempts, budget)
	result.Strategy = "complete_with_limit_growth_and_time_split"
	result.CoverageComplete = true
	result.StopReason = "scope_exhausted"
	result.Warnings = []string{}
	return result, nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
