package logs

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"testing"
)

type adaptiveTransport struct{ calls []Request }

func (t *adaptiveTransport) Do(_ context.Context, request Request) ([]byte, error) {
	t.calls = append(t.calls, request)
	parsed, _ := url.Parse(request.URL)
	limit, _ := strconv.Atoi(parsed.Query().Get("limit"))
	startNanos, _ := strconv.ParseInt(parsed.Query().Get("start"), 10, 64)
	endNanos, _ := strconv.ParseInt(parsed.Query().Get("end"), 10, 64)
	at := float64(startNanos)/1e9 + 1
	count := limit
	if limit > 2 {
		count = 2
	}
	if float64(endNanos-startNanos)/1e9 <= 10 {
		count = 1
	}
	values := ""
	for index := 0; index < count; index++ {
		if index > 0 {
			values += ","
		}
		values += fmt.Sprintf(`["%d","event-%d-%d"]`, int64((at+float64(index))*1e9), int64(at), index)
	}
	return []byte(fmt.Sprintf(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[%s]}]}}`, values)), nil
}

func TestInvestigationGrowsLimitThenMovesBackward(t *testing.T) {
	cfg := testConfig("loki")
	cfg.InitialWindow = 60
	cfg.MaxWindowSeconds = 120
	cfg.MaxScopeSeconds = 300
	cfg.InitialRecords = 2
	cfg.MaxRecords = 8
	cfg.TargetRecords = 4
	cfg.MaxCycleRecords = 100
	transport := &adaptiveTransport{}
	service := NewService(&Config{Sources: map[string]*SourceConfig{"prod": cfg}}, transport)
	result, err := NewAdaptiveFetcher(service).Investigate(context.Background(), "prod", 1000, 1300, 4, "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.TargetReached || result.StopReason != "target_reached" {
		t.Fatalf("unexpected result: %#v", result)
	}
	if len(result.Attempts) != 3 {
		t.Fatalf("expected limit retry and older band, got %#v", result.Attempts)
	}
	if result.Attempts[0].Limit != 2 || result.Attempts[1].Limit != 4 || result.Attempts[2].Limit != 8 {
		t.Fatalf("unexpected limit growth: %#v", result.Attempts)
	}
	if result.Attempts[2].End != result.Attempts[0].Start {
		t.Fatalf("older band overlaps recent band: %#v", result.Attempts)
	}
}

func TestCompleteFetchSplitsDenseRanges(t *testing.T) {
	cfg := testConfig("loki")
	cfg.InitialRecords = 2
	cfg.MaxRecords = 2
	cfg.MaxWindowSeconds = 20
	cfg.MaxScopeSeconds = 20
	cfg.MinSplitWindow = 5
	cfg.MaxCycleRecords = 100
	transport := &adaptiveTransport{}
	service := NewService(&Config{Sources: map[string]*SourceConfig{"prod": cfg}}, transport)
	result, err := NewAdaptiveFetcher(service).FetchComplete(context.Background(), "prod", 1000, 1020)
	if err != nil {
		t.Fatal(err)
	}
	if !result.CoverageComplete || len(result.Attempts) != 3 {
		t.Fatalf("expected root plus two split attempts: %#v", result)
	}
}
