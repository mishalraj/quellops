package logs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

type liveProviderConfig struct {
	Values      map[string]any    `json:"values"`
	Credentials map[string]string `json:"credentials"`
}

// TestLiveProviders is an opt-in conformance test against actual provider APIs.
// Set QUELLOPS_LIVE_<PROVIDER> to a JSON object containing values and credentials.
// It performs metadata discovery, lets QuellOps generate the provider query from
// the first discovered scope, and executes one bounded fetch.
func TestLiveProviders(t *testing.T) {
	providers := []string{"loki", "elasticsearch", "opensearch", "splunk", "graylog", "datadog", "cloudwatch", "gcp", "azure"}
	run := 0
	for _, provider := range providers {
		envName := "QUELLOPS_LIVE_" + strings.ToUpper(provider)
		raw := os.Getenv(envName)
		if raw == "" {
			continue
		}
		run++
		t.Run(provider, func(t *testing.T) {
			var live liveProviderConfig
			if err := json.Unmarshal([]byte(raw), &live); err != nil {
				t.Fatalf("%s is not valid JSON: %v", envName, err)
			}
			service := NewService(&Config{Sources: map[string]*SourceConfig{}}, nil)
			if err := service.ApplySource("live", provider, live.Values, live.Credentials); err != nil {
				t.Fatalf("configure: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			discovery, err := service.Discover(ctx, "live")
			if err != nil {
				t.Fatalf("discover: %v", err)
			}
			if len(discovery.Scopes) == 0 {
				t.Fatal("provider returned no monitorable scope")
			}
			scope := discovery.Scopes[0]
			live.Values["scope_key"] = scope.Key
			live.Values["scope_value"] = scope.Value
			if err := service.ApplySource("live", provider, live.Values, live.Credentials); err != nil {
				t.Fatalf("apply discovered scope %s=%s: %v", scope.Key, scope.Value, err)
			}
			end := float64(time.Now().Unix())
			result, err := service.Fetch(ctx, "live", end-300, end, 20)
			if err != nil {
				t.Fatalf("bounded fetch: %v", err)
			}
			t.Logf("validated %s: discovery_requests=%d fetch_requests=%d records=%d bytes=%d", provider, discovery.Requests, result.Requests, len(result.Records), result.BytesReceived)
		})
	}
	if run == 0 {
		t.Skip(fmt.Sprintf("set one of QUELLOPS_LIVE_%s to run provider conformance", strings.ToUpper(strings.Join(providers, ", QUELLOPS_LIVE_"))))
	}
}
