package logs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

// Discover reads provider metadata with a small, fixed budget. It does not
// return log bodies and never starts continuous monitoring.
func (s *Service) Discover(ctx context.Context, source string) (DiscoveryResult, error) {
	cfg, ok := s.source(source)
	if !ok {
		return DiscoveryResult{}, fmt.Errorf("unknown log source")
	}
	budget := &fetchBudget{maxRequests: min(8, cfg.MaxPages), remainingBytes: min64(2*1024*1024, cfg.MaxBytes), initialBytes: min64(2*1024*1024, cfg.MaxBytes), deadline: time.Now().Add(time.Duration(min(20, cfg.TimeoutSeconds)) * time.Second)}
	headers, err := requestHeaders(cfg)
	if err != nil {
		return DiscoveryResult{}, err
	}
	result := DiscoveryResult{Source: source, Provider: cfg.Provider, Scopes: []DiscoveredScope{}}
	switch cfg.Provider {
	case "loki":
		err = s.discoverLoki(ctx, cfg, budget, headers, &result)
	case "elasticsearch", "opensearch":
		err = s.discoverElastic(ctx, cfg, budget, headers, &result)
	case "cloudwatch":
		err = s.discoverCloudWatch(ctx, cfg, budget, &result)
	case "datadog":
		err = s.discoverDatadog(ctx, cfg, budget, headers, &result)
	case "splunk":
		err = s.discoverSplunk(ctx, cfg, budget, headers, &result)
	case "graylog":
		err = s.discoverGraylog(ctx, cfg, budget, headers, &result)
	case "gcp":
		err = s.discoverGCP(ctx, cfg, budget, headers, &result)
		if err != nil {
			for _, value := range cfg.ResourceNames {
				result.Scopes = append(result.Scopes, DiscoveredScope{Kind: "resource", Key: "resource_name", Value: value, Label: value})
			}
			result.Warnings = append(result.Warnings, "Fine-grained Google Cloud discovery failed; configured resource names remain available")
			err = nil
		}
	case "azure":
		err = s.discoverAzure(ctx, cfg, budget, headers, &result)
		if err != nil {
			result.Scopes = append(result.Scopes, DiscoveredScope{Kind: "workspace", Key: "workspace", Value: cfg.WorkspaceID, Label: "Workspace " + cfg.WorkspaceID})
			result.Warnings = append(result.Warnings, "Fine-grained Azure discovery failed; the configured workspace remains available")
			err = nil
		}
	}
	if err != nil {
		return DiscoveryResult{}, err
	}
	result.Requests = budget.requests
	result.Scopes = normalizeScopes(result.Scopes)
	if len(result.Scopes) == 0 {
		result.Warnings = append(result.Warnings, "No monitorable services or namespaces were found in the discovery window")
	}
	return result, nil
}

func (s *Service) discoverGCP(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, result *DiscoveryResult) error {
	if headers.Get("Authorization") == "" {
		token, err := googleAccessToken(ctx)
		if err != nil {
			return err
		}
		headers.Set("Authorization", "Bearer "+token)
	}
	payload := map[string]any{
		"resourceNames": cfg.ResourceNames,
		"filter":        `timestamp >= "` + time.Now().Add(-24*time.Hour).UTC().Format(time.RFC3339) + `"`,
		"orderBy":       "timestamp desc",
		"pageSize":      200,
	}
	var response map[string]any
	if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, "/v2/entries:list", nil, payload, &response); err != nil {
		return err
	}
	for _, raw := range asSlice(response["entries"]) {
		entry := asMap(raw)
		resource := asMap(entry["resource"])
		if value := stringify(resource["type"]); value != "" {
			result.Scopes = append(result.Scopes, DiscoveredScope{Kind: "service", Key: "resource.type", Value: value, Label: value + " · resource type"})
		}
		for key, rawValue := range asMap(resource["labels"]) {
			value := stringify(rawValue)
			if value != "" && discoveryLabel(key) {
				path := "resource.labels." + key
				result.Scopes = append(result.Scopes, DiscoveredScope{Kind: scopeKind(key), Key: path, Value: value, Label: value + " · " + path})
			}
		}
		for key, rawValue := range asMap(entry["labels"]) {
			value := stringify(rawValue)
			if value != "" && discoveryLabel(key) {
				path := "labels." + key
				result.Scopes = append(result.Scopes, DiscoveredScope{Kind: scopeKind(key), Key: path, Value: value, Label: value + " · " + path})
			}
		}
	}
	return nil
}

func (s *Service) discoverAzure(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, result *DiscoveryResult) error {
	if headers.Get("Authorization") == "" {
		token, err := azureAccessToken(ctx)
		if err != nil {
			return err
		}
		headers.Set("Authorization", "Bearer "+token)
	}
	query := `union isfuzzy=true *
| where TimeGenerated > ago(24h)
| project AppRoleName=tostring(column_ifexists("AppRoleName", "")), ServiceName=tostring(column_ifexists("ServiceName", "")), Computer=tostring(column_ifexists("Computer", "")), Namespace=tostring(column_ifexists("Namespace", "")), ContainerName=tostring(column_ifexists("ContainerName", ""))
| take 1000`
	payload := map[string]any{"query": query, "timespan": "P1D"}
	var response map[string]any
	path := "/v1/workspaces/" + url.PathEscape(cfg.WorkspaceID) + "/query"
	if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, path, nil, payload, &response); err != nil {
		return err
	}
	for _, tableValue := range asSlice(response["tables"]) {
		table := asMap(tableValue)
		columns := []string{}
		for _, raw := range asSlice(table["columns"]) {
			columns = append(columns, stringify(asMap(raw)["name"]))
		}
		for _, raw := range asSlice(table["rows"]) {
			values := asSlice(raw)
			for index, key := range columns {
				if index >= len(values) {
					continue
				}
				value := stringify(values[index])
				if value == "" {
					continue
				}
				result.Scopes = append(result.Scopes, DiscoveredScope{Kind: scopeKind(key), Key: key, Value: value, Label: value + " · " + key})
			}
		}
	}
	return nil
}

func discoveryLabel(key string) bool {
	lower := strings.ToLower(key)
	for _, part := range []string{"service", "app", "namespace", "cluster", "pod", "container", "environment", "env"} {
		if strings.Contains(lower, part) {
			return true
		}
	}
	return false
}

func (s *Service) discoverLoki(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, result *DiscoveryResult) error {
	end := time.Now().Unix()
	params := url.Values{"start": {strconv.FormatInt(end-86400, 10)}, "end": {strconv.FormatInt(end, 10)}}
	var labelsResponse map[string]any
	if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodGet, "/loki/api/v1/labels", params, nil, &labelsResponse); err != nil {
		return err
	}
	priority := []string{"service", "service_name", "app", "namespace", "namespace_name", "job", "container", "pod"}
	available := map[string]bool{}
	for _, item := range asSlice(labelsResponse["data"]) {
		available[stringify(item)] = true
	}
	for _, label := range priority {
		if !available[label] || budget.requests >= budget.maxRequests {
			continue
		}
		var valuesResponse map[string]any
		if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodGet, "/loki/api/v1/label/"+url.PathEscape(label)+"/values", params, nil, &valuesResponse); err != nil {
			return err
		}
		for _, item := range asSlice(valuesResponse["data"]) {
			value := stringify(item)
			if value != "" {
				result.Scopes = append(result.Scopes, DiscoveredScope{Kind: scopeKind(label), Key: label, Value: value, Label: value + " · " + label})
			}
		}
	}
	return nil
}

func (s *Service) discoverElastic(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, result *DiscoveryResult) error {
	fields := []string{"service.name.keyword", "service.keyword", "kubernetes.namespace_name.keyword", "kubernetes.namespace.keyword", "host.name.keyword"}
	aggs := map[string]any{}
	for i, field := range fields {
		aggs[fmt.Sprintf("scope_%d", i)] = map[string]any{"terms": map[string]any{"field": field, "size": 100}}
	}
	payload := map[string]any{"size": 0, "query": map[string]any{"range": map[string]any{cfg.TimestampField: map[string]any{"gte": "now-24h", "lte": "now"}}}, "aggs": aggs}
	var response map[string]any
	if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, "/"+cfg.Index+"/_search", nil, payload, &response); err != nil {
		return err
	}
	aggregations := asMap(response["aggregations"])
	for i, field := range fields {
		key := strings.TrimSuffix(field, ".keyword")
		for _, raw := range asSlice(asMap(aggregations[fmt.Sprintf("scope_%d", i)])["buckets"]) {
			value := stringify(asMap(raw)["key"])
			if value != "" {
				result.Scopes = append(result.Scopes, DiscoveredScope{Kind: scopeKind(key), Key: key, Value: value, Label: value + " · " + key})
			}
		}
	}
	return nil
}

func (s *Service) discoverCloudWatch(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, result *DiscoveryResult) error {
	awsConfig, err := loadAWSConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("cloudwatch credentials or configuration failed")
	}
	client := cloudwatchlogs.NewFromConfig(awsConfig)
	var cursor *string
	for budget.requests < budget.maxRequests && len(result.Scopes) < 200 {
		requestCtx, cancel := context.WithDeadline(ctx, budget.deadline)
		output, requestErr := client.DescribeLogGroups(requestCtx, &cloudwatchlogs.DescribeLogGroupsInput{Limit: aws.Int32(50), NextToken: cursor})
		cancel()
		budget.requests++
		if requestErr != nil {
			return fmt.Errorf("cloudwatch discovery failed; check logs:DescribeLogGroups permission")
		}
		encoded, _ := json.Marshal(output)
		budget.remainingBytes -= int64(len(encoded))
		for _, group := range output.LogGroups {
			value := aws.ToString(group.LogGroupName)
			if value != "" {
				result.Scopes = append(result.Scopes, DiscoveredScope{Kind: "log_group", Key: "log_group", Value: value, Label: value})
			}
		}
		cursor = output.NextToken
		if cursor == nil || budget.remainingBytes < 1024 {
			break
		}
	}
	return nil
}

func (s *Service) discoverDatadog(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, result *DiscoveryResult) error {
	for _, facet := range []string{"service", "env", "host", "kube_namespace"} {
		if budget.requests >= budget.maxRequests {
			break
		}
		payload := map[string]any{"filter": map[string]any{"from": "now-24h", "to": "now", "query": "*"}, "compute": []any{map[string]any{"aggregation": "count", "type": "total"}}, "group_by": []any{map[string]any{"facet": facet, "limit": 100}}}
		var response map[string]any
		if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, "/api/v2/logs/analytics/aggregate", nil, payload, &response); err != nil {
			return err
		}
		for _, raw := range asSlice(asMap(response["data"])["buckets"]) {
			value := stringify(asMap(asMap(raw)["by"])[facet])
			if value == "" {
				continue
			}
			result.Scopes = append(result.Scopes, DiscoveredScope{Kind: scopeKind(facet), Key: facet, Value: value, Label: value + " · " + facet})
		}
	}
	return nil
}

func (s *Service) discoverSplunk(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, result *DiscoveryResult) error {
	for _, item := range []struct{ kind, key, query string }{{"host", "host", "| metadata type=hosts | head 100"}, {"source_type", "sourcetype", "| metadata type=sourcetypes | head 100"}} {
		form := url.Values{"search": {item.query}, "earliest_time": {"-24h"}, "latest_time": {"now"}, "output_mode": {"json"}, "count": {"100"}, "preview": {"false"}}
		raw, err := s.rawRequest(ctx, cfg, budget, headers, http.MethodPost, "/services/search/v2/jobs/export", nil, []byte(form.Encode()), "application/x-www-form-urlencoded")
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(strings.NewReader(string(raw)))
		for scanner.Scan() {
			var line map[string]any
			if json.Unmarshal(scanner.Bytes(), &line) == nil {
				row := asMap(line["result"])
				value := stringify(row[item.key])
				if value != "" {
					result.Scopes = append(result.Scopes, DiscoveredScope{Kind: item.kind, Key: item.key, Value: value, Label: value + " · " + item.key})
				}
			}
		}
	}
	return nil
}

func (s *Service) discoverGraylog(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, result *DiscoveryResult) error {
	headers.Set("X-Requested-By", "aidbg")
	params := url.Values{"field": {"source"}, "query": {"*"}, "range": {"86400"}, "limit": {"100"}}
	var response map[string]any
	if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodGet, "/search/universal/relative/terms", params, nil, &response); err != nil {
		return err
	}
	for value := range asMap(response["terms"]) {
		result.Scopes = append(result.Scopes, DiscoveredScope{Kind: "host", Key: "source", Value: value, Label: value + " · source"})
	}
	return nil
}

func scopeKind(key string) string {
	lower := strings.ToLower(key)
	if strings.Contains(lower, "namespace") {
		return "namespace"
	}
	if strings.Contains(lower, "host") {
		return "host"
	}
	if strings.Contains(lower, "pod") || strings.Contains(lower, "container") {
		return "workload"
	}
	return "service"
}

func normalizeScopes(scopes []DiscoveredScope) []DiscoveredScope {
	seen := map[string]bool{}
	result := make([]DiscoveredScope, 0, len(scopes))
	for _, scope := range scopes {
		id := scope.Key + "\x00" + scope.Value
		if scope.Key == "" || scope.Value == "" || seen[id] {
			continue
		}
		seen[id] = true
		result = append(result, scope)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Kind != result[j].Kind {
			return result[i].Kind < result[j].Kind
		}
		return result[i].Label < result[j].Label
	})
	if len(result) > 200 {
		result = result[:200]
	}
	return result
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
