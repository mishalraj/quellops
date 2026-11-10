package logs

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"golang.org/x/oauth2/google"
)

type Service struct {
	Config    *Config
	Transport Transport
	mu        sync.RWMutex
}

func (s *Service) source(name string) (*SourceConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.Config.Sources[name]
	return value, ok
}

func (s *Service) ListSources() []Source {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Config.ListSources()
}

func (s *Service) ApplySource(name, provider string, values map[string]any, credentialsMap map[string]string) error {
	if name == "" || !safeSourceName.MatchString(name) {
		return fmt.Errorf("source name must contain only letters, numbers, dot, dash or underscore")
	}
	source, err := sourceConfigFromMap(name, provider, values, credentialsMap)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.Config.Sources[name] = source
	s.mu.Unlock()
	return nil
}

func (s *Service) DeleteSource(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.Config.Sources[name]; !exists {
		return false
	}
	delete(s.Config.Sources, name)
	return true
}

func NewService(config *Config, transport Transport) *Service {
	if transport == nil {
		transport = NewHTTPTransport()
	}
	return &Service{Config: config, Transport: transport}
}

type fetchBudget struct {
	maxRequests    int
	remainingBytes int64
	deadline       time.Time
	requests       int
	initialBytes   int64
}

type requestOptions struct {
	maxPages int
	maxBytes int64
	timeout  time.Duration
	focus    string
}

func (s *Service) Fetch(ctx context.Context, source string, start, end float64, limit int) (FetchResult, error) {
	return s.fetch(ctx, source, start, end, limit, requestOptions{})
}

func (s *Service) fetch(ctx context.Context, source string, start, end float64, limit int, options requestOptions) (FetchResult, error) {
	cfg, ok := s.source(source)
	if !ok {
		return FetchResult{}, fmt.Errorf("unknown log source")
	}
	if err := cfg.validateFetchScope(source); err != nil {
		return FetchResult{}, err
	}
	if options.focus != "" {
		focused, err := focusedQuery(cfg, options.focus)
		if err != nil {
			return FetchResult{}, err
		}
		copy := *cfg
		copy.Query = focused
		cfg = &copy
	}
	if math.IsNaN(start) || math.IsNaN(end) || math.IsInf(start, 0) || math.IsInf(end, 0) || start < 0 || end <= start {
		return FetchResult{}, fmt.Errorf("expected finite Unix timestamps with 0 <= start < end")
	}
	if end-start > float64(cfg.MaxWindowSeconds) {
		return FetchResult{}, fmt.Errorf("query exceeds configured time window")
	}
	if limit < 1 || limit > cfg.MaxRecords {
		return FetchResult{}, fmt.Errorf("query exceeds configured record limit")
	}
	maxPages := cfg.MaxPages
	if options.maxPages > 0 {
		maxPages = min(maxPages, options.maxPages)
	}
	maxBytes := cfg.MaxBytes
	if options.maxBytes > 0 {
		maxBytes = min(maxBytes, options.maxBytes)
	}
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if options.timeout > 0 && options.timeout < timeout {
		timeout = options.timeout
	}
	if maxPages < 1 || maxBytes < 1024 || timeout <= 0 {
		return FetchResult{}, fmt.Errorf("remaining adaptive query budget is too small")
	}
	budget := &fetchBudget{maxRequests: maxPages, remainingBytes: maxBytes, initialBytes: maxBytes, deadline: time.Now().Add(timeout)}
	headers, err := requestHeaders(cfg)
	if err != nil {
		return FetchResult{}, err
	}
	records := make([]Record, 0, limit)
	warnings := map[string]bool{}

	add := func(at any, message any, attributes map[string]any, id any) error {
		atSeconds, err := parseTimestamp(at)
		if err != nil {
			return err
		}
		if atSeconds < start || atSeconds > end {
			return nil
		}
		if attributes == nil {
			attributes = map[string]any{}
		}
		messageString := cfg.redactString(stringify(message))
		attributes = cfg.redactMap(attributes)
		identity := stringify(id)
		if identity == "" {
			encoded, _ := json.Marshal([]any{iso(atSeconds), messageString, attributes})
			sum := sha256.Sum256(encoded)
			identity = hex.EncodeToString(sum[:])
		}
		records = append(records, Record{Timestamp: iso(atSeconds), Message: messageString, Source: source, ID: identity, Attributes: attributes})
		return nil
	}

	switch cfg.Provider {
	case "loki":
		var response map[string]any
		params := url.Values{"query": {cfg.Query}, "start": {strconv.FormatInt(int64(start*1e9), 10)}, "end": {strconv.FormatInt(int64(end*1e9), 10)}, "direction": {"forward"}, "limit": {strconv.Itoa(limit)}}
		if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodGet, "/loki/api/v1/query_range", params, nil, &response); err != nil {
			return FetchResult{}, err
		}
		data := asMap(response["data"])
		if stringify(response["status"]) != "success" || stringify(data["resultType"]) != "streams" {
			return FetchResult{}, fmt.Errorf("Loki query must return log streams")
		}
		for _, streamValue := range asSlice(data["result"]) {
			stream := asMap(streamValue)
			attrs := asMap(stream["stream"])
			for _, value := range asSlice(stream["values"]) {
				parts := asSlice(value)
				if len(parts) < 2 {
					continue
				}
				nanos, err := strconv.ParseInt(stringify(parts[0]), 10, 64)
				if err != nil {
					return FetchResult{}, fmt.Errorf("loki response contains an invalid timestamp")
				}
				if err := add(float64(nanos)/1e9, parts[1], attrs, nil); err != nil {
					return FetchResult{}, err
				}
			}
		}
	case "elasticsearch", "opensearch":
		payload := map[string]any{"size": limit, "track_total_hits": true, "sort": []any{map[string]any{cfg.TimestampField: "asc"}}, "query": map[string]any{"bool": map[string]any{"filter": []any{map[string]any{"range": map[string]any{cfg.TimestampField: map[string]any{"gte": iso(start), "lte": iso(end)}}}, map[string]any{"query_string": map[string]any{"query": cfg.Query}}}}}}
		var response map[string]any
		if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, "/"+cfg.Index+"/_search", nil, payload, &response); err != nil {
			return FetchResult{}, err
		}
		if truthy(response["timed_out"]) || number(asMap(response["_shards"])["failed"]) > 0 {
			warnings["backend_partial_result"] = true
		}
		hits := asMap(response["hits"])
		for _, hitValue := range asSlice(hits["hits"]) {
			hit := asMap(hitValue)
			row := asMap(hit["_source"])
			if err := add(row[cfg.TimestampField], valueOr(row["message"], row), row, stringify(hit["_index"])+":"+stringify(hit["_id"])); err != nil {
				return FetchResult{}, err
			}
		}
		total := asMap(hits["total"])
		if number(total["value"]) > float64(limit) || stringify(total["relation"]) == "gte" {
			warnings["more_results"] = true
		}
	case "splunk":
		form := url.Values{"search": {cfg.Query}, "earliest_time": {strconv.FormatFloat(start, 'f', -1, 64)}, "latest_time": {strconv.FormatFloat(end, 'f', -1, 64)}, "output_mode": {"json"}, "count": {strconv.Itoa(limit)}, "preview": {"false"}}
		raw, err := s.rawRequest(ctx, cfg, budget, headers, http.MethodPost, "/services/search/v2/jobs/export", nil, []byte(form.Encode()), "application/x-www-form-urlencoded")
		if err != nil {
			return FetchResult{}, err
		}
		scanner := bufio.NewScanner(strings.NewReader(string(raw)))
		for scanner.Scan() {
			var line map[string]any
			if json.Unmarshal(scanner.Bytes(), &line) != nil {
				return FetchResult{}, fmt.Errorf("splunk response could not be decoded")
			}
			if line["messages"] != nil {
				return FetchResult{}, fmt.Errorf("Splunk returned search diagnostics; inspect the saved query")
			}
			if result := asMap(line["result"]); len(result) > 0 && !truthy(line["preview"]) {
				if err := add(result["_time"], valueOr(result["_raw"], result), result, result["_cd"]); err != nil {
					return FetchResult{}, err
				}
			}
		}
	case "graylog":
		headers.Set("X-Requested-By", "aidbg")
		var response map[string]any
		if cfg.GraylogAPI == "legacy" {
			params := url.Values{"query": {cfg.Query}, "from": {iso(start)}, "to": {iso(end)}, "limit": {strconv.Itoa(limit)}, "sort": {"timestamp:asc"}}
			if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodGet, "/search/universal/absolute", params, nil, &response); err != nil {
				return FetchResult{}, err
			}
			for _, hitValue := range asSlice(response["messages"]) {
				row := asMap(asMap(hitValue)["message"])
				if err := add(row["timestamp"], row["message"], row, row["_id"]); err != nil {
					return FetchResult{}, err
				}
			}
			if number(response["total_results"]) > float64(limit) {
				warnings["more_results"] = true
			}
		} else {
			payload := map[string]any{"query": cfg.Query, "fields": []string{"timestamp", "message", "source"}, "from": 0, "size": limit, "timerange": map[string]any{"type": "absolute", "from": iso(start), "to": iso(end)}, "sort": "timestamp", "sort_order": "asc"}
			if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, "/search/messages", nil, payload, &response); err != nil {
				return FetchResult{}, err
			}
			fields := []string{}
			for _, column := range asSlice(response["schema"]) {
				fields = append(fields, stringify(asMap(column)["field"]))
			}
			for _, valuesValue := range asSlice(response["datarows"]) {
				values := asSlice(valuesValue)
				row := map[string]any{}
				for i, field := range fields {
					if i < len(values) {
						row[field] = values[i]
					}
				}
				if err := add(row["timestamp"], row["message"], row, nil); err != nil {
					return FetchResult{}, err
				}
			}
		}
	case "datadog":
		apiKey, err := cfg.credential("api_key", cfg.APIKeyEnv, true)
		if err != nil {
			return FetchResult{}, err
		}
		appKey, err := cfg.credential("app_key", cfg.AppKeyEnv, true)
		if err != nil {
			return FetchResult{}, err
		}
		headers.Set("DD-API-KEY", apiKey)
		headers.Set("DD-APPLICATION-KEY", appKey)
		cursor := ""
		for len(records) < limit {
			page := map[string]any{"limit": min(1000, limit-len(records))}
			if cursor != "" {
				page["cursor"] = cursor
			}
			payload := map[string]any{"filter": map[string]any{"from": iso(start), "to": iso(end), "query": cfg.Query}, "sort": "timestamp", "page": page}
			var response map[string]any
			if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, "/api/v2/logs/events/search", nil, payload, &response); err != nil {
				return FetchResult{}, err
			}
			for _, value := range asSlice(response["data"]) {
				row := asMap(value)
				attrs := asMap(row["attributes"])
				if err := add(attrs["timestamp"], attrs["message"], attrs, row["id"]); err != nil {
					return FetchResult{}, err
				}
			}
			meta := asMap(response["meta"])
			if stringify(meta["status"]) == "timeout" || len(asSlice(meta["warnings"])) > 0 {
				warnings["backend_partial_result"] = true
			}
			cursor = stringify(asMap(meta["page"])["after"])
			if cursor == "" {
				break
			}
			if budget.requests >= budget.maxRequests || len(records) >= limit {
				warnings["more_results"] = true
				break
			}
		}
	case "gcp":
		if headers.Get("Authorization") == "" {
			token, err := googleAccessToken(ctx)
			if err != nil {
				return FetchResult{}, err
			}
			headers.Set("Authorization", "Bearer "+token)
		}
		cursor := ""
		for len(records) < limit {
			payload := map[string]any{"resourceNames": cfg.ResourceNames, "filter": fmt.Sprintf("(%s) AND timestamp >= %q AND timestamp <= %q", cfg.Query, iso(start), iso(end)), "orderBy": "timestamp asc", "pageSize": min(1000, limit-len(records))}
			if cursor != "" {
				payload["pageToken"] = cursor
			}
			var response map[string]any
			if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, "/v2/entries:list", nil, payload, &response); err != nil {
				return FetchResult{}, err
			}
			for _, value := range asSlice(response["entries"]) {
				row := asMap(value)
				message := valueOr(row["textPayload"], valueOr(row["jsonPayload"], row["protoPayload"]))
				id := any(nil)
				if stringify(row["insertId"]) != "" {
					id = stringify(row["logName"]) + ":" + stringify(row["insertId"])
				}
				if err := add(row["timestamp"], message, row, id); err != nil {
					return FetchResult{}, err
				}
			}
			cursor = stringify(response["nextPageToken"])
			if cursor == "" {
				break
			}
			if budget.requests >= budget.maxRequests || len(records) >= limit {
				warnings["more_results"] = true
				break
			}
		}
	case "azure":
		if headers.Get("Authorization") == "" {
			token, err := azureAccessToken(ctx)
			if err != nil {
				return FetchResult{}, err
			}
			headers.Set("Authorization", "Bearer "+token)
		}
		query := fmt.Sprintf("%s\n| where %s between (datetime(%s) .. datetime(%s))\n| sort by %s asc\n| take %d", cfg.Query, cfg.TimestampField, iso(start), iso(end), cfg.TimestampField, limit)
		payload := map[string]any{"query": query, "timespan": iso(start) + "/" + iso(end)}
		var response map[string]any
		if err := s.jsonRequest(ctx, cfg, budget, headers, http.MethodPost, "/v1/workspaces/"+url.PathEscape(cfg.WorkspaceID)+"/query", nil, payload, &response); err != nil {
			return FetchResult{}, err
		}
		if response["error"] != nil {
			warnings["backend_partial_result"] = true
		}
		for _, tableValue := range asSlice(response["tables"]) {
			table := asMap(tableValue)
			names := []string{}
			for _, col := range asSlice(table["columns"]) {
				names = append(names, stringify(asMap(col)["name"]))
			}
			for _, valuesValue := range asSlice(table["rows"]) {
				values := asSlice(valuesValue)
				row := map[string]any{}
				for i, name := range names {
					if i < len(values) {
						row[name] = values[i]
					}
				}
				if err := add(row[cfg.TimestampField], valueOr(row[cfg.MessageField], row), row, nil); err != nil {
					return FetchResult{}, err
				}
			}
		}
	case "cloudwatch":
		groups := cfg.LogGroups
		if len(groups) == 0 {
			groups = []string{cfg.LogGroup}
		}
		for _, group := range groups {
			if len(records) >= limit || budget.requests >= budget.maxRequests {
				warnings["more_results"] = true
				break
			}
			groupConfig := *cfg
			groupConfig.LogGroup = group
			if err := s.fetchCloudWatch(ctx, &groupConfig, budget, start, end, limit-len(records), &records, warnings, source); err != nil {
				return FetchResult{}, err
			}
		}
	}

	if len(records) >= limit {
		warnings["record_limit_reached"] = true
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Timestamp == records[j].Timestamp {
			return records[i].ID < records[j].ID
		}
		return records[i].Timestamp < records[j].Timestamp
	})
	if len(records) > limit {
		records = records[:limit]
	}
	warningList := mapKeys(warnings)
	return FetchResult{Source: source, Provider: cfg.Provider, Start: iso(start), End: iso(end), Records: records, Truncated: len(warningList) > 0, Warnings: warningList, Requests: budget.requests, BytesReceived: budget.initialBytes - budget.remainingBytes}, nil
}

func focusedQuery(cfg *SourceConfig, focus string) (string, error) {
	patterns := map[string]string{
		"error":   "error|exception|failed|fatal|panic",
		"timeout": "timeout|timed out|deadline exceeded",
		"refused": "connection refused|unavailable|connection reset",
		"warning": "warn|warning",
	}
	pattern, ok := patterns[focus]
	if !ok {
		return "", fmt.Errorf("unsupported investigation focus")
	}
	switch cfg.Provider {
	case "loki":
		return cfg.Query + fmt.Sprintf(` |~ "(?i)(%s)"`, pattern), nil
	case "splunk":
		return cfg.Query + fmt.Sprintf(` ("%s")`, strings.ReplaceAll(pattern, "|", `" OR "`)), nil
	case "elasticsearch", "opensearch", "graylog":
		return cfg.Query + fmt.Sprintf(` AND message:(%s)`, pattern), nil
	case "datadog":
		return cfg.Query + fmt.Sprintf(` ("%s")`, strings.ReplaceAll(pattern, "|", `" OR "`)), nil
	case "cloudwatch":
		return cfg.Query + " ?" + strings.ReplaceAll(pattern, "|", " ?"), nil
	case "gcp":
		return cfg.Query + fmt.Sprintf(` AND textPayload =~ "(?i)(%s)"`, pattern), nil
	case "azure":
		return cfg.Query + fmt.Sprintf(` | where tostring(%s) matches regex @"(?i)(%s)"`, cfg.MessageField, pattern), nil
	default:
		return "", fmt.Errorf("provider does not support focused investigation")
	}
}

func requestHeaders(cfg *SourceConfig) (http.Header, error) {
	headers := http.Header{"Accept": {"application/json"}, "User-Agent": {"aidbg-logs/0.2"}}
	if cfg.TokenEnv != "" || cfg.runtimeSecrets["token"] != "" {
		token, err := cfg.credential("token", cfg.TokenEnv, true)
		if err != nil {
			return nil, err
		}
		headers.Set("Authorization", cfg.TokenScheme+" "+token)
	}
	if cfg.UsernameEnv != "" || cfg.runtimeSecrets["username"] != "" {
		username, err := cfg.credential("username", cfg.UsernameEnv, true)
		if err != nil {
			return nil, err
		}
		password, err := cfg.credential("password", cfg.PasswordEnv, true)
		if err != nil {
			return nil, err
		}
		headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(username+":"+password)))
	}
	for name, env := range cfg.HeadersEnv {
		value, err := cfg.credential("header:"+name, env, true)
		if err != nil {
			return nil, err
		}
		headers.Set(name, value)
	}
	for key, value := range cfg.runtimeSecrets {
		if strings.HasPrefix(key, "header:") && value != "" {
			headers.Set(strings.TrimPrefix(key, "header:"), value)
		}
	}
	return headers, nil
}

func (s *Service) rawRequest(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, method, path string, params url.Values, body []byte, contentType string) ([]byte, error) {
	if budget.requests >= budget.maxRequests || time.Now().After(budget.deadline) {
		return nil, fmt.Errorf("query request/time budget exhausted; narrow the query")
	}
	if budget.remainingBytes < 1024 {
		return nil, fmt.Errorf("query byte budget exhausted")
	}
	endpoint := strings.TrimRight(cfg.URL, "/") + path
	if len(params) > 0 {
		endpoint += "?" + params.Encode()
	}
	requestHeaders := headers.Clone()
	if contentType != "" {
		requestHeaders.Set("Content-Type", contentType)
	}
	if cfg.AWSSigV4 {
		awsConfig, err := loadAWSConfig(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("opensearch AWS credentials are unavailable")
		}
		credentials, err := awsConfig.Credentials.Retrieve(ctx)
		if err != nil {
			return nil, fmt.Errorf("opensearch AWS credentials are unavailable")
		}
		signingRequest, err := http.NewRequest(method, endpoint, strings.NewReader(string(body)))
		if err != nil {
			return nil, fmt.Errorf("opensearch signing request could not be created")
		}
		signingRequest.Header = requestHeaders.Clone()
		bodyHash := sha256.Sum256(body)
		if err := awsv4.NewSigner().SignHTTP(
			ctx,
			credentials,
			signingRequest,
			hex.EncodeToString(bodyHash[:]),
			cfg.AWSService,
			cfg.Region,
			time.Now(),
		); err != nil {
			return nil, fmt.Errorf("opensearch request signing failed")
		}
		requestHeaders = signingRequest.Header
	}
	remaining := time.Until(budget.deadline)
	if remaining <= 0 {
		return nil, fmt.Errorf("query request/time budget exhausted; narrow the query")
	}
	budget.requests++
	raw, err := s.Transport.Do(ctx, Request{Method: method, URL: endpoint, Headers: requestHeaders, Body: body, Timeout: remaining, MaxBytes: budget.remainingBytes})
	if err != nil {
		return nil, err
	}
	budget.remainingBytes -= int64(len(raw))
	return raw, nil
}

func (s *Service) jsonRequest(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, headers http.Header, method, path string, params url.Values, payload any, target any) error {
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("backend request could not be encoded")
		}
	}
	raw, err := s.rawRequest(ctx, cfg, budget, headers, method, path, params, body, map[bool]string{true: "application/json", false: ""}[payload != nil])
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, target) != nil {
		return fmt.Errorf("%s response could not be decoded", cfg.Provider)
	}
	return nil
}

func (s *Service) fetchCloudWatch(ctx context.Context, cfg *SourceConfig, budget *fetchBudget, start, end float64, limit int, records *[]Record, warnings map[string]bool, source string) error {
	awsConfig, err := loadAWSConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("cloudwatch credentials or configuration failed")
	}
	client := cloudwatchlogs.NewFromConfig(awsConfig)
	var cursor *string
	for len(*records) < limit {
		if budget.requests >= budget.maxRequests || time.Now().After(budget.deadline) {
			warnings["request_or_time_budget"] = true
			break
		}
		pageLimit := int32(min(10000, limit-len(*records)))
		input := &cloudwatchlogs.FilterLogEventsInput{LogGroupName: aws.String(cfg.LogGroup), StartTime: aws.Int64(int64(start * 1000)), EndTime: aws.Int64(int64(end * 1000)), FilterPattern: aws.String(cfg.Query), Limit: aws.Int32(pageLimit), NextToken: cursor}
		requestCtx, cancel := context.WithDeadline(ctx, budget.deadline)
		output, err := client.FilterLogEvents(requestCtx, input)
		cancel()
		budget.requests++
		if err != nil {
			return fmt.Errorf("cloudwatch request failed; check permissions and configuration")
		}
		encoded, _ := json.Marshal(output)
		budget.remainingBytes -= int64(len(encoded))
		if budget.remainingBytes < 0 {
			return fmt.Errorf("CloudWatch response exceeded byte budget; narrow query")
		}
		for _, event := range output.Events {
			at := float64(aws.ToInt64(event.Timestamp)) / 1000
			if at < start || at > end {
				continue
			}
			message := cfg.redactString(aws.ToString(event.Message))
			attributes := cfg.redactMap(map[string]any{"logStreamName": aws.ToString(event.LogStreamName)})
			id := aws.ToString(event.EventId)
			if id == "" {
				sum := sha256.Sum256([]byte(fmt.Sprintf("%f:%s", at, message)))
				id = hex.EncodeToString(sum[:])
			}
			*records = append(*records, Record{Timestamp: iso(at), Message: message, Source: source, ID: id, Attributes: attributes})
		}
		previous := aws.ToString(cursor)
		cursor = output.NextToken
		if cursor == nil || aws.ToString(cursor) == previous {
			break
		}
	}
	if cursor != nil {
		warnings["more_results"] = true
	}
	return nil
}

func googleAccessToken(ctx context.Context) (string, error) {
	credentials, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/logging.read")
	if err != nil {
		return "", fmt.Errorf("gcp credentials are unavailable")
	}
	token, err := credentials.TokenSource.Token()
	if err != nil {
		return "", fmt.Errorf("gcp credentials are unavailable")
	}
	return token.AccessToken, nil
}
func azureAccessToken(ctx context.Context) (string, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return "", fmt.Errorf("azure credentials are unavailable")
	}
	token, err := credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://api.loganalytics.io/.default"}})
	if err != nil {
		return "", fmt.Errorf("azure credentials are unavailable")
	}
	return token.Token, nil
}

func loadAWSConfig(ctx context.Context, cfg *SourceConfig) (aws.Config, error) {
	options := []func(*awscfg.LoadOptions) error{awscfg.WithRegion(cfg.Region)}
	accessKey := cfg.runtimeSecrets["aws_access_key_id"]
	secretKey := cfg.runtimeSecrets["aws_secret_access_key"]
	if accessKey != "" || secretKey != "" {
		if accessKey == "" || secretKey == "" {
			return aws.Config{}, fmt.Errorf("AWS access key and secret key must be supplied together")
		}
		provider := credentials.NewStaticCredentialsProvider(accessKey, secretKey, cfg.runtimeSecrets["aws_session_token"])
		options = append(options, awscfg.WithCredentialsProvider(provider))
	}
	return awscfg.LoadDefaultConfig(ctx, options...)
}

func parseTimestamp(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	case string:
		if number, err := strconv.ParseFloat(v, 64); err == nil {
			return number, nil
		}
		parsed, err := time.Parse(time.RFC3339Nano, v)
		if err == nil {
			return float64(parsed.UnixNano()) / 1e9, nil
		}
	}
	return 0, fmt.Errorf("backend response contains an invalid timestamp")
}
func stringify(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
func asMap(value any) map[string]any {
	if result, ok := value.(map[string]any); ok {
		return result
	}
	return map[string]any{}
}
func asSlice(value any) []any {
	if result, ok := value.([]any); ok {
		return result
	}
	return nil
}
func number(value any) float64 { result, _ := parseTimestamp(value); return result }
func truthy(value any) bool    { result, _ := value.(bool); return result }
func valueOr(value, fallback any) any {
	if value == nil {
		return fallback
	}
	return value
}
func mapKeys(values map[string]bool) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}
