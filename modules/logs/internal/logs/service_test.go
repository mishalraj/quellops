package logs

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeTransport struct {
	responses [][]byte
	requests  []Request
}

func (f *fakeTransport) Do(_ context.Context, request Request) ([]byte, error) {
	f.requests = append(f.requests, request)
	if len(f.responses) == 0 {
		return nil, fmt.Errorf("unexpected request")
	}
	response := f.responses[0]
	f.responses = f.responses[1:]
	return response, nil
}

func testConfig(provider string) *SourceConfig {
	cfg := &SourceConfig{Provider: provider, URL: "https://logs.example.com", Query: "service=checkout"}
	if provider == "gcp" {
		cfg.ResourceNames = []string{"projects/test"}
		cfg.TokenEnv = "TEST_GCP_TOKEN"
	}
	if provider == "azure" {
		cfg.WorkspaceID = "workspace"
		cfg.TokenEnv = "TEST_AZURE_TOKEN"
	}
	cfg.defaults()
	return cfg
}

func TestHTTPProviderNormalization(t *testing.T) {
	start := float64(1700000000)
	end := start + 60
	at := iso(start + 10)
	t.Setenv("TEST_GCP_TOKEN", "secret")
	t.Setenv("TEST_AZURE_TOKEN", "secret")
	tests := []struct {
		name, provider, response, message string
		configure                         func(*SourceConfig)
	}{
		{"loki", "loki", fmt.Sprintf(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"checkout"},"values":[["%d","loki error"]]}]}}`, int64((start+10)*1e9)), "loki error", nil},
		{"elasticsearch", "elasticsearch", fmt.Sprintf(`{"timed_out":false,"_shards":{"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_index":"logs","_id":"1","_source":{"@timestamp":%q,"message":"elastic error"}}]}}`, at), "elastic error", nil},
		{"opensearch", "opensearch", fmt.Sprintf(`{"timed_out":false,"_shards":{"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_index":"logs","_id":"1","_source":{"@timestamp":%q,"message":"opensearch error"}}]}}`, at), "opensearch error", nil},
		{"splunk", "splunk", fmt.Sprintf("{\"preview\":false,\"result\":{\"_time\":%q,\"_raw\":\"splunk error\",\"_cd\":\"1\"}}\n", at), "splunk error", nil},
		{"graylog", "graylog", fmt.Sprintf(`{"schema":[{"field":"timestamp"},{"field":"message"}],"datarows":[[%q,"graylog error"]]}`, at), "graylog error", nil},
		{"datadog", "datadog", fmt.Sprintf(`{"data":[{"id":"1","attributes":{"timestamp":%q,"message":"datadog error"}}],"meta":{}}`, at), "datadog error", func(c *SourceConfig) { c.APIKeyEnv = "TEST_DD_API"; c.AppKeyEnv = "TEST_DD_APP" }},
		{"gcp", "gcp", fmt.Sprintf(`{"entries":[{"timestamp":%q,"textPayload":"gcp error","insertId":"1","logName":"test"}]}`, at), "gcp error", nil},
		{"azure", "azure", fmt.Sprintf(`{"tables":[{"columns":[{"name":"TimeGenerated"},{"name":"Message"}],"rows":[[%q,"azure error"]]}]}`, at), "azure error", nil},
	}
	t.Setenv("TEST_DD_API", "api")
	t.Setenv("TEST_DD_APP", "app")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig(test.provider)
			if test.configure != nil {
				test.configure(cfg)
			}
			transport := &fakeTransport{responses: [][]byte{[]byte(test.response)}}
			service := NewService(&Config{Sources: map[string]*SourceConfig{"prod": cfg}}, transport)
			result, err := service.Fetch(context.Background(), "prod", start, end, 100)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Records) != 1 || result.Records[0].Message != test.message {
				t.Fatalf("unexpected records: %#v", result.Records)
			}
			if result.Requests != 1 || result.BytesReceived == 0 {
				t.Fatalf("unexpected usage: %#v", result)
			}
		})
	}
}

func TestConfigValidationAndDefaults(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "logs.yaml")
	content := "sources:\n  local:\n    provider: loki\n    url: http://localhost:3100\n    query: '{app=\"checkout\"}'\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Sources["local"].InitialRecords != 50 || config.Sources["local"].MaxCycleBytes != 16<<20 {
		t.Fatalf("defaults not applied: %#v", config.Sources["local"])
	}
	splunk := testConfig("splunk")
	if splunk.TokenScheme != "Splunk" {
		t.Fatalf("unexpected Splunk token scheme: %q", splunk.TokenScheme)
	}
	content = strings.Replace(content, "http://localhost:3100", "http://remote.example.com", 1)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected remote HTTP URL rejection")
	}
}

func TestConfiguredRedactionCoversMessageAndAttributes(t *testing.T) {
	start := float64(1700000000)
	cfg := testConfig("loki")
	cfg.RedactPatterns = []string{`secret=[^ ]+`}
	if err := cfg.validate("prod"); err != nil {
		t.Fatal(err)
	}
	transport := &fakeTransport{responses: [][]byte{[]byte(fmt.Sprintf(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"credential":"secret=abc"},"values":[["%d","failed secret=abc"]]}]}}`, int64((start+1)*1e9)))}}
	result, err := NewService(&Config{Sources: map[string]*SourceConfig{"prod": cfg}}, transport).Fetch(context.Background(), "prod", start, start+60, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Records[0].Message != "failed [REDACTED]" || result.Records[0].Attributes["credential"] != "[REDACTED]" {
		t.Fatalf("redaction failed: %#v", result.Records[0])
	}
}

func TestDynamicSourceLifecycleAndRuntimeCredentials(t *testing.T) {
	start := float64(1700000000)
	transport := &fakeTransport{responses: [][]byte{[]byte(fmt.Sprintf(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["%d","ok"]]}]}}`, int64((start+1)*1e9)))}}
	service := NewService(&Config{Sources: map[string]*SourceConfig{}}, transport)
	err := service.ApplySource("prod.logs", "loki", map[string]any{
		"url":   "https://loki.example.com",
		"query": `{app="checkout"}`,
	}, map[string]string{"token": "runtime-secret", "header:X-Scope-OrgID": "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if sources := service.ListSources(); len(sources) != 1 || sources[0].Name != "prod.logs" {
		t.Fatalf("dynamic source not listed: %#v", sources)
	}
	result, err := service.Fetch(context.Background(), "prod.logs", start, start+60, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || transport.requests[0].Headers.Get("Authorization") != "Bearer runtime-secret" || transport.requests[0].Headers.Get("X-Scope-OrgID") != "tenant-a" {
		t.Fatalf("runtime source was not used: %#v %#v", result.Records, transport.requests[0].Headers)
	}
	if !service.DeleteSource("prod.logs") {
		t.Fatal("expected dynamic source deletion")
	}
	if _, ok := service.source("prod.logs"); ok {
		t.Fatal("deleted source remains configured")
	}
	if err := service.ApplySource("bad/name", "loki", map[string]any{"url": "https://loki.example.com", "query": "x"}, nil); err == nil {
		t.Fatal("expected unsafe source name rejection")
	}
}

func TestUnscopedConnectionCanDiscoverButCannotFetch(t *testing.T) {
	service := NewService(&Config{Sources: map[string]*SourceConfig{}}, &fakeTransport{})
	if err := service.ApplySource("prod", "loki", map[string]any{"url": "https://loki.example.com"}, nil); err != nil {
		t.Fatalf("connection details should be accepted before scope discovery: %v", err)
	}
	if _, err := service.Fetch(context.Background(), "prod", 1700000000, 1700000060, 10); err == nil || !strings.Contains(err.Error(), "no selected monitoring scope") {
		t.Fatalf("expected unscoped fetch rejection, got %v", err)
	}
}

func TestLokiDiscoveryAndGeneratedScope(t *testing.T) {
	transport := &fakeTransport{responses: [][]byte{
		[]byte(`{"status":"success","data":["service","namespace","filename"]}`),
		[]byte(`{"status":"success","data":["checkout-service","payments"]}`),
		[]byte(`{"status":"success","data":["production"]}`),
	}}
	service := NewService(&Config{Sources: map[string]*SourceConfig{}}, transport)
	if err := service.ApplySource("prod", "loki", map[string]any{"url": "https://loki.example.com"}, nil); err != nil {
		t.Fatal(err)
	}
	result, err := service.Discover(context.Background(), "prod")
	if err != nil {
		t.Fatal(err)
	}
	if result.Requests != 3 || len(result.Scopes) != 3 {
		t.Fatalf("unexpected discovery result: %#v", result)
	}
	if err := service.ApplySource("prod", "loki", map[string]any{"url": "https://loki.example.com", "scope_key": "service", "scope_value": "checkout-service"}, nil); err != nil {
		t.Fatal(err)
	}
	cfg, _ := service.source("prod")
	if cfg.Query != `{service="checkout-service"}` {
		t.Fatalf("unexpected generated LogQL: %q", cfg.Query)
	}
}

func TestMultipleDiscoveredValuesGenerateBoundedProviderQueries(t *testing.T) {
	tests := []struct {
		provider string
		key      string
		want     string
	}{
		{"loki", "service", `{service=~"^(checkout|payments)$"}`},
		{"splunk", "service", `search (service="checkout" OR service="payments")`},
		{"elasticsearch", "service.name", `(service.name:"checkout" OR service.name:"payments")`},
		{"datadog", "service", `(service:"checkout" OR service:"payments")`},
		{"gcp", "resource.labels.service_name", `(resource.labels.service_name="checkout" OR resource.labels.service_name="payments")`},
		{"azure", "ServiceName", `union * | where tostring(ServiceName) in ("checkout","payments")`},
	}
	for _, test := range tests {
		t.Run(test.provider, func(t *testing.T) {
			cfg := testConfig(test.provider)
			cfg.Query = ""
			cfg.ScopeKey = test.key
			cfg.ScopeValue = ""
			cfg.ScopeValues = []string{"checkout", "payments"}
			cfg.defaults()
			if cfg.Query != test.want {
				t.Fatalf("unexpected generated query: %q", cfg.Query)
			}
		})
	}
}

func TestProviderDiscoveryConformanceFixtures(t *testing.T) {
	t.Setenv("TEST_GCP_TOKEN", "secret")
	t.Setenv("TEST_AZURE_TOKEN", "secret")
	t.Setenv("TEST_DD_API", "api")
	t.Setenv("TEST_DD_APP", "app")
	tests := []struct {
		provider  string
		responses []string
		configure func(*SourceConfig)
		wantKey   string
		wantValue string
	}{
		{"elasticsearch", []string{`{"aggregations":{"scope_0":{"buckets":[{"key":"checkout"}]}}}`}, nil, "service.name", "checkout"},
		{"datadog", []string{`{"data":{"buckets":[{"by":{"service":"checkout"}}]}}`, `{"data":{"buckets":[]}}`, `{"data":{"buckets":[]}}`, `{"data":{"buckets":[]}}`}, func(c *SourceConfig) { c.APIKeyEnv = "TEST_DD_API"; c.AppKeyEnv = "TEST_DD_APP" }, "service", "checkout"},
		{"splunk", []string{"{\"result\":{\"host\":\"api-1\"}}\n", "{\"result\":{\"sourcetype\":\"kube:container\"}}\n"}, nil, "host", "api-1"},
		{"graylog", []string{`{"terms":{"api-1":12}}`}, nil, "source", "api-1"},
		{"gcp", []string{`{"entries":[{"resource":{"type":"k8s_container","labels":{"namespace_name":"production","container_name":"checkout"}},"labels":{"service":"checkout"},"textPayload":"must not be returned"}]}`}, nil, "labels.service", "checkout"},
		{"azure", []string{`{"tables":[{"columns":[{"name":"AppRoleName"},{"name":"Namespace"}],"rows":[["checkout","production"]]}]}`}, nil, "AppRoleName", "checkout"},
	}
	for _, test := range tests {
		t.Run(test.provider, func(t *testing.T) {
			cfg := testConfig(test.provider)
			if test.configure != nil {
				test.configure(cfg)
			}
			cfg.Query = ""
			transport := &fakeTransport{}
			for _, response := range test.responses {
				transport.responses = append(transport.responses, []byte(response))
			}
			service := NewService(&Config{Sources: map[string]*SourceConfig{"prod": cfg}}, transport)
			result, err := service.Discover(context.Background(), "prod")
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, scope := range result.Scopes {
				if scope.Key == test.wantKey && scope.Value == test.wantValue {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected %s=%s in %#v", test.wantKey, test.wantValue, result.Scopes)
			}
		})
	}
}

func TestFocusedInvestigationBuildsNarrowProviderQuery(t *testing.T) {
	cfg := testConfig("loki")
	query, err := focusedQuery(cfg, "timeout")
	if err != nil {
		t.Fatal(err)
	}
	if query != `service=checkout |~ "(?i)(timeout|timed out|deadline exceeded)"` {
		t.Fatalf("unexpected focused query: %q", query)
	}
	if _, err := focusedQuery(cfg, "arbitrary user text"); err == nil {
		t.Fatal("expected unsupported focus to be rejected")
	}
}

func TestManagedOpenSearchSigV4(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	start := float64(1700000000)
	cfg := testConfig("opensearch")
	cfg.AWSSigV4 = true
	cfg.Region = "us-east-1"
	response := fmt.Sprintf(`{"timed_out":false,"_shards":{"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_index":"logs","_id":"1","_source":{"@timestamp":%q,"message":"signed"}}]}}`, iso(start+1))
	transport := &fakeTransport{responses: [][]byte{[]byte(response)}}
	_, err := NewService(&Config{Sources: map[string]*SourceConfig{"prod": cfg}}, transport).Fetch(context.Background(), "prod", start, start+60, 10)
	if err != nil {
		t.Fatal(err)
	}
	authorization := transport.requests[0].Headers.Get("Authorization")
	if !strings.HasPrefix(authorization, "AWS4-HMAC-SHA256 ") || transport.requests[0].Headers.Get("X-Amz-Date") == "" {
		t.Fatalf("request was not signed: %q", authorization)
	}
}

func TestHTTPTransportCapsBodiesAndRefusesRedirects(t *testing.T) {
	destinationReached := false
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/redirect":
			response.Header().Set("Location", "/destination")
			response.WriteHeader(http.StatusFound)
		case "/destination":
			destinationReached = true
			response.WriteHeader(http.StatusOK)
		default:
			_, _ = response.Write([]byte(strings.Repeat("x", 2048)))
		}
	}))
	defer server.Close()
	transport := NewHTTPTransport()
	for _, path := range []string{"/redirect", "/large"} {
		_, err := transport.Do(context.Background(), Request{Method: http.MethodGet, URL: server.URL + path, Headers: http.Header{"Authorization": {"Bearer secret"}}, Timeout: time.Second, MaxBytes: 1024})
		if err == nil {
			t.Fatalf("expected safe failure for %s", path)
		}
	}
	if destinationReached {
		t.Fatal("authorization-bearing redirect was followed")
	}
}
