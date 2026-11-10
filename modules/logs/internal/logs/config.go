package logs

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

var safeIdentifier = regexp.MustCompile(`^[A-Za-z0-9_@.*,-]+$`)
var safeSourceName = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

var supportedProviders = map[string]bool{
	"loki": true, "splunk": true, "elasticsearch": true, "opensearch": true,
	"graylog": true, "datadog": true, "cloudwatch": true, "gcp": true, "azure": true,
}

type Config struct {
	Sources map[string]*SourceConfig `yaml:"sources"`
}

type SourceConfig struct {
	Provider       string            `yaml:"provider"`
	URL            string            `yaml:"url"`
	Query          string            `yaml:"query"`
	TokenEnv       string            `yaml:"token_env"`
	TokenScheme    string            `yaml:"token_scheme"`
	UsernameEnv    string            `yaml:"username_env"`
	PasswordEnv    string            `yaml:"password_env"`
	HeadersEnv     map[string]string `yaml:"headers_env"`
	APIKeyEnv      string            `yaml:"api_key_env"`
	AppKeyEnv      string            `yaml:"app_key_env"`
	Index          string            `yaml:"index"`
	TimestampField string            `yaml:"timestamp_field"`
	MessageField   string            `yaml:"message_field"`
	GraylogAPI     string            `yaml:"graylog_api"`
	Region         string            `yaml:"region"`
	AWSSigV4       bool              `yaml:"aws_sigv4"`
	AWSService     string            `yaml:"aws_service"`
	LogGroup       string            `yaml:"log_group"`
	LogGroups      []string          `yaml:"log_groups"`
	ResourceNames  []string          `yaml:"resource_names"`
	WorkspaceID    string            `yaml:"workspace_id"`
	ScopeKey       string            `yaml:"scope_key"`
	ScopeValue     string            `yaml:"scope_value"`
	ScopeValues    []string          `yaml:"scope_values"`
	CustomMessage  string            `yaml:"custom_message"`
	CustomLevel    string            `yaml:"custom_level"`
	RedactPatterns []string          `yaml:"redact_patterns"`
	redactors      []*regexp.Regexp
	runtimeSecrets map[string]string

	MaxBytes         int64 `yaml:"max_bytes"`
	TimeoutSeconds   int   `yaml:"timeout_s"`
	MaxPages         int   `yaml:"max_pages"`
	MaxWindowSeconds int   `yaml:"max_window_s"`
	MaxScopeSeconds  int   `yaml:"max_scope_s"`
	InitialWindow    int   `yaml:"initial_window_s"`
	InitialRecords   int   `yaml:"initial_records"`
	TargetRecords    int   `yaml:"target_records"`
	MaxRecords       int   `yaml:"max_records"`
	GrowthFactor     int   `yaml:"growth_factor"`
	MinSplitWindow   int   `yaml:"min_split_window_s"`
	MaxCycleFetches  int   `yaml:"max_cycle_fetches"`
	MaxCycleRequests int   `yaml:"max_cycle_requests"`
	MaxCycleBytes    int64 `yaml:"max_cycle_bytes"`
	MaxCycleRecords  int   `yaml:"max_cycle_records"`
	MaxCycleSeconds  int   `yaml:"max_cycle_seconds"`
}

func sourceConfigFromMap(name, provider string, values map[string]any, credentials map[string]string) (*SourceConfig, error) {
	copyValues := make(map[string]any, len(values)+1)
	for key, value := range values {
		copyValues[key] = value
	}
	copyValues["provider"] = provider
	raw, err := yaml.Marshal(copyValues)
	if err != nil {
		return nil, fmt.Errorf("source %s configuration could not be encoded", name)
	}
	var source SourceConfig
	if err := yaml.Unmarshal(raw, &source); err != nil {
		return nil, fmt.Errorf("source %s configuration is invalid", name)
	}
	source.runtimeSecrets = make(map[string]string, len(credentials))
	for key, value := range credentials {
		if value != "" {
			source.runtimeSecrets[key] = value
		}
	}
	source.defaults()
	if err := source.validate(name); err != nil {
		return nil, err
	}
	return &source, nil
}

func (c *SourceConfig) credential(key, envName string, required bool) (string, error) {
	if value := c.runtimeSecrets[key]; value != "" {
		return value, nil
	}
	return secret(envName, required)
}

type Source struct {
	Name     string
	Provider string
}

func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot load log config")
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("cannot load log config")
	}
	if cfg.Sources == nil {
		return nil, fmt.Errorf("sources must be a mapping")
	}
	for name, source := range cfg.Sources {
		if source == nil {
			return nil, fmt.Errorf("invalid source %s", name)
		}
		source.defaults()
		if err := source.validate(name); err != nil {
			return nil, err
		}
		if err := source.validateFetchScope(name); err != nil {
			return nil, err
		}
	}
	return &cfg, nil
}

func (c *Config) ListSources() []Source {
	result := make([]Source, 0, len(c.Sources))
	for name, source := range c.Sources {
		result = append(result, Source{Name: name, Provider: source.Provider})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func (c *SourceConfig) defaults() {
	if len(c.ScopeValues) == 0 && c.ScopeValue != "" {
		c.ScopeValues = []string{c.ScopeValue}
	}
	if c.ScopeValue == "" && len(c.ScopeValues) > 0 {
		c.ScopeValue = c.ScopeValues[0]
	}
	if c.TokenScheme == "" {
		if c.Provider == "splunk" {
			c.TokenScheme = "Splunk"
		} else {
			c.TokenScheme = "Bearer"
		}
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 2 * 1024 * 1024
	}
	if c.TimeoutSeconds == 0 {
		c.TimeoutSeconds = 15
	}
	if c.MaxPages == 0 {
		c.MaxPages = 5
	}
	if c.MaxWindowSeconds == 0 {
		c.MaxWindowSeconds = 900
	}
	if c.MaxScopeSeconds == 0 {
		c.MaxScopeSeconds = c.MaxWindowSeconds
	}
	if c.InitialWindow == 0 {
		c.InitialWindow = min(60, c.MaxWindowSeconds)
	}
	if c.MaxRecords == 0 {
		c.MaxRecords = 500
	}
	if c.InitialRecords == 0 {
		c.InitialRecords = min(50, c.MaxRecords)
	}
	if c.TargetRecords == 0 {
		c.TargetRecords = min(100, c.MaxRecords)
	}
	if c.GrowthFactor == 0 {
		c.GrowthFactor = 2
	}
	if c.MinSplitWindow == 0 {
		c.MinSplitWindow = 5
	}
	if c.MaxCycleFetches == 0 {
		c.MaxCycleFetches = 20
	}
	if c.MaxCycleRequests == 0 {
		c.MaxCycleRequests = 50
	}
	if c.MaxCycleBytes == 0 {
		c.MaxCycleBytes = 16 * 1024 * 1024
	}
	if c.MaxCycleRecords == 0 {
		c.MaxCycleRecords = max(5000, c.MaxRecords)
	}
	if c.MaxCycleSeconds == 0 {
		c.MaxCycleSeconds = 60
	}
	if c.GraylogAPI == "" {
		c.GraylogAPI = "scripting"
	}
	if c.Index == "" {
		c.Index = "logs-*"
	}
	if c.TimestampField == "" {
		if c.Provider == "azure" {
			c.TimestampField = "TimeGenerated"
		} else {
			c.TimestampField = "@timestamp"
		}
	}
	if c.MessageField == "" {
		c.MessageField = "Message"
	}
	if c.AWSService == "" {
		c.AWSService = "es"
	}
	c.deriveQuery()
}

func (c *SourceConfig) validate(name string) error {
	if !supportedProviders[c.Provider] {
		return fmt.Errorf("unsupported provider for source %s", name)
	}
	if c.Provider != "cloudwatch" {
		parsed, err := url.Parse(c.URL)
		if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("source %s requires a safe backend URL", name)
		}
		local := parsed.Hostname() == "localhost" || net.ParseIP(parsed.Hostname()).IsLoopback()
		if parsed.Scheme != "https" && !(parsed.Scheme == "http" && local) {
			return fmt.Errorf("source %s requires HTTPS; HTTP is allowed only on localhost", name)
		}
	}
	if c.Provider == "cloudwatch" && c.Region == "" {
		return fmt.Errorf("source %s requires region", name)
	}
	if c.AWSSigV4 && (c.Provider != "opensearch" || c.Region == "") {
		return fmt.Errorf("source %s requires opensearch and region for aws_sigv4", name)
	}
	if c.AWSSigV4 && c.AWSService != "es" && c.AWSService != "aoss" {
		return fmt.Errorf("source %s requires aws_service es or aoss", name)
	}
	if c.Provider == "gcp" && len(c.ResourceNames) == 0 {
		return fmt.Errorf("source %s requires resource_names", name)
	}
	if c.Provider == "azure" && c.WorkspaceID == "" {
		return fmt.Errorf("source %s requires workspace_id", name)
	}
	if c.GraylogAPI != "scripting" && c.GraylogAPI != "legacy" {
		return fmt.Errorf("graylog_api must be scripting or legacy")
	}
	checks := []struct {
		name             string
		value, low, high int64
	}{
		{"max_bytes", c.MaxBytes, 1024, 16 << 20}, {"timeout_s", int64(c.TimeoutSeconds), 1, 120},
		{"max_pages", int64(c.MaxPages), 1, 100}, {"max_window_s", int64(c.MaxWindowSeconds), 1, 86400},
		{"max_scope_s", int64(c.MaxScopeSeconds), 1, 86400}, {"initial_window_s", int64(c.InitialWindow), 1, 86400},
		{"initial_records", int64(c.InitialRecords), 1, 10000}, {"target_records", int64(c.TargetRecords), 1, 50000},
		{"max_records", int64(c.MaxRecords), 1, 10000}, {"growth_factor", int64(c.GrowthFactor), 2, 8},
		{"min_split_window_s", int64(c.MinSplitWindow), 1, 3600}, {"max_cycle_fetches", int64(c.MaxCycleFetches), 1, 200},
		{"max_cycle_requests", int64(c.MaxCycleRequests), 1, 500}, {"max_cycle_bytes", c.MaxCycleBytes, 1024, 256 << 20},
		{"max_cycle_records", int64(c.MaxCycleRecords), 1, 50000}, {"max_cycle_seconds", int64(c.MaxCycleSeconds), 1, 900},
	}
	for _, check := range checks {
		if check.value < check.low || check.value > check.high {
			return fmt.Errorf("%s must be between %d and %d", check.name, check.low, check.high)
		}
	}
	if c.InitialWindow > c.MaxWindowSeconds || c.MaxWindowSeconds > c.MaxScopeSeconds {
		return fmt.Errorf("source %s requires initial_window_s <= max_window_s <= max_scope_s", name)
	}
	if c.InitialRecords > c.MaxRecords || c.TargetRecords > c.MaxCycleRecords {
		return fmt.Errorf("source %s has inconsistent record limits", name)
	}
	if !safeIdentifier.MatchString(c.Index) || !safeIdentifier.MatchString(c.TimestampField) {
		return fmt.Errorf("source %s has an invalid index or timestamp_field", name)
	}
	for _, pattern := range c.RedactPatterns {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("source %s has an invalid redact pattern", name)
		}
		c.redactors = append(c.redactors, compiled)
	}
	return nil
}

func (c *SourceConfig) validateFetchScope(name string) error {
	if c.Provider == "cloudwatch" {
		if strings.TrimSpace(c.LogGroup) == "" && len(c.LogGroups) == 0 {
			return fmt.Errorf("source %s has no selected log group; run discovery and choose one", name)
		}
		return nil
	}
	if strings.TrimSpace(c.Query) == "" {
		return fmt.Errorf("source %s has no selected monitoring scope; run discovery and choose one", name)
	}
	return nil
}

func quoteScope(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`)
}

func (c *SourceConfig) deriveQuery() {
	if strings.TrimSpace(c.Query) != "" || len(c.ScopeValues) == 0 {
		return
	}
	key := c.ScopeKey
	quoted := make([]string, 0, len(c.ScopeValues))
	for _, value := range c.ScopeValues {
		if strings.TrimSpace(value) != "" {
			quoted = append(quoted, quoteScope(value))
		}
	}
	if len(quoted) == 0 {
		return
	}
	joined := func(format, separator string) string {
		parts := make([]string, 0, len(quoted))
		for _, value := range quoted {
			parts = append(parts, fmt.Sprintf(format, key, value))
		}
		return strings.Join(parts, separator)
	}
	switch c.Provider {
	case "loki":
		if len(quoted) == 1 {
			c.Query = fmt.Sprintf(`{%s="%s"}`, key, quoted[0])
		} else {
			values := make([]string, 0, len(c.ScopeValues))
			for _, value := range c.ScopeValues {
				values = append(values, regexp.QuoteMeta(value))
			}
			c.Query = fmt.Sprintf(`{%s=~"^(%s)$"}`, key, quoteScope(strings.Join(values, "|")))
		}
	case "splunk":
		c.Query = `search (` + joined(`%s="%s"`, " OR ") + `)`
	case "elasticsearch", "opensearch", "graylog":
		c.Query = `(` + joined(`%s:"%s"`, " OR ") + `)`
	case "datadog":
		c.Query = `(` + joined(`%s:"%s"`, " OR ") + `)`
	case "cloudwatch":
		c.LogGroups = append([]string(nil), c.ScopeValues...)
		c.LogGroup = c.LogGroups[0]
	case "gcp":
		if key == "resource_name" {
			c.ResourceNames = append([]string(nil), c.ScopeValues...)
			c.Query = `logName:*`
		} else {
			c.Query = `(` + joined(`%s="%s"`, " OR ") + `)`
		}
	case "azure":
		if key == "workspace" {
			c.Query = `union *`
		} else {
			values := make([]string, 0, len(quoted))
			for _, value := range quoted {
				values = append(values, fmt.Sprintf(`"%s"`, value))
			}
			c.Query = fmt.Sprintf(`union * | where tostring(%s) in (%s)`, key, strings.Join(values, ","))
		}
	}
	if c.CustomMessage != "" {
		message := quoteScope(c.CustomMessage)
		switch c.Provider {
		case "loki":
			c.Query += fmt.Sprintf(` |= "%s"`, message)
		case "splunk", "datadog":
			c.Query += fmt.Sprintf(` "%s"`, message)
		case "elasticsearch", "opensearch", "graylog":
			c.Query += fmt.Sprintf(` AND message:"%s"`, message)
		case "cloudwatch":
			c.Query = fmt.Sprintf(`"%s"`, message)
		case "gcp":
			c.Query += fmt.Sprintf(` AND textPayload:"%s"`, message)
		case "azure":
			c.Query += fmt.Sprintf(` | where tostring(%s) contains "%s"`, c.MessageField, message)
		}
	}
	if c.CustomLevel == "error" {
		switch c.Provider {
		case "loki":
			c.Query += ` |~ "(?i)(error|fatal|panic|exception|failed)"`
		case "splunk":
			c.Query += ` (level=error OR level=fatal OR level=critical)`
		case "elasticsearch", "opensearch", "graylog":
			c.Query += ` AND (level:error OR level:fatal OR level:critical)`
		case "datadog":
			c.Query += ` status:error`
		case "cloudwatch":
			c.Query += ` ?ERROR ?Error ?error ?FATAL ?Exception`
		case "gcp":
			c.Query += ` AND severity>=ERROR`
		case "azure":
			c.Query += ` | where SeverityLevel >= 3`
		}
	}
}

func (c *SourceConfig) redactString(value string) string {
	for _, redactor := range c.redactors {
		value = redactor.ReplaceAllString(value, "[REDACTED]")
	}
	return value
}

func (c *SourceConfig) redactMap(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = c.redactValue(item)
	}
	return result
}

func (c *SourceConfig) redactValue(value any) any {
	switch item := value.(type) {
	case string:
		return c.redactString(item)
	case map[string]any:
		return c.redactMap(item)
	case []any:
		result := make([]any, len(item))
		for index, child := range item {
			result[index] = c.redactValue(child)
		}
		return result
	default:
		return value
	}
}

func secret(envName string, required bool) (string, error) {
	if envName == "" {
		if required {
			return "", fmt.Errorf("missing credential environment variable name")
		}
		return "", nil
	}
	value := os.Getenv(envName)
	if value == "" {
		return "", fmt.Errorf("missing credential environment variable for configured credential")
	}
	return value, nil
}
