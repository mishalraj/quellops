package logs

import "time"

type Record struct {
	Timestamp  string         `json:"timestamp"`
	Message    string         `json:"message"`
	Source     string         `json:"source"`
	ID         string         `json:"id"`
	Attributes map[string]any `json:"attributes"`
}

type FetchResult struct {
	Source        string   `json:"source"`
	Provider      string   `json:"provider"`
	Start         string   `json:"start"`
	End           string   `json:"end"`
	Records       []Record `json:"records"`
	Truncated     bool     `json:"truncated"`
	Warnings      []string `json:"warnings"`
	Requests      int      `json:"requests"`
	BytesReceived int64    `json:"bytes_received"`
}

type DiscoveredScope struct {
	Kind  string `json:"kind"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Label string `json:"label"`
}

type DiscoveryResult struct {
	Source   string            `json:"source"`
	Provider string            `json:"provider"`
	Scopes   []DiscoveredScope `json:"scopes"`
	Warnings []string          `json:"warnings"`
	Requests int               `json:"requests"`
}

type Attempt struct {
	Start     string   `json:"start"`
	End       string   `json:"end"`
	Limit     int      `json:"limit"`
	Records   int      `json:"records"`
	Truncated bool     `json:"truncated"`
	Warnings  []string `json:"warnings"`
}

type Usage struct {
	Fetches         int   `json:"fetches"`
	BackendRequests int   `json:"backend_requests"`
	BytesReceived   int64 `json:"bytes_received"`
	Records         int   `json:"records"`
}

type Investigation struct {
	Source           string    `json:"source"`
	Provider         string    `json:"provider"`
	ScopeStart       string    `json:"scope_start"`
	ScopeEnd         string    `json:"scope_end"`
	Records          []Record  `json:"records"`
	Attempts         []Attempt `json:"attempts"`
	Usage            Usage     `json:"usage"`
	Strategy         string    `json:"strategy"`
	RequestedStart   string    `json:"requested_start,omitempty"`
	TargetRecords    int       `json:"target_records,omitempty"`
	TargetReached    bool      `json:"target_reached"`
	CoverageComplete bool      `json:"coverage_complete"`
	BackendPartial   bool      `json:"backend_partial"`
	StopReason       string    `json:"stop_reason"`
	Warnings         []string  `json:"warnings"`
}

func iso(unixSeconds float64) string {
	seconds := int64(unixSeconds)
	nanos := int64((unixSeconds - float64(seconds)) * 1e9)
	return time.Unix(seconds, nanos).UTC().Format(time.RFC3339Nano)
}
