package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type AgentConfig struct {
	ClusterID      string   `json:"cluster_id"`
	ConfigRevision int64    `json:"config_revision"`
	Namespaces     []string `json:"namespaces"`
	PollIntervalS  int      `json:"poll_interval_s"`
	MaxBatchEvents int      `json:"max_batch_events"`
	AgentToken     string   `json:"agent_token,omitempty"`
}

type Registration struct {
	ClusterID         string     `json:"cluster_id"`
	Name              string     `json:"name"`
	AgentVersion      string     `json:"agent_version"`
	KubernetesVersion string     `json:"kubernetes_version"`
	Namespaces        []string   `json:"namespaces"`
	Workloads         []Workload `json:"workloads"`
	QueueDepth        int        `json:"queue_depth"`
	DroppedEvents     uint64     `json:"dropped_events"`
	LastError         string     `json:"last_error,omitempty"`
}

type Workload struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
}

type controlClient struct {
	baseURL string
	token   string
	mu      sync.RWMutex
	http    *http.Client
}

func (c *controlClient) SetToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
}

func (c *controlClient) Token() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.token
}

func newControlClient(baseURL, token string, timeout time.Duration) *controlClient {
	return &controlClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http:    &http.Client{Timeout: timeout},
	}
}

func (c *controlClient) call(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token())
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("QuellOps returned %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
	}
	if output != nil && len(responseBody) > 0 {
		if err := json.Unmarshal(responseBody, output); err != nil {
			return fmt.Errorf("decode QuellOps response: %w", err)
		}
	}
	return nil
}

func (c *controlClient) Register(ctx context.Context, registration Registration) (AgentConfig, error) {
	var response struct {
		Config AgentConfig `json:"config"`
	}
	err := c.call(ctx, http.MethodPost, "/api/kubernetes/v1/register", registration, &response)
	return response.Config, err
}

func (c *controlClient) Heartbeat(ctx context.Context, clusterID string, depth int, dropped uint64, lastError string) (AgentConfig, error) {
	input := map[string]any{
		"cluster_id": clusterID, "queue_depth": depth, "dropped_events": dropped,
	}
	if lastError != "" {
		input["last_error"] = lastError
	}
	var response struct {
		Config AgentConfig `json:"config"`
	}
	err := c.call(ctx, http.MethodPost, "/api/kubernetes/v1/heartbeat", input, &response)
	return response.Config, err
}

func (c *controlClient) SendEvents(ctx context.Context, clusterID string, events []Event) error {
	return c.call(ctx, http.MethodPost, "/api/kubernetes/v1/events", map[string]any{
		"cluster_id": clusterID, "events": events,
	}, nil)
}
