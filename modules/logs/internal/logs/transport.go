package logs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Request struct {
	Method   string
	URL      string
	Headers  http.Header
	Body     []byte
	Timeout  time.Duration
	MaxBytes int64
}

type Transport interface {
	Do(context.Context, Request) ([]byte, error)
}

type HTTPTransport struct {
	client *http.Client
}

func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{client: &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return fmt.Errorf("backend redirects are disabled")
		},
	}}
}

func (t *HTTPTransport) Do(parent context.Context, request Request) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, request.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, request.Method, request.URL, bytes.NewReader(request.Body))
	if err != nil {
		return nil, fmt.Errorf("backend request could not be created")
	}
	req.Header = request.Headers.Clone()
	response, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("backend connection failed or timed out")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("backend HTTP %d; check credentials, permissions and configuration", response.StatusCode)
	}
	limited := io.LimitReader(response.Body, request.MaxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("backend response could not be read")
	}
	if int64(len(body)) > request.MaxBytes {
		return nil, fmt.Errorf("backend response exceeded byte budget; narrow the query")
	}
	return body, nil
}
