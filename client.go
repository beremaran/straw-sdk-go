// Package sdk provides a minimal Go client for Straw's public API.
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const requestsPath = "/api/v1/requests"

// Client submits requests to Straw's public API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient returns a minimal Straw API client.
func NewClient(baseURL, apiKey string, opts ...Option) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		httpClient: http.DefaultClient,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}

	return c
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client used for API requests.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// Do submits a P0 REST request and returns the Straw success envelope.
func (c *Client) Do(ctx context.Context, req Request) (*Response, error) {
	req.applyReplayableDefault()

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+requestsPath, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	defer func() {
		_ = httpResp.Body.Close()
	}()

	raw, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if httpResp.StatusCode != http.StatusOK {
		var errResp ErrorResponse

		err = json.Unmarshal(raw, &errResp)
		if err != nil {
			return nil, fmt.Errorf("decode error response: %w", err)
		}

		return nil, &APIError{HTTPStatus: httpResp.StatusCode, Response: errResp}
	}

	var resp Response

	err = json.Unmarshal(raw, &resp)
	if err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &resp, nil
}
