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

const (
	requestsPath = "/api/v1/requests"
	receiptsPath = "/api/v1/receipts"
)

// Client submits requests to Straw's public API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient returns a minimal Straw API client.
func NewClient(baseURL, apiKey string, opts ...Option) *Client {
	client := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		httpClient: http.DefaultClient,
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.httpClient == nil {
		client.httpClient = http.DefaultClient
	}

	return client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client used for API requests.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(client *Client) {
		client.httpClient = httpClient
	}
}

// Do submits a REST request and returns the Straw success envelope.
func (c *Client) Do(ctx context.Context, request Request) (*Response, error) {
	request.applyReplayableDefault()

	raw, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	var response Response

	err = c.doJSON(ctx, http.MethodPost, requestsPath, bytes.NewReader(raw), "application/json", &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// CreateReceipt creates or idempotently returns a durable upload receipt.
func (c *Client) CreateReceipt(ctx context.Context, input CreateReceiptInput) (*Receipt, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal receipt: %w", err)
	}

	var receipt Receipt

	err = c.doJSON(ctx, http.MethodPost, receiptsPath, bytes.NewReader(raw), "application/json", &receipt)
	if err != nil {
		return nil, err
	}

	return &receipt, nil
}

// UploadReceiptPart uploads or replaces one resumable, one-based part.
func (c *Client) UploadReceiptPart(ctx context.Context, receiptID string, part int, body io.Reader, size int64, checksum string) (*Receipt, error) {
	path := fmt.Sprintf("%s/%s/parts/%d", receiptsPath, receiptID, part)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+path, body)
	if err != nil {
		return nil, fmt.Errorf("build receipt part request: %w", err)
	}

	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")

	if checksum != "" {
		req.Header.Set("X-Straw-Part-SHA256", checksum)
	}

	c.authorize(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upload receipt part: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var receipt Receipt

	err = decodeAPIResponse(resp, &receipt)
	if err != nil {
		return nil, err
	}

	return &receipt, nil
}

// CompleteReceipt verifies all uploaded parts and makes the receipt eligible.
func (c *Client) CompleteReceipt(ctx context.Context, receiptID string) (*Receipt, error) {
	var receipt Receipt

	err := c.doJSON(ctx, http.MethodPost, receiptsPath+"/"+receiptID+"/complete", nil, "application/json", &receipt)
	if err != nil {
		return nil, err
	}

	return &receipt, nil
}

// GetReceipt returns the current durable state.
func (c *Client) GetReceipt(ctx context.Context, receiptID string) (*Receipt, error) {
	var receipt Receipt

	err := c.doJSON(ctx, http.MethodGet, receiptsPath+"/"+receiptID, nil, "", &receipt)
	if err != nil {
		return nil, err
	}

	return &receipt, nil
}

// DownloadReceipt opens an authorized stored response body. The caller closes it.
func (c *Client) DownloadReceipt(ctx context.Context, receiptID string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+receiptsPath+"/"+receiptID+"/content", nil)
	if err != nil {
		return nil, fmt.Errorf("build receipt download request: %w", err)
	}

	c.authorize(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download receipt: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()

		return nil, decodeAPIResponse(resp, nil)
	}

	return resp.Body, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, body io.Reader, contentType string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("build API request: %w", err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	c.authorize(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send API request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	return decodeAPIResponse(resp, out)
}

func (c *Client) authorize(req *http.Request) {
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
}

func decodeAPIResponse(resp *http.Response, out any) error {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read API response: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		var apiErr ErrorResponse

		_ = json.Unmarshal(raw, &apiErr)

		return &APIError{HTTPStatus: resp.StatusCode, Response: apiErr}
	}

	if out == nil || len(raw) == 0 {
		return nil
	}

	err = json.Unmarshal(raw, out)
	if err != nil {
		return fmt.Errorf("decode API response: %w", err)
	}

	return nil
}
