package sdk

import "net/http"

// Request is the JSON envelope for POST /api/v1/requests.
type Request struct {
	Method             string       `json:"method"`
	URL                string       `json:"url"`
	Headers            []Header     `json:"headers,omitempty"`
	Body               *RequestBody `json:"body,omitempty"`
	FingerprintProfile string       `json:"fingerprint_profile,omitempty"`
	TimeoutMs          uint64       `json:"timeout_ms,omitempty"`
	Replayable         bool         `json:"replayable"`
	ResponseBodyMode   string       `json:"response_body_mode,omitempty"`
}

// Header carries one ordered HTTP header value as base64-encoded bytes.
type Header struct {
	Name        string `json:"name"`
	ValueBase64 string `json:"value_base64"`
}

// RequestBody carries an inline base64-encoded request body.
type RequestBody struct {
	Mode       string `json:"mode"`
	DataBase64 string `json:"data_base64,omitempty"`
	ReceiptID  string `json:"receipt_id,omitempty"`
}

// Response is the Straw success envelope. Status is the upstream HTTP status;
// the outer API HTTP status only says whether Straw transported the request.
type Response struct {
	RequestID string       `json:"request_id"`
	Status    int          `json:"status"`
	Headers   []Header     `json:"headers,omitempty"`
	Body      ResponseBody `json:"body"`
	Timing    Timing       `json:"timing"`
}

// ResponseBody carries the upstream response body as base64-encoded bytes.
type ResponseBody struct {
	Mode       string `json:"mode"`
	DataBase64 string `json:"data_base64,omitempty"`
	Truncated  bool   `json:"truncated"`
	ReceiptID  string `json:"receipt_id,omitempty"`
	SizeBytes  uint64 `json:"size_bytes,omitempty"`
	SHA256Hex  string `json:"sha256_hex,omitempty"`
}

// Receipt is a durable request or response body and its lifecycle state.
type Receipt struct {
	ReceiptID          string `json:"receipt_id"`
	Direction          string `json:"direction"`
	State              string `json:"state"`
	SizeBytes          int64  `json:"size_bytes"`
	SHA256Hex          string `json:"sha256_hex"`
	StatusURL          string `json:"status_url"`
	PartUploadTemplate string `json:"part_upload_template,omitempty"`
	CompleteURL        string `json:"complete_url,omitempty"`
	DownloadURL        string `json:"download_url,omitempty"`
}

// CreateReceiptInput declares the final object before upload begins.
type CreateReceiptInput struct {
	Direction      string `json:"direction"`
	SizeBytes      int64  `json:"size_bytes"`
	SHA256Hex      string `json:"sha256_hex"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// Timing reports request phase durations in milliseconds.
type Timing struct {
	RoutingMs    int64 `json:"routing_ms"`
	AssignmentMs int64 `json:"assignment_ms"`
	EgressMs     int64 `json:"egress_ms"`
	TotalMs      int64 `json:"total_ms"`
}

// ErrorResponse is Straw's public error envelope.
type ErrorResponse struct {
	Category     string            `json:"category"`
	Code         string            `json:"code"`
	Message      string            `json:"message"`
	Retryable    bool              `json:"retryable"`
	RequestID    string            `json:"request_id"`
	TimeoutType  string            `json:"timeout_type,omitempty"`
	RetryAfterMs int64             `json:"retry_after_ms,omitempty"`
	Details      map[string]string `json:"details,omitempty"`
}

// APIError wraps a non-200 Straw API response and its parsed error envelope.
type APIError struct {
	HTTPStatus int
	Response   ErrorResponse
}

func (e *APIError) Error() string {
	if e.Response.Message == "" {
		return e.Response.Code
	}

	return e.Response.Message
}

func (r *Request) applyReplayableDefault() {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		r.Replayable = true
	}
}
