package straw

import (
	"encoding/json"
	"net/http"
)

// Request is the JSON envelope for POST /api/v1/requests.
type Request struct {
	Method             string        `json:"method"`
	URL                string        `json:"url"`
	Headers            []Header      `json:"headers,omitempty"`
	Body               *RequestBody  `json:"body,omitempty"`
	Routing            *RoutingHints `json:"routing,omitempty"`
	FingerprintProfile string        `json:"fingerprint_profile,omitempty"`
	TimeoutMs          uint64        `json:"timeout_ms,omitempty"`
	// Replayable is retained as a bool for source compatibility. For GET,
	// HEAD, and OPTIONS, its false zero value means "use the client default"
	// unless ReplayableOverride is provided.
	Replayable bool `json:"replayable"`
	// ReplayableOverride supplies presence-sensitive replayability. Set it to
	// BoolPtr(false) to explicitly disable replayability for a method that is
	// replayable by default.
	ReplayableOverride *bool  `json:"-"`
	ResponseBodyMode   string `json:"response_body_mode,omitempty"`
}

// RoutingHints constrain routing-rule matching and worker selection for one request.
type RoutingHints struct {
	Tags            []string `json:"tags,omitempty"`
	Country         string   `json:"country,omitempty"`
	Region          string   `json:"region,omitempty"`
	IPType          string   `json:"ip_type,omitempty"`
	StickySessionID string   `json:"sticky_session_id,omitempty"`
}

// BoolPtr returns a pointer to value for presence-sensitive optional fields.
func BoolPtr(value bool) *bool {
	return &value
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

func (r Request) effectiveReplayable() bool {
	if r.ReplayableOverride != nil {
		return *r.ReplayableOverride
	}

	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}

	return r.Replayable
}

// MarshalJSON emits the REST request shape and applies the method default
// without mutating the request. Avoiding mutation keeps one Request safe to
// serialize concurrently when its caller does not mutate its fields.
func (r Request) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Method             string        `json:"method"`
		URL                string        `json:"url"`
		Headers            []Header      `json:"headers,omitempty"`
		Body               *RequestBody  `json:"body,omitempty"`
		Routing            *RoutingHints `json:"routing,omitempty"`
		FingerprintProfile string        `json:"fingerprint_profile,omitempty"`
		TimeoutMs          uint64        `json:"timeout_ms,omitempty"`
		Replayable         bool          `json:"replayable"`
		ResponseBodyMode   string        `json:"response_body_mode,omitempty"`
	}{
		Method:             r.Method,
		URL:                r.URL,
		Headers:            r.Headers,
		Body:               r.Body,
		Routing:            r.Routing,
		FingerprintProfile: r.FingerprintProfile,
		TimeoutMs:          r.TimeoutMs,
		Replayable:         r.effectiveReplayable(),
		ResponseBodyMode:   r.ResponseBodyMode,
	})
}
