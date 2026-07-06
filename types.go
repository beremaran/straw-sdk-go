package sdk

import "net/http"

// Request is the JSON envelope for POST /api/v1/requests.
type Request struct {
	Method             string        `json:"method"`
	URL                string        `json:"url"`
	Headers            []Header      `json:"headers,omitempty"`
	Body               *RequestBody  `json:"body,omitempty"`
	Routing            *RoutingHints `json:"routing,omitempty"`
	FingerprintProfile string        `json:"fingerprint_profile,omitempty"`
	TimeoutMs          uint64        `json:"timeout_ms,omitempty"`
	Replayable         bool          `json:"replayable"`
	CaptureHint        string        `json:"capture_hint,omitempty"`
}

// Header carries one ordered HTTP header value as base64-encoded bytes.
type Header struct {
	Name        string `json:"name"`
	ValueBase64 string `json:"value_base64"`
}

// RequestBody is the P0 inline request body.
type RequestBody struct {
	Mode       string `json:"mode"`
	DataBase64 string `json:"data_base64,omitempty"`
}

// RoutingHints constrain worker selection.
type RoutingHints struct {
	Tags            []string `json:"tags,omitempty"`
	Country         string   `json:"country,omitempty"`
	Region          string   `json:"region,omitempty"`
	IPType          string   `json:"ip_type,omitempty"`
	StickySessionID string   `json:"sticky_session_id,omitempty"`
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
