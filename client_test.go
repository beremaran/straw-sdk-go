package straw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestRequestMarshalsRoutingHints(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(Request{Method: http.MethodGet, URL: "https://example.com", Routing: &RoutingHints{
		Tags: []string{"premium"}, Country: "AU", Region: "ap-southeast-2", IPType: "residential", StickySessionID: "cart-123",
	}})
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	routing, ok := envelope["routing"].(map[string]any)
	if !ok || len(routing) != 5 || routing["country"] != "AU" || routing["region"] != "ap-southeast-2" || routing["ip_type"] != "residential" || routing["sticky_session_id"] != "cart-123" {
		t.Fatalf("routing = %#v", envelope["routing"])
	}
	if tags, ok := routing["tags"].([]any); !ok || len(tags) != 1 || tags[0] != "premium" {
		t.Fatalf("routing.tags = %#v", routing["tags"])
	}
}

func TestRequestReplayableDefaultsAndExplicitFalse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		request  Request
		expected bool
	}{
		{name: "GET default", request: Request{Method: http.MethodGet}, expected: true},
		{name: "HEAD default", request: Request{Method: http.MethodHead}, expected: true},
		{name: "OPTIONS default", request: Request{Method: http.MethodOptions}, expected: true},
		{name: "POST legacy false", request: Request{Method: http.MethodPost}, expected: false},
		{name: "GET explicit false", request: Request{Method: http.MethodGet, ReplayableOverride: BoolPtr(false)}, expected: false},
		{name: "GET explicit true", request: Request{Method: http.MethodGet, ReplayableOverride: BoolPtr(true)}, expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := json.Marshal(tt.request)
			if err != nil {
				t.Fatal(err)
			}

			var envelope map[string]any
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatal(err)
			}
			if got, ok := envelope["replayable"].(bool); !ok || got != tt.expected {
				t.Fatalf("replayable = %#v, want %v", envelope["replayable"], tt.expected)
			}
		})
	}
}

func TestClientDoSerializesExplicitReplayableFalse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var envelope map[string]any
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if got, ok := envelope["replayable"].(bool); !ok || got {
			t.Errorf("replayable = %#v, want false", envelope["replayable"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"req_explicit_false","status":200,"body":{"mode":"inline_base64","truncated":false},"timing":{}}`))
	}))
	defer server.Close()

	_, err := NewClient(server.URL, "").Do(context.Background(), Request{
		Method:             http.MethodGet,
		URL:                "https://example.com",
		ReplayableOverride: BoolPtr(false),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientDoCanSerializeRequestConcurrently(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var envelope map[string]any
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if got, ok := envelope["replayable"].(bool); !ok || !got {
			t.Errorf("replayable = %#v, want true", envelope["replayable"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"req_concurrent","status":200,"body":{"mode":"inline_base64","truncated":false},"timing":{}}`))
	}))
	defer server.Close()

	request := Request{
		Method:  http.MethodGet,
		URL:     "https://example.com",
		Routing: &RoutingHints{Tags: []string{"premium"}, Country: "AU"},
	}
	client := NewClient(server.URL, "")
	const calls = 32
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.Do(context.Background(), request)
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if request.Replayable {
		t.Fatal("Do mutated the caller's request")
	}
}

func TestClientDo(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != requestsPath || r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("request = %s auth=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"req_1","status":200,"body":{"mode":"inline_base64","truncated":false},"timing":{}}`))
	}))
	defer server.Close()

	response, err := NewClient(server.URL, "secret").Do(context.Background(), Request{Method: http.MethodGet, URL: "https://example.com"})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if response.Status != http.StatusOK || response.RequestID != "req_1" {
		t.Fatalf("response = %+v", response)
	}
}

func TestClientReceiptLifecycleMethods(t *testing.T) {
	t.Parallel()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("auth=%q", r.Header.Get("Authorization"))
		}
		if calls == 1 && (r.Method != http.MethodPost || r.URL.Path != receiptsPath) {
			t.Errorf("create request=%s %s", r.Method, r.URL.Path)
		}
		if calls == 2 && (r.Method != http.MethodPut || r.ContentLength != 4) {
			t.Errorf("part request=%s size=%d", r.Method, r.ContentLength)
		}
		_, _ = w.Write([]byte(`{"receipt_id":"rcpt_1","direction":"request","state":"uploading","size_bytes":4,"sha256_hex":"sum"}`))
	}))
	defer server.Close()
	client := NewClient(server.URL, "secret")
	receipt, err := client.CreateReceipt(context.Background(), CreateReceiptInput{Direction: "request", SizeBytes: 4, SHA256Hex: "sum"})
	if err != nil || receipt.ReceiptID != "rcpt_1" {
		t.Fatalf("CreateReceipt=%#v %v", receipt, err)
	}
	_, err = client.UploadReceiptPart(context.Background(), receipt.ReceiptID, 1, bytes.NewReader([]byte("body")), 4, "")
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientDoParsesAPIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"category":"client","code":"auth_failure","message":"Authentication failed","retryable":false,"request_id":"req_error","details":{"field":"routing.country"}}`))
	}))
	defer server.Close()

	_, err := NewClient(server.URL, "wrong").Do(context.Background(), Request{Method: http.MethodGet, URL: "https://example.com"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusUnauthorized || apiErr.Response.Code != "auth_failure" || apiErr.Response.RequestID != "req_error" || apiErr.Response.Details["field"] != "routing.country" || apiErr.Error() != "Authentication failed" {
		t.Fatalf("error = %#v", err)
	}
}
