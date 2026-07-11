package sdk

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

func TestClientDoParsesAPIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"category":"client","code":"auth_failure","message":"Authentication failed","retryable":false}`))
	}))
	defer server.Close()

	_, err := NewClient(server.URL, "wrong").Do(context.Background(), Request{Method: http.MethodGet, URL: "https://example.com"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusUnauthorized || apiErr.Response.Code != "auth_failure" {
		t.Fatalf("error = %#v", err)
	}
}
