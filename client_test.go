package sdk

import (
	"bytes"
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
		_, _ = w.Write([]byte(`{"category":"client","code":"auth_failure","message":"Authentication failed","retryable":false}`))
	}))
	defer server.Close()

	_, err := NewClient(server.URL, "wrong").Do(context.Background(), Request{Method: http.MethodGet, URL: "https://example.com"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusUnauthorized || apiErr.Response.Code != "auth_failure" {
		t.Fatalf("error = %#v", err)
	}
}
