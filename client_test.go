package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestDoEncodesRequestAndDefaultsReplayable(t *testing.T) {
	t.Parallel()

	gotAuth := ""
	var got Request

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != requestsPath {
			t.Fatalf("path = %q, want %q", r.URL.Path, requestsPath)
		}

		err := json.NewDecoder(r.Body).Decode(&got)
		if err != nil {
			t.Fatalf("decode request: %v", err)
		}

		_ = json.NewEncoder(w).Encode(Response{
			RequestID: "req_1",
			Status:    http.StatusAccepted,
			Body:      ResponseBody{Mode: "inline_base64"},
		})
	}))
	defer server.Close()

	resp, err := NewClient(server.URL, "key_1").Do(context.Background(), Request{
		Method: http.MethodGet,
		URL:    "https://example.com/path",
		Headers: []Header{{
			Name:        "X-Test",
			ValueBase64: "dmFsdWU=",
		}},
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}

	if gotAuth != "Bearer key_1" {
		t.Fatalf("Authorization = %q, want Bearer key_1", gotAuth)
	}
	if !got.Replayable {
		t.Fatal("GET replayable default was not applied before submission")
	}
	if resp.Status != http.StatusAccepted {
		t.Fatalf("response status = %d, want upstream status %d", resp.Status, http.StatusAccepted)
	}
}

func TestReplayableDefaultsOnlySafeMethods(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		req := Request{Method: method}
		req.applyReplayableDefault()
		if !req.Replayable {
			t.Fatalf("%s replayable = false, want true", method)
		}
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		req := Request{Method: method}
		req.applyReplayableDefault()
		if req.Replayable {
			t.Fatalf("%s replayable = true, want false", method)
		}
	}
}

func TestDoParsesCanonicalErrorResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Category:     "client",
			Code:         "rate_limit_exceeded",
			Message:      "Rate limit exceeded",
			Retryable:    true,
			RequestID:    "req_2",
			RetryAfterMs: 1500,
			Details:      map[string]string{"reason": "too_many"},
		})
	}))
	defer server.Close()

	_, err := NewClient(server.URL, "").Do(context.Background(), Request{
		Method: http.MethodPost,
		URL:    "https://example.com",
	})

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %T, want *APIError", err)
	}
	if apiErr.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("HTTPStatus = %d, want %d", apiErr.HTTPStatus, http.StatusTooManyRequests)
	}
	if apiErr.Response.Category != "client" || apiErr.Response.Code != "rate_limit_exceeded" {
		t.Fatalf("canonical error = %s/%s, want client/rate_limit_exceeded", apiErr.Response.Category, apiErr.Response.Code)
	}
	if !apiErr.Response.Retryable || apiErr.Response.RetryAfterMs != 1500 || apiErr.Response.Details["reason"] != "too_many" {
		t.Fatalf("decoded error response = %+v", apiErr.Response)
	}
}

func TestDoTreatsOriginStatusAsSuccessEnvelope(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(Response{
			RequestID: "req_origin",
			Status:    http.StatusNotFound,
			Body:      ResponseBody{Mode: "inline_base64"},
		})
	}))
	defer server.Close()

	resp, err := NewClient(server.URL, "").Do(context.Background(), Request{
		Method: http.MethodPost,
		URL:    "https://example.com/missing",
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if resp.Status != http.StatusNotFound {
		t.Fatalf("upstream status = %d, want %d", resp.Status, http.StatusNotFound)
	}
}

func TestDoUsesContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewClient("http://127.0.0.1", "").Do(ctx, Request{
		Method: http.MethodGet,
		URL:    "https://example.com",
	})
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("error = %v, want context canceled", err)
	}
}

func TestRequestExposesNoP2OnlyBodyRefSurface(t *testing.T) {
	t.Parallel()

	reqType := reflect.TypeFor[Request]()
	bodyType := reflect.TypeFor[RequestBody]()

	for _, name := range []string{"BodyRef", "BodyRefID", "BodyRefURL", "Stream", "MITM"} {
		if _, ok := reqType.FieldByName(name); ok {
			t.Fatalf("Request exposes P2-only field %s", name)
		}
		if _, ok := bodyType.FieldByName(name); ok {
			t.Fatalf("RequestBody exposes P2-only field %s", name)
		}
	}
}
