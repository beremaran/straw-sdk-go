package sdk

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

const (
	testRequestID = "req_stream"
	testURL       = "https://example.com"
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
		URL:    testURL,
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
		URL:    testURL + "/missing",
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
		URL:    testURL,
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

func TestDoStreamParsesDocumentedFrames(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != requestsStreamPath {
			t.Fatalf("path = %q, want %q", r.URL.Path, requestsStreamPath)
		}
		if r.Header.Get("Accept") != requestsStreamContentType {
			t.Fatalf("Accept = %q", r.Header.Get("Accept"))
		}

		writeStreamTestFrame(t, w, StreamFrameMetadata, StreamMetadata{RequestID: testRequestID, Status: http.StatusAccepted})
		writeStreamTestFrame(t, w, StreamFrameBody, []byte("hel"))
		writeStreamTestFrame(t, w, StreamFrameBody, []byte("lo"))
		writeStreamTestFrame(t, w, StreamFrameTrailers, StreamTrailers{Headers: []Header{{Name: "X-Trailer", ValueBase64: "ZG9uZQ=="}}})
		writeStreamTestFrame(t, w, StreamFrameEnd, StreamEnd{Timing: Timing{TotalMs: 17}})
	}))
	defer server.Close()

	stream, err := NewClient(server.URL, "").DoStream(context.Background(), Request{Method: http.MethodGet, URL: testURL})
	if err != nil {
		t.Fatalf("DoStream() error = %v", err)
	}
	defer func() {
		_ = stream.Close()
	}()

	frame, err := stream.Next()
	if err != nil || frame.Metadata == nil || frame.Metadata.RequestID != testRequestID {
		t.Fatalf("metadata frame = %#v, err = %v", frame, err)
	}
	frame, err = stream.Next()
	if err != nil || string(frame.Body) != "hel" {
		t.Fatalf("body frame 1 = %#v, err = %v", frame, err)
	}
	frame, err = stream.Next()
	if err != nil || string(frame.Body) != "lo" {
		t.Fatalf("body frame 2 = %#v, err = %v", frame, err)
	}
	frame, err = stream.Next()
	if err != nil || frame.Trailers == nil || frame.Trailers.Headers[0].Name != "X-Trailer" {
		t.Fatalf("trailers frame = %#v, err = %v", frame, err)
	}
	frame, err = stream.Next()
	if err != nil || frame.End == nil || frame.End.Timing.TotalMs != 17 {
		t.Fatalf("end frame = %#v, err = %v", frame, err)
	}
}

func TestDoStreamSurfacesPreMetadataAndPostMetadataErrors(t *testing.T) {
	t.Parallel()

	t.Run("pre-metadata HTTP error", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(ErrorResponse{Category: "auth", Code: "auth_failure", Message: "bad key"})
		}))
		defer server.Close()

		_, err := NewClient(server.URL, "").DoStream(context.Background(), Request{Method: http.MethodPost, URL: testURL})
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Response.Code != "auth_failure" {
			t.Fatalf("error = %#v, want auth APIError", err)
		}
	})

	t.Run("post-metadata error frame", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeStreamTestFrame(t, w, StreamFrameMetadata, StreamMetadata{RequestID: testRequestID, Status: http.StatusOK})
			writeStreamTestFrame(t, w, StreamFrameError, ErrorResponse{Category: "egress", Code: "upstream_reset", Message: "reset"})
		}))
		defer server.Close()

		stream, err := NewClient(server.URL, "").DoStream(context.Background(), Request{Method: http.MethodPost, URL: testURL})
		if err != nil {
			t.Fatalf("DoStream() error = %v", err)
		}
		defer func() {
			_ = stream.Close()
		}()

		_, err = stream.Next()
		if err != nil {
			t.Fatalf("metadata Next() error = %v", err)
		}
		frame, err := stream.Next()
		if err != nil || frame.Error == nil || frame.Error.Code != "upstream_reset" {
			t.Fatalf("error frame = %#v, err = %v", frame, err)
		}
	})
}

func TestDoStreamMalformedFrameLength(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte{byte(StreamFrameBody), 0, 0, 0, 5, 'x'})
		if err != nil {
			t.Fatalf("write malformed frame: %v", err)
		}
	}))
	defer server.Close()

	stream, err := NewClient(server.URL, "").DoStream(context.Background(), Request{Method: http.MethodGet, URL: testURL})
	if err != nil {
		t.Fatalf("DoStream() error = %v", err)
	}
	defer func() {
		_ = stream.Close()
	}()

	_, err = stream.Next()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Next() error = %v, want unexpected EOF", err)
	}
}

func TestDoStreamUsesContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewClient("http://127.0.0.1", "").DoStream(ctx, Request{Method: http.MethodGet, URL: testURL})
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("error = %v, want context canceled", err)
	}
}

func writeStreamTestFrame(t *testing.T, w io.Writer, typ StreamFrameType, payload any) {
	t.Helper()

	var raw []byte
	switch v := payload.(type) {
	case []byte:
		raw = v
	default:
		var err error
		raw, err = json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal frame payload: %v", err)
		}
	}

	header := [5]byte{byte(typ)}
	payloadLen, err := strconv.ParseUint(strconv.Itoa(len(raw)), 10, 32)
	if err != nil {
		t.Fatalf("payload too large: %d", len(raw))
	}
	binary.BigEndian.PutUint32(header[1:], uint32(payloadLen))

	_, err = w.Write(header[:])
	if err != nil {
		t.Fatalf("write frame header: %v", err)
	}
	_, err = w.Write(raw)
	if err != nil {
		t.Fatalf("write frame payload: %v", err)
	}
}
