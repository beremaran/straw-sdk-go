package egress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	strawpb "github.com/beremaran/straw-oss/api/proto/straw/v1"
)

func TestHTTPBodyRefResolverDownloadsAndVerifies(t *testing.T) {
	t.Parallel()

	body := []byte("request body")
	sum := sha256.Sum256(body)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	got, failure := (HTTPBodyRefResolver{}).DownloadBodyRef(context.Background(), bodyRefFrame(server.URL, uint64(len(body)), hex.EncodeToString(sum[:]), 0))
	if failure != nil {
		t.Fatalf("DownloadBodyRef() failure = %#v", failure)
	}
	if string(got) != string(body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestHTTPBodyRefResolverMapsValidationFailures(t *testing.T) {
	t.Parallel()

	body := []byte("request body")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	missingServer := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(missingServer.Close)
	now := time.UnixMilli(2000)

	tests := map[string]struct {
		frame *strawpb.BodyRefFrame
		fact  string
	}{
		"unavailable": {
			frame: bodyRefFrame("", uint64(len(body)), "", 0),
			fact:  bodyRefUnavailableFact,
		},
		"unavailable object": {
			frame: bodyRefFrame(missingServer.URL, uint64(len(body)), "", 0),
			fact:  bodyRefUnavailableFact,
		},
		"expired": {
			frame: bodyRefFrame(server.URL, uint64(len(body)), "", 1000),
			fact:  bodyRefExpiredFact,
		},
		"size mismatch": {
			frame: bodyRefFrame(server.URL, uint64(len(body)+1), "", 0),
			fact:  bodyRefSizeMismatchFact,
		},
		"checksum mismatch": {
			frame: bodyRefFrame(server.URL, uint64(len(body)), "bad", 0),
			fact:  bodyRefChecksumMismatchFact,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, failure := (HTTPBodyRefResolver{Now: func() time.Time { return now }}).DownloadBodyRef(context.Background(), tc.frame)
			if failure == nil {
				t.Fatal("DownloadBodyRef() failure = nil")
			}
			if got := failure.GetDetails()[errorFactDetailKey]; got != tc.fact {
				t.Fatalf("fact = %q, want %q", got, tc.fact)
			}
		})
	}
}

func TestRequestBodyStateRejectsOutOfScopeBodyRef(t *testing.T) {
	t.Parallel()

	state := &requestBodyState{
		start:        &strawpb.RequestStart{},
		deploymentID: "ten_sdk",
		requestID:    "req_sdk",
		refs:         staticBodyRefResolver{},
	}
	ok := state.acceptBodyRef(context.Background(), &strawpb.BodyRefFrame{
		Ref: &strawpb.BodyRefFrame_S3{S3: &strawpb.S3BodyRef{
			ObjectKey: "deployment/other/request/req_sdk/request/body",
			SignedUrl: "https://body.example",
		}},
	})
	if ok {
		t.Fatal("acceptBodyRef() = true, want false")
	}
	if got := state.failure.GetDetails()[errorFactDetailKey]; got != "body_ref_scope_mismatch" {
		t.Fatalf("fact = %q, want body_ref_scope_mismatch", got)
	}
}

func bodyRefFrame(url string, size uint64, sha256Hex string, expiresUnixMs int64) *strawpb.BodyRefFrame {
	return &strawpb.BodyRefFrame{
		Ref: &strawpb.BodyRefFrame_S3{S3: &strawpb.S3BodyRef{
			ObjectKey:     "deployment/ten_sdk/request/req_sdk/request/body",
			SignedUrl:     url,
			ExpiresUnixMs: expiresUnixMs,
		}},
		ExpectedSizeBytes: size,
		Sha256Hex:         sha256Hex,
	}
}

type staticBodyRefResolver struct{}

func (staticBodyRefResolver) DownloadBodyRef(context.Context, *strawpb.BodyRefFrame) ([]byte, *strawpb.ErrorFrame) {
	return []byte("unused"), nil
}
