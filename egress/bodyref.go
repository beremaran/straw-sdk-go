package egress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	strawpb "github.com/beremaran/straw-oss/v2/api/proto/straw/v1"
)

const (
	bodyRefUnavailableFact      = "body_ref_unavailable"
	bodyRefExpiredFact          = "body_ref_expired"
	bodyRefSizeMismatchFact     = "body_ref_size_mismatch"
	bodyRefChecksumMismatchFact = "body_ref_checksum_mismatch"
)

// HTTPBodyRefResolver downloads and verifies S3 BodyRef request bodies.
type HTTPBodyRefResolver struct {
	Client *http.Client
	Now    func() time.Time
}

// DownloadBodyRef implements BodyRefResolver.
func (r HTTPBodyRefResolver) DownloadBodyRef(ctx context.Context, frame *strawpb.BodyRefFrame) ([]byte, *strawpb.ErrorFrame) {
	ref, failure := r.validate(frame)
	if failure != nil {
		return nil, failure
	}

	body, failure := r.fetch(ctx, ref, frame.GetExpectedSizeBytes())
	if failure != nil {
		return nil, failure
	}

	if failure := verifyBodyRef(frame, body); failure != nil {
		return nil, failure
	}

	return body, nil
}

func (r HTTPBodyRefResolver) validate(frame *strawpb.BodyRefFrame) (*strawpb.S3BodyRef, *strawpb.ErrorFrame) {
	ref := frame.GetS3()
	if ref == nil || ref.GetSignedUrl() == "" {
		return nil, bodyRefFailure(bodyRefUnavailableFact)
	}

	if ref.GetExpiresUnixMs() > 0 && !r.now().Before(time.UnixMilli(ref.GetExpiresUnixMs())) {
		return nil, bodyRefFailure(bodyRefExpiredFact)
	}

	return ref, nil
}

func (r HTTPBodyRefResolver) fetch(ctx context.Context, ref *strawpb.S3BodyRef, expected uint64) ([]byte, *strawpb.ErrorFrame) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.GetSignedUrl(), nil)
	if err != nil {
		return nil, bodyRefFailure(bodyRefUnavailableFact)
	}

	resp, err := r.client().Do(req)
	if err != nil {
		return nil, bodyRefFailure(bodyRefUnavailableFact)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, bodyRefFailure(bodyRefUnavailableFact)
	}

	if expected >= math.MaxInt64 {
		return nil, bodyRefFailure(bodyRefSizeMismatchFact)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(expected)+1))
	if err != nil {
		return nil, bodyRefFailure(bodyRefUnavailableFact)
	}

	return body, nil
}

func (r HTTPBodyRefResolver) client() *http.Client {
	if r.Client != nil {
		return r.Client
	}

	return http.DefaultClient
}

func (r HTTPBodyRefResolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

func verifyBodyRef(frame *strawpb.BodyRefFrame, body []byte) *strawpb.ErrorFrame {
	if want := frame.GetExpectedSizeBytes(); (want > 0 || frame.GetSha256Hex() != "") && want != uint64(len(body)) {
		return bodyRefFailure(bodyRefSizeMismatchFact)
	}

	if want := frame.GetSha256Hex(); want != "" {
		sum := sha256.Sum256(body)
		if !strings.EqualFold(hex.EncodeToString(sum[:]), want) {
			return bodyRefFailure(bodyRefChecksumMismatchFact)
		}
	}

	return nil
}

func bodyRefFailure(fact string) *strawpb.ErrorFrame {
	return &strawpb.ErrorFrame{
		Code:    strawpb.ErrorCode_ERROR_CODE_BODY_REF_UNAVAILABLE,
		Details: map[string]string{errorFactDetailKey: fact},
	}
}
