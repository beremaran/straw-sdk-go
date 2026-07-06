package sdk

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const (
	requestsStreamPath        = "/api/v1/requests:stream"
	requestsStreamContentType = "application/vnd.straw.request-stream.v1+binary"
)

var errUnknownStreamFrameType = errors.New("unknown stream frame type")

// Stream reads frames from POST /api/v1/requests:stream.
type Stream struct {
	body io.ReadCloser
}

// DoStream submits a request to the streaming endpoint and returns a frame reader.
func (c *Client) DoStream(ctx context.Context, req Request) (*Stream, error) {
	req.applyReplayableDefault()

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+requestsStreamPath, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", requestsStreamContentType)

	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	if httpResp.StatusCode != http.StatusOK {
		defer func() {
			_ = httpResp.Body.Close()
		}()

		raw, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}

		var errResp ErrorResponse

		err = json.Unmarshal(raw, &errResp)
		if err != nil {
			return nil, fmt.Errorf("decode error response: %w", err)
		}

		return nil, &APIError{HTTPStatus: httpResp.StatusCode, Response: errResp}
	}

	return &Stream{body: httpResp.Body}, nil
}

// Close closes the streaming response body.
func (s *Stream) Close() error {
	err := s.body.Close()
	if err != nil {
		return fmt.Errorf("close stream response: %w", err)
	}

	return nil
}

// Next returns the next decoded streaming frame. It returns io.EOF after the end frame.
func (s *Stream) Next() (*StreamFrame, error) {
	var header [5]byte

	_, err := io.ReadFull(s.body, header[:])
	if err != nil {
		return nil, fmt.Errorf("read stream frame header: %w", err)
	}

	payload := make([]byte, binary.BigEndian.Uint32(header[1:]))

	_, err = io.ReadFull(s.body, payload)
	if err != nil {
		return nil, fmt.Errorf("read stream frame payload: %w", err)
	}

	frame := &StreamFrame{Type: StreamFrameType(header[0])}

	err = frame.decodePayload(payload)
	if err != nil {
		return nil, err
	}

	return frame, nil
}

func (f *StreamFrame) decodePayload(payload []byte) error {
	switch f.Type {
	case StreamFrameMetadata:
		return f.decodeMetadata(payload)
	case StreamFrameBody:
		f.Body = payload

		return nil
	case StreamFrameTrailers:
		return f.decodeTrailers(payload)
	case StreamFrameEnd:
		return f.decodeEnd(payload)
	case StreamFrameError:
		return f.decodeError(payload)
	default:
		return fmt.Errorf("%w %d", errUnknownStreamFrameType, f.Type)
	}
}

func (f *StreamFrame) decodeMetadata(payload []byte) error {
	var metadata StreamMetadata

	err := json.Unmarshal(payload, &metadata)
	if err != nil {
		return fmt.Errorf("decode metadata frame: %w", err)
	}

	f.Metadata = &metadata
	f.RequestID = metadata.RequestID

	return nil
}

func (f *StreamFrame) decodeTrailers(payload []byte) error {
	var trailers StreamTrailers

	err := json.Unmarshal(payload, &trailers)
	if err != nil {
		return fmt.Errorf("decode trailers frame: %w", err)
	}

	f.Trailers = &trailers

	return nil
}

func (f *StreamFrame) decodeEnd(payload []byte) error {
	var end StreamEnd

	err := json.Unmarshal(payload, &end)
	if err != nil {
		return fmt.Errorf("decode end frame: %w", err)
	}

	f.End = &end

	return nil
}

func (f *StreamFrame) decodeError(payload []byte) error {
	var errResp ErrorResponse

	err := json.Unmarshal(payload, &errResp)
	if err != nil {
		return fmt.Errorf("decode error frame: %w", err)
	}

	f.Error = &errResp

	return nil
}
