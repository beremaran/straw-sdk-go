// Package sdk provides Straw's minimal Go SDK.
//
// Supported endpoint:
//   - POST /api/v1/requests
//   - POST /api/v1/requests:stream
//
// Public types:
//   - Client, Option
//   - Request, Header, RequestBody, RoutingHints
//   - Response, ResponseBody, Timing
//   - Stream, StreamFrame, StreamFrameType, StreamMetadata, StreamTrailers, StreamEnd
//   - ErrorResponse, APIError
//
// Response.Status is the upstream HTTP status from the JSON envelope. The outer
// API HTTP status only reports whether Straw accepted and transported the
// request.
package sdk
