// Package straw provides the Go client for Straw's REST request endpoint.
//
// Request.Routing uses the public straw-oss routing object shape. GET, HEAD,
// and OPTIONS requests default to replayable; use Request.ReplayableOverride
// with BoolPtr(false) when an explicit false is required.
package straw
