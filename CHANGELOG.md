# Changelog

## Unreleased (proposed v0.3.0)

- Preserve the existing `Request.Replayable bool` API while adding an explicit-false `ReplayableOverride` path.
- Apply GET, HEAD, and OPTIONS replayability defaults during JSON serialization without mutating caller-owned requests.
- Add serialization, defaulting, explicit-false, concurrency, and API-error coverage for the restored REST contract.
- Document straw-oss integration and keep the worker protocol and generated protobuf bindings unchanged.

## v0.2.0

- Restore typed per-request routing hints for tags, country, region, IP type, and sticky sessions.

## v0.1.0 - pre-1.0 release

- Extract the Control REST client and common Egress worker machinery from `straw-oss` with preserved history.
- Consume exact `straw-protos-go v0.3.0` bindings.
