# API boundary

The root package owns request/response types, receipts, retries, and the Control REST client. `egress` owns worker
identity, registration, sessions, heartbeats, assignment admission, streams, credit/backpressure, cancellation,
body-reference resolution, and public executor seams. Runtime configuration, the official HTTP executor, TLS
profiles, logging, metrics, and operations remain in `straw-oss`.

`Request.Routing` accepts optional `Tags`, `Country`, `Region`, `IPType`, and `StickySessionID` hints. These serialize
to the exact `routing.tags`, `routing.country`, `routing.region`, `routing.ip_type`, and
`routing.sticky_session_id` members used by straw-oss. Control validates the values and uses them as hard constraints
for routing-rule and worker-capability selection.

The REST client defaults `replayable` to true for GET, HEAD, and OPTIONS. `Request.Replayable` remains a bool for
source compatibility; set `Request.ReplayableOverride` to `BoolPtr(false)` to explicitly send false for a default-safe
method. Do not mutate a request or its nested slices while it is being submitted concurrently.
