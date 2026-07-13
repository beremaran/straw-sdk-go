# API boundary

The root package owns request/response types, receipts, retries, and the Control REST client. `egress` owns worker
identity, registration, sessions, heartbeats, assignment admission, streams, credit/backpressure, cancellation,
body-reference resolution, and public executor seams. Runtime configuration, the official HTTP executor, TLS
profiles, logging, metrics, and operations remain in `straw-oss`.

`Request.Routing` accepts optional tags, country, region, IP type, and sticky-session ID hints. Control validates the
values and uses them as hard constraints for routing-rule and worker-capability selection.
