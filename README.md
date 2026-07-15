# Straw Go SDK

This module provides the public Control REST client and common Egress worker machinery.

```go
import (
    straw "github.com/beremaran/straw-sdk-go"
    "github.com/beremaran/straw-sdk-go/egress"
)
```

It depends on the exact `straw-protos-go` tag recorded in `go.mod` and never imports runtime-internal packages.
The canonical HTTP Egress product implementation remains in `straw-oss`.

Per-request worker constraints and affinity are available through `Request.Routing` and `RoutingHints`. The routing
object is serialized exactly as Straw's `POST /api/v1/requests` contract expects:
`tags`, `country`, `region`, `ip_type`, and `sticky_session_id`.

`GET`, `HEAD`, and `OPTIONS` requests serialize `replayable: true` by default. The legacy `Request.Replayable bool`
field remains source-compatible; use `ReplayableOverride: straw.BoolPtr(false)` when an explicit false is required for
one of those methods.

```sh
make check
```
