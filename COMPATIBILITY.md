# Compatibility

During private `v0.x` development, only the currently approved SDK, protocol-binding, and runtime tags are tested.
Older private tags are best-effort. Public wire and SDK support guarantees are deferred until coordinated release
readiness.

## REST request compatibility

The restored routing contract is additive to `Request`: `RoutingHints` is public and its JSON members are exactly
`tags`, `country`, `region`, `ip_type`, and `sticky_session_id`. It does not change the worker protocol or generated
protobuf bindings.

`Request.Replayable` remains a `bool` so existing struct literals and field reads keep compiling. A bool alone cannot
represent both “unset” and “explicit false”, so `Request.ReplayableOverride *bool` supplies the new presence-sensitive
path. Use `BoolPtr(false)` to send an explicit false for GET, HEAD, or OPTIONS. With no override, those methods still
serialize true; other methods preserve the bool value. This avoids the more disruptive alternative of changing
`Replayable` itself to `*bool`, which would break existing literals such as `Request{Replayable: false}` and existing
code that reads it as a bool. No unavoidable source break is introduced by this release.
