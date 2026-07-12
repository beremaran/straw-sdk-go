# Straw Go SDK

This module provides the public Control REST client and common Egress worker machinery.

```go
import (
    straw "github.com/beremaran/straw-sdk-go"
    "github.com/beremaran/straw-sdk-go/egress"
)
```

It depends on the exact private `straw-protos-go` tag recorded in `go.mod` and never imports private runtime packages.
The canonical HTTP Egress product implementation remains in `straw-oss`.

```sh
make check
```
