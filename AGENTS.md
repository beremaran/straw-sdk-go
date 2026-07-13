# Repository Guidelines

## Project Structure & Module Organization

This repository is a standalone Go module (`github.com/beremaran/straw-sdk-go`). The root package contains the public Control REST client and shared request, response, receipt, and retry types (`client.go`, `types.go`). The `egress/` package contains public worker protocol machinery, including registration, sessions, heartbeats, assignment admission, streams, flow control, cancellation, and body-reference resolution. Package tests are colocated with implementation files. `examples/client/` contains runnable usage examples; Markdown files document the API, compatibility, contribution, security, and release context.

Keep the SDK independent of `straw-oss/internal/...`. Protocol changes must first be released as an exact `straw-protos-go` tag and then pinned in `go.mod`/`go.sum`.

## Build, Test, and Development Commands

Run commands from the repository root:

```sh
make check       # gofmt check, go vet, and all tests
go test ./...    # run the complete test suite
go vet ./...     # run static analysis
gofmt -w <files> # format changed Go files
```

Use the Go version declared by `go.mod` (currently Go 1.25 or newer). CI runs `make check` on pushes to `main` and pull requests.

## Coding Style & Naming Conventions

Use standard, uncompromised `gofmt` output and idiomatic Go naming: exported identifiers use PascalCase, unexported identifiers use camelCase, and initialisms remain capitalized (`HTTP`, `ID`, `URL`). Add or update documentation comments for exported API. Prefer small focused functions, explicit error handling, and context-aware operations. Preserve the public package boundary and avoid private runtime dependencies.

## Testing Guidelines

Write focused `*_test.go` tests next to the code they cover, naming them `Test<Type><Behavior>` (for example, `TestClientDo`). Test public behavior, error mapping, protocol sequencing, and wire compatibility. Existing tests use `httptest`, package-local fakes, and conformance fixtures; follow those patterns rather than requiring external services. No numeric coverage threshold is configured, but every public behavior change should include regression coverage.

## Commit & Pull Request Guidelines

Use a short, imperative commit subject, such as `Restore per-request routing hints` or `Update straw-protos-go to v0.3.1`; include an issue or PR reference when relevant. Pull requests should explain the change and its compatibility impact, identify tests run (normally `make check`), and update public documentation or `CHANGELOG.md` when behavior changes. Keep unrelated refactors out of focused changes. Report vulnerabilities through `SECURITY.md`, not a public issue.
