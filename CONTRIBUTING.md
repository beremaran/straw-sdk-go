# Contributing to Straw

Thanks for helping make Straw easier to run and understand.

## Before you start

- Search existing issues and pull requests.
- Open an issue before a large behavior change, new dependency, or protocol change.
- Keep one pull request focused on one outcome.
- Never include credentials, production traffic, or customer data.

By participating, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md). Report security issues through
[SECURITY.md](SECURITY.md), not a public issue.

## Development setup

Required for all changes:

- Go 1.24 or later
- Docker with Compose v2
- `make`
- `golangci-lint`

Python changes also require Python 3.13 and uv. Documentation-site changes require Node.js 20 or later.

```sh
git clone https://github.com/beremaran/straw-oss.git
cd straw
make dev
make check
```

The default stack uses ports 4222, 8222, 8080, and 9090. See `deploy/local/README.md` for overrides.

## Making a change

1. Branch from the current default branch.
2. Add the smallest test that demonstrates the behavior.
3. Keep changes within existing package boundaries when possible.
4. Update `docs/public` and `CHANGELOG.md` when public behavior changes.
5. Run the checks below before opening a pull request.

```sh
make check
make production-deploy-check
make docs-website
```

For Python:

```sh
uv sync --all-packages --frozen
uv run --all-packages --frozen python -m unittest discover python/tests
```

Use the root uv workspace. Do not create a lock file, version pin, or virtual environment under `python/`.

## Pull requests

Explain the problem, the chosen boundary, verification performed, and any compatibility or operational effect. Add
screenshots only when they clarify a documentation/UI change. Maintainers may ask to split unrelated work.

Commits should be reviewable and use imperative summaries such as `Simplify worker startup`. Never bypass hooks with
`--no-verify`.

## Design principles

- local development is the shortest supported path;
- one deployment is one trust boundary;
- NATS is the only required backing service;
- production files are adaptable patterns, not a claimed turnkey platform;
- prefer the standard library and existing dependencies;
- keep public behavior documented from installation through operation.
