# Contributing to Straw

Thank you for your interest in contributing to the Straw Proxy project.

## Development Workflow

1. **Find a Task**: Look at the issue tracker for available tasks.
2. **Create a Branch**: Use a descriptive name for your branch.
    * Features: `feature/description`
    * Bugfixes: `bugfix/issue-number-description`
    * Chore/Refactor: `chore/description`
3. **Make Changes**: Write clean, idiomatic Go code.
4. **Test**: Ensure all tests pass.

    ```bash
    make test
    make lint
    ```

5. **Submit PR**: Create a Pull Request targeting the `main` branch.

## Code Style

* We use standard Go formatting (`gofmt`).
* We enforce code quality with `golangci-lint`.
* Variable naming should be descriptive and follow CamelCase (for exported) or camelCase (for unexported).
* Comments should explain *why*, not just *what*.

## Commit Messages

Please follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

* `feat: add egress transport option`
* `fix: reject invalid target URLs`
* `docs: update control usage`
* `chore: upgrade dependencies`

## Pull Request Process

* **Description**: Clearly explain the changes and the problem they solve.
* **Context**: Link to relevant tasks or design documents.
* **Verification**: Describe how you verified the changes (manual tests, new unit tests).

## Review Process

* All code changes require at least one approval from a peer.
* CI checks must pass before merging.
