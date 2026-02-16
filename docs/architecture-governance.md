# Architecture Governance

This project enforces hexagonal architecture boundaries with automated governance checks.

## Why

- Keep dependency direction stable as the codebase grows.
- Make AI-generated changes safer by failing fast on layer violations.
- Keep architecture policy in CI, not only in code review.

## Enforcement model

- `golangci-lint` (`depguard`) provides fast local and CI feedback.
- `go test` in `internal/architecture` provides explicit governance failures and richer checks.
- Governance failures include the `governance:` prefix for easy filtering.

## Import boundary rules

- `internal/domain/**`
  - may import: `internal/domain/**`
  - must not import: `internal/api/**`, `internal/service/**`, `internal/db/**`, `internal/engine/**`, `internal/middleware/**`, `internal/declarative/**`, `cmd/**`, `pkg/cli/**`

- `internal/service/**`
  - may import: `internal/domain/**`, `internal/service/**`
  - must not import: `internal/api/**`, `internal/db/**`, `internal/engine/**`, `internal/middleware/**`, `cmd/**`, `pkg/cli/**`

- `internal/api/**`
  - may import: `internal/service/**`, `internal/domain/**`, `internal/api/**`
  - must not import: `internal/db/**`, `internal/engine/**`, `internal/declarative/**`, `cmd/**`, `pkg/cli/**`

- `internal/db/**`
  - may import: `internal/domain/**`, `internal/db/**`
  - must not import: `internal/api/**`, `internal/service/**`, `internal/engine/**`, `internal/middleware/**`, `cmd/**`, `pkg/cli/**`

- `internal/engine/**`
  - may import: `internal/domain/**`, `internal/engine/**`
  - must not import: `internal/api/**`, `internal/service/**`, `cmd/**`, `pkg/cli/**`

- `internal/middleware/**`
  - may import: `internal/domain/**`, `internal/middleware/**`
  - must not import: `internal/service/**`, `internal/db/**`, `internal/engine/**`

Generated files are excluded from governance checks.

## Test policy

- Production files are strict.
- Cross-layer test composition is allowed only for integration tests.
- Unit tests should stay mostly same-layer and depend on `internal/domain` and `internal/testutil` when possible.

## Typical refactor direction

- If service code imports infrastructure (`internal/engine`, `internal/db`), extract and inject a domain port.
- If one service package imports another service package for shared contracts, move shared contracts to `internal/domain`.
- Keep handlers translating API DTOs to domain contracts at the boundary.

## Commands

- `task lint:go`
- `task test:arch`
- `task check`

## Next phase

After import governance stabilizes:

- enforce service signature and handler boundary contracts
- prevent concrete `internal/service/<feature>` type leakage across layers
