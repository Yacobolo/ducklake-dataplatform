# Architecture Governance

This project enforces hexagonal architecture boundaries with automated governance checks.

## Why this exists

- Keep dependency direction stable as the codebase grows.
- Make AI-generated code safer by failing fast on layer violations.
- Keep governance objective and CI-enforced, not reviewer-memory-enforced.

## Enforcement model

- `golangci-lint` (`depguard`) provides fast static import checks.
- `go test` in `internal/architecture` provides clear governance-focused failures and supports future custom checks.
- Governance failures include the `governance:` tag.

## Import boundary rules (phase 1)

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

## Temporary relaxations

If current violations are too large to fix in one PR, keep the governance framework and add small, explicit temporary relaxations that are easy to remove.

Current temporary relaxations:

- `internal/service/catalog` importing `internal/engine`
- `internal/service/ingestion` importing `internal/service/query`

Relaxations should stay tagged as `governance` and be removed in follow-up refactors.

## Common refactor direction

- If a service imports infrastructure internals, extract a domain port in `internal/domain` and inject adapter implementations from composition roots.
- If one service imports another service package, move shared contract types into `internal/domain` and depend on those contracts instead.
- Keep handlers translating API DTOs to domain contracts at the boundary.

## Commands

- `task lint:go`
- `task test:arch`
- `task check`

## Next phase

After import governance is stable, add contract governance:

- service signatures and handler boundaries based on domain contracts
- prevention of concrete `internal/service/<feature>` type leakage across layers
