# Linting Policy

This repo uses two lint layers:

- Go lint via `golangci-lint`
- OpenAPI lint via `vacuum` plus project-specific APIGen rules

Run them together with:

```bash
task lint
```

Run the full CI-style gate with:

```bash
task check
```

## Go Lint

Configured in [.golangci.yml](/Users/yacobolo/.codex/worktrees/d9a7/main/.golangci.yml).

The active Go lint policy is grouped roughly like this:

- Core correctness:
  - `errcheck`
  - `govet`
  - `staticcheck`
  - `unused`
- Error handling:
  - `errname`
  - `errorlint`
  - `nilerr`
- Database and logging:
  - `sqlclosecheck`
  - `rowserrcheck`
  - `sloglint`
  - `decorder`
- Quality and security:
  - `revive`
  - `gosec`
  - `gocritic`
  - `misspell`
  - `nolintlint`
  - `testifylint`
  - `noctx`
- Architecture boundaries:
  - `depguard`

`depguard` is not just dependency hygiene here; it enforces layer direction across `internal/domain`, `internal/service`, `internal/api`, `internal/db`, `internal/engine`, and `internal/middleware`.

## OpenAPI Lint

Implemented in:

- [cmd/lint-api/main.go](/Users/yacobolo/.codex/worktrees/d9a7/main/cmd/lint-api/main.go)
- [pkg/apilint/linter.go](/Users/yacobolo/.codex/worktrees/d9a7/main/pkg/apilint/linter.go)
- [pkg/apilint/ruleset.yaml](/Users/yacobolo/.codex/worktrees/d9a7/main/pkg/apilint/ruleset.yaml)

The engine is `vacuum`, with:

- Vacuum OAS recommended rules
- Vacuum OWASP API rules
- project-specific custom rules implemented in Go

List the active rules with:

```bash
go run ./cmd/lint-api -list-rules
```

## Rule Groups

We treat OpenAPI rules as three policy groups.

### 1. Correctness

These are the high-signal rules that should stay strict:

- valid OpenAPI document/schema
- unique and present `operationId`
- valid paths and path params
- unambiguous routing
- valid security scheme references
- valid pagination schema structure
- valid `x-authz` metadata shape

Examples:

- `oas3-schema`
- `operation-operationId`
- `operation-operationId-unique`
- `path-params`
- `no-ambiguous-paths`
- `check-authz-metadata-present`
- `check-authz-metadata-shape`

### 2. Repo Conventions

These capture the API design rules we intentionally enforce in this repo:

- snake_case wire names
- no colon-style or verb-suffix action paths
- standard pagination parameters
- standard error-schema references
- expected REST response codes such as `DELETE 204`

Examples:

- `check-snake-case-wire-names`
- `check-no-colon-action-paths`
- `check-no-verb-suffix-paths`
- `check-pagination-params`
- `check-error-schema-ref`
- `check-delete-returns-204`
- `check-post-create-status`

### 3. Security and Hardening

These come mostly from the OWASP ruleset. We keep the valuable ones enabled and explicitly relax the ones that do not fit the current contract shape.

Examples still enforced:

- `owasp-security-hosts-https-oas3`
- `owasp-auth-insecure-schemes`
- `owasp-no-api-keys-in-url`
- `owasp-no-http-basic`
- `owasp-rate-limit-retry-after`

Examples intentionally relaxed or disabled today:

- `owasp-string-limit`
- `owasp-string-restricted`
- `owasp-array-limit`
- `owasp-integer-limit`
- `owasp-no-additionalProperties`
- `owasp-no-numeric-ids`

Those are not “bad rules”; they are currently too blunt for this contract and would create high churn with low review value. If we re-enable them later, it should be as a deliberate schema-hardening project.

## Intentional Deviations

The ruleset is curated, not blindly inherited.

Notable deliberate choices:

- `camel-case-properties` is off because this API uses `snake_case`.
- `owasp-protection-global-unsafe-strict` is off because public/manual auth endpoints are modeled intentionally and verified through `x-authz` plus architecture tests.
- map-like schemas intentionally use `additionalProperties`, so the strict OWASP additional-properties rules are off.

## Recommendation

The current policy should stay:

- Vacuum as the OpenAPI lint engine
- OAS + OWASP premade rules as the baseline
- custom repo-specific rules for API design conventions
- explicit rule overrides for intentional contract decisions

That gives us a strong default without letting generic lint rules override deliberate API design choices.
