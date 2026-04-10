# JSON IR Contract (`v1`)

`duck-demo/apigen/ir` defines the versioned JSON intermediate representation consumed by APIGen.

## Versioning

- Current supported version: `v1`
- The root document must contain `schema_version: "v1"`
- Breaking IR changes require a new schema version
- Additive fields may be introduced within `v1` only when existing consumers can safely ignore them

## Root Document

Required fields:

- `schema_version`
- `info.title`
- `info.version`
- `endpoints` with at least one entry

Optional fields:

- `info.description`
- `servers`
- `tags`
- `schemas`
- `extensions`

## Endpoints

Each endpoint must define:

- `method`
- `path`
- `operation_id`
- `responses`

Endpoint routes are unique by `lower(method) + " " + path`.
`operation_id` values are unique across the document.

Supported endpoint-level extensions in current consumers:

- `x-authz`
- `x-cli-command`
- `x-apigen-manual`

`x-apigen-manual` marks operations that are intentionally excluded from generated transport and CLI surfaces before IR reaches APIGen.

## Responses

Each response must define:

- `status_code`
- `description`

Optional fields:

- `headers`
- `schema`
- `extensions`

Supported APIGen-owned response extension:

- `x-apigen-response-shape`

Current supported response shape:

- `wrapped_json`
  - requires `body_type`
  - indicates the generated server should treat the response as a JSON wrapper whose body type is named explicitly by `body_type`

Response headers are unique case-insensitively per response.

## Schemas

`schemas` is a named registry used by both emitted OpenAPI and generated Go code.

`SchemaRef.ref` values are normalized against component-style paths and resolved against this registry.

## Contract Roles

- JSON IR is the generator input contract
- canonical OpenAPI is the published API contract artifact
- canonical OpenAPI may carry repo-owned metadata extensions such as `x-authz` and `x-cli-command`
