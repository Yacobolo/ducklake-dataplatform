# Getting Started

This guide gets you from zero to your first successful query in a few minutes.

## 1) Start the server

```bash
go run ./cmd/server
```

If startup succeeds, the server listens on `:8080` by default.

## 2) Check service health

```bash
curl http://localhost:8080/healthz
```

Expected response:

```json
{"status":"ok"}
```

## 3) Choose an auth method

The API supports either:

- JWT bearer token in `Authorization: Bearer <token>`
- API key in `X-API-Key: <key>`

## 4) Run your first query

```bash
curl -X POST "http://localhost:8080/v1/query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"sql":"SELECT 1 AS ok"}'
```

## 5) Explore capabilities

- Feature catalog: [/reference/generated/api/features](/reference/generated/api/features)
- Full API reference: [/reference/generated/api/index](/reference/generated/api/index)
- Declarative schema reference: [/reference/generated/declarative/index](/reference/generated/declarative/index)

## Next

Continue with [Core Concepts](/core-concepts) to understand catalogs, access control,
and governance controls.
