---
title: Quickstart
description: Reach your first governed query and understand the secure request path.
---

# Quickstart

Use this path when you want to confirm how Duck feels to a data consumer: authenticate, run a query, and see governance enforced on the way out.

## Prerequisites

- a Duck deployment URL
- a bearer token, browser-backed sign-in, or approved API key
- network access to the deployment
- permission to query the built-in `sample_data` catalog

## 1. Choose the access surface

Most teams expose one or more of these surfaces:

- browser-based product surfaces
- SQL-compatible clients
- API-backed tools or scripts
- CLI workflows for discovery and automation

For this quickstart, the API path is fastest because it makes the request flow explicit.

## 2. Authenticate as a principal

Common authentication paths are:

- browser sign-in backed by your identity provider
- bearer tokens in `Authorization: Bearer <token>`
- API keys in `X-API-Key: <key>` for approved automation use cases

Use a bearer token unless your team has explicitly approved API keys for automation.

## 3. Run your first governed query

```bash
curl -X POST "https://your-duck-host/v1/query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"sql":"SELECT pickup_date, trip_count, gross_revenue FROM sample_data.nyc_taxi.daily_metrics ORDER BY pickup_date LIMIT 5"}'
```

Expected result:

```json
{"columns":["pickup_date","trip_count","gross_revenue"],"rows":[["2024-01-01",8000,219862.25],["2024-01-02",8000,277281.32]]}
```

The exact JSON shape may include metadata fields, but you should see rows coming back from the built-in `sample_data` catalog with no extra setup.

## 4. Interpret the result correctly

- a `200` means authentication, authorization, and query execution all succeeded
- a `401` means the credential type is wrong, missing, or expired
- a `403` means the principal is real but lacks grants or policy-compatible access
- a restricted-looking result may still be correct if row filters or column masks apply

## Request flow at a glance

<figure class="site-mermaid">
  <img src="/_site/diagrams/secure-query-path.svg" alt="Flow diagram showing a principal moving through identity, the Duck API, policy enforcement, DuckDB execution, and a governed result." loading="lazy" decoding="async">
</figure>

## 5. What to do next

Once the first query works, most users continue in one of these directions:

- browse catalogs, schemas, tables, views, and data products
- compare what different principals can see
- move into builder workflows to create reusable products
- ask an operator how the environment is configured and routed

## Troubleshooting

### `curl` cannot connect

- verify the deployment URL with your admin
- confirm you are on the correct network path or VPN

### Query returns auth errors

- verify whether your team expects browser sign-in, bearer tokens, or API keys
- confirm the credential is still valid
- see [Authentication And Identities](/govern/authentication-and-identities) for auth-path details

### Query returns `403 Forbidden`

Authentication worked, but your current principal does not have the required grants or policy-compatible access.

## Next Steps

- [Ways to Access Duck](/start-here/deployment-modes)
- [Platform Architecture](/concepts/platform-architecture)
- [Governance Model](/concepts/governance-model)
- [Authentication And Identities](/govern/authentication-and-identities)

## Related Reference

- [Glossary](/reference/glossary)
- [API Entry Guide](/reference/api)
