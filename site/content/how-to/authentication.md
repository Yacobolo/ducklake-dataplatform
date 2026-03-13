---
title: Access the Platform
description: Understand the main sign-in and credential paths available to platform users.
---

# Access the Platform

Duck supports browser-backed identity flows, bearer tokens, and API keys. The right path depends on whether you are an interactive user, an admin, or an automation client.

## Inputs

- a Duck deployment URL
- a credential or sign-in method provided by your organization
- enough permissions for the workflow you want to perform

## Recommended Modes

| Scenario | Recommended access path |
| --- | --- |
| Interactive platform use | browser or bearer-token sign-in |
| Scripted automation | API keys |
| Admin workflows | organization-approved admin credentials |

## Interactive User Path

Use a bearer token when working directly with the API:

```bash
curl -X POST "https://your-duck-host/v1/query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"sql":"SELECT current_timestamp AS now"}'
```

Expected result: a successful query response instead of `401` or `403`.

## Automation Path

Use API keys only when your platform admins allow them for service or scripted usage:

```bash
curl -X POST "https://your-duck-host/v1/query" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <key>" \
  -d '{"sql":"SELECT current_timestamp AS now"}'
```

## Production Guidance

::: warning Security
Do not share personal tokens or over-broad admin API keys across people or systems. Use the smallest credential scope that supports the job.
:::

Admins should prefer identity-provider-backed access for people and keep API keys limited to explicit automation use cases.

## Troubleshooting

- `401 Unauthorized`: verify the credential type and expiration
- `403 Forbidden`: auth worked, but your principal lacks the required grants
- browser sign-in loops or failures: confirm the deployment's identity configuration with your admin

## Next Steps

- [Manage Access](/how-to/access-control)
- [Govern & Administer](/operations/)

## Related Reference

- [Glossary](/reference/glossary)
- [Advanced API Reference](/reference/api)
