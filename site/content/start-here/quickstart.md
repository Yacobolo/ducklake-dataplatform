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

<figure class="my-8 overflow-x-auto rounded-[1.5rem] border border-[var(--borderColor-default)] bg-[var(--bgColor-inset)] p-5">
  <img class="mx-auto block h-auto w-max max-w-none rounded-none border-0 bg-transparent" src="/_site/diagrams/secure-query-path.svg" alt="Flow diagram showing a principal moving through identity, the Duck API, policy enforcement, DuckDB execution, and a governed result." loading="lazy" decoding="async">
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
- see [Authentication And Identities](/docs/govern/authentication-and-identities) for auth-path details

### Query returns `403 Forbidden`

Authentication worked, but your current principal does not have the required grants or policy-compatible access.

## Next Steps

<style>
  .doc-next-steps {
    display: grid;
    gap: 0.75rem;
  }

  .doc-next-step-card {
    position: relative;
    display: flex;
    align-items: center;
    gap: 0.875rem;
    border: 1px solid var(--borderColor-default);
    background: transparent;
    color: inherit;
    padding: 1rem 1.25rem !important;
    text-decoration: none;
    transition:
      transform 180ms ease,
      border-color 180ms ease,
      box-shadow 180ms ease,
      background-color 180ms ease;
  }

  .doc-next-step-card,
  .doc-next-step-card:hover,
  .doc-next-step-card:focus,
  .doc-next-step-card:visited {
    text-decoration: none !important;
  }

  .doc-next-step-card:hover {
    transform: translateY(-2px);
    border-color: var(--fgColor-accent);
    background: color-mix(in srgb, var(--fgColor-accent) 3%, transparent);
    box-shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
  }

  .doc-next-step-card:hover .doc-next-step-arrow {
    color: var(--fgColor-accent);
    transform: translate(2px, -2px);
  }

  .doc-next-step-icon {
    display: inline-flex;
    height: 3rem;
    width: 3rem;
    flex-shrink: 0;
    align-items: center;
    justify-content: center;
    align-self: center;
    border-radius: 1rem;
    background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent);
    color: var(--fgColor-accent);
  }

  .doc-next-step-copy {
    min-width: 0;
    flex: 1;
    text-align: left;
  }

  .doc-next-step-title {
    margin: 0;
    color: var(--fgColor-default);
    font-size: 0.98rem;
    font-weight: 600;
  }

  .doc-next-step-subtitle {
    margin: 0.2rem 0 0;
    color: var(--fgColor-muted);
    font-size: 0.875rem;
    line-height: 1.35;
    white-space: nowrap;
  }

  .doc-next-step-arrow {
    position: absolute;
    right: 1.25rem;
    top: 1.25rem;
    height: 1rem;
    width: 1rem;
    color: var(--fgColor-muted);
    transition: color 180ms ease, transform 180ms ease;
  }

  @media (max-width: 767px) {
    .doc-next-step-card {
      align-items: flex-start;
      padding-right: 3.25rem !important;
    }

    .doc-next-step-subtitle {
      white-space: normal;
    }
  }
</style>

<div class="doc-next-steps">
  <a class="site-card doc-next-step-card" href="/start-here/deployment-modes">
    <span class="doc-next-step-icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M4 7h16"></path>
        <path d="M7 12h10"></path>
        <path d="M9 17h6"></path>
      </svg>
    </span>
    <span class="doc-next-step-copy">
      <h3 class="doc-next-step-title">Ways to Access Duck</h3>
      <p class="doc-next-step-subtitle">Choose the right query surface.</p>
    </span>
    <svg class="doc-next-step-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 17 17 7"></path><path d="M7 7h10v10"></path></svg>
  </a>
  <a class="site-card doc-next-step-card" href="/docs/concepts/platform-architecture">
    <span class="doc-next-step-icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <rect x="4" y="5" width="16" height="6" rx="1"></rect>
        <rect x="7" y="13" width="10" height="6" rx="1"></rect>
      </svg>
    </span>
    <span class="doc-next-step-copy">
      <h3 class="doc-next-step-title">Platform Architecture</h3>
      <p class="doc-next-step-subtitle">See the secure request path.</p>
    </span>
    <svg class="doc-next-step-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 17 17 7"></path><path d="M7 7h10v10"></path></svg>
  </a>
  <a class="site-card doc-next-step-card" href="/docs/concepts/governance-model">
    <span class="doc-next-step-icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 3l7 3v5c0 5-3 8-7 10-4-2-7-5-7-10V6l7-3z"></path>
      </svg>
    </span>
    <span class="doc-next-step-copy">
      <h3 class="doc-next-step-title">Governance Model</h3>
      <p class="doc-next-step-subtitle">Understand grants, filters, and masks.</p>
    </span>
    <svg class="doc-next-step-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 17 17 7"></path><path d="M7 7h10v10"></path></svg>
  </a>
  <a class="site-card doc-next-step-card" href="/docs/govern/authentication-and-identities">
    <span class="doc-next-step-icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 12a4 4 0 1 0-4-4 4 4 0 0 0 4 4z"></path>
        <path d="M5 20a7 7 0 0 1 14 0"></path>
      </svg>
    </span>
    <span class="doc-next-step-copy">
      <h3 class="doc-next-step-title">Authentication And Identities</h3>
      <p class="doc-next-step-subtitle">Match auth to real principals.</p>
    </span>
    <svg class="doc-next-step-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 17 17 7"></path><path d="M7 7h10v10"></path></svg>
</a>
</div>

## Related Reference

- [Glossary](/reference/glossary)
- [API Entry Guide](/reference/api)
