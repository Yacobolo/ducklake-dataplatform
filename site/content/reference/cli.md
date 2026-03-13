---
title: Advanced CLI Reference
description: Use the CLI when your organization exposes command-line workflows and you need command-level detail.
---

# CLI Entry Guide

The CLI is an advanced access surface for users and admins who need scripted workflows, discovery, or declarative change management.

## Core Command Families

| Area | Commands to start with |
| --- | --- |
| Declarative workflows | `validate`, `plan`, `apply`, `export` |
| Auth and profiles | `auth`, `config` |
| Discovery | `commands`, `api`, `find`, `describe` |
| Generated resource management | command families under `catalog`, `security`, `models`, `storage`, `semantic`, `notebooks`, and others |

## High-Value Commands

```bash
./bin/duck version
./bin/duck commands --filter query
./bin/duck api search "grant"
./bin/duck config show
./bin/duck find tables movie
```

## Output and Auth Defaults

- `--output` supports machine-friendly and human-friendly modes
- host, token, and API key can be resolved from flags, environment, or profiles
- profile-based setup is useful for shared or multi-environment work

## Best Use Cases

- declarative validation and apply workflows
- discovery of command surfaces
- scripting against API-backed operations without hand-writing HTTP
- operator diagnostics across profiles and environments

## Next Steps

<div class="site-card-grid">
  <a class="site-card" href="/build/declarative-delivery" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 5h12v14H6z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Declarative Delivery</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Use the CLI in rollout workflows.</span></span></a>
  <a class="site-card" href="/reference/generated/api/index" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16"></path><path d="M7 12h10"></path><path d="M9 17h6"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Generated API Reference</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Cross-check exact operations.</span></span></a>
</div>
