---
title: First Operator Setup
description: Stand up the mental model for auth, governance, storage, and compute in a production-minded deployment.
---

# First Operator Setup

Use this quickstart when you are responsible for platform posture. The goal is not just to make Duck start, but to make it safe, governable, and observable.

## What You Are Establishing

- a trusted identity path
- secure runtime configuration
- a storage and external-data strategy
- clear policy ownership
- a compute topology that matches your load and isolation needs

## 1. Establish identity first

Before exposing data, decide:

- how people authenticate
- how services authenticate
- how principals, groups, and grants are managed
- which credential types are allowed in production

## 2. Lock down the runtime baseline

Confirm the production minimums:

- `ENV=production`
- encryption key configured from a managed secret source
- listener addresses aligned to network boundaries
- auth configured before opening shared access

## 3. Decide how storage and integration work

Operators should know:

- where data lives
- how external locations and storage credentials are managed
- how Git or other integrations are approved and monitored

## 4. Choose the compute topology

Start with local execution unless you have a reason to separate workers. Move to remote compute when you need:

- execution isolation
- staged routing and fallback
- lifecycle-style asynchronous workloads

## Deployment shape at a glance

<figure class="site-mermaid">
  <img src="/_site/diagrams/control-plane-remote-compute.svg" alt="Topology diagram showing principals reaching the Duck control plane, policy enforcement, local execution, optional remote workers, and storage." loading="lazy" decoding="async">
</figure>

## 5. Add health and troubleshooting paths

Before broad rollout, make sure the team can answer:

- is the service healthy
- is auth working
- is a policy denying access
- is a worker unhealthy
- is storage misconfigured

## Next Steps

<div class="site-card-grid">
  <a class="site-card" href="/operations/" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14"></path><path d="M5 12h14"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Operate Duck</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Go deeper on runtime ops.</span></span></a>
  <a class="site-card" href="/operations/configuration" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 5h12v14H6z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Platform Settings</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Set the production baseline.</span></span></a>
  <a class="site-card" href="/operations/security-checklist" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7 3v5c0 5-3 8-7 10-4-2-7-5-7-10V6l7-3z"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Security Checklist</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Review release-gate controls.</span></span></a>
  <a class="site-card" href="/operations/observability-and-troubleshooting" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 18V9"></path><path d="M12 18V5"></path><path d="M19 18v-6"></path><path d="M4 18h16"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Observability And Troubleshooting</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Find failures faster.</span></span></a>
</div>

## Related Reference

- [Privileges](/reference/privileges)
- [API Entry Guide](/reference/api)
