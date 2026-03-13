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

- [Operate Duck](/operations/)
- [Platform Settings](/operations/configuration)
- [Security Checklist](/operations/security-checklist)
- [Observability And Troubleshooting](/operations/observability-and-troubleshooting)

## Related Reference

- [Privileges](/reference/privileges)
- [API Entry Guide](/reference/api)
