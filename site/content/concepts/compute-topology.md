---
title: Compute Topology
description: Understand local execution, remote workers, routing, fallback, and why policy stays in the control plane.
---

# Compute Topology

Duck separates policy from execution so teams can scale the runtime without weakening governance.

## Why It Matters

Operators often need different execution shapes for different environments. Local execution is simple. Remote workers add isolation, queueing, and rollout control.

## Mental Model

- the control plane authenticates and authorizes
- the control plane resolves grants, filters, masks, and routing
- execution happens locally or on remote workers
- fallback behavior can keep the platform available while remote rollout is maturing

## Topology Diagram

<figure class="site-mermaid">
  <img src="/_site/diagrams/control-plane-remote-compute.svg" alt="Diagram showing the Duck control plane sending authorized work to local execution or remote workers while storage and identity remain centralized." loading="lazy" decoding="async">
</figure>

## Key Objects

- control-plane API and policy engine
- compute endpoints and assignments
- routing defaults and fallback policies
- worker health and lifecycle execution

## Related Tasks

- [First Operator Setup](/start-here/first-operator-setup)
- [Distributed Compute](/operations/distributed-compute)
- [Platform Settings](/operations/configuration)

## Related Reference

- [Compute API](/reference/generated/api/endpoints/compute)
- [Declarative Compute Routing Defaults Kind](/reference/generated/declarative/kinds/compute-routing-defaults)
