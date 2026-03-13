---
title: Compute Topology
description: Understand local execution, remote workers, routing, fallback, and why policy stays in the control plane.
---

# Compute Topology

If you only remember one thing, remember this: Duck can move execution between local and remote workers without moving identity and policy out of the control plane.

## Why It Matters

Operators need different execution shapes for different stages of growth. Local execution is simpler to reason about and easier to deploy. Remote workers add isolation, queueing, and rollout control. Compute topology matters because it changes performance, failure boundaries, and operational complexity, but it should not change governance semantics.

## Local Versus Remote Execution

Local execution means the control plane handles authorization and also runs the work itself.

Remote execution means the control plane still authorizes and routes the work, but one or more workers perform the execution.

Fallback means the platform can route work back to local execution if remote workers are unhealthy or the rollout is intentionally partial.

## What Stays The Same

- the control plane authenticates and authorizes
- the control plane resolves grants, filters, masks, and routing
- execution happens locally or on remote workers
- fallback behavior can keep the platform available while remote rollout is maturing

## Topology Diagram

<figure class="site-mermaid">
  <img src="/_site/diagrams/control-plane-remote-compute.svg" alt="Diagram showing the Duck control plane sending authorized work to local execution or remote workers while storage and identity remain centralized." loading="lazy" decoding="async">
</figure>

Read the diagram from the principal inward. Every request still enters the Duck control plane first. From there, Duck may choose local execution or a remote worker. Both paths still depend on the same governance and storage context. The important lesson is that runtime shape can branch while governance remains centralized.

## Routing, Assignments, And Health

Routing decides which execution target should receive work. Assignments and defaults control who or what is eligible for remote execution. Worker health matters because remote routing is only useful if the operator can tell whether workers are keeping up, failing, or triggering fallback.

This is why compute topology is not only a scaling topic. It is also an operability topic.

## Example In Duck

A team may start with local execution in development and early shared environments. As asynchronous jobs and heavier workloads grow, the operator introduces remote workers for selected groups or job types. Queries still pass through the same identity and policy layer, but execution now happens on a worker fleet. If a worker goes unhealthy during rollout, fallback can temporarily send requests back to local execution so the platform stays available while the operator investigates.

## Common Misunderstandings

- Remote workers do not replace the control plane.
- Fallback is not a permanent architecture plan by itself; it is a rollout and resilience tool.
- Compute routing should change runtime behavior, not data visibility semantics.

## Related Tasks

- [First Operator Setup](/start-here/first-operator-setup)
- [Distributed Compute](/operations/distributed-compute)
- [Platform Settings](/operations/configuration)

## Related Reference

- [Compute API](/reference/generated/api/endpoints/compute)
- [Declarative Compute Routing Defaults Kind](/reference/generated/declarative/kinds/compute-routing-defaults)
