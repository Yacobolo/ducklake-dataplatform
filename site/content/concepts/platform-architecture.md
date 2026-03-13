---
title: Platform Architecture
description: Understand the secure control-plane path from principal to governed result and product metadata.
---

# Platform Architecture

If you only remember one thing, remember this: Duck keeps identity, policy, and product metadata in the control plane, then sends authorized work to execution.

## Why It Matters

This separation is what lets Duck scale or relocate compute without changing who can see what. Users care about safe access. Operators care about performance and runtime shape. The platform architecture is designed so those concerns do not fight each other.

## What The Control Plane Is

The control plane is the part of Duck that knows who the principal is, what objects exist, what products are published, and which policy rules apply. It is the decision-making side of the platform.

The execution layer is the part that actually runs queries or jobs. It is the work-performing side of the platform.

That means Duck is not just “a DuckDB process with auth in front.” It is a governed platform that decides what is allowed before work reaches the engine.

## How It Works

At a high level:

1. a principal reaches Duck through browser, SQL, API, or CLI
2. identity is resolved
3. the control plane checks grants, row filters, column masks, and product metadata
4. the rewritten or authorized plan executes
5. the result comes back as a governed response

## Architecture Diagram

<figure class="site-mermaid">
  <img src="/_site/diagrams/secure-query-path.svg" alt="Diagram showing a principal flowing through identity, Duck API, policy enforcement, execution, and a governed result." loading="lazy" decoding="async">
</figure>

Read the diagram from left to right. The principal is authenticated first. The request then reaches Duck’s API and policy layer, where grants, row filters, and column masks are applied. Only after that does the platform hand work to DuckDB execution. The result that comes back is already governed, which is why consumers do not need a separate mental model for “secure mode” versus “normal mode.”

## What Changes When Compute Moves

When teams enable remote workers, the main thing that changes is where execution happens. What does not change is just as important:

- principals are still authenticated by the control plane
- grants, filters, and masks are still resolved before execution
- products, assets, and semantic metadata still live in the control plane

This is why remote compute is different from letting users bypass Duck and talk directly to storage or an engine. Remote workers can change isolation, scale, and failure boundaries, but they should not change governance outcomes.

## How This Relates To Other Concepts

Platform architecture explains where decisions are made.

- [Governance Model](/docs/concepts/governance-model) explains what those access decisions are
- [Compute Topology](/docs/concepts/compute-topology) explains where execution can run
- [Data Products](/docs/concepts/data-products) explains what consumers discover once governance allows them through

## Example In Duck

Suppose an analyst submits a metric query through a BI client. The analyst may think they are “just querying DuckDB,” but the platform is doing more than engine execution. Duck first resolves the analyst’s identity, checks whether the metric and its underlying objects are reachable, applies row filters and masks where necessary, and only then runs the plan. If the same query is later routed to a remote worker, the analyst should still see the same governed result because the control-plane decisions did not move.

## Related Tasks

- [Quickstart](/start-here/quickstart)
- [Access Design](/docs/govern/access-design)
- [Platform Settings](/operations/configuration)

## Related Reference

- [API Entry Guide](/reference/api)
