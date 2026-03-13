---
title: Concepts
description: Build the mental model for how Duck’s control plane, products, orchestration, semantics, and compute fit together.
---

# Concepts

The concepts section should teach the system, not just label its parts. If you only remember one thing, remember this: Duck becomes much easier to reason about once you separate governance, products, orchestration, transformation, semantics, and compute.

When teams first encounter Duck, many ideas can sound similar because they all touch the same outputs. A model, asset, semantic model, and data product may all point at the same business domain, but they answer different questions:

- governance asks who is allowed to see it
- products ask how it is packaged and owned
- orchestration asks how it is kept healthy and current
- transformation asks how it is built
- semantics ask how consumers should query it
- compute asks where the work runs

## Read These First

- [Platform Architecture](/concepts/platform-architecture)
- [Governance Model](/concepts/governance-model)
- [Data Products](/concepts/data-products)
- [Asset Orchestration](/concepts/asset-orchestration)
- [Transformation Framework](/concepts/transformation-framework)
- [Semantic Layer](/concepts/semantic-layer)
- [Lineage And Freshness](/concepts/lineage-and-freshness)
- [Compute Topology](/concepts/compute-topology)

## How To Use This Section

Read these pages when you want a mental model before implementation details. Each page should answer three questions:

1. what is this concept in plain English
2. why does Duck have it as a separate concept
3. how is it different from the nearby concepts people often confuse it with

## Example In Duck

Imagine one business question: "is daily revenue healthy across pickup zones?"

Duck answers that one question through several separate concepts that are easy to blur together if the docs do not name them clearly:

- [Governance Model](/concepts/governance-model) explains which principals are allowed to see the answer and whether rows or columns should be filtered or masked
- [Transformation Framework](/concepts/transformation-framework) explains how builders turn raw trip data into curated revenue logic
- [Asset Orchestration](/concepts/asset-orchestration) explains how the curated output is refreshed, checked, backfilled, and monitored for freshness
- [Semantic Layer](/concepts/semantic-layer) explains how the business-facing metric is defined so consumers do not have to rebuild it by hand
- [Data Products](/concepts/data-products) explains how the final output is packaged, owned, versioned, and published for discovery
- [Compute Topology](/concepts/compute-topology) explains where the work runs without changing governance outcomes

That is why the concepts section exists. Duck is one platform, but the platform solves several different problems at once. The articles in this section separate those concerns so the rest of the docs can stay practical instead of constantly redefining the same terms.

## Common Misunderstandings

- A concept page is not the same thing as a task guide. Concepts explain what something is and why it exists; task guides explain how to use it.
- Many Duck objects touch the same output, but that does not make them interchangeable. A model, asset, semantic model, and data product are related, not synonymous.

## Recommended Next Steps

- [Build](/build/)
- [Govern](/govern/)
- [Operate](/operations/)

## Related Reference

- [Feature Map](/reference/feature-map)
- [Glossary](/reference/glossary)
