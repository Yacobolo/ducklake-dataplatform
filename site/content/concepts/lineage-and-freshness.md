---
title: Lineage And Freshness
description: Understand how QuackStack explains provenance, impact, blockers, and data staleness.
---

# Lineage And Freshness

If you only remember one thing, remember this: lineage tells you how an output was derived, and freshness tells you whether that output is recent enough to trust right now.

## Why It Matters

Teams need both answers during normal product use and during incident response. Lineage tells you what is connected. Freshness tells you whether the connected output is meeting its timeliness expectation. One explains provenance. The other explains staleness.

## What Lineage And Freshness Each Mean

Lineage is the derivation graph. It tells you which upstream tables, models, or other objects contributed to a downstream output. It is about provenance and impact.

Freshness is the timeliness state of an output relative to its expected lag. It is about operational trustworthiness.

They are related because a stale downstream output is often caused by something you can only understand by walking the lineage graph upstream.

## What QuackStack Shows

- table lineage shows upstream and downstream object relationships
- column lineage shows how specific fields were derived
- freshness status shows whether the asset or metric meets its expected lag target
- blocker and requirement views explain what upstream state is preventing recovery

## Freshness And Impact View

<figure class="my-8 overflow-x-auto rounded-[1.5rem] border border-[var(--borderColor-default)] bg-[var(--bgColor-inset)] p-5">
  <img class="mx-auto block h-auto w-max max-w-none rounded-none border-0 bg-transparent" src="/_site/diagrams/lineage-flow.svg" alt="Diagram showing lineage from source tables through models and assets into metrics, with freshness and impact indicators." loading="lazy" decoding="async">
</figure>

Read this first diagram left to right. The point is not only that data moves from source to model to asset to metric. The point is that once a downstream metric looks wrong or stale, QuackStack can trace upstream connections and show impact in both directions.

## Blocker Tree

<figure class="my-8 overflow-x-auto rounded-[1.5rem] border border-[var(--borderColor-default)] bg-[var(--bgColor-inset)] p-5">
  <img class="mx-auto block h-auto w-max max-w-none rounded-none border-0 bg-transparent" src="/_site/diagrams/freshness-blocker-tree.svg" alt="Diagram showing an unhealthy downstream asset tracing freshness blockers back through upstream assets and checks." loading="lazy" decoding="async">
</figure>

Read the second diagram as an incident response view. The downstream asset is unhealthy, but the reason may sit above it in the graph: a failed check, a stale upstream asset, or a missing source update. The purpose of blockers is to avoid vague reasoning like “the dashboard is stale for some reason.”

## Blockers, Impact, And Reconcile

A blocker is the upstream condition preventing a stale output from becoming healthy again. Impact answers the reverse question: if this object changes or fails, what downstream things are affected?

Reconcile is the platform’s attempt to identify useful recovery targets from the current freshness state and dependency graph. In practice, that means QuackStack can help answer whether you should rerun a downstream asset, wait for an upstream source, or backfill a missing partition instead.

## Example In QuackStack

Suppose a weekly finance metric is stale. Freshness tells you the metric has exceeded its lag target. Lineage then shows that the metric depends on a curated revenue model, which depends on an upstream `zone_metrics` asset. The blocker view shows that `zone_metrics` failed its quality check for yesterday’s partition. Now the incident is no longer mysterious: the downstream metric is stale because a specific upstream asset and partition prevented the graph from recovering.

## Common Misunderstandings

- Lineage does not tell you whether data is on time. That is freshness.
- Freshness does not explain derivation by itself. That is lineage.
- Impact is not only for debugging failures. It also helps teams understand the blast radius of planned changes.

## Related Tasks

- [Asset Orchestration Guide](/docs/build/asset-orchestration)
- [Policy Verification](/docs/govern/policy-verification)
- [Observability And Troubleshooting](/operations/observability-and-troubleshooting)

## Related Reference

- [Lineage API](/reference/generated/api/endpoints/lineage)
- [Assets API](/reference/generated/api/endpoints/assets)
