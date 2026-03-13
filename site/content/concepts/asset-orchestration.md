---
title: Asset Orchestration
description: Understand assets, dependency graphs, checks, partitions, materialization, backfills, and freshness.
---

# Asset Orchestration

If you only remember one thing, remember this: an asset in Duck is a durable output that the platform knows how to track, refresh, validate, and explain operationally.

## Why It Matters

Teams usually start with the question, “what SQL produces this table?” That question is useful, but it is not enough when an output becomes important to other people. Once a dataset powers a dashboard, a metric, or a published product, teams also need to know whether it ran, whether it is fresh enough, what upstreams it depends on, and what should happen when it falls behind. Asset orchestration exists to answer those operational questions explicitly.

Duck uses assets to describe outputs that matter operationally. The asset layer sits above raw SQL and above a single run of a query. It tells the platform how an output should behave over time, not just how it was produced once.

## What An Asset Is

In Duck, an asset is a named output in the dependency graph that the platform can monitor and act on. An asset might represent a table, a view, a model output, a notebook output, a semantic object, or another durable result that downstream users depend on.

That is why an asset is not the same thing as a model, table, notebook, or pipeline:

- a table is a storage object
- a model is transformation logic that produces a result
- a notebook is an authoring and exploration surface
- a pipeline is a job-level workflow that can orchestrate work
- an asset is the durable thing the platform tracks for dependency, freshness, checks, and materialization

You can think of the asset layer as the operational contract around an output.

## What An Asset Definition Contains

An asset definition is the metadata that tells Duck how to treat that output. In plain English, it answers:

- what is this output called and what kind of thing is it
- who owns it
- what upstream assets have to be healthy before it should be trusted
- what checks must pass
- how fresh it is expected to stay
- how it should be materialized or rebuilt
- whether it is partitioned, and if so, how partial rebuilds should work

Those fields matter because they change what the platform can explain and automate. Without an asset definition, Duck may know an output exists. With an asset definition, Duck can tell you why it is stale, which upstream failed, what partitions are missing, and which remediation action makes sense.

## Asset Graph

<figure class="site-mermaid">
  <img src="/_site/diagrams/asset-dag-materialization.svg" alt="Diagram showing source assets flowing through checks and freshness policies into materialized downstream assets and data products." loading="lazy" decoding="async">
</figure>

Read the diagram from left to right. A source asset feeds a staging asset, which then feeds a curated asset that is published to a data product. Checks sit alongside the flow because they decide whether the downstream output should be trusted. Freshness policy sits beside the curated asset because it describes how current the output is expected to be. Materialization and backfill actions point back into the curated asset because they are ways of producing or repairing that durable output.

## Runs, Materializations, And Backfills

These three terms are easy to blur together, so the page needs to separate them clearly.

A run is an execution record. It answers, “what happened when Duck tried to do the work?” A run has a status such as pending, running, success, or failed. Runs are about execution history.

A materialization is the durable produced result of that execution. It answers, “what concrete output was written or updated?” You can think of a materialization as the successful produced state that downstream users now rely on.

A backfill is a targeted request to rebuild historical slices of an asset. It answers, “how do we repair or populate older partitions or time ranges that were missed, changed, or need recomputation?” Backfills are not the same as ordinary forward progress; they are explicit remediation or historical population work.

## Checks, Freshness, Blockers, And Reconcile

Checks are automated validations attached to an asset. A check might confirm row counts, null handling, key uniqueness, or another expectation that should hold before downstream users trust the output. Check results are the outcome records of those validations.

Freshness is the operational statement of how up to date an asset is expected to be. In practice, freshness means “how old can this output get before it stops meeting its consumer contract?” It is not just metadata for display. It affects incident response, alerting, and which outputs are safe to consume.

A blocker is an upstream reason a stale or unhealthy asset cannot recover yet. For example, a downstream daily revenue asset may be stale because an upstream zone metrics asset failed its check, or because a required source partition never arrived.

A reconcile action is Duck’s way of saying, “given the current freshness state and dependency graph, what should be rematerialized or repaired to move this output back toward healthy?” Reconcile does not mean “blindly rerun everything.” It means analyzing the graph and identifying the nearest useful recovery targets.

## Partitions And Dependency Graphs

Partitions matter when an asset is produced in slices rather than all at once, such as by day, region, or customer segment. Partitioned assets let Duck reason about partial freshness and partial repair. Instead of saying “the whole asset is broken,” Duck can say “the 2026-03-12 partition is missing” or “only the APAC slice needs a backfill.”

The dependency graph is the map that connects assets to the assets they rely on. This graph is what lets Duck explain blockers, freshness propagation, and downstream impact. Without the graph, every failure looks isolated. With the graph, the platform can tell a coherent operational story.

## Example In Duck

Imagine a daily revenue dashboard built from a `zone_metrics` asset. That dashboard’s product page says it should be current by 7:00 AM every day.

One morning the dashboard looks stale. Duck can now answer a chain of questions:

1. is the dashboard stale because its downstream asset missed freshness
2. did the dashboard asset fail to materialize, or did it materialize from stale upstream data
3. did the upstream `zone_metrics` asset fail a check
4. was the failure limited to one partition, such as yesterday’s load
5. should the operator rematerialize the downstream asset, or backfill the missing upstream slice first

That is the real value of asset orchestration. It turns a vague complaint like “the dashboard is stale” into a traceable, repairable workflow.

## Common Misunderstandings

- An asset is not just another word for a model. A model describes transformation logic; an asset describes an operationally tracked output.
- A run is not the same thing as a materialization. A run records execution; a materialization records the produced durable result.
- Freshness is not only a UI badge. It is part of the consumer contract around whether an output is trustworthy.
- Reconcile is not “rerun everything.” It is the platform reasoning about the smallest useful recovery path.

## Related Tasks

- [Asset Orchestration Guide](/build/asset-orchestration)
- [Lineage And Freshness](/concepts/lineage-and-freshness)
- [Policy Verification](/govern/policy-verification)

## Related Reference

- [Assets API](/reference/generated/api/endpoints/assets)
- [Declarative Asset Kind](/reference/generated/declarative/kinds/asset)
