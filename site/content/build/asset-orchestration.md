---
title: Asset Orchestration Guide
description: Define dependency graphs, checks, freshness, and materialization behavior for durable outputs.
---

# Asset Orchestration Guide

Use this guide when outputs need explicit operational guarantees instead of informal build order.

## Inputs

- a set of upstream and downstream outputs
- ownership information
- freshness and quality expectations
- a plan for backfills or rematerialization

## Flow

1. create asset definitions for the durable outputs that matter operationally
2. declare upstream dependencies explicitly
3. add checks and severity that match your release expectations
4. define freshness and materialization policy
5. run or trigger materialization, then inspect runs, blockers, and requirements

## Execution Graph

<figure class="site-mermaid">
  <img src="/_site/diagrams/asset-dag-materialization.svg" alt="Diagram showing source, staging, and downstream assets connected by checks, freshness, and materialization actions." loading="lazy" decoding="async">
</figure>

## Verification

- the graph matches the real dependency order
- checks catch bad runs before consumers do
- freshness expectations are visible and explainable
- backfills can be scoped intentionally rather than guessed

## Related Reference

- [Assets API](/reference/generated/api/endpoints/assets)
