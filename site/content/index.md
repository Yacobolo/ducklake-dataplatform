---
layout: home

hero:
  name: Duck Data Platform
  eyebrow: Governed data platform
  headline: Governed queries. Real products. One control plane.
  tagline: Duck puts policy in path on top of DuckDB and DuckLake, then carries that same model into products, orchestration, and operations.
  snapshot_title: What makes Duck different
  snapshot_note: Policy stays in the control plane even when compute, products, and orchestration expand around it.
  actions:
    - theme: brand
      text: Query Governed Data
      link: /start-here/quickstart
    - theme: alt
      text: Build A Data Product
      link: /start-here/first-data-product
  proofs:
    - icon: shield
      text: RBAC, row filters, masks
    - icon: workflow
      text: products, assets, models
    - icon: network
      text: semantics, lineage, compute
  snapshot:
    - icon: database
      label: Query layer
      value: Governed DuckDB execution with policy in path.
    - icon: fingerprint
      label: Identity model
      value: Principals, groups, grants, filters, and masks.
    - icon: workflow
      label: Builder path
      value: Models, notebooks, assets, semantics, and products.
    - icon: network
      label: Operator path
      value: Storage, compute routing, integrations, health, and rollout.

pillars:
  eyebrow: Choose a journey
  title: Start where your job starts.
  details: The docs are organized around the outcomes people actually need, not just the API groups behind them.
  items:
    - label: Query
      title: Reach your first governed query fast.
      icon: shield
      details: Understand the secure request path, authentication choices, and how policy shapes every result set.
      link: /start-here/quickstart
    - label: Build
      title: Package trusted outputs into real products.
      icon: workflow
      details: Move from sources to models, assets, semantic metrics, and product contracts without leaving the platform.
      link: /start-here/first-data-product
    - label: Operate
      title: Run Duck like a shared platform.
      icon: cpu
      details: Configure auth, storage, distributed compute, and observability so teams can scale without weakening governance.
      link: /start-here/first-operator-setup

workflow:
  eyebrow: One coherent story
  title: Follow the same example from source to product.
  details: The docs reuse a single `sample_data.nyc_taxi` story so concepts, tutorials, and reference pages reinforce each other instead of feeling disconnected.
  steps:
    - stage: "01"
      icon: database
      title: Start with a governed source.
      details: Users query trusted tables through the secure control plane instead of talking directly to the engine.
    - stage: "02"
      icon: workflow
      title: Transform source data into curated models.
      details: Builders author models, reuse macros, add tests, and promote notebook outputs into durable assets.
    - stage: "03"
      icon: network
      title: Publish semantic entrypoints and product contracts.
      details: The same outputs become metrics, relationships, semantic models, and discoverable data products.
    - stage: "04"
      icon: shield
      title: Govern and verify what different principals can see.
      details: Operators and data stewards verify grants, row filters, masks, freshness, and run behavior through one shared control surface.

capabilities:
  eyebrow: Cover the full platform
  title: Learn the feature set from authored docs first.
  details: Each major platform area now has a concept page, a practical build or governance guide, and a precise reference entrypoint.
  items:
    - label: Concepts
      title: Architecture, products, orchestration, transformations, semantics
      icon: database
      details: Build the mental model for how the control plane, object graph, and execution model fit together.
      link: /concepts/
    - label: Build
      title: Sources, models, macros, notebooks, assets, and metrics
      icon: workflow
      details: Go from raw inputs to reusable data products with end-to-end builder workflows.
      link: /build/
    - label: Govern
      title: Identity, access design, policy verification, and ownership
      icon: network
      details: Design safe access paths and prove they work against the real query and product surfaces.
      link: /govern/
    - label: Operate
      title: Configuration, storage, integrations, distributed compute, troubleshooting
      icon: cpu
      details: Run Duck reliably in shared and production environments with central governance intact.
      link: /operations/

oss_cta:
  eyebrow: Keep the detail close
  title: Drop into reference only when you need precision.
  details: Generated API and declarative docs stay available, but the authored docs should get you oriented before you need payload fields and schema tables.
  actions:
    - theme: brand
      text: Open Concepts
      link: /concepts/
    - theme: alt
      text: Open Build Guides
      link: /build/
    - theme: alt
      text: Open Reference
      link: /reference/
  notes:
    - The generated reference already covers the broader surface, including products, assets, models, notebooks, pipelines, lineage, and semantic APIs.
    - The authored docs now focus on journeys, mental models, and verified workflows.
---

Duck is a governed data platform built on DuckDB and DuckLake, with product-first workflows for data consumers, builders, and operators.
