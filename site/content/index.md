---
layout: home

hero:
  name: Duck Data Platform
  eyebrow: Open source data platform
  headline: Governed Data Infrastructure
  tagline: Query, build, and ship trusted data products on DuckDB and DuckLake.
  snapshot_title: What makes Duck different
  snapshot_note: Policy stays in the control plane even when compute, products, and orchestration expand around it.
  actions:
    - theme: brand
      text: Quickstart
      link: /start-here/quickstart
    - theme: alt
      text: Browse Docs
      link: /docs/
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
  eyebrow: Core features
  title: Core features
  details: ""
  items:
    - label: Dashboards
      title: Build dashboards on governed sources.
      icon: layout
      details: Serve dashboards from SQL, notebook cells, and semantic queries while keeping policy enforcement in the request path.
      link: /reference/generated/api/endpoints/dashboards
    - label: Data products
      title: Package trusted outputs for real consumers.
      icon: package
      details: Publish ownership, contracts, semantic entrypoints, and versioned outputs as discoverable data products.
      link: /build/data-product-lifecycle
    - label: Transformation framework
      title: Model, test, and reuse SQL logic.
      icon: workflow
      details: Author models, macros, and quality checks with a declarative workflow that stays close to DuckDB execution.
      link: /concepts/transformation-framework
    - label: Semantic layer
      title: Publish metrics and relationships that travel well.
      icon: network
      details: Turn curated models into business-facing semantic entrypoints for APIs, dashboards, and shared products.
      link: /build/semantic-models-and-metrics
---

Duck is a governed data platform built on DuckDB and DuckLake, with product-first workflows for data consumers, builders, and operators.
