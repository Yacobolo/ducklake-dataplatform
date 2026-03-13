---
title: Mermaid Diagram Guide
description: Author Mermaid diagrams for the docs using the checked-in SVG pipeline and consistent icon language.
---

# Mermaid Diagram Guide

The docs use checked-in Mermaid source files rendered to SVG through the site diagram pipeline.

## Workflow

1. add a `.mmd` file under `site/diagrams/src`
2. generate the SVG with `task site:diagrams`
3. embed the SVG with a `site-mermaid` figure
4. add descriptive alt text on the image

## Diagram Rules

- keep one concept per diagram
- prefer left-to-right flow for process diagrams
- use short labels
- pair icons with text, never icons alone
- reuse the same icon for the same concept across the site

## Icon Language

- `fa:user` or `fa:circle-user` for principals and end users
- `fa:id-card` or `fa:address-book` for identity and group-oriented nodes
- `fa:folder-open` for containers like catalogs, schemas, or projects
- `fa:file-lines` or `fa:file-code` for SQL, models, and declarative resources
- `fa:note-sticky` for notebooks
- `fa:chart-bar` for metrics and dashboards
- `fa:clock`, `fa:calendar`, and `fa:hourglass-half` for scheduling and freshness
- `fa:circle-check`, `fa:circle-xmark`, and `fa:bell` for checks and status
- `fa:hard-drive` for storage and external locations
- `fa:handshake` for contracts and subscriptions
- `fa:map` or `fa:compass` for lineage and navigation concepts

## Related Reference

- [Feature Map](/reference/feature-map)
