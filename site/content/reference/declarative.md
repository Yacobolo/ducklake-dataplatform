---
title: Advanced Declarative Reference
description: Use the generated declarative schema reference for field-level detail after the workflow is clear.
---

# Declarative Entry Guide

The generated declarative reference documents the versioned JSON Schema artifacts that define supported document shapes. Treat it as the field-level companion to the declarative workflow.

## Best Entry Points

- [Generated Declarative Reference](/docs/reference/generated/declarative/)
- [Declarative Delivery](/docs/build/declarative-delivery)

## Where To Start

| Task | Start With | Why |
| --- | --- | --- |
| Checking supported kinds | Generated declarative reference | It lists the schema-backed kinds directly |
| Confirming field names and types | Generated declarative reference | It is the field-level source of truth |
| Understanding per-kind schema files | Generated declarative reference | It points straight at the per-kind artifacts |
| Wiring editor schema mappings | Generated declarative reference | It exposes the exact schema path to map |
| Planning a safe rollout workflow | Product guides | They explain validate, plan, apply, and operational safety |
| Understanding schema vs semantic validation | Product guides | They explain why both validation layers exist |
| Learning how assets, models, semantics, and products fit together | Product guides | They connect the schema to the platform mental model |

## Runtime Contract

- declarative reads are strict by default, so missing supported endpoints fail the operation instead of being skipped
- `plan` uses exit code `0` for clean, `2` for actionable drift, and `1` for blocking declarative errors
- `apply` aborts before mutation when blocking declarative errors are present
- the operational success condition is a clean follow-up `plan` after `apply`

## Editor Integration

Map the union schema:

`schemas/declarative/v1/duck.declarative.schema.json`

to your config file patterns in your editor.

## Next Steps

<div class="site-card-grid">
  <a class="site-card" href="/docs/reference/generated/declarative/" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 5h12v14H6z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Generated Declarative Reference</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Inspect schema fields and kinds.</span></span></a>
  <a class="site-card" href="/docs/build/declarative-delivery" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16"></path><path d="M7 12h10"></path><path d="M9 17h6"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Declarative Delivery</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Apply schema in real workflows.</span></span></a>
</div>
