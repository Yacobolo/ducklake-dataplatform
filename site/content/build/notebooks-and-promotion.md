---
title: Notebooks And Promotion
description: Explore logic in notebooks and promote durable outputs into managed models.
---

# Notebooks And Promotion

Use notebooks when you need an exploratory surface that can still graduate into shared production assets.

## Inputs

- a notebook with executable SQL cells
- an agreed owner and purpose
- a target project and model name for promotion

## Flow

1. create a notebook and capture the exploratory logic
2. mark the output cell or publish target you want to preserve
3. run the notebook and confirm the output shape
4. promote the notebook output into a model
5. move ongoing changes into the durable model workflow

## Promotion Flow

<figure class="site-mermaid">
  <img src="/_site/diagrams/notebook-promotion-flow.svg" alt="Diagram showing a notebook session producing an output cell that is promoted into a managed model and downstream asset." loading="lazy" decoding="async">
</figure>

## Verification

- the published model retains the intended SQL logic
- notebook exploration is no longer the only place the business logic lives
- downstream dependencies point at the model, not a transient session

## Related Reference

- [Notebooks API](/reference/generated/api/endpoints/notebooks)
- [Models API](/reference/generated/api/endpoints/models)
- [Declarative Notebook Kind](/reference/generated/declarative/kinds/notebook)
