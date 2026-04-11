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

<figure class="my-8 overflow-x-auto rounded-[1.5rem] border border-[var(--borderColor-default)] bg-[var(--bgColor-inset)] p-5">
  <img class="mx-auto block h-auto w-max max-w-none rounded-none border-0 bg-transparent" src="/_site/diagrams/notebook-promotion-flow.svg" alt="Diagram showing a notebook session producing an output cell that is promoted into a managed model and downstream asset." loading="lazy" decoding="async">
</figure>

## Git-Backed Notebook Sync

Use Git sync when you want notebooks to live as reviewed declarative files instead of only inside the UI.

### Expected Repo Layout

- set the Git repo record `path` to the declarative project root
- place notebook documents under `workspaces/<workspace>/workspace.cue`
- use the declarative [reference](/reference/generated/declarative/) as the source format

Example:

```text
analytics/
  notebooks/
    sales.cue
    margin-review.cue
```

### Sync Behavior

1. register a Git repo and branch with the notebooks integrations API
2. call sync to clone the branch and load declarative notebook files from `<path>/notebooks`
3. create or update local notebooks using the declarative notebook spec
4. remove previously linked notebooks when their source CUE fragment is deleted from Git
5. carry forward notebook publish metadata so declared model promotion targets stay attached

Git is the source of truth for linked notebooks. A later sync overwrites linked notebook structure from Git, including cell order, roles, test config, visuals, and publish metadata.

## Git Sync Flow

<figure class="site-mermaid">
  <img src="/_site/diagrams/git-notebook-sync-flow.svg" alt="Diagram showing a Git repo with declarative notebook CUE being synced into local notebooks and optional published models." loading="lazy" decoding="async">
</figure>

## Verification

- the published model retains the intended SQL logic
- notebook exploration is no longer the only place the business logic lives
- downstream dependencies point at the model, not a transient session
- linked notebooks match the declarative files in Git after each sync
- removing a linked notebook file from Git removes that linked notebook locally on the next sync

## Related Reference

- [Notebooks API](/reference/generated/api/endpoints/notebooks)
- [Models API](/reference/generated/api/endpoints/models)
- [Integrations API](/reference/generated/api/endpoints/integrations)
- [Declarative Reference](/reference/generated/declarative/)
