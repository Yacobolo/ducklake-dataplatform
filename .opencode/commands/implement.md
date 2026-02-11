---
description: Generate implementation steps from the current plan
---

You are helping the user prepare to implement an approved plan. Your job is to read the plan below and output a step-by-step guide of commands they can copy-paste. Do NOT execute any commands yourself.

## Current plan

!`cat $(ls -t .opencode/plans/*.md | head -1)`

## Current worktrees

!`git worktree list`

## Instructions

Based on the plan above:

1. **Derive a branch name** from the plan content. Use conventional prefixes and kebab-case:
   - `feat/` — new feature
   - `fix/` — bug fix
   - `refactor/` — restructuring code
   - `chore/` — tooling, config, maintenance
   - Example: `feat/add-rbac-column-masking`

2. **Output a numbered step-by-step command guide** that covers the full workflow. Use the derived branch name throughout. The steps should include:

   **Commit the plan**
   - Stage and commit the plan so it carries over to the worktree: `git add .opencode/plans/ && git commit -m "chore: add implementation plan for <branch-name>"`

   **Create the worktree**
   - Create the worktree: `git worktree add ../duck-demo-worktrees/<branch-name> -b <branch-name>`
   - Verify the worktree: `cd ../duck-demo-worktrees/<branch-name> && task build`

   **Implementation**
   - List the specific files to create or modify, in order, based on the plan
   - Note any code generation steps (e.g., `task generate-api`, `task sqlc`) if the plan touches API or query files

   **Commit & PR**
   - Stage and commit: `git add . && git commit -m "<type>: <concise description>"`
   - Use conventional commit messages: `feat:`, `fix:`, `refactor:`, `chore:`, `test:`, `docs:`
   - Create PR: `gh pr create --title "<title>" --body "<summary from plan>"`

   **Cleanup**
   - Remove worktree when merged: `git worktree remove ../duck-demo-worktrees/<branch-name>`

3. **Do NOT run any commands.** Only print the guide for the user to review and execute.
