---
description: Implement plan using git worktrees
---

## Current worktrees

!`git worktree list`

## Instructions

**Derive a branch name** from the plan content. Use conventional prefixes and kebab-case:

- `feat/` — new feature
- `fix/` — bug fix
- `refactor/` — restructuring code
- `chore/` — tooling, config, maintenance
- Example: `feat/add-rbac-column-masking`

**Commit the plan**

- Stage and commit the plan so it carries over to the worktree: `git add .opencode/plans/<plan-name> && git commit -m "chore: add implementation plan for <branch-name>"`

**Create the worktree**

- Create the worktree: `git worktree add ../duck-demo-worktrees/<branch-name> -b <branch-name>`
- Verify the worktree: `cd ../duck-demo-worktrees/<branch-name> && task build`

**Start working**

## Guidelines

**Commit & PR**

- Stage and commit: `git add . && git commit -m "<type>: <concise description>"`
- Use conventional commit messages: `feat:`, `fix:`, `refactor:`, `chore:`, `test:`, `docs:`
- Create PR: `gh pr create --title "<title>" --body "<summary from plan>"`

**Cleanup**

- Remove worktree when merged: `git worktree remove ../duck-demo-worktrees/<branch-name>`
