---
description: Fetch latest and clean up the worktree for the next task
agent: build
---

## Current state

**Branch:** !`git branch --show-current || echo "(detached HEAD)"`
**Dirty files:** !`git status --short`

## Instructions

Prepare this worktree for the next task by fetching the latest changes and cleaning up any old branch.

**1. Fetch latest**

```bash
git fetch origin
```

**2. Handle the current branch**

- If there are uncommitted changes, **warn the user** and ask whether to discard them (`git checkout -- .`) or abort.
- If on a feature branch (anything other than `main` or detached HEAD):
  - Check if the branch has a PR and its status: `gh pr view --json state --jq .state`
  - If the PR is merged or closed, proceed. If still open, inform the user and ask whether to continue.
  - Switch off the branch: `git switch --detach origin/main`
  - Delete the local branch: `git branch -D <branch-name>`
- If already detached or on `main`: just move to latest origin/main: `git switch --detach origin/main`

**3. Confirm clean state**

Run and show the output of:
```bash
git log --oneline -1
git status
```

The worktree is now ready. The user can describe their next task and start planning.
