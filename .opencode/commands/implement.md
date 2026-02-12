---
description: Implement the current plan — branch, build, test, push, PR
agent: build
---

## Current state

**HEAD:** !`git log --oneline -1`
**Latest plan:** !`ls -t .opencode/plans/*.md 2>/dev/null | head -1`

## Instructions

Implement the most recent plan in this worktree.

**1. Read the plan**

Read the most recent plan file from `.opencode/plans/` (shown above). Understand the full scope before writing any code.

**2. Create a branch**

Derive a branch name from the plan content using the `ai/<type>/<name>` convention:
- `ai/feat/<name>` — new feature
- `ai/fix/<name>` — bug fix
- `ai/refactor/<name>` — restructuring
- `ai/chore/<name>` — tooling, config, maintenance

Create it from origin/main:
```bash
git switch -c ai/<type>/<name> origin/main
```

**3. Implement**

- Work through the plan step by step.
- Commit incrementally with conventional commit messages (`feat:`, `fix:`, `refactor:`, `chore:`, `test:`, `docs:`).
- After all changes, verify the build and tests pass:
  ```bash
  task build
  task test
  ```
- Fix any failures before proceeding.

**4. Rebase onto latest main**

Other agents may have merged to main during implementation. Rebase to ensure a clean PR:
```bash
git fetch origin
git rebase origin/main
```
If there are conflicts, resolve them and continue the rebase. Re-run `task build` and `task test` after rebasing.

**5. Push and create a draft PR**

```bash
git push -u origin ai/<type>/<name>
gh pr create --draft --title "<type>: <concise description>" --body "<summary from plan>"
```

**6. Report the PR URL** when done so the user can review it.

## Guidelines

- Follow the project conventions in `AGENTS.md`.
- Use conventional commits: `feat:`, `fix:`, `refactor:`, `chore:`, `test:`, `docs:`.
- Do not skip tests. If tests fail, fix them before pushing.
- The PR body should summarize what was done and reference the plan.
