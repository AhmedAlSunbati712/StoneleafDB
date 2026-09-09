# StoneleafDB — Agent Instructions

An educational SQL/storage engine in C++, modeled on SQLite. Repository
conventions live in [AGENTS.md](AGENTS.md); read it for structure, style, and
testing guidance.

## Implementation work follows one workflow

Any task that writes or changes code, tests, or design docs MUST follow
[`.claude/skills/feature-workflow/SKILL.md`](.claude/skills/feature-workflow/SKILL.md).
Invoke it with the `feature-workflow` skill at the START of the task, before
editing anything — not retroactively once the code is written.

The non-negotiable spine:

1. `git switch main && git pull --ff-only origin main`
2. Create (or find) a Linear ticket in team `StoneleafDB`, project
   `Raft Replication`, using the ticket template.
3. Branch `<type>/sto-<id>-<slug>` — the ticket id is required.
4. Commit in coherent, individually buildable slices. Conventional Commits.
   Never mix a refactor with a behavior change.
5. Open a PR using the PR template, with `Closes STO-<id>`.
6. Comment the outcome on the Linear ticket. That comment is the handoff
   record for the next agent.

A `PreToolUse` hook refuses commits on `main` and commits from branches
carrying no ticket id. If it blocks you, you skipped a step — go back and do
it rather than working around the guard.

Questions, reads, and exploration do not need the workflow. Writing code does.

## Before claiming completion

- `make all && make test-unit && make test-integration` pass, or you state
  plainly which failed and why.
- `design-review.md` matches what the repo now does (see AGENTS.md).
- The Linear ticket has the handoff comment.
