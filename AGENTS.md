# Repository Guidelines

## Project Structure & Module Organization

An educational SQL engine inspired by SQLite, focused on backend
storage-engine concerns (pager, B-tree, WAL, transactions, recovery,
replication) rather than a parser or virtual machine.

- `include/` — public headers, grouped by subsystem (`disk/`, `Log/`,
  `LockManager/`, `TransactionManager/`, `server/`, `API/`, `client/`).
- `src/` — implementations, mirroring the `include/` layout.
- `tests/` — unit and integration tests.
- `benchmarks/` — performance harnesses.
- `Docs/Technical Design Docs/` — the design of record for each subsystem.
  Keep document names aligned with subsystem names (`Pager + Cache.md`).
- `Docs/System Constraints/` — system-level constraints.
- The Haldar SQLite reference PDF at the repository root is background
  material, not executable project content.

Design docs lead implementation here. When behavior and the design doc
disagree, that is a bug in one of them — resolve it, do not leave it.

## Build, Test, and Development Commands

```sh
make all               # library + unit and integration test binaries
make test-unit         # unit suite
make test-integration  # integration suite
make test              # both
make server            # server binary
make clean
```

CI (`.github/workflows/ci.yaml`) runs `make all`, `make test-unit`, and
`make test-integration` on every PR to `main`. A change is not ready for
review until all three pass locally.

## Coding Style & Naming Conventions

C++: 4-space indentation, `PascalCase` for types (`Page`, `PCache`),
`snake_case` for functions and fields (`page_num`, `commit_phase_one`).
Headers in `include/`, implementations in the mirroring `src/` path.

Markdown: concise sections with clear hierarchy, title-style headings, bullet
lists for requirements, fenced blocks for pseudocode and commands. Keep
filenames descriptive and stable.

## Testing Guidelines

Add tests alongside the implementation, named after the unit under test
(`pager_test.cpp`, `pcache_test.cpp`). Cover the invariant the change
establishes, not just the happy path — for storage-engine work that means
crash points, ordering, and recovery behavior. Beyond the suites, validate
contributions for internal consistency against the design docs.

## Commit & Pull Request Guidelines

All implementation work follows the workflow in
`.claude/skills/feature-workflow/SKILL.md`: sync `main`, open a Linear ticket
in the `StoneleafDB` team, branch as `<type>/sto-<id>-<slug>`, commit in
coherent slices, open a PR, and report back on the ticket. Agents with skill
support should invoke the `feature-workflow` skill; other agents should read
that file directly and follow it. A git hook refuses commits made on `main` or
from a branch with no ticket id.

Commits use Conventional Commits scoped to the subsystem, matching existing
history:

```
refactor(btree): restart unsafe deletes pessimistically
fix(server): derive WAL Log initial_lsn from existing segments on disk
```

Each commit must build and pass `make test-unit` on its own. Never mix a
refactor with a behavior change in the same commit.

Pull request bodies follow `.github/pull_request_template.md`: summary, a
walkthrough of the commit slices, design impact, validation results, risks,
and a `Closes STO-<id>` line. If a change updates architecture or behavior,
update the relevant document in `Docs/` in the same PR.

## Agent Collaboration Notes

This repository is an educational systems project. Beyond writing code, the
assistant's role is to act as an architecture and design partner: clarify
tradeoffs, challenge assumptions, compare approaches, and refine subsystem
boundaries and invariants. Design discussion, pseudocode, and interface
sketches grounded in the SQLite reference PDF and the repository's own design
notes are first-class deliverables, not preamble to code.

When a task is ambiguous about whether it wants design or implementation, ask
before writing production code.

## Mandatory Agent Handoff

The handoff is a completion requirement, not an optional documentation step.
An agent MUST NOT claim that a task is complete or send its final response
until it has posted the handoff comment on the task's Linear ticket, in the
shape given in `.claude/skills/feature-workflow/SKILL.md` (step 6): what
landed, the validation performed and its result, what changed in `Docs/` or
why nothing needed to change, and any open questions or follow-ups.

The Linear ticket is the handoff record. `STATUS.md` is no longer used for
handoff entries.

If Linear is unreachable, the task is not complete: report the handoff as a
blocker instead of claiming completion.
