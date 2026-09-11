---
name: feature-workflow
description: >-
  The mandatory end-to-end procedure for implementing anything in StoneleafDB:
  sync main, open a Linear ticket in the right project, branch, slice the work
  into coherent commits, open a PR, and report back on the ticket. Use whenever
  a task involves writing or changing code, tests, or design docs in this repo —
  including "implement X", "fix Y", "add tests for Z", and follow-up work on an
  existing ticket. Not needed for pure questions, reads, or exploration.
---

# Feature Workflow

Every code change in this repo travels the same path:

```
sync main -> Linear ticket -> branch -> sliced commits -> PR -> ticket comment
```

Skipping a step is not a shortcut; it is a defect. The `git` guard hook in
`.claude/settings.json` enforces the first three steps mechanically — a commit
on `main`, or on a branch with no ticket id, is refused.

## Workspace facts

| | |
|---|---|
| Linear workspace | StoneleafDB |
| Team | `StoneleafDB` (key `STO`) |
| Default project | `Raft Replication` — almost all current work |
| GitHub repo | `AhmedAlSunbati712/StoneleafDB` |
| Base branch | `main` |
| Statuses | Backlog, Todo, In Progress, In Review, Done |
| Labels | Feature, Improvement, Bug |

If work clearly does not belong to `Raft Replication`, list the projects and
pick the right one; if none fits, ask before inventing a project.

---

## 1. Sync main

```sh
git switch main
git pull --ff-only origin main
git status --porcelain   # must be clean before branching
```

If the tree is dirty with someone else's work in progress, stop and ask. Never
stash or discard changes you did not make.

## 2. Open the Linear ticket

Search first — the ticket may already exist:

```
list_issues(team: "StoneleafDB", project: "Raft Replication", query: "<keywords>")
```

If it exists, use it and skip to step 3. Otherwise create one with
`save_issue`:

- `team`: `"StoneleafDB"`
- `project`: `"Raft Replication"` (or the project you established above)
- `title`: imperative and scoped — `"Persist Raft term and vote across restarts"`,
  not `"Raft persistence work"`
- `description`: **exactly** the template in
  `templates/linear-ticket.md` — every heading, filled in
- `labels`: one of `Feature` / `Improvement` / `Bug`
- `state`: `"In Progress"` (you are about to start)

One ticket is one coherent, reviewable change. If the description's Scope
section is turning into a list of loosely related things, split it into
several tickets and implement them one at a time — each gets its own branch
and PR.

Record the returned identifier (e.g. `STO-14`); every later step needs it.

## 3. Branch

```sh
git switch -c <type>/sto-<id>-<slug>
```

- `<type>`: `feat` | `fix` | `refactor` | `perf` | `docs` | `test` | `chore`
- `<id>`: the Linear ticket number, lowercase `sto-14`
- `<slug>`: 2-4 kebab-case words

`feat/sto-14-persist-term-and-vote`, `fix/sto-21-checkpoint-lsn-off-by-one`.

The guard hook reads the ticket id out of this branch name, so the name is
load-bearing, not decoration.

## 4. Slice the work into commits

A reviewer should be able to read the commit list top to bottom and follow the
reasoning. Aim for commits that are individually **buildable** and
**self-explanatory**.

Good slices, in dependency order:

1. Pure refactors and interface moves that make room for the change
2. New types / headers / structures with no behavior yet
3. The behavior itself, one invariant at a time
4. Tests
5. Design-doc updates under `Docs/Technical Design Docs/`

Rules:

- **Never** mix a refactor and a behavior change in one commit. Split them.
- Run `make all && make test-unit` before each commit. A commit that does not
  build is not a slice, it is a bisect trap.
- Format: Conventional Commits, matching existing history.

```
<type>(<subsystem>): <imperative summary under 72 chars>

<why this change is needed, and any non-obvious tradeoff.
 Wrap at 72. Omit for changes whose subject line says everything.>
```

Subsystems in use: `btree`, `pager`, `keystore`, `server`, `log`, `wal`,
`transaction`, `raft`, `storage`. Real examples from this repo:

```
refactor(btree): restart unsafe deletes pessimistically
fix(server): derive WAL Log initial_lsn from existing segments on disk
feat(pager): isolate wal-backed page mutations
```

Do not put the ticket id in commit subjects — the branch and PR carry it.

## 5. Push and open the PR

```sh
git push -u origin <branch>
gh pr create --base main --title "<same shape as commit subject>" --body-file <file>
```

The body follows `templates/pr-body.md` exactly. Write it to a scratch file and
pass `--body-file`; do not inline a long body as a shell argument.

The PR title mirrors a commit subject: `feat(raft): persist term and vote
across restarts`. The body's `Closes STO-14` line is what links Linear to
GitHub — never omit it.

Then move the ticket to **In Review** and attach the PR:

```
save_issue(id: "STO-14", state: "In Review",
           links: [{url: "<pr url>", title: "PR #NN"}])
```

## 6. Comment on the ticket

Post one comment with `save_comment(issueId: "STO-14", body: ...)`:

```markdown
**PR:** <url>

**What landed**
- <commit-level summary, one bullet per slice>

**Validation**
- `make test-unit` — <result>
- `make test-integration` — <result>

**Design contracts**
- <what changed in Docs/, or "no design contract changed">

**Open questions / follow-ups**
- <anything the reviewer must decide, or "none">
```

Comment again on the same ticket when review feedback lands and when the PR
merges. The ticket is the handoff record for the next agent — everything a
successor needs to resume must be readable there.

## 7. Close out

After merge: move the ticket to **Done**, then `git switch main && git pull
--ff-only`. Delete the local branch.

---

## Handoff obligations

Before claiming a task is complete, **the Linear ticket must carry the step-6
comment.** This replaced the old `STATUS.md` handoff entry; do not write
handoff entries to `STATUS.md`.

If Linear is unreachable, say so explicitly in your final response and put the
handoff comment content in the PR body instead. Do not silently drop it.

## When the hook blocks you

The guard denies the tool call and explains why. Do not work around it by
disabling the hook or setting `ALLOW_MAIN_COMMIT`. Go back and do the step you
skipped — that env var exists for the user, not for you.
