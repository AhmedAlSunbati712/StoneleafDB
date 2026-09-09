## Summary

One paragraph: what changed and why, in terms a reviewer who has not read the
ticket can follow.

## Commits

Walk the reviewer through the slices in order, one line each.

1. `<type>(<scope>): <subject>` — what it does and why it comes first
2. ...

## Design impact

Invariants established or changed, on-disk format changes, recovery or
concurrency implications. State "none" if the change is behavior-preserving.

Design docs updated: `Docs/Technical Design Docs/<file>.md` (or "none needed").

## Validation

- `make all` — result
- `make test-unit` — result
- `make test-integration` — result
- Manual scenarios exercised, if any

## Risks and rollback

What could break that CI would not catch, and how to back this out.

Closes STO-<id>
