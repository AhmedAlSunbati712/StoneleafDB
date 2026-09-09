#!/usr/bin/env bash
# PreToolUse/Bash guard for the StoneleafDB feature workflow.
#
# Denies:
#   - `git commit` on main/master               -> forces branching off first
#   - `git commit` on a branch with no STO-<n>  -> forces creating the Linear ticket first
#   - `git push` that targets main/master       -> forces the PR path
#
# Escape hatch for deliberate one-offs: ALLOW_MAIN_COMMIT=1 in the environment.
set -uo pipefail

payload=$(cat)
cmd=$(printf '%s' "$payload" | jq -r '.tool_input.command // ""')

[ -n "${ALLOW_MAIN_COMMIT:-}" ] && exit 0

deny() {
  jq -n --arg r "$1" '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: $r
    }
  }'
  exit 0
}

branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")

# `git commit` anywhere in the command line (start, or after ; && || | )
if printf '%s' "$cmd" | grep -Eq '(^|[;&|][[:space:]]*)git[[:space:]]+(-[^[:space:]]+[[:space:]]+)*commit([[:space:]]|$)'; then
  case "$branch" in
    main|master)
      deny "Blocked: you are on '$branch'. The feature workflow requires a Linear ticket and a feature branch before committing. Run the /feature-workflow skill: pull main, create the STO ticket, then branch as <type>/sto-<id>-<slug>."
      ;;
  esac
  if ! printf '%s' "$branch" | grep -Eqi '(^|[/-])sto-[0-9]+'; then
    deny "Blocked: branch '$branch' carries no Linear ticket id. Create the STO ticket first, then branch as <type>/sto-<id>-<slug> (e.g. feat/sto-14-election-timer). See .claude/skills/feature-workflow/SKILL.md."
  fi
fi

# `git push` aimed at main/master
if printf '%s' "$cmd" | grep -Eq '(^|[;&|][[:space:]]*)git[[:space:]]+(-[^[:space:]]+[[:space:]]+)*push([[:space:]]|$)'; then
  if printf '%s' "$cmd" | grep -Eq '[[:space:]](origin|upstream)[[:space:]]+(main|master)([[:space:]]|$)|:(main|master)([[:space:]]|$)'; then
    deny "Blocked: direct push to main/master. Push the feature branch and open a PR instead."
  fi
  case "$branch" in
    main|master)
      deny "Blocked: pushing from '$branch'. Work belongs on a <type>/sto-<id>-<slug> branch behind a PR."
      ;;
  esac
fi

exit 0
