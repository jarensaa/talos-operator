#!/usr/bin/env bash
# Checks that every commit between a base and a head is a Conventional Commit.
#
# Releases are decided from commit messages (.releaserc.json): a `feat:` is a minor release, a
# `fix:` a patch, anything else none. Pull requests are merged with a merge commit, so it is the
# commits of the branch that count, not the pull request's title. A commit that is not in that
# form says nothing to the release: a feature committed as "wip: ..." is merged and never
# released, which is how adding and removing machines once reached main without a version.
#
# usage: commits.sh <base> [<head>]
set -euo pipefail

base=${1:?usage: commits.sh <base> [<head>]}
head=${2:-HEAD}

# The types of the conventionalcommits preset.
types='feat|fix|docs|test|ci|chore|refactor|perf|build|revert|style'

bad=$(git log --no-merges --format='%h %s' "$base..$head" | grep -Ev "^[0-9a-f]+ ($types)(\([^)]+\))?!?: .+" || true)

if [ -n "$bad" ]; then
  printf 'These commits are not Conventional Commits, so no release would know of them:\n%s\n' "$bad" >&2
  printf 'Reword them as <type>(<scope>): <what>, with a type of: %s\n' "${types//|/, }" >&2
  exit 1
fi
