#!/usr/bin/env bash
# Installs or removes the repository's git hooks (hack/hooks/pre-commit, pre-push).
#
# The hooks directory git uses is asked for, not assumed: it may be set by core.hooksPath and
# hold other tools' hooks already. Those are left as they are. Each hook gets a marked block that
# calls the script in the checkout the commit or push is made from, so the checks are always the
# ones of that checkout, and a checkout that has none is not checked.
set -euo pipefail

begin='# --- BEGIN TALOS-OPERATOR CHECKS ---'
end='# --- END TALOS-OPERATOR CHECKS ---'

dir=$(git rev-parse --path-format=absolute --git-path hooks)
mkdir -p "$dir"

remove_block() {
  local file=$1
  [ -f "$file" ] || return 0
  awk -v b="$begin" -v e="$end" '$0 == b {skip = 1} !skip {print} $0 == e {skip = 0}' "$file" >"$file.tmp"
  cat "$file.tmp" >"$file"
  rm -f "$file.tmp"
}

for hook in pre-commit pre-push; do
  file="$dir/$hook"
  remove_block "$file"

  if [ "${1:-install}" = remove ]; then
    echo "removed the $hook checks from $file"
    continue
  fi

  [ -f "$file" ] || printf '#!/usr/bin/env sh\n' >"$file"
  cat >>"$file" <<BLOCK
$begin
# Installed by \`make hooks\`; remove with \`make hooks-remove\`. The checks are hack/hooks/$hook.
_checks="\$(git rev-parse --show-toplevel)/hack/hooks/$hook"
if [ -x "\$_checks" ]; then "\$_checks" "\$@" || exit \$?; fi
$end
BLOCK
  chmod +x "$file"
  echo "installed the $hook checks in $file"
done
