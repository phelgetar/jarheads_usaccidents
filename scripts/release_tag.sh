#!/usr/bin/env bash
#
###################################################################
# Project: USAccidents
# File: scripts/release_tag.sh
# Purpose: Create an annotated tag from versions.yml (post-bump).
#
# Description of code and how it works:
# - Reads versions.yml, creates an annotated Git tag vX.Y.Z.
# - Optional push if --push is supplied.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.6.0
# Last Modified: 2025-10-06 by Tim Canady
#
# Revision History:
# - 0.6.0 (2025-10-06): Initial script.
###################################################################
#
set -euo pipefail

if [[ ! -f versions.yml ]]; then
  echo "versions.yml not found" >&2
  exit 1
fi

if command -v python3 >/dev/null 2>&1; then
  VERSION=$(python3 - <<'PY'
import sys
try:
    import yaml
    print(yaml.safe_load(open('versions.yml'))['version'])
except Exception:
    import re
    import pathlib
    m = re.search(r"version:\s*([0-9]+\.[0-9]+\.[0-9]+)", pathlib.Path('versions.yml').read_text())
    print(m.group(1) if m else '0.0.0')
PY
)
else
  VERSION=$(grep -Eo 'version:\s*[0-9]+\.[0-9]+\.[0-9]+' versions.yml | awk '{print $2}')
fi

git add versions.yml CHANGELOG.md || true
if ! git diff --cached --quiet; then
  git commit -m "chore(release): prepare v${VERSION}" || true
fi

if git rev-parse "v${VERSION}" >/dev/null 2>&1; then
  echo "Tag v${VERSION} already exists" >&2
else
  git tag -a "v${VERSION}" -m "Release v${VERSION}"
  echo "Created tag v${VERSION}"
fi

if [[ "${1:-}" == "--push" ]]; then
  git push --follow-tags
fi
