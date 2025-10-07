#!/usr/bin/env bash
#
###################################################################
# Project: USAccidents
# File: scripts/git_bootstrap.sh
# Purpose: Initialize git repo, make initial commit, create annotated tag.
#
# Description of code and how it works:
# - Creates a Git repository if missing, adds files per .gitignore.
# - Reads version from versions.yml to craft commit and tag.
# - Installs pre-commit hook if available.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.1.0
# Last Modified: 2025-10-06 by Tim Canady
#
# Revision History:
# - 0.1.0 (2025-10-06): Initial script.
###################################################################
#
set -euo pipefail
#set python3 = /usr/local/bin/python3

if [[ ! -d .git ]]; then
  git init
  echo "[git] Initialized new repository"
fi

# Ensure .gitignore exists
if [[ ! -f .gitignore ]]; then
  cat > .gitignore <<'EOF'
# (generated) see project canvas for full list
__pycache__/
*.py[cod]
.venv/
.env
.env.*
*.egg-info/
build/
dist/
.mypy_cache/
.pytest_cache/
.coverage
htmlcov/
coverage.xml
.tox/
*.bak
.vscode/
.idea/
.DS_Store
*.log
*.sql
*.sql.gz
alembic/versions/*.pyc
*.pid
EOF
fi

# Determine version (prefer PyYAML; fall back to grep)
if command -v python3 >/dev/null 2>&1; then
  VERSION=$(python3 - <<'PY'
import sys
try:
    import yaml
    v = yaml.safe_load(open('versions.yml'))['version']
except Exception:
    import re
    import pathlib
    m = re.search(r"version:\s*([0-9]+\.[0-9]+\.[0-9]+)", pathlib.Path('versions.yml').read_text())
    v = m.group(1) if m else '0.0.0'
print(v)
PY
)
else
  VERSION=$(grep -Eo 'version:\s*[0-9]+\.[0-9]+\.[0-9]+' versions.yml | awk '{print $2}')
fi

# Pre-commit (optional)
if command -v pre-commit >/dev/null 2>&1; then
  pre-commit install || true
fi

# Stage and commit
git add -A
if git diff --cached --quiet; then
  echo "[git] No changes to commit"
else
  git commit -m "chore(repo): bootstrap project v${VERSION}"               -m "Scaffold, OHGO connector, scheduler, Alembic, header tools."
  echo "[git] Committed bootstrap v${VERSION}"
fi

# Tag (annotated); don't overwrite existing tags
if git rev-parse "v${VERSION}" >/dev/null 2>&1; then
  echo "[git] Tag v${VERSION} already exists"
else
  git tag -a "v${VERSION}" -m "Release v${VERSION}"
  echo "[git] Created tag v${VERSION}"
fi

echo "[git] Done."
