#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/bump_file_versions.py
# Purpose: FastAPI usaccidents_app to ingest, normalize, and serve incidents.
#
# Author: Tim Canady
# Created: 2025-10-04
#
# Description:
#   Safely bump a single file's version in versions.yml and update ONLY
#   the header's "Version:" line + "Revision History" (append one entry).
#   Preserves the rest of the header verbatim.
#
# Version: 0.4.1
# Last Modified: 2025-10-04 by Tim Canady
#
# Revision History:
# - 0.4.1 (2025-10-04): Auto-synced update — Tim Canady
# - 0.1.0 (2025-09-28): Initial — Tim Canady
###################################################################
#


import sys
import argparse
import pathlib
import datetime
import subprocess
import fnmatch
import yaml
import re
from typing import Dict, List, Tuple, Optional

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSIONS_FILE = REPO_ROOT / "versions.yml"
CHANGELOG_FILE = REPO_ROOT / "CHANGELOG.md"


def load_versions() -> dict:
    with open(VERSIONS_FILE, "r") as f:
        return yaml.safe_load(f)


def write_versions(data: dict) -> None:
    with open(VERSIONS_FILE, "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def semver_bump(ver: str, part: str = "patch") -> str:
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)$", ver)
    if not m:
        raise ValueError(f"Invalid semver: {ver}")
    major, minor, patch = map(int, m.groups())
    if part == "major":
        return f"{major+1}.0.0"
    if part == "minor":
        return f"{major}.{minor+1}.0"
    return f"{major}.{minor}.{patch+1}"  # patch


def now_date() -> str:
    return datetime.date.today().strftime("%Y-%m-%d")


def ensure_changelog_exists() -> None:
    if not CHANGELOG_FILE.exists():
        CHANGELOG_FILE.write_text("# Changelog\n", encoding="utf-8")


def prepend_changelog_block(block: str) -> None:
    ensure_changelog_exists()
    existing = CHANGELOG_FILE.read_text(encoding="utf-8")
    CHANGELOG_FILE.write_text(block + "\n\n" + existing, encoding="utf-8")


def append_release_entry(
    project_version_after: str,
    message: str,
    bumped_files: Dict[str, str],
    project_bumped: bool,
) -> None:
    """
    Top-append a nice CHANGELOG entry.
    - Header uses the project version *after* bump (even if only files were bumped),
      so entries are grouped by the current release train.
    """
    date = now_date()
    lines: List[str] = [f"## [{project_version_after}] - {date}", f"- {message}"]
    if project_bumped:
        lines.append(f"  - Project version → v{project_version_after}")
    if bumped_files:
        lines.append("  - File bumps:")
        for fpath, new_ver in bumped_files.items():
            lines.append(f"    • {fpath} → v{new_ver}")
    block = "\n".join(lines)
    prepend_changelog_block(block)


def expand_glob(files_dict: Dict[str, str], pattern: str) -> List[str]:
    return [path for path in files_dict.keys() if fnmatch.fnmatch(path, pattern)]


def run_apply_headers(summary: str) -> None:
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "apply_headers.py"), summary],
        check=True,
    )


def bump_files(
    files_dict: Dict[str, str],
    targets: List[str],
    level: str,
) -> Dict[str, str]:
    """
    Mutates files_dict to new versions; returns {file: new_version} for changed files.
    """
    changed: Dict[str, str] = {}
    for t in targets:
        old = files_dict[t]
        new = semver_bump(old, level)
        files_dict[t] = new
        changed[t] = new
        print(f"{t}: {old} → {new}")
    return changed


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Bump file versions (single/glob/all) and/or the project version; update CHANGELOG and headers."
    )
    ap.add_argument(
        "target",
        help="File path, glob (e.g., 'usaccidents_app/*'), 'all', or 'project' (if only bumping project).",
    )
    ap.add_argument(
        "message",
        help="One-line changelog summary (applies to both project and file bumps if both are used).",
    )
    ap.add_argument(
        "level",
        nargs="?",
        choices=["major", "minor", "patch"],
        default="patch",
        help="Semver level for file bumps (ignored if target='project' and only --project is used).",
    )
    ap.add_argument(
        "--project",
        choices=["major", "minor", "patch"],
        help="Also bump the project_version by this level (or use with target='project' to bump only the project).",
    )

    args = ap.parse_args()

    # Load current versions
    versions = load_versions()
    files_dict: Dict[str, str] = versions.get("files", {})
    if not isinstance(files_dict, dict):
        print("versions.yml: 'files' section is missing or invalid.")
        sys.exit(1)

    # Determine bump actions
    project_bump_level: Optional[str] = args.project
    target = args.target

    # Prepare target file list (if applicable)
    targets: List[str] = []
    if target.lower() == "project":
        # Only project bump; no file bumps unless combined with a real glob later.
        pass
    elif target.lower() == "all":
        targets = list(files_dict.keys())
        if not targets:
            print("No tracked files in versions.yml -> run scripts/init_versions.py first.")
            sys.exit(1)
    elif "*" in target or "?" in target:
        targets = expand_glob(files_dict, target)
        if not targets:
            print(f"No matches for {target} in versions.yml.")
            sys.exit(1)
    else:
        if target not in files_dict:
            print(f"File '{target}' not tracked in versions.yml.")
            sys.exit(1)
        targets = [target]

    # Bump project_version first (so we can use the new version in the changelog header)
    project_old = versions.get("project_version", "0.0.0")
    project_new = project_old
    project_bumped = False
    if project_bump_level:
        project_new = semver_bump(project_old, project_bump_level)
        versions["project_version"] = project_new
        project_bumped = True
        print(f"Project: {project_old} → {project_new}")

    # Bump files if any targets were provided (and not just 'project')
    bumped_files: Dict[str, str] = {}
    if targets:
        bumped_files = bump_files(files_dict, targets, args.level)
        versions["files"] = files_dict

    # Persist versions.yml
    write_versions(versions)

    # Append/Prepend changelog entry (uses project_new even if unchanged)
    append_release_entry(project_new, args.message, bumped_files, project_bumped)

    # Re-apply headers so versions appear at the top of changed files
    run_apply_headers(args.message)


if __name__ == "__main__":
    main()
