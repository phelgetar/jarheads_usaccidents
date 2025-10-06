#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: scripts/edit_header_sections.py
# Purpose: CLI to patch specific sections in standardized file headers.
#
# Description of code and how it works:
# - Locates header lines like "# Version:", "# Created:", "# Revision History:".
# - Allows setting any single-line section (e.g., Version, Purpose, Author).
# - Allows appending a new bullet to Revision History with version + desc,
#   using the current file's Last Modified date by default.
#
# Author: Tim Canady
# Created: 2025-10-06
#
# Version: 0.1.0
# Last Modified: 2025-10-06 by Tim Canady
#
# Revision History:
# - 0.1.0 (2025-10-06): Initial release of header patcher.
#       --set "Version=0.7.0" update a single section line (by section name)
#       --append-revision "0.7.0|Fix OHGO ingest type handling" add a bullet to # Revision History
#       keep Created/Last Modified dates untouched (default)
#       use the existing Last Modified date as the date in the new revision bullet (or override via --revision-date)
#       target specific files or recurse a directory with --glob filters
###################################################################
#

import argparse
import glob
import os
import re
import sys
from typing import Iterable, List, Optional, Tuple

SECTION_LINE_RE = re.compile(r"^(?P<prefix>\s*#\s*)(?P<name>[^:]+):\s*(?P<value>.*)$")
REVISION_HEADER_RE = re.compile(r"^\s*#\s*Revision History\s*:\s*$")
REVISION_BULLET_RE = re.compile(r"^\s*#\s*-\s+")
LAST_MODIFIED_DATE_RE = re.compile(r"^\s*#\s*Last Modified:\s*(\d{4}-\d{2}-\d{2})\b")

ALLOWED_EXTS = {".py", ".sh"}


def iter_target_files(paths: List[str], globs: List[str]) -> Iterable[str]:
    seen = set()
    for p in paths:
        if os.path.isdir(p):
            pattern_list = globs or ["**/*.py", "**/*.sh"]
            for pat in pattern_list:
                for fp in glob.iglob(os.path.join(p, pat), recursive=True):
                    if os.path.isfile(fp) and os.path.splitext(fp)[1] in ALLOWED_EXTS:
                        if fp not in seen:
                            seen.add(fp)
                            yield fp
        elif os.path.isfile(p):
            if os.path.splitext(p)[1] in ALLOWED_EXTS and p not in seen:
                seen.add(p)
                yield p


def find_section_line_idx(lines: List[str], section_name: str) -> Optional[int]:
    section_lower = section_name.strip().lower()
    for idx, line in enumerate(lines[:200]):  # header should be near top
        m = SECTION_LINE_RE.match(line)
        if not m:
            continue
        if m.group("name").strip().lower() == section_lower:
            return idx
    return None


def set_section_value(lines: List[str], section_name: str, new_value: str) -> Tuple[List[str], bool]:
    idx = find_section_line_idx(lines, section_name)
    if idx is None:
        # Try to insert it right after Project/File/Purpose if missing.
        insert_after = None
        for candidate in ("Purpose", "File", "Project"):
            ci = find_section_line_idx(lines, candidate)
            if ci is not None:
                insert_after = max(insert_after or ci, ci)
        anchor = (insert_after + 1) if insert_after is not None else 0
        prefix = "# "
        lines.insert(anchor, f"{prefix}{section_name}: {new_value}\n")
        return lines, True

    m = SECTION_LINE_RE.match(lines[idx])
    if m:
        prefix = m.group("prefix")
        name = m.group("name")
        lines[idx] = f"{prefix}{name}: {new_value}\n"
        return lines, True
    return lines, False


def find_revision_history_block(lines: List[str]) -> Tuple[Optional[int], Optional[int]]:
    """
    Returns (header_idx, end_idx_exclusive_for_bullets_block) where end is the index after the last bullet line
    (stopping before a non-bullet or an all-hash divider).
    """
    header_idx = None
    for i, line in enumerate(lines[:400]):
        if REVISION_HEADER_RE.match(line):
            header_idx = i
            break
    if header_idx is None:
        return None, None

    # bullets start at header_idx + 1 while they match bullet pattern
    j = header_idx + 1
    while j < len(lines):
        if REVISION_BULLET_RE.match(lines[j]):
            j += 1
            continue
        # Stop on the next banner line or on a non-comment or blank? We'll stop when a line isn't a bullet.
        break
    return header_idx, j


def extract_last_modified_date(lines: List[str]) -> Optional[str]:
    for line in lines[:200]:
        m = LAST_MODIFIED_DATE_RE.match(line)
        if m:
            return m.group(1)
    return None


def append_revision_bullet(
    lines: List[str],
    version: str,
    description: str,
    author: Optional[str],
    revision_date: Optional[str],
    prepend: bool = True,
) -> Tuple[List[str], bool]:
    header_idx, bullets_end = find_revision_history_block(lines)
    if header_idx is None:
        # Try to create the section near the end of header block.
        insert_at = min(len(lines), 60)
        lines.insert(insert_at, "# Revision History:\n")
        header_idx, bullets_end = insert_at, insert_at + 1

    date_str = revision_date
    if not date_str:
        # default to the file's Last Modified date (keep date as shown in the current file)
        date_str = extract_last_modified_date(lines) or ""

    author_suffix = f" — {author}" if author else ""
    bullet = f"# - {version} ({date_str}): {description}{author_suffix}\n" if date_str else f"# - {version}: {description}{author_suffix}\n"

    # Decide insertion point
    insert_at = header_idx + 1 if prepend else (bullets_end or header_idx + 1)
    lines.insert(insert_at, bullet)
    return lines, True


def main():
    ap = argparse.ArgumentParser(description="Edit standardized header sections in-place.")
    ap.add_argument("--paths", nargs="+", required=True, help="Files and/or directories to process.")
    ap.add_argument("--glob", action="append", default=[], help="Glob(s) when a path is a directory (e.g. **/*.py).")
    ap.add_argument("--set", action="append", default=[], help='Set a section: e.g. --set "Version=0.7.0" (repeatable).')
    ap.add_argument("--append-revision", default=None, help='Append revision: "VERSION|DESCRIPTION".')
    ap.add_argument("--author", default=None, help="Optional author suffix for revision bullet.")
    ap.add_argument("--revision-date", default=None, help="Override date used in revision bullet (defaults to file's Last Modified).")
    ap.add_argument("--dry-run", action="store_true", help="Show changes without writing.")
    ap.add_argument("--no-backup", action="store_true", help="Do not create .bak files.")
    args = ap.parse_args()

    # Parse --set entries
    sets: List[Tuple[str, str]] = []
    for s in args.__dict__["set"]:
        if "=" not in s:
            print(f"Invalid --set value (expected Section=Value): {s}", file=sys.stderr)
            sys.exit(2)
        name, value = s.split("=", 1)
        sets.append((name.strip(), value.strip()))

    # Parse append revision
    rev_pair: Optional[Tuple[str, str]] = None
    if args.append_revision:
        if "|" not in args.append_revision:
            print('Invalid --append-revision value (expected "VERSION|DESCRIPTION")', file=sys.stderr)
            sys.exit(2)
        ver, desc = args.append_revision.split("|", 1)
        rev_pair = (ver.strip(), desc.strip())

    changed_total = 0
    for fp in iter_target_files(args.paths, args.glob):
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            orig = f.readlines()

        lines = orig[:]
        changed = False

        # Apply section sets
        for name, value in sets:
            lines, did = set_section_value(lines, name, value)
            changed = changed or did

        # Apply revision append
        if rev_pair:
            version, desc = rev_pair
            lines, did = append_revision_bullet(
                lines,
                version=version,
                description=desc,
                author=args.author,
                revision_date=args.revision_date,
                prepend=True,
            )
            changed = changed or did

        if changed and lines != orig:
            changed_total += 1
            if args.dry_run:
                sys.stdout.write(f"--- DRY RUN: {fp} (modified) ---\n")
                sys.stdout.write("".join(lines[:200]))  # preview first 200 lines
            else:
                if not args.no_backup:
                    with open(fp + ".bak", "w", encoding="utf-8") as b:
                        b.writelines(orig)
                with open(fp, "w", encoding="utf-8") as w:
                    w.writelines(lines)
                print(f"Updated: {fp}")
        else:
            if args.dry_run:
                sys.stdout.write(f"--- DRY RUN: {fp} (no changes) ---\n")

    if args.dry_run:
        print(f"Dry run complete. {changed_total} file(s) would change.")
    else:
        print(f"Done. {changed_total} file(s) updated.")


if __name__ == "__main__":
    main()