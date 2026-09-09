#!/usr/bin/env python3
"""Verify decomposition work-item dependency edge orientation.

A decomposition artifact (gc.build.decomposition.v1) that declares its work
items in a machine-readable dependency table binds the live bead graph to that
declaration: each work item's bead must depend on the beads of the work items
it declares under Depends On, and no earlier work item's bead may depend on a
later one's. This is the creation-time orientation assert for the recurring
failure where a sequential chain is wired backwards (`gc bd dep add` read
temporally, "1 before 2", instead of as "dependent needs prerequisite") and
the convoy drains the chain back-to-front.

The dependency table lives in the artifact body (conventionally under the
Work Items section) and is any Markdown table whose header row includes ID,
Bead, and Depends On columns:

| ID | Bead | Depends On |
| --- | --- | --- |
| WI-1 | gc-aaa111 | - |
| WI-2 | gc-bbb222 | WI-1 |

Rows are the intended execution order, so every Depends On entry must
reference an earlier row (comma-separated for several, `-` or empty for
none). The table must not carry a Status column: the build-artifact validator
reads any ID/Status table as the coverage matrix.

Artifacts with no dependency table skip live verification (methodology packs
that produce the same schema without bead-level tables stay valid); declaring
the table opts the artifact into strict orientation checking.

Exit status: 0 when the declaration is absent or fully consistent with the
live graph, 1 with machine-readable `decomposition-edge-check:` lines on
stderr otherwise. Requires `gc` for live edge reads unless --skip-live.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from validate_build_artifact import clean_table_cell, is_separator_row, split_table_row

# Mirrors gascity's beads.readyBlockingDependencyTypes: only these edge types
# hold a bead out of Ready(), so only these express execution ordering.
READY_BLOCKING_TYPES = {"blocks", "waits-for", "conditional-blocks"}
CHECK_PREFIX = "decomposition-edge-check"


class EdgeCheckError(Exception):
    pass


@dataclass(frozen=True)
class WorkItemRow:
    item_id: str
    bead_id: str
    depends_on: tuple[str, ...]


def normalize_header_cell(cell: str) -> str:
    return " ".join(clean_table_cell(cell).lower().replace("_", " ").replace("-", " ").split())


def parse_depends_cell(raw: str) -> tuple[str, ...]:
    cleaned = clean_table_cell(raw)
    if cleaned in {"", "-", "none"}:
        return ()
    parts = [clean_table_cell(part) for part in cleaned.split(",")]
    return tuple(part for part in parts if part and part != "-")


def parse_work_item_tables(body: str) -> list[WorkItemRow]:
    rows: list[WorkItemRow] = []
    lines = body.splitlines()
    index = 0
    while index < len(lines):
        cells = split_table_row(lines[index])
        header = [normalize_header_cell(cell) for cell in cells]
        if not header or "id" not in header or "bead" not in header or "depends on" not in header:
            index += 1
            continue
        if "status" in header:
            raise EdgeCheckError(
                "work-item dependency table must not carry a Status column; "
                "the build-artifact validator reads any ID/Status table as the coverage matrix"
            )
        id_index = header.index("id")
        bead_index = header.index("bead")
        depends_index = header.index("depends on")
        index += 1
        if index < len(lines) and is_separator_row(lines[index]):
            index += 1
        while index < len(lines):
            row = split_table_row(lines[index])
            if not row or len(row) <= max(id_index, bead_index, depends_index):
                break
            item_id = clean_table_cell(row[id_index])
            bead_id = clean_table_cell(row[bead_index])
            if item_id:
                rows.append(
                    WorkItemRow(
                        item_id=item_id,
                        bead_id="" if bead_id == "-" else bead_id,
                        depends_on=parse_depends_cell(row[depends_index]),
                    )
                )
            index += 1
    return rows


def validate_declaration(rows: list[WorkItemRow]) -> list[str]:
    errors: list[str] = []
    position: dict[str, int] = {}
    beads_seen: dict[str, str] = {}
    for index, row in enumerate(rows):
        if row.item_id in position:
            errors.append(f"duplicate work item id {row.item_id!r} in dependency table")
            continue
        position[row.item_id] = index
        if not row.bead_id:
            errors.append(f"work item {row.item_id} has no bead id in the dependency table")
            continue
        if row.bead_id in beads_seen:
            errors.append(
                f"work items {beads_seen[row.bead_id]} and {row.item_id} share bead id {row.bead_id}"
            )
            continue
        beads_seen[row.bead_id] = row.item_id
    for index, row in enumerate(rows):
        for dep in row.depends_on:
            if dep == row.item_id:
                errors.append(f"work item {row.item_id} declares a dependency on itself")
                continue
            if dep not in position:
                errors.append(f"work item {row.item_id} depends on unknown work item {dep!r}")
                continue
            if position[dep] >= index:
                errors.append(
                    f"work item {row.item_id} depends on {dep}, which is declared later: "
                    "the table must list work items in execution order and every "
                    "Depends On entry must reference an earlier row"
                )
    return errors


def parse_dep_list_output(output: str) -> list[dict[str, str]]:
    start = output.find("[")
    if start < 0:
        return []
    try:
        data, _ = json.JSONDecoder().raw_decode(output[start:])
    except json.JSONDecodeError as exc:
        raise EdgeCheckError(f"gc bd dep list output was not JSON: {exc}") from exc
    if not isinstance(data, list):
        return []
    deps: list[dict[str, str]] = []
    for raw in data:
        if not isinstance(raw, dict):
            continue
        depends_on = str(raw.get("depends_on_id") or raw.get("id") or "").strip()
        dep_type = str(raw.get("type") or raw.get("dependency_type") or "").strip() or "blocks"
        if depends_on:
            deps.append({"depends_on_id": depends_on, "type": dep_type})
    return deps


def live_blocking_deps(gc_bin: str, bead_id: str) -> set[str]:
    cmd = [gc_bin, "bd", "dep", "list", bead_id, "--json"]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        raise EdgeCheckError(f"gc bd dep list {bead_id} failed ({proc.returncode}): {stderr}")
    return {
        dep["depends_on_id"]
        for dep in parse_dep_list_output(proc.stdout)
        if dep["type"] in READY_BLOCKING_TYPES
    }


def validate_live_edges(rows: list[WorkItemRow], gc_bin: str) -> list[str]:
    errors: list[str] = []
    bead_by_item = {row.item_id: row.bead_id for row in rows}
    deps_by_bead: dict[str, set[str]] = {}
    for row in rows:
        try:
            deps_by_bead[row.bead_id] = live_blocking_deps(gc_bin, row.bead_id)
        except EdgeCheckError as exc:
            errors.append(str(exc))
    if errors:
        return errors
    for row in rows:
        for dep in row.depends_on:
            prerequisite = bead_by_item[dep]
            if prerequisite not in deps_by_bead[row.bead_id]:
                errors.append(
                    f"missing declared edge: {row.item_id} ({row.bead_id}) must depend on "
                    f"{dep} ({prerequisite}); fix: gc bd dep add {row.bead_id} {prerequisite}"
                )
    for earlier_index, earlier in enumerate(rows):
        for later in rows[earlier_index + 1 :]:
            if later.bead_id in deps_by_bead[earlier.bead_id]:
                errors.append(
                    f"inverted edge: {earlier.item_id} ({earlier.bead_id}) depends on "
                    f"{later.item_id} ({later.bead_id}), which is declared later in execution "
                    f"order; fix: gc bd dep remove {earlier.bead_id} {later.bead_id}"
                )
    return errors


def check_artifact(path: Path, gc_bin: str, skip_live: bool) -> tuple[int, str]:
    text = path.read_text(encoding="utf-8")
    rows = parse_work_item_tables(text)
    if not rows:
        return 0, "decomposition edge check: no work-item dependency table declared; skipping edge verification"
    errors = validate_declaration(rows)
    if not errors and not skip_live:
        errors = validate_live_edges(rows, gc_bin)
    if errors:
        for error in errors:
            print(f"{CHECK_PREFIX}: {error}", file=sys.stderr)
        return 1, ""
    declared_edges = sum(len(row.depends_on) for row in rows)
    scope = "declaration only" if skip_live else "declaration and live edges"
    return 0, f"decomposition edges valid: {len(rows)} work items, {declared_edges} declared edges ({scope})"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify decomposition work-item dependency edge orientation")
    parser.add_argument("--path", required=True, type=Path, help="Decomposition artifact markdown path")
    parser.add_argument("--gc-bin", default="gc", help="gc binary used for live edge reads (default: gc)")
    parser.add_argument(
        "--skip-live",
        action="store_true",
        help="Only validate the declared table (no gc bead reads)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    try:
        status, message = check_artifact(args.path, args.gc_bin, args.skip_live)
    except (OSError, UnicodeDecodeError, EdgeCheckError) as exc:
        print(f"{CHECK_PREFIX}: {exc}", file=sys.stderr)
        return 1
    if message:
        print(message)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
