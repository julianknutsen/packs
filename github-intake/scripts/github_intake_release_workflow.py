#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

import github_intake_common as common


def bead_metadata(bead_id: str) -> dict[str, object]:
    bead_id = bead_id.strip()
    if not bead_id:
        return {}
    bd_bin = os.environ.get("BD_BIN", "bd")
    city_root = common.city_root() or "."
    try:
        result = subprocess.run(
            [bd_bin, "show", bead_id, "--json"],
            cwd=city_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return {}
    if result.returncode != 0:
        return {}
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        return metadata
    return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Release a stuck /gc workflow lock for a GitHub issue")
    parser.add_argument("repository", help="owner/repo")
    parser.add_argument("issue_number", help="GitHub issue number")
    parser.add_argument("--command", default="fix", help="slash command name to unlock (default: fix)")
    parser.add_argument("--force", action="store_true", help="release even if the previous bead already recorded GitHub side effects")
    args = parser.parse_args()

    request = common.find_request(args.repository, args.issue_number, args.command)
    if not request:
        print(
            json.dumps(
                {
                    "status": "not_found",
                    "repository": args.repository,
                    "issue_number": args.issue_number,
                    "command": args.command,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    workflow_key = str(request.get("workflow_key", "")).strip()
    if not workflow_key:
        print(
            json.dumps(
                {
                    "status": "no_workflow_key",
                    "request_id": request.get("request_id", ""),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    if not args.force:
        metadata = bead_metadata(str(request.get("bead_id", "")))
        for key in ("github_fix_started_comment_id", "github_fix_pr_url", "github_fix_complete_comment_id"):
            if str(metadata.get(key, "")).strip():
                print(
                    json.dumps(
                        {
                            "status": "blocked",
                            "reason": "workflow_has_github_side_effects",
                            "request_id": request.get("request_id", ""),
                            "workflow_key": workflow_key,
                            "bead_id": request.get("bead_id", ""),
                            "metadata_key": key,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 1

    common.remove_workflow_link(workflow_key)
    print(
        json.dumps(
            {
                "status": "released",
                "request_id": request.get("request_id", ""),
                "workflow_key": workflow_key,
                "repository": request.get("repository_full_name", ""),
                "issue_number": request.get("issue_number", ""),
                "command": request.get("command", ""),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
