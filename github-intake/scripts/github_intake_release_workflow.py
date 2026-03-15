#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys

import github_intake_common as common


def main() -> int:
    parser = argparse.ArgumentParser(description="Release a stuck /gc workflow lock for a GitHub issue")
    parser.add_argument("repository", help="owner/repo")
    parser.add_argument("issue_number", help="GitHub issue number")
    parser.add_argument("--command", default="fix", help="slash command name to unlock (default: fix)")
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
