#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json

import github_intake_common as common


def split_repository(value: str) -> tuple[str, str]:
    owner, sep, repo = value.strip().partition("/")
    if not owner or not sep or not repo:
        raise SystemExit("repository must be in owner/repo format")
    return owner, repo


def read_body(args: argparse.Namespace) -> str:
    if args.body_file:
        with open(args.body_file, "r", encoding="utf-8") as handle:
            return handle.read()
    return args.body


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a pull request via the workspace GitHub App")
    parser.add_argument("repository", help="owner/repo")
    parser.add_argument("--installation-id", default="", help="GitHub App installation id")
    parser.add_argument(
        "--github-app-identity",
        default="",
        help="separately scoped delivery GitHub App identity",
    )
    parser.add_argument("--base", required=True, help="base branch")
    parser.add_argument("--head", required=True, help="head branch")
    parser.add_argument("--title", required=True, help="PR title")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--body", default="", help="PR markdown body")
    group.add_argument("--body-file", default="", help="path to a markdown file")
    args = parser.parse_args()

    try:
        app_cfg = common.github_app_config_for_identity(args.github_app_identity)
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    if not isinstance(app_cfg, dict) or not app_cfg.get("app_id") or not app_cfg.get("private_key_pem"):
        raise SystemExit("GitHub App configuration is incomplete")
    installation_id = str(args.installation_id or app_cfg.get("installation_id", "")).strip()
    if not installation_id:
        raise SystemExit("GitHub App installation id is required")
    owner, repo = split_repository(args.repository)
    pull_request = common.create_pull_request(
        app_cfg,
        installation_id,
        owner,
        repo,
        args.title,
        args.head,
        args.base,
        read_body(args),
    )
    print(json.dumps(pull_request, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
