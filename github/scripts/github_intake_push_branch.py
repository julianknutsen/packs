#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json

import github_intake_common as common


def main() -> int:
    parser = argparse.ArgumentParser(description="Push a branch via the workspace GitHub App")
    parser.add_argument("repository", help="owner/repo")
    parser.add_argument("--installation-id", default="", help="GitHub App installation id")
    parser.add_argument(
        "--github-app-identity",
        default="",
        help="separately scoped delivery GitHub App identity",
    )
    parser.add_argument("--branch", required=True, help="branch name to create or update")
    parser.add_argument("--ref", default="HEAD", help="source ref to push")
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
    result = common.git_push_branch(
        app_cfg,
        installation_id,
        args.repository,
        args.branch,
        ref=args.ref,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
