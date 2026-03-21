#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import pathlib
import sys

import discord_intake_common as common


def workflow_key_from_args(args: argparse.Namespace) -> tuple[str, str]:
    if args.request_id:
        request = common.load_request(args.request_id)
        if not request:
            raise SystemExit(f"request not found: {args.request_id}")
        workflow_key = str(request.get("workflow_key", "")).strip()
        if not workflow_key:
            raise SystemExit(f"request has no workflow key: {args.request_id}")
        return workflow_key, str(request.get("request_id", "")).strip()
    if not args.guild_id or not args.conversation_id:
        raise SystemExit("either --request-id or <guild_id> <conversation_id> is required")
    workflow_key = common.build_workflow_key(args.guild_id, args.conversation_id, args.command)
    linked = common.load_workflow_link(workflow_key) or {}
    return workflow_key, str(linked.get("request_id", "")).strip()


def release_matching_workflows(workflow_prefix: str) -> tuple[bool, list[str]]:
    released_keys: list[str] = []
    for path in pathlib.Path(common.workflows_dir()).glob("*.json"):
        payload = common.read_json(str(path), {}, allow_invalid=True)
        if not isinstance(payload, dict):
            continue
        workflow_key = str(payload.get("workflow_key", "")).strip()
        if workflow_key != workflow_prefix and not workflow_key.startswith(workflow_prefix + ":rig:"):
            continue
        common.remove_workflow_link(workflow_key)
        released_keys.append(workflow_key)
    return bool(released_keys), released_keys


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Release a stuck Discord workflow lock")
    parser.add_argument("guild_id", nargs="?", help="Discord guild id")
    parser.add_argument("conversation_id", nargs="?", help="Discord conversation id (channel or thread)")
    parser.add_argument("--request-id", default="", help="Release the workflow key recorded on an existing request")
    parser.add_argument("--command", default="fix", help="Slash command name, default: fix")
    args = parser.parse_args(argv)

    workflow_key, request_id = workflow_key_from_args(args)
    exit_code = 0
    if request_id:
        released_ok = common.remove_workflow_link_if_request(workflow_key, request_id)
        if not released_ok:
            exit_code = 1
    else:
        released_ok, released_keys = release_matching_workflows(workflow_key)

    released = {
        "workflow_key": workflow_key,
        "released": released_ok,
    }
    if not request_id:
        released["released_keys"] = released_keys if released_ok else []
    if request_id and released_ok:
        request = common.load_request(request_id)
        if request:
            request["workflow_released_at"] = common.utcnow()
            common.save_request(request)
            released["request_id"] = request_id
    print(json.dumps(released, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
