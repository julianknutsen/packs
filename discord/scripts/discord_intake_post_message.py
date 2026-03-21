#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import pathlib
import sys

import discord_intake_common as common


def _load_body(args: argparse.Namespace) -> str:
    if args.body:
        return args.body
    if args.body_file:
        return pathlib.Path(args.body_file).read_text(encoding="utf-8")
    raise SystemExit("either --body or --body-file is required")


def _resolve_target(args: argparse.Namespace) -> str:
    if args.request_id:
        request = common.load_request(args.request_id)
        if not request:
            raise SystemExit(f"request not found: {args.request_id}")
        thread_id = str(request.get("thread_id", "")).strip()
        if thread_id:
            return thread_id
        channel_id = str(request.get("channel_id", "")).strip()
        if channel_id:
            return channel_id
        raise SystemExit(f"request has no message target: {args.request_id}")
    if args.thread_id:
        return args.thread_id
    if args.channel_id:
        return args.channel_id
    raise SystemExit("either --request-id or --channel-id is required")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Post a message using the Discord workspace bot")
    parser.add_argument("--request-id", default="", help="Saved request id to route back to the original conversation")
    parser.add_argument("--channel-id", default="", help="Discord channel id")
    parser.add_argument("--thread-id", default="", help="Discord thread id")
    parser.add_argument("--body", default="", help="Inline message body")
    parser.add_argument("--body-file", default="", help="Read the message body from a file")
    args = parser.parse_args(argv)

    target_channel = _resolve_target(args)
    body = _load_body(args)
    response = common.post_channel_message(target_channel, body)
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
