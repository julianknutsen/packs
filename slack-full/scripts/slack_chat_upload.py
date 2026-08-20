#!/usr/bin/env python3
"""Upload a file to a session's bound Slack channel.

Two routing paths are supported, mirroring ``slack_chat_reply_current``:

* **gc-routed (default)** — POST to gc's
  ``/v0/city/{city}/extmsg/outbound-file``. gc records the upload in the
  conversation transcript, fans out a peer notification to other
  sessions bound to the same room, and emits an ``extmsg.outbound``
  event. This is the production path because it keeps the multi-session
  coordination story consistent between text and files.

* **adapter-direct (``--via adapter``)** — POST straight to the local
  adapter's ``/publish-file``. Bypasses gc entirely; no transcript, no
  peer fanout. Kept as a fast path for adapter-only smoke tests and
  diagnostics — when the gc API is down or you're testing the adapter
  in isolation.

Either path delegates to the adapter (``adapter/``, pack-relative),
which handles Slack's three-step files-upload-v2 protocol
(``files.getUploadURLExternal`` → ``PUT`` bytes →
``files.completeUploadExternal``).

The bot token must hold the ``files:write`` scope. Without it, the
adapter returns ``failure_kind=auth`` with ``error=missing_scope`` and
the post falls through cleanly (no exception); the user can grant the
scope without restarting anything and the next upload picks it up.

Files post under the bot's default identity, NOT the per-session
``chat:write.customize`` identity — Slack's file-upload API doesn't
honor that override on the file post itself. Use a chat reply
(``gc slack reply-current``) when identity matters more than the file
preview, or pair an upload with a follow-up reply.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

import slack_intake_common as common
import slack_mrkdwn


def _resolve_conversation(session_id: str) -> dict[str, str]:
    binding = common.look_up_binding(session_id)
    if not binding:
        raise SystemExit(
            f"session {session_id!r} has no active extmsg binding; "
            f"run `gc slack bind-dm` or `gc slack bind-room` first")
    return {
        "scope_id": binding.get("scope_id", common.gc_city_name()),
        "provider": binding.get("provider", "slack"),
        "account_id": binding.get("account_id",
                                  os.environ.get("SLACK_WORKSPACE_ID", "")),
        "conversation_id": binding.get("conversation_id", ""),
        "kind": binding.get("kind", "dm"),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Upload a file to a session's bound Slack channel "
                    "via the local adapter (files-upload-v2)",
    )
    parser.add_argument("--file", dest="file_path", required=True,
                        help="Path to the local file to upload.")
    parser.add_argument("--session", default="",
                        help="Session id whose binding to upload into. "
                             "Defaults to the current session ($GC_SESSION_ID).")
    parser.add_argument("--filename", default="",
                        help="Override the displayed filename. Defaults to "
                             "basename(--file).")
    parser.add_argument("--title", default="",
                        help="Display title in Slack. Defaults to filename.")
    parser.add_argument("--initial-comment", default="",
                        help="Comment posted alongside the file (acts as the "
                             "message body for threading purposes).")
    parser.add_argument("--thread-ts", default="",
                        help="Slack message ts to thread under. Mutually "
                             "exclusive with --thread-current.")
    parser.add_argument("--thread-current", action="store_true",
                        help="Thread under the latest inbound for this session "
                             "(same logic as `gc slack reply-current`). "
                             "Mutually exclusive with --thread-ts.")
    parser.add_argument("--idempotency-key", default="",
                        help="Caller-supplied idempotency key for retries.")
    parser.add_argument(
        "--raw", action="store_true",
        help=("Send --initial-comment verbatim, skipping the accidental-"
              "mrkdwn guard (by default, tildes that would pair into "
              "unintended Slack strikethrough are neutralized)."))
    parser.add_argument("--via", choices=("gc", "adapter"), default="gc",
                        help="Routing path. 'gc' (default) records the upload "
                             "in the transcript and fans out to peer sessions; "
                             "'adapter' bypasses gc for diagnostics only.")
    args = parser.parse_args(argv)

    if args.thread_ts and args.thread_current:
        raise SystemExit("pass --thread-ts OR --thread-current, not both")

    file_path = pathlib.Path(args.file_path).expanduser().resolve()
    if not file_path.exists():
        raise SystemExit(f"file not found: {file_path}")
    if not file_path.is_file():
        raise SystemExit(f"not a regular file: {file_path}")

    session_id = (args.session or "").strip()
    if not session_id:
        try:
            session_id = common.current_session_id()
        except common.GCAPIError as exc:
            raise SystemExit(str(exc)) from exc

    conv = _resolve_conversation(session_id)
    if not conv.get("conversation_id"):
        raise SystemExit(
            f"session {session_id!r} binding has no conversation_id "
            "(corrupt binding record?)")

    initial_comment = args.initial_comment
    if initial_comment and not args.raw:
        # gp-o42: the comment renders as mrkdwn alongside the file.
        initial_comment = slack_mrkdwn.escape_accidental_mrkdwn(initial_comment)

    thread_ts = args.thread_ts.strip()
    if args.thread_current:
        match = common.find_latest_inbound_message_id_for_session(session_id)
        if not match:
            raise SystemExit(
                f"session {session_id!r} has no inbound to thread under; "
                "pass --thread-ts <ts> explicitly or omit threading")
        thread_ts = match[0]

    try:
        if args.via == "adapter":
            result = common.upload_via_adapter(
                session_id=session_id,
                conversation_id=conv["conversation_id"],
                kind=conv["kind"],
                file_path=str(file_path),
                filename=args.filename,
                initial_comment=initial_comment,
                thread_ts=thread_ts,
                title=args.title,
                idempotency_key=args.idempotency_key,
            )
        else:
            result = common.upload_via_gc_outbound_file(
                session_id=session_id,
                scope_id=conv["scope_id"],
                provider=conv["provider"],
                account_id=conv["account_id"],
                conversation_id=conv["conversation_id"],
                kind=conv["kind"],
                file_path=str(file_path),
                filename=args.filename,
                initial_comment=initial_comment,
                thread_ts=thread_ts,
                title=args.title,
                idempotency_key=args.idempotency_key,
            )
    except (common.AdapterError, common.GCAPIError) as exc:
        raise SystemExit(str(exc)) from exc

    print(json.dumps({
        "session_id": session_id,
        "conversation_id": conv["conversation_id"],
        "kind": conv["kind"],
        "file_path": str(file_path),
        "thread_ts": thread_ts,
        "via": args.via,
        "result": result,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
