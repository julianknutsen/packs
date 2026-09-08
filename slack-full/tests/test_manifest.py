"""Structural validation for manifest/app.json (pack-relative).

This pack ships a canonical Slack app manifest at ``manifest/app.json``
(pack-relative — i.e. relative to this slack-full/ directory). The
manifest is the source of truth for the slack-full's OAuth scopes,
bot events, and slash commands.

These tests guard against accidental breakage of that file:

  * the JSON parses cleanly,
  * the top-level keys Slack's manifest schema requires are present,
  * the scopes and bot_events arrays are non-empty (catches a
    well-meaning edit that empties them out),
  * scopes are unique and sorted (style guard — keeps diffs readable).

Schema reference: https://api.slack.com/reference/manifests
"""

from __future__ import annotations

import json
import pathlib

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
MANIFEST_PATH = PACK_DIR / "manifest" / "app.json"
AGENT_MANIFEST_PATH = PACK_DIR / "manifest" / "agent-app.json"


@pytest.fixture(scope="module")
def manifest() -> dict:
    assert MANIFEST_PATH.is_file(), f"manifest missing: {MANIFEST_PATH}"
    with MANIFEST_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def agent_manifest() -> dict:
    assert AGENT_MANIFEST_PATH.is_file(), (
        f"agent manifest missing: {AGENT_MANIFEST_PATH}"
    )
    with AGENT_MANIFEST_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def test_manifest_parses_as_json(manifest: dict) -> None:
    assert isinstance(manifest, dict)


def test_manifest_has_display_information(manifest: dict) -> None:
    di = manifest.get("display_information")
    assert isinstance(di, dict), "display_information must be an object"
    assert di.get("name"), "display_information.name is required"
    assert di.get("description"), "display_information.description is required"


def test_manifest_has_bot_user(manifest: dict) -> None:
    bot = manifest.get("features", {}).get("bot_user")
    assert isinstance(bot, dict), "features.bot_user must be an object"
    assert bot.get("display_name"), "features.bot_user.display_name is required"


def test_manifest_declares_bot_scopes(manifest: dict) -> None:
    scopes = manifest.get("oauth_config", {}).get("scopes", {}).get("bot")
    assert isinstance(scopes, list) and scopes, (
        "oauth_config.scopes.bot must be a non-empty array"
    )
    # Every scope the live adapter actually relies on must be present.
    # If the adapter starts using something new, add it here AND to the
    # manifest in the same change.
    # Each subscribed message.* bot event Slack delivers requires the
    # matching *:history scope on the install. If we subscribe to
    # message.channels but only declare im:history, Slack rejects the
    # install or silently drops the channel subscription. Lock the
    # invariant scope-by-event into this assertion.
    required = {
        "channels:history",
        # company rooms: verify switchboard membership of public
        # directory rooms (conversations.info / conversations.members)
        "channels:read",
        "chat:write",
        "chat:write.customize",
        "files:read",
        "files:write",
        "groups:history",
        # company rooms: same membership check for private rooms
        "groups:read",
        "im:history",
        # DM privacy gate: conversations.info membership probe on every
        # message.im inbound (adapter/dm_gate.go). Without im:read the
        # probe returns missing_scope, the gate fails closed, and every
        # legitimate DM is dropped.
        "im:read",
        "mpim:history",
        "reactions:write",
        # company rooms: bots.info author resolution for peer trust
        "users:read",
    }
    missing = required - set(scopes)
    assert not missing, f"manifest missing required bot scopes: {sorted(missing)}"


def test_manifest_scopes_unique_and_sorted(manifest: dict) -> None:
    scopes = manifest["oauth_config"]["scopes"]["bot"]
    assert len(scopes) == len(set(scopes)), "duplicate scopes detected"
    assert scopes == sorted(scopes), "scopes must be sorted alphabetically"


def test_manifest_subscribes_to_required_bot_events(manifest: dict) -> None:
    events = (
        manifest.get("settings", {})
        .get("event_subscriptions", {})
        .get("bot_events")
    )
    assert isinstance(events, list) and events, (
        "settings.event_subscriptions.bot_events must be a non-empty array"
    )
    # The adapter dispatches across all four message channel-type
    # buckets plus app_mention. Drop one and that channel type goes
    # silent; the assertion locks the full set in.
    required = {
        "app_mention",
        "message.channels",
        "message.groups",
        "message.im",
        "message.mpim",
    }
    missing = required - set(events)
    assert not missing, f"manifest missing required bot events: {sorted(missing)}"


def test_manifest_bot_events_unique_and_sorted(manifest: dict) -> None:
    events = manifest["settings"]["event_subscriptions"]["bot_events"]
    assert len(events) == len(set(events)), "duplicate bot events detected"
    assert events == sorted(events), "bot events must be sorted alphabetically"


def test_manifest_slash_commands_field_present(manifest: dict) -> None:
    # gc-cby.2 (sync-commands) needs this field to exist as a list so
    # it can append/diff. Empty is fine today; missing is not.
    cmds = manifest.get("features", {}).get("slash_commands")
    assert isinstance(cmds, list), "features.slash_commands must be a list"


# --- Agent identity app template (manifest/agent-app.json) -----------
#
# Company-rooms provisioning stamps one Slack app per named agent from
# this template (see docs/company-rooms.md → Membership and
# Provisioning). Its surface is deliberately minimal; these tests lock
# that minimality in so a well-meaning edit can't silently widen it.


def test_agent_manifest_parses_as_json(agent_manifest: dict) -> None:
    assert isinstance(agent_manifest, dict)


def test_agent_manifest_has_display_information(agent_manifest: dict) -> None:
    di = agent_manifest.get("display_information")
    assert isinstance(di, dict), "display_information must be an object"
    assert di.get("name"), "display_information.name is required"


def test_agent_manifest_has_bot_user(agent_manifest: dict) -> None:
    bot = agent_manifest.get("features", {}).get("bot_user")
    assert isinstance(bot, dict), "features.bot_user must be an object"
    assert bot.get("display_name"), "features.bot_user.display_name is required"


def test_agent_manifest_scopes_are_dm_phase_minimal(agent_manifest: dict) -> None:
    # Phase 4 (per-agent DMs) + Phase 4b (group DMs) widen the agent app's scopes
    # exactly to the direct-message-actor set (specs §Wire/manifest changes):
    # chat:write (outbound sender) + im:history (DM hydration) + mpim:history
    # (group-DM hydration + membership probe) + reactions:write (the ack actor is
    # the agent app itself). Nothing beyond these four is justified — the
    # switchboard stays the single room-admission owner.
    scopes = agent_manifest.get("oauth_config", {}).get("scopes", {}).get("bot")
    assert scopes == ["chat:write", "im:history", "mpim:history", "reactions:write"], (
        "agent app must declare exactly [chat:write, im:history, mpim:history, "
        f"reactions:write], got {scopes!r}"
    )
    assert scopes == sorted(scopes) and len(scopes) == len(set(scopes))


def test_agent_manifest_messages_tab_enabled(agent_manifest: dict) -> None:
    # Hard Slack prerequisite for the per-agent DM phase: humans cannot
    # DM a bot whose App Home Messages tab is disabled.
    home = agent_manifest.get("features", {}).get("app_home")
    assert isinstance(home, dict), "features.app_home must be an object"
    assert home.get("messages_tab_enabled") is True, (
        "app_home.messages_tab_enabled must be true"
    )
    assert home.get("messages_tab_read_only_enabled") is False, (
        "app_home.messages_tab_read_only_enabled must be false"
    )


def test_agent_manifest_subscribes_to_direct_message_events(agent_manifest: dict) -> None:
    # Phase 4 + 4b: the agent app subscribes to its own direct-message events —
    # message.im (1:1 DMs) and message.mpim (group DMs) — and NOTHING else. It
    # must never observe room traffic (message.channels/groups or app_mention),
    # which would make it a second admission owner; the switchboard remains the
    # single room-admission owner. The event Request URL is operator-supplied on
    # save (same public funnel), so the template carries no request_url.
    subs = agent_manifest.get("settings", {}).get("event_subscriptions", {})
    events = subs.get("bot_events")
    assert events == ["message.im", "message.mpim"], (
        f"agent app must subscribe to exactly [message.im, message.mpim], got {events!r}"
    )


def test_agent_manifest_interactivity_and_rotation_off(agent_manifest: dict) -> None:
    settings = agent_manifest.get("settings", {})
    assert settings.get("interactivity", {}).get("is_enabled") is False, (
        "agent app interactivity must be disabled"
    )
    assert settings.get("token_rotation_enabled") is False, (
        "agent app token_rotation_enabled must be false (pilot uses long-lived tokens)"
    )
