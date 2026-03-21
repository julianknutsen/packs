#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys

import discord_intake_common as common


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Map a Discord guild rig name to a workflow target")
    parser.add_argument("guild_id", help="Discord guild id")
    parser.add_argument("rig_name", help="Rig name used in /gc fix <rig>")
    parser.add_argument("target", help="gc sling target, usually rig/pool")
    parser.add_argument("--fix-formula", default=common.FIX_FORMULA_DEFAULT, help="Formula for /gc fix")
    args = parser.parse_args(argv)

    try:
        config = common.set_rig_mapping(
            common.load_config(),
            args.guild_id,
            args.rig_name,
            args.target,
            args.fix_formula,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    key = common.normalize_rig_key(args.guild_id, args.rig_name)
    print(json.dumps(config["rigs"][key], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
