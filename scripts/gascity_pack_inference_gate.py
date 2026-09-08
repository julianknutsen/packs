#!/usr/bin/env python3
"""Run model-backed inference gates for first-class Gas City packs.

The gate builds a disposable city and rig, imports a local pack at city scope,
imports gascity/roles at rig scope when needed, then runs real formulas against
known subjects. Review gates verify produced review artifacts. Build gates ask
the selected pack's build formula to make a code change and then execute the
fixture tests in the resulting implementation worktree. The Gastown gate checks
orchestration agents and runs a bounded review-leg workflow through polecat.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
REVIEW_GATE = "review"
BUILD_GATE = "build"
BUILD_BASIC_GATE = "build-basic"
GASTOWN_ORCHESTRATION_GATE = "gastown-orchestration"
SMOKE_GATE = "smoke"
ALL_GATE = "all"
GASCITY_PACK = "gascity"
GASTOWN_PACK = "gastown"
MODEL_SMOKE_PACKS = ("superpowers", "compound-engineering", "gstack", "bmad", GASTOWN_PACK)
GASCITY_REMOTE_SOURCE = "https://github.com/gastownhall/gascity.git"
BEADS_MODULE = "github.com/steveyegge/beads"
REVIEW_SUBJECT_PATH = Path(".gc/inference-gate/review-subject.diff")
REVIEW_REPORT_PATH = Path(".gc/inference-gate/review-report.md")
REVIEW_REPORT_METADATA_KEYS = (
    "gc.build.code_review_report_path",
    "gc.build.review_report_path",
    "code_review.report_path",
    "code_review.review_report_path",
    "gc.var.report_path",
    "report_path",
)
METHODOLOGY_REVIEW_REPORT_FALLBACKS = (
    Path(".gc/inference-gate/artifacts/review-fix-summary.md"),
    Path(".gc/inference-gate/artifacts/implementation-review-report.md"),
    Path(".gc/inference-gate/artifacts/gap-analysis-report.md"),
)
REVIEW_FORMULA = "review"
REVIEW_TITLE = "gascity pack inference gate: review"
BUILD_BASIC_FORMULA = "build-basic"
BUILD_ARTIFACT_ROOT = Path(".gc/inference-gate/build-basic")
BUILD_TITLE = "gascity pack inference gate: build-basic"
BUILD_SOURCE_TITLE = "Implement slugify and make pytest pass"
GASTOWN_REVIEW_TITLE = "Gastown orchestration gate: review leg"
GASTOWN_REVIEW_ASSIGNMENT_TITLE = "Review Gastown orchestration gate fixture"
SMOKE_TITLE_PREFIX = "RC model smoke"
GASTOWN_ALWAYS_ON_AGENTS = ("mayor", "deacon", "boot", "witness")
GASTOWN_FORMULA_CONTRACTS = {
    "mol-review-leg": (
        "write the FULL report into the bead notes",
        "gc bd update \"$WORK_BEAD_ID\" --notes",
        "gc mail send \"$COORD\"",
        "gc bd update \"$WORK_BEAD_ID\" --status=closed",
    ),
    "mol-idea-to-plan": (
        "review task beads",
        "gc sling \"$REVIEW_TARGET\" \"$LEG_BEAD\" --on {{review_formula}}",
        "review_phase=<phase>",
        "Convert the refined plan into beads",
        "gc bd dep add",
    ),
    "mol-polecat-work": (
        'extends = ["mol-polecat-base"]',
        "git worktree add",
        "--set-metadata branch=\"$BRANCH\"",
        "{{test_command}}",
        "REFINERY_TARGET=\"${GC_RIG:+$GC_RIG/}{{binding_prefix}}refinery\"",
        "--assignee=\"$REFINERY_TARGET\"",
    ),
    "mol-refinery-patrol": (
        "metadata.branch",
        "fast-forward merge",
        "run tests before merging",
        "metadata.target",
        "closes the bead",
    ),
    "mol-witness-patrol": (
        "Orphaned bead recovery",
        "metadata.work_dir",
        "return beads to the pool",
        "gc session list --state=all --json",
    ),
}
GASTOWN_BUILD_WORKFLOW_CONTRACTS = {
    "mol-polecat-work": (
        "EXPECTED_BRANCH=\"polecat/$WORK_BEAD_ID\"",
        "{{typecheck_command}}",
        "{{lint_command}}",
        "{{build_command}}",
        "{{test_command}}",
        "git push origin HEAD",
        "gc bd update \"$WORK_BEAD_ID\" \\",
        "--set-metadata target={{base_branch}}",
        "--status=open --assignee=\"$REFINERY_TARGET\"",
        "gc session wake \"$REFINERY_TARGET\"",
        "gc runtime drain-ack",
    ),
    "mol-refinery-patrol": (
        "gc bd list ${GC_RIG:+--rig=\"$GC_RIG\"} --assignee=$GC_AGENT --status=open",
        "git rebase origin/$TARGET",
        "{{typecheck_command}}",
        "{{lint_command}}",
        "{{build_command}}",
        "{{test_command}}",
        "branch_has_real_change",
        "branch_missing_patches",
        'git worktree add --detach "$MERGE_WT" "origin/$TARGET"',
        'git -C "$MERGE_WT" merge --ff-only "$TEMP_SHA"',
        'halt_unverified_merge "origin/$TARGET" "origin/$BRANCH" "$MISSING_PATCHES"',
        "--set-metadata merge_result=verification_failed",
        "--set-metadata merge_result=merged",
        '--set-metadata merged_sha="$MERGED_SHA"',
        'gc bd close "$WORK" --reason "Merged to $TARGET at $MERGED_SHORT"',
        "gh pr create",
        "--set-metadata pr_url=\"$PR_URL\"",
        "gc bd close $WORK --reason \"Pull request ready: $PR_URL\"",
    ),
    "mol-witness-patrol": (
        "LIVENESS_MAP=$(jq -n",
        "FAIL-SAFE: empty liveness map",
        "git push origin HEAD",
        "gc workflow delete-source <bead> --apply && gc workflow reopen-source <bead>",
        "gc bd update <bead> --set-metadata recovered=true",
        "gc session nudge <rig>/{{binding_prefix}}refinery",
        "--labels=warrant",
        "\"gc.routed_to\":\"{{binding_prefix}}dog\"",
    ),
    "mol-deacon-patrol": (
        "Work-layer health",
        "queue-starvation-check",
        "gc agents list --json --active",
        "gc bd create --type=task --labels=warrant",
        "\"gc.routed_to\":\"{{binding_prefix}}dog\"",
    ),
    "mol-idea-to-plan": (
        "Dispatch 6 PRD review legs in parallel",
        "Dispatch 6 design legs",
        "Run 3 PRD-alignment rounds",
        "Run 3 plan self-review rounds",
        "gc sling \"$REVIEW_TARGET\" \"$LEG_BEAD\" --on {{review_formula}}",
        "gc bd dep add",
    ),
}
METHODOLOGY_FLOW_CONTRACTS = {
    "superpowers": {
        "review_expansion": "superpowers-code-review",
        "build_steps": {
            "requirements": {
                "run_target": "superpowers.brainstorming",
                "artifact_schema": "gc.build.requirements.v1",
                "expand": "superpowers-brainstorming",
            },
            "plan": {
                "run_target": "superpowers.writing-plans",
                "artifact_schema": "gc.build.plan.v1",
                "check": "build-artifact-valid.sh",
            },
            "plan-review": {
                "run_target": "superpowers.plan-reviewer",
                "expand": "superpowers-plan-review",
            },
            "decompose": {
                "run_target": "gc.task-decomposer",
                "artifact_schema": "gc.build.decomposition.v1",
                "check": "build-artifact-valid.sh",
            },
            "implement": {
                "run_target": "{{implementation_target}}",
                "drain_formula": "superpowers-development",
                "drain_context": "separate",
            },
            "implement-same-session": {
                "run_target": "{{implementation_target}}",
                "drain_formula": "superpowers-development-item",
                "drain_context": "shared",
                "single_lane": True,
            },
            "review": {
                "run_target": "superpowers.code-reviewer",
                "artifact_schema": "gc.build.review.v1",
                "expand": "superpowers-code-review",
                "needs": ("summarize-implementation",),
            },
            "finalize": {
                "run_target": "superpowers.finisher",
                "artifact_schema": "gc.build.final-report.v1",
                "check": "build-artifact-valid.sh",
            },
        },
        "expansion_routes": {
            "superpowers-code-review": (
                "superpowers.code-reviewer",
                "superpowers.code-quality-reviewer",
                "{implementation_target}",
            ),
            "superpowers-plan-review": ("superpowers.plan-reviewer", "superpowers.writing-plans"),
            "superpowers-brainstorming": ("superpowers.brainstorming", "superpowers.spec-reviewer"),
        },
        "expansion_checks": {
            "superpowers-code-review": "implementation-review-approved.sh",
            "superpowers-plan-review": "design-review-approved.sh",
            "superpowers-brainstorming": "design-review-approved.sh",
        },
    },
    "compound-engineering": {
        "review_expansion": "compound-code-review",
        "build_steps": {
            "requirements": {
                "run_target": "compound-engineering.ce-brainstorm",
                "artifact_schema": "gc.build.requirements.v1",
                "check": "build-artifact-valid.sh",
            },
            "plan": {
                "run_target": "compound-engineering.ce-plan",
                "artifact_schema": "gc.build.plan.v1",
                "check": "build-artifact-valid.sh",
            },
            "plan-review": {
                "run_target": "compound-engineering.ce-plan-review-synthesizer",
                "expand": "compound-plan-review",
            },
            "implement": {
                "run_target": "{{implementation_target}}",
                "drain_formula": "compound-work",
                "drain_context": "separate",
            },
            "implement-same-session": {
                "run_target": "{{implementation_target}}",
                "drain_formula": "compound-work-item",
                "drain_context": "shared",
                "single_lane": True,
            },
            "review": {
                "run_target": "compound-engineering.ce-code-review-synthesizer",
                "artifact_schema": "gc.build.review.v1",
                "expand": "compound-code-review",
                "needs": ("summarize-implementation",),
            },
            "finalize": {
                "run_target": "compound-engineering.ce-compound",
                "artifact_schema": "gc.build.final-report.v1",
                "expand": "compound-resolution",
            },
        },
        "expansion_routes": {
            "compound-code-review": (
                "compound-engineering.ce-code-review-selector",
                "compound-engineering.ce-correctness-reviewer",
                "compound-engineering.ce-testing-reviewer",
                "compound-engineering.ce-maintainability-reviewer",
                "compound-engineering.ce-security-reviewer",
                "compound-engineering.ce-code-review-synthesizer",
                "{implementation_target}",
            ),
            "compound-plan-review": (
                "compound-engineering.ce-coherence-reviewer",
                "compound-engineering.ce-feasibility-reviewer",
                "compound-engineering.ce-scope-guardian-reviewer",
                "compound-engineering.ce-architecture-strategist",
                "compound-engineering.ce-plan-review-synthesizer",
                "compound-engineering.ce-plan",
            ),
            "compound-resolution": (
                "compound-engineering.ce-pr-comment-resolver",
                "compound-engineering.ce-compound",
            ),
        },
        "expansion_checks": {
            "compound-code-review": "implementation-review-approved.sh",
            "compound-plan-review": "design-review-approved.sh",
        },
    },
    "gstack": {
        "review_expansion": "gstack-code-review",
        "build_steps": {
            "requirements": {
                "run_target": "gstack.office-hours",
                "artifact_schema": "gc.build.requirements.v1",
                "check": "build-artifact-valid.sh",
            },
            "plan": {
                "run_target": "gstack.founder-reviewer",
                "artifact_schema": "gc.build.plan.v1",
                "check": "build-artifact-valid.sh",
            },
            "plan-review": {
                "run_target": "gstack.review-synthesizer",
                "expand": "gstack-plan-review",
            },
            "decompose": {
                "run_target": "gstack.decomposer",
                "artifact_schema": "gc.build.decomposition.v1",
                "check": "build-artifact-valid.sh",
            },
            "implement": {
                "run_target": "{{implementation_target}}",
                "drain_formula": "gstack-work",
                "drain_context": "separate",
            },
            "implement-same-session": {
                "run_target": "{{implementation_target}}",
                "drain_formula": "gstack-work-item",
                "drain_context": "shared",
                "single_lane": True,
            },
            "review": {
                "run_target": "gstack.review-synthesizer",
                "artifact_schema": "gc.build.review.v1",
                "expand": "gstack-code-review",
                "needs": ("summarize-implementation",),
            },
            "qa": {
                "run_target": "gstack.qa-lead",
                "expand": "gstack-qa-review",
                "needs": ("review",),
            },
            "release-readiness": {
                "run_target": "gstack.release-engineer",
                "expand": "gstack-release-readiness",
                "needs": ("qa",),
            },
            "finalize": {
                "run_target": "gstack.release-engineer",
                "artifact_schema": "gc.build.final-report.v1",
                "needs": ("release-readiness",),
                "check": "build-artifact-valid.sh",
            },
            "publish": {
                "run_target": "gc.publisher",
                "needs": ("finalize",),
            },
        },
        "expansion_routes": {
            "gstack-code-review": (
                "gstack.staff-reviewer",
                "gstack.qa-lead",
                "gstack.security-officer",
                "gstack.review-synthesizer",
                "{implementation_target}",
            ),
            "gstack-plan-review": (
                "gstack.founder-reviewer",
                "gstack.design-reviewer",
                "gstack.eng-reviewer",
                "gstack.devex-reviewer",
                "gstack.review-synthesizer",
            ),
            "gstack-qa-review": (
                "gstack.qa-lead",
                "gstack.staff-reviewer",
                "gstack.review-synthesizer",
                "{implementation_target}",
            ),
            "gstack-release-readiness": (
                "gstack.docs-engineer",
                "gstack.release-engineer",
                "gstack.review-synthesizer",
            ),
        },
        "expansion_checks": {
            "gstack-code-review": "implementation-review-approved.sh",
            "gstack-plan-review": "design-review-approved.sh",
            "gstack-qa-review": "implementation-review-approved.sh",
            "gstack-release-readiness": "implementation-review-approved.sh",
        },
    },
    "bmad": {
        "review_expansion": "bmad-code-review-flow",
        "build_steps": {
            "requirements": {
                "run_target": "bmad.prd-writer",
                "artifact_schema": "gc.build.requirements.v1",
                "check": "build-artifact-valid.sh",
            },
            "plan": {
                "run_target": "bmad.architect",
                "artifact_schema": "gc.build.plan.v1",
                "check": "build-artifact-valid.sh",
            },
            "plan-review": {
                "run_target": "bmad.architect",
                "needs": ("plan",),
            },
            "decompose": {
                "run_target": "bmad.epic-story-decomposer",
                "artifact_schema": "gc.build.decomposition.v1",
                "check": "build-artifact-valid.sh",
            },
            "implementation-readiness": {
                "run_target": "bmad.readiness-reviewer",
                "needs": ("decompose",),
            },
            "implement": {
                "run_target": "{{implementation_target}}",
                "drain_formula": "bmad-story-development",
                "drain_context": "separate",
                "needs": ("implementation-readiness",),
            },
            "implement-same-session": {
                "run_target": "{{implementation_target}}",
                "drain_formula": "bmad-story-development-item",
                "drain_context": "shared",
                "single_lane": True,
                "needs": ("implementation-readiness",),
            },
            "review": {
                "run_target": "bmad.bmad-review-synthesizer",
                "artifact_schema": "gc.build.review.v1",
                "expand": "bmad-code-review-flow",
                "needs": ("summarize-implementation",),
            },
        },
        "expansion_routes": {
            "bmad-code-review-flow": (
                "bmad.blind-hunter-reviewer",
                "bmad.edge-case-reviewer",
                "bmad.acceptance-auditor",
                "bmad.story-self-checker",
                "bmad.bmad-review-synthesizer",
                "{implementation_target}",
            ),
        },
        "expansion_checks": {
            "bmad-code-review-flow": "implementation-review-approved.sh",
        },
    },
}
BUILD_BASIC_ARTIFACT_CONTRACTS = (
    ("gc.build.requirements_path", "gc.build.requirements.v1"),
    ("gc.build.plan_path", "gc.build.plan.v1"),
    ("gc.build.decomposition_path", "gc.build.decomposition.v1"),
    ("gc.build.implementation_summary_path", "gc.build.implementation-summary.v1"),
    ("gc.build.review_report_path", "gc.build.review.v1"),
    ("gc.build.final_report_path", "gc.build.final-report.v1"),
)
DEFAULT_GATE = "all"
DEFAULT_TIMEOUT = "75m"
DEFAULT_POLL_INTERVAL = "5s"
BD_LIST_LIMIT = "1000"
DEFAULT_INFERENCE_MODEL = "kimi-k2.7-code"
INFERENCE_EXPECTED_MODEL_ENV = "GC_INFERENCE_EXPECTED_MODEL"
INFERENCE_MODEL_ENV_KEYS = (
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "CLAUDE_CODE_SUBAGENT_MODEL",
)
INHERITED_ENV_KEYS = (
    "PATH",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "USER",
    "HOME",
    "SHELL",
    "SSH_AUTH_SOCK",
    "TERM",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
    "CLAUDE_CODE_EFFORT_LEVEL",
    "CLAUDE_CODE_SUBAGENT_MODEL",
    INFERENCE_EXPECTED_MODEL_ENV,
    "OLLAMA_API_KEY",
)
REQUIRED_INFERENCE_ENV_KEYS = (
    "OLLAMA_API_KEY",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "CLAUDE_CODE_SUBAGENT_MODEL",
)


@dataclass(frozen=True)
class GateWorkspace:
    root: Path
    city_dir: Path
    rig_dir: Path
    gc_home: Path
    runtime_dir: Path
    claude_config_dir: Path
    city_name: str
    rig_name: str


@dataclass(frozen=True)
class PackSpec:
    name: str
    binding: str
    source: Path
    roles_source: Path
    validator_source: Path
    review_formula: str | None
    build_formula: str | None
    default_gates: tuple[str, ...]
    setup_formulas: tuple[str, ...]
    required_review_routes: tuple[str, ...] = ()
    required_build_routes: tuple[str, ...] = ()
    gastown: bool = False
    smoke_agent: str | None = None


class GateError(RuntimeError):
    pass


def make_pack_specs() -> dict[str, PackSpec]:
    roles_source = REPO_ROOT / "gascity" / "roles"
    validator_source = REPO_ROOT / "gascity"
    return {
        GASCITY_PACK: PackSpec(
            name=GASCITY_PACK,
            binding="gc",
            source=REPO_ROOT / "gascity",
            roles_source=roles_source,
            validator_source=validator_source,
            review_formula=REVIEW_FORMULA,
            build_formula=BUILD_BASIC_FORMULA,
            default_gates=(REVIEW_GATE, BUILD_BASIC_GATE),
            setup_formulas=(REVIEW_FORMULA, BUILD_BASIC_FORMULA),
            required_review_routes=("gc.implementation-reviewer",),
            required_build_routes=(
                "gc.requirements-planner",
                "gc.design-author",
                "gc.task-decomposer",
                "gc.implementation-worker",
                "gc.implementation-reviewer",
                "gc.review-synthesizer",
            ),
        ),
        "superpowers": PackSpec(
            name="superpowers",
            binding="superpowers",
            source=REPO_ROOT / "superpowers",
            roles_source=roles_source,
            validator_source=validator_source,
            review_formula="superpowers-review",
            build_formula="superpowers-build",
            default_gates=(REVIEW_GATE, BUILD_GATE),
            setup_formulas=("superpowers-review", "superpowers-build"),
            required_review_routes=(
                "superpowers.code-reviewer",
                "superpowers.code-quality-reviewer",
            ),
            required_build_routes=(
                "superpowers.brainstorming",
                "superpowers.writing-plans",
                "superpowers.plan-reviewer",
                "gc.task-decomposer",
                "superpowers.implementer",
                "superpowers.code-reviewer",
                "superpowers.code-quality-reviewer",
                "superpowers.finisher",
            ),
            smoke_agent="superpowers.brainstorming",
        ),
        "compound-engineering": PackSpec(
            name="compound-engineering",
            binding="compound-engineering",
            source=REPO_ROOT / "compound-engineering",
            roles_source=roles_source,
            validator_source=validator_source,
            review_formula="compound-review",
            build_formula="compound-build",
            default_gates=(REVIEW_GATE, BUILD_GATE),
            setup_formulas=("compound-review", "compound-build"),
            required_review_routes=(
                "compound-engineering.ce-code-review-selector",
                "compound-engineering.ce-correctness-reviewer",
                "compound-engineering.ce-testing-reviewer",
                "compound-engineering.ce-maintainability-reviewer",
                "compound-engineering.ce-code-review-synthesizer",
            ),
            required_build_routes=(
                "compound-engineering.ce-brainstorm",
                "compound-engineering.ce-plan",
                "compound-engineering.ce-plan-review-synthesizer",
                "compound-engineering.ce-work",
                "compound-engineering.ce-code-review-synthesizer",
                "compound-engineering.ce-compound",
            ),
            smoke_agent="compound-engineering.ce-brainstorm",
        ),
        "gstack": PackSpec(
            name="gstack",
            binding="gstack",
            source=REPO_ROOT / "gstack",
            roles_source=roles_source,
            validator_source=validator_source,
            review_formula="gstack-review",
            build_formula="gstack-build",
            default_gates=(REVIEW_GATE, BUILD_GATE),
            setup_formulas=("gstack-review", "gstack-build"),
            required_review_routes=(
                "gstack.staff-reviewer",
                "gstack.qa-lead",
                "gstack.security-officer",
                "gstack.review-synthesizer",
            ),
            required_build_routes=(
                "gstack.office-hours",
                "gstack.founder-reviewer",
                "gstack.decomposer",
                "gstack.implementer",
                "gstack.review-synthesizer",
                "gstack.qa-lead",
                "gstack.security-officer",
                "gstack.release-engineer",
            ),
            smoke_agent="gstack.office-hours",
        ),
        "bmad": PackSpec(
            name="bmad",
            binding="bmad",
            source=REPO_ROOT / "bmad",
            roles_source=roles_source,
            validator_source=validator_source,
            review_formula="bmad-review",
            build_formula="bmad-build",
            default_gates=(REVIEW_GATE, BUILD_GATE),
            setup_formulas=("bmad-review", "bmad-build"),
            required_review_routes=(
                "bmad.blind-hunter-reviewer",
                "bmad.edge-case-reviewer",
                "bmad.acceptance-auditor",
                "bmad.story-self-checker",
                "bmad.bmad-review-synthesizer",
            ),
            required_build_routes=(
                "bmad.prd-writer",
                "bmad.architect",
                "bmad.epic-story-decomposer",
                "bmad.readiness-reviewer",
                "bmad.story-implementer",
                "bmad.bmad-review-synthesizer",
            ),
            smoke_agent="bmad.prd-writer",
        ),
        GASTOWN_PACK: PackSpec(
            name=GASTOWN_PACK,
            binding=GASTOWN_PACK,
            source=REPO_ROOT / "gastown",
            roles_source=roles_source,
            validator_source=validator_source,
            review_formula=None,
            build_formula=None,
            default_gates=(GASTOWN_ORCHESTRATION_GATE,),
            setup_formulas=(
                "mol-review-leg",
                "mol-idea-to-plan",
                "mol-polecat-work",
                "mol-refinery-patrol",
                "mol-witness-patrol",
                "mol-deacon-patrol",
                "mol-shutdown-dance",
            ),
            gastown=True,
            smoke_agent="gastown.polecat",
        ),
    }


PACK_SPECS = make_pack_specs()
METHODOLOGY_PACKS = ("superpowers", "compound-engineering", "gstack", "bmad")
SUPPORTED_PACK_CHOICES = (*PACK_SPECS.keys(), "methodology", "model-smoke", "all-supported")


def toml_string(value: str | Path) -> str:
    text = str(value)
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def builtin_pack_sources() -> dict[str, str | Path]:
    gascity_root = discover_gascity_source_root()
    if gascity_root is not None:
        return {
            "core": gascity_root / "internal" / "bootstrap" / "packs" / "core",
            "bd": gascity_root / "examples" / "bd",
        }
    return {
        "core": f"{GASCITY_REMOTE_SOURCE}//internal/bootstrap/packs/core",
        "bd": f"{GASCITY_REMOTE_SOURCE}//examples/bd",
    }


def discover_gascity_source_root() -> Path | None:
    candidates: list[Path] = []
    env_root = os.environ.get("GASCITY_SOURCE_ROOT") or os.environ.get("GASCITY_REPO_ROOT")
    if env_root:
        candidates.append(Path(env_root))
    candidates.extend(
        [
            REPO_ROOT / ".gascity-ci",
            REPO_ROOT.parent / "gascity",
            Path("/data/projects/gascity"),
        ]
    )
    for candidate in candidates:
        root = candidate.expanduser().resolve()
        if (
            (root / "internal" / "bootstrap" / "packs" / "core" / "pack.toml").is_file()
            and (root / "examples" / "bd" / "pack.toml").is_file()
        ):
            return root
    return None


def parse_duration(value: str) -> float:
    value = value.strip()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smh]?)", value)
    if not match:
        raise ValueError(f"invalid duration {value!r}; use seconds, 30s, 5m, or 1h")
    amount = float(match.group(1))
    unit = match.group(2) or "s"
    return amount * {"s": 1, "m": 60, "h": 3600}[unit]


def expand_gate_selection(selection: str, pack_spec: PackSpec | None = None) -> list[str]:
    spec = pack_spec or PACK_SPECS[GASCITY_PACK]
    if selection == ALL_GATE:
        return list(spec.default_gates)
    if selection == SMOKE_GATE and spec.smoke_agent:
        return [SMOKE_GATE]
    if selection == BUILD_GATE and spec.name == GASCITY_PACK:
        return [BUILD_BASIC_GATE]
    if selection == BUILD_BASIC_GATE and spec.name != GASCITY_PACK:
        raise ValueError(f"{selection!r} is only valid for the gascity pack; use 'build' for {spec.name}")
    if selection in spec.default_gates:
        return [selection]
    allowed = sorted(
        {
            *spec.default_gates,
            ALL_GATE,
            *(("build",) if spec.build_formula else ()),
            *((SMOKE_GATE,) if spec.smoke_agent else ()),
        }
    )
    raise ValueError(f"invalid gate {selection!r} for pack {spec.name}; choose one of {', '.join(allowed)}")


def write_gate_workspace(
    work_root: Path,
    *,
    pack_source: Path,
    roles_source: Path,
    validator_source: Path | None = None,
    pack_binding: str = "gc",
    pack_name: str = GASCITY_PACK,
    gastown: bool = False,
    include_pack_at_city_scope: bool = True,
    city_name: str,
    rig_name: str,
) -> GateWorkspace:
    root = work_root.resolve()
    city_dir = root / "city"
    rig_dir = root / rig_name
    gc_home = root / "gc-home"
    runtime_dir = root / "runtime"
    claude_config_dir = gc_home / ".claude"

    for path in (city_dir, rig_dir, gc_home, runtime_dir, claude_config_dir):
        path.mkdir(parents=True, exist_ok=False)
    (city_dir / ".gc").mkdir(exist_ok=True)
    (rig_dir / ".gc" / "inference-gate").mkdir(parents=True, exist_ok=True)

    pack_source = pack_source.resolve()
    roles_source = roles_source.resolve()
    validator_source = (validator_source or pack_source).resolve()

    city_lines = [
        "[workspace]",
        'provider = "claude"',
    ]
    if gastown:
        city_lines.append('global_fragments = ["command-glossary", "operational-awareness"]')
    city_lines.extend(
        [
            "",
            "[workspace.env]",
            f"HOME = {toml_string(gc_home)}",
            "",
            "[providers.claude]",
            'base = "builtin:claude"',
            "",
            "[session]",
            'startup_timeout = "3m"',
            "",
            "[daemon]",
            "formula_v2 = true",
            'patrol_interval = "1s"',
            "",
        ]
    )
    city_lines.extend(
        [
            "[[rigs]]",
            f"name = {toml_string(rig_name)}",
        ]
    )
    if gastown:
        city_lines.extend(
            [
                "",
                "[rigs.imports.gastown]",
                f"source = {toml_string(pack_source)}",
            ]
        )
    else:
        city_lines.extend(
            [
                "",
                "[rigs.imports.gc]",
                f"source = {toml_string(roles_source)}",
            ]
        )
        if pack_binding != "gc":
            city_lines.extend(
                [
                    "",
                    f"[rigs.imports.{pack_binding}]",
                    f"source = {toml_string(pack_source)}",
                ]
            )
    city_lines.append("")
    (city_dir / "city.toml").write_text("\n".join(city_lines), encoding="utf-8")
    (city_dir / ".gc" / "site.toml").write_text(
        "\n".join(
            [
                f"workspace_name = {toml_string(city_name)}",
                "",
                "[[rig]]",
                f"name = {toml_string(rig_name)}",
                f"path = {toml_string(rig_dir)}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    pack_lines = [
        "[pack]",
        f"name = {toml_string(f'{pack_name}-pack-inference-gate')}",
        "schema = 2",
        "",
    ]
    for binding, source in builtin_pack_sources().items():
        pack_lines.extend([f"[imports.{binding}]", f"source = {toml_string(source)}", ""])
    if include_pack_at_city_scope:
        pack_lines.extend([f"[imports.{pack_binding}]", f"source = {toml_string(pack_source)}", ""])
    (city_dir / "pack.toml").write_text("\n".join(pack_lines), encoding="utf-8")

    materialize_pack_check_scripts(validator_source, rig_dir)
    write_build_basic_fixture(rig_dir)
    return GateWorkspace(
        root=root,
        city_dir=city_dir,
        rig_dir=rig_dir,
        gc_home=gc_home,
        runtime_dir=runtime_dir,
        claude_config_dir=claude_config_dir,
        city_name=city_name,
        rig_name=rig_name,
    )


def materialize_pack_check_scripts(pack_source: Path, rig_dir: Path) -> None:
    """Install formula check scripts and their local validation schemas."""

    scripts_source = pack_source / "assets" / "scripts"
    checks_source = scripts_source / "checks"
    if not checks_source.is_dir():
        return

    scripts_target = rig_dir / ".gc" / "scripts"
    checks_target = scripts_target / "checks"
    checks_target.mkdir(parents=True, exist_ok=True)
    for script in sorted(checks_source.glob("*.sh")):
        shutil.copy2(script, checks_target / script.name)

    validator_source = scripts_source / "validate_build_artifact.py"
    if validator_source.is_file():
        shutil.copy2(validator_source, scripts_target / validator_source.name)

    schemas_source = pack_source / "schemas" / "build"
    if schemas_source.is_dir():
        schemas_target = rig_dir / "schemas" / "build"
        schemas_target.mkdir(parents=True, exist_ok=True)
        for schema in sorted(schemas_source.glob("*.yaml")):
            shutil.copy2(schema, schemas_target / schema.name)


def review_subject() -> str:
    return """\
# Review subject

Review the following implementation diff as a production code review. The
function accepts user-controlled `destination` and writes an archive.

```diff
diff --git a/exporter.py b/exporter.py
index 1111111..2222222 100644
--- a/exporter.py
+++ b/exporter.py
@@
+import subprocess
+
+def export_user_archive(user_id: str, destination: str) -> None:
+    source = f"/srv/users/{user_id}"
+    subprocess.run(f"tar czf {destination} {source}", shell=True, check=True)
```

Expected security property: user-controlled paths must not be interpolated
into a shell command. The review report should call out the command-injection
or shell-injection risk and require a safer argument-vector based invocation.
"""


def write_review_subject(root: Path) -> Path:
    subject_path = root / REVIEW_SUBJECT_PATH
    subject_path.parent.mkdir(parents=True, exist_ok=True)
    subject_path.write_text(review_subject(), encoding="utf-8")
    return subject_path


def write_build_basic_fixture(root: Path) -> None:
    tests_dir = root / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    (root / "slugger.py").write_text(
        '''\
"""Small fixture module for the gascity pack build-basic inference gate."""


def slugify(value: str) -> str:
    """Return a URL slug for value."""
    raise NotImplementedError("slugify is intentionally missing")
''',
        encoding="utf-8",
    )
    (tests_dir / "test_slugger.py").write_text(
        '''\
from slugger import slugify


def test_slugify_basic_phrase() -> None:
    assert slugify("Hello, World!") == "hello-world"


def test_slugify_collapses_separators() -> None:
    assert slugify("  Multiple---spaces___OK  ") == "multiple-spaces-ok"


def test_slugify_handles_no_alphanumerics() -> None:
    assert slugify("!!!") == ""
''',
        encoding="utf-8",
    )
    (root / "pyproject.toml").write_text(
        """\
[tool.pytest.ini_options]
testpaths = ["tests"]
""",
        encoding="utf-8",
    )


def build_basic_work_item() -> str:
    return f"""\
{BUILD_SOURCE_TITLE}

The repository contains a deliberately failing Python fixture. Implement
`slugify` in `slugger.py` so the existing tests pass.

Expected behavior:
- Lowercase ASCII alphanumeric words.
- Treat any run of non-alphanumeric characters as a separator.
- Join non-empty groups with single hyphens.
- Return an empty string when the input contains no alphanumeric characters.

Constraints:
- Do not change tests/test_slugger.py.
- Keep the implementation small and deterministic.
- Run `python3 -m pytest -q` from the repository root and record that proof.
"""


def install_service_manager_shims(gc_home: Path) -> Path:
    shim_dir = gc_home / "bin"
    shim_dir.mkdir(parents=True, exist_ok=True)
    body = "#!/bin/sh\n# inference-gate shim: force bare supervisor startup.\nexit 1\n"
    for name in ("launchctl", "systemctl"):
        path = shim_dir / name
        path.write_text(body, encoding="utf-8")
        path.chmod(0o755)
    return shim_dir


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def write_supervisor_config(gc_home: Path) -> None:
    port = reserve_port()
    (gc_home / "supervisor.toml").write_text(
        f'[supervisor]\nport = {port}\nbind = "127.0.0.1"\n',
        encoding="utf-8",
    )


def build_gate_env(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    bd_bin: str | None = None,
    inherited: Mapping[str, str] | None = None,
) -> dict[str, str]:
    source = dict(inherited or os.environ)
    env = {key: source[key] for key in INHERITED_ENV_KEYS if source.get(key)}
    if not env.get("HOME"):
        env["HOME"] = str(Path.home())

    shim_dir = install_service_manager_shims(workspace.gc_home)
    gc_bin_dir = str(Path(gc_bin).resolve().parent)
    bd_bin_dir = str(Path(bd_bin).resolve().parent) if bd_bin else ""
    env["PATH"] = os.pathsep.join(
        part for part in (str(shim_dir), gc_bin_dir, bd_bin_dir, env.get("PATH", "")) if part
    )
    env["GC_ACCEPTANCE_GC_BIN"] = gc_bin
    env["GC_HOME"] = str(workspace.gc_home)
    env["XDG_RUNTIME_DIR"] = str(workspace.runtime_dir)
    env["DOLT_ROOT_PATH"] = str(workspace.gc_home)
    env["CLAUDE_CONFIG_DIR"] = str(workspace.claude_config_dir)
    pythonpath = pythonpath_with_host_modules(source.get("PYTHONPATH"), ("pytest",))
    if pythonpath:
        env["PYTHONPATH"] = pythonpath
    env.setdefault("CLAUDE_CODE_EFFORT_LEVEL", "auto")
    env.setdefault("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
    env.setdefault(INFERENCE_EXPECTED_MODEL_ENV, DEFAULT_INFERENCE_MODEL)
    write_dolt_global_config(workspace.gc_home)

    if env.get("OLLAMA_API_KEY"):
        env.setdefault("ANTHROPIC_BASE_URL", "https://ollama.com")
        env.setdefault("ANTHROPIC_AUTH_TOKEN", env["OLLAMA_API_KEY"])
    return env


def beads_module_version(build_metadata: str) -> str | None:
    """Return the embedded beads module version from `go version -m` output."""
    for line in build_metadata.splitlines():
        fields = line.split()
        if len(fields) < 3 or fields[0] not in {"dep", "mod"} or fields[1] != BEADS_MODULE:
            continue
        return fields[2].strip("()")
    return None


def normalized_beads_module_version(version: str) -> str:
    """Ignore the release archive's build-state marker, not version drift."""
    return version.removesuffix("+dirty")


def require_matching_beads_modules(gc_metadata: str, bd_metadata: str) -> None:
    gc_version = beads_module_version(gc_metadata)
    bd_version = beads_module_version(bd_metadata)
    if (
        not gc_version
        or not bd_version
        or normalized_beads_module_version(gc_version) == normalized_beads_module_version(bd_version)
    ):
        return
    raise GateError(
        "incompatible gc/bd beads modules: "
        f"gc embeds {BEADS_MODULE}@{gc_version}, but bd embeds {BEADS_MODULE}@{bd_version}. "
        "Use --bd-bin (or GC_BEADS_BIN) with a bd binary built from the same beads module as --gc-bin."
    )


def go_build_metadata(binary: str, *, env: Mapping[str, str]) -> str:
    """Best-effort Go build-info inspection used to catch gc/bd schema skew."""
    try:
        result = subprocess.run(
            ["go", "version", "-m", binary],
            env=dict(env),
            text=True,
            capture_output=True,
            timeout=parse_duration("15s"),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout or ""


def validate_gc_bd_compatibility(gc_bin: str, bd_bin: str, *, env: Mapping[str, str]) -> None:
    require_matching_beads_modules(
        go_build_metadata(gc_bin, env=env),
        go_build_metadata(bd_bin, env=env),
    )


def pythonpath_with_host_modules(existing: str | None, module_names: Sequence[str]) -> str:
    parts: list[str] = []
    if existing:
        parts.extend(part for part in existing.split(os.pathsep) if part)
    for root in host_python_import_roots(module_names):
        parts.append(str(root))
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        deduped.append(part)
    return os.pathsep.join(deduped)


def host_python_import_roots(module_names: Sequence[str]) -> list[Path]:
    roots: list[Path] = []
    seen: set[Path] = set()
    for module_name in module_names:
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            continue
        candidates: list[Path] = []
        if spec.submodule_search_locations:
            candidates.extend(Path(location).resolve().parent for location in spec.submodule_search_locations)
        elif spec.origin and spec.origin not in {"built-in", "frozen"}:
            candidates.append(Path(spec.origin).resolve().parent)
        for candidate in candidates:
            if not candidate.is_dir() or candidate in seen:
                continue
            roots.append(candidate)
            seen.add(candidate)
    return roots


def write_dolt_global_config(gc_home: Path) -> None:
    dolt_dir = gc_home / ".dolt"
    dolt_dir.mkdir(parents=True, exist_ok=True)
    config_path = dolt_dir / "config_global.json"
    if config_path.exists():
        return
    save_json_object(
        config_path,
        {
            "user.name": "Gas City Pack Gate",
            "user.email": "gascity-pack-gate@example.invalid",
        },
    )


def seed_claude_project_state(*, home: Path, config_dir: Path, project_paths: Sequence[Path]) -> None:
    for state_path in claude_state_paths(home, config_dir):
        state = load_json_object(state_path)
        state["hasCompletedOnboarding"] = True
        if not str(state.get("theme") or "").strip():
            state["theme"] = "light"
        projects = state.get("projects")
        if not isinstance(projects, dict):
            projects = {}
            state["projects"] = projects
        for project_path in project_paths:
            key = str(project_path.resolve())
            entry = projects.get(key)
            if not isinstance(entry, dict):
                entry = {}
            entry["hasCompletedProjectOnboarding"] = True
            entry["hasTrustDialogAccepted"] = True
            entry.setdefault("projectOnboardingSeenCount", 1)
            projects[key] = entry
        save_json_object(state_path, state)


def claude_state_paths(home: Path, config_dir: Path) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for path in (home / ".claude.json", config_dir / ".claude.json"):
        resolved = path.resolve()
        if resolved not in seen:
            paths.append(path)
            seen.add(resolved)
    return paths


def load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise GateError(f"{path} must contain a JSON object")
    return data


def save_json_object(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o600)


def configured_inference_model(env: Mapping[str, str]) -> str:
    expected_model = str(env.get(INFERENCE_EXPECTED_MODEL_ENV) or "").strip()
    if not expected_model:
        raise GateError(f"missing required inference model variable: {INFERENCE_EXPECTED_MODEL_ENV}")
    if expected_model != DEFAULT_INFERENCE_MODEL:
        raise GateError(f"{INFERENCE_EXPECTED_MODEL_ENV} must be {DEFAULT_INFERENCE_MODEL}")

    mismatches = [
        f"{key}={str(env.get(key) or '').strip()}"
        for key in INFERENCE_MODEL_ENV_KEYS
        if str(env.get(key) or "").strip() != expected_model
    ]
    if mismatches:
        raise GateError(
            f"all Claude inference routes must use {INFERENCE_EXPECTED_MODEL_ENV}={expected_model}; "
            + ", ".join(mismatches)
        )
    return expected_model


def validate_inference_env(env: Mapping[str, str]) -> str:
    missing = [key for key in REQUIRED_INFERENCE_ENV_KEYS if not str(env.get(key) or "").strip()]
    if missing:
        raise GateError(
            "missing inference environment variable(s): "
            + ", ".join(missing)
            + ". Configure the same Ollama-backed Claude variables used by Gas City's nightly Tier C workflow."
        )
    return configured_inference_model(env)


def run_checked(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float | None = None,
    log_output: bool = False,
    input_text: str | None = None,
) -> str:
    print("+ " + shlex.join(command), flush=True)
    result = subprocess.run(
        list(command),
        cwd=str(cwd) if cwd else None,
        env=dict(env) if env is not None else None,
        text=True,
        input=input_text,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    output = (result.stdout or "") + (result.stderr or "")
    if log_output and output:
        print(output, end="" if output.endswith("\n") else "\n")
    if result.returncode == 0:
        return output
    if output:
        print(output, file=sys.stderr, end="" if output.endswith("\n") else "\n")
    raise subprocess.CalledProcessError(result.returncode, command, output=result.stdout, stderr=result.stderr)


def claude_result_payload(output: str) -> Mapping[str, Any]:
    for line in reversed(output.splitlines()):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("type") == "result":
            return payload
    raise GateError("Claude preflight did not return a JSON result payload")


def normalized_model_usage_name(model_name: object) -> str:
    return re.sub(r"\[\d+[km]\]$", "", str(model_name).strip(), flags=re.IGNORECASE)


def preflight_inference_model(expected_model: str, *, env: Mapping[str, str]) -> None:
    try:
        output = run_checked(
            ["claude", "-p", "--model", expected_model, "--output-format", "json", "Reply with exactly OK."],
            env=env,
            timeout=parse_duration("2m"),
        )
    except subprocess.CalledProcessError as exc:
        output = (exc.output or "") + (exc.stderr or "")
        try:
            payload = claude_result_payload(output)
        except GateError as parse_error:
            raise GateError(f"inference preflight command failed before reporting model usage: {exc.returncode}") from parse_error
    else:
        payload = claude_result_payload(output)
    result = str(payload.get("result") or "").replace("\n", " ").strip()
    if payload.get("is_error"):
        raise GateError(f"inference preflight rejected model {expected_model!r}: {result or 'unknown error'}")

    model_usage = payload.get("modelUsage")
    actual_models = tuple(model_usage) if isinstance(model_usage, Mapping) else ()
    if expected_model not in {normalized_model_usage_name(model) for model in actual_models}:
        reported = ", ".join(str(model) for model in actual_models) or "none"
        raise GateError(
            f"inference preflight reported modelUsage [{reported}], expected {expected_model!r}; refusing to run a fallback model"
        )
    print(f"inference model preflight passed: {expected_model}", flush=True)


def initialize_rig_git(rig_dir: Path, *, env: Mapping[str, str]) -> None:
    if (rig_dir / ".git").exists():
        return
    try:
        run_checked(["git", "init", "-b", "main"], cwd=rig_dir, env=env)
    except subprocess.CalledProcessError:
        run_checked(["git", "init"], cwd=rig_dir, env=env)
        run_checked(["git", "checkout", "-B", "main"], cwd=rig_dir, env=env)
    run_checked(["git", "config", "user.email", "gascity-pack-gate@example.invalid"], cwd=rig_dir, env=env)
    run_checked(["git", "config", "user.name", "Gas City Pack Gate"], cwd=rig_dir, env=env)
    run_checked(["git", "add", "."], cwd=rig_dir, env=env)
    run_checked(["git", "commit", "-m", "Add inference gate fixtures"], cwd=rig_dir, env=env)


def should_validate_gastown_orchestration_contract(gates: Sequence[str]) -> bool:
    """Keep the fast RC smoke to its formula/route compatibility boundary."""
    return SMOKE_GATE not in gates


def initialize_city(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    pack_spec: PackSpec,
    gates: Sequence[str],
    env: Mapping[str, str],
) -> None:
    write_supervisor_config(workspace.gc_home)
    seed_claude_project_state(
        home=Path(env["HOME"]),
        config_dir=Path(env["CLAUDE_CONFIG_DIR"]),
        project_paths=[workspace.city_dir, workspace.rig_dir],
    )
    initialize_rig_git(workspace.rig_dir, env=env)

    run_checked(
        [
            gc_bin,
            "init",
            "--skip-provider-readiness",
            "--name",
            workspace.city_name,
            "--file",
            str(workspace.city_dir / "city.toml"),
            "--preserve-existing",
            "--yes",
            str(workspace.city_dir),
        ],
        env=env,
        timeout=parse_duration("5m"),
        log_output=True,
    )
    run_checked([gc_bin, "--city", str(workspace.city_dir), "import", "install"], env=env, timeout=parse_duration("5m"))
    run_checked([gc_bin, "--city", str(workspace.city_dir), "import", "check"], env=env, timeout=parse_duration("5m"))
    run_checked([gc_bin, "--city", str(workspace.city_dir), "config", "show"], env=env, timeout=parse_duration("2m"))
    formulas = selected_setup_formulas(pack_spec, gates)
    for formula in formulas:
        run_checked(
            [gc_bin, "--city", str(workspace.city_dir), "--rig", workspace.rig_name, "formula", "show", formula],
            env=env,
            timeout=parse_duration("2m"),
        )
    if pack_spec.gastown and should_validate_gastown_orchestration_contract(gates):
        validate_gastown_orchestration_contract(pack_spec.source)
    else:
        validate_methodology_flow_contract(pack_spec)


def start_city(gc_bin: str, workspace: GateWorkspace, *, env: Mapping[str, str]) -> None:
    run_checked([gc_bin, "start", str(workspace.city_dir), "--verbose"], env=env, timeout=parse_duration("5m"), log_output=True)


def selected_setup_formulas(pack_spec: PackSpec, gates: Sequence[str]) -> list[str]:
    if SMOKE_GATE in gates:
        return list(pack_spec.setup_formulas)
    selected: list[str] = []
    if pack_spec.gastown:
        return list(pack_spec.setup_formulas)
    if REVIEW_GATE in gates and pack_spec.review_formula:
        selected.append(pack_spec.review_formula)
    if any(gate in gates for gate in (BUILD_GATE, BUILD_BASIC_GATE)) and pack_spec.build_formula:
        selected.append(pack_spec.build_formula)
    return selected


def review_title(pack_spec: PackSpec) -> str:
    if pack_spec.name == GASCITY_PACK:
        return REVIEW_TITLE
    return f"{pack_spec.name} pack inference gate: review"


def build_title(pack_spec: PackSpec) -> str:
    if pack_spec.name == GASCITY_PACK:
        return BUILD_TITLE
    if not pack_spec.build_formula:
        raise GateError(f"pack {pack_spec.name} does not define a build formula")
    return f"{pack_spec.name} pack inference gate: {pack_spec.build_formula}"


def build_artifact_root(pack_spec: PackSpec) -> Path:
    if pack_spec.name == GASCITY_PACK:
        return BUILD_ARTIFACT_ROOT
    if not pack_spec.build_formula:
        raise GateError(f"pack {pack_spec.name} does not define a build formula")
    return Path(".gc/inference-gate") / pack_spec.name / pack_spec.build_formula


def smoke_title(pack_spec: PackSpec) -> str:
    return f"{SMOKE_TITLE_PREFIX}: {pack_spec.name}"


def smoke_target(workspace: GateWorkspace, pack_spec: PackSpec) -> str:
    if not pack_spec.smoke_agent:
        raise GateError(f"pack {pack_spec.name} does not define a direct model smoke agent")
    return f"{workspace.rig_name}/{pack_spec.smoke_agent}"


def smoke_task(pack_spec: PackSpec) -> str:
    acknowledgment = f"PACK_SMOKE_OK: {pack_spec.name}"
    return (
        f"{smoke_title(pack_spec)}\n\n"
        "This is a release-candidate compatibility smoke. Do not start a formula, modify\n"
        f"project files, or delegate work. Respond with exactly `{acknowledgment}` in the bead\n"
        "notes, then close this bead using the normal bead tooling. Do not create additional beads.\n"
    )


def launch_smoke_bead(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    pack_spec: PackSpec,
) -> str:
    target = smoke_target(workspace, pack_spec)
    output = run_checked(
        [
            gc_bin,
            "--city",
            str(workspace.city_dir),
            "--rig",
            workspace.rig_name,
            "sling",
            target,
            "--stdin",
            "--force",
            "--no-formula",
            "--json",
        ],
        cwd=workspace.rig_dir,
        env=env,
        timeout=parse_duration("2m"),
        log_output=True,
        input_text=smoke_task(pack_spec),
    )
    bead_id = extract_sling_root_id(output)
    if bead_id:
        return bead_id
    bead = wait_for_root_by_title(
        gc_bin,
        workspace,
        env=env,
        title=smoke_title(pack_spec),
        timeout=parse_duration("30s"),
    )
    if bead and bead.get("id"):
        return str(bead["id"])
    raise GateError(f"could not determine model smoke bead from sling output:\n{output}")


def launch_review_formula(gc_bin: str, workspace: GateWorkspace, *, env: Mapping[str, str], pack_spec: PackSpec) -> str:
    if not pack_spec.review_formula:
        raise GateError(f"pack {pack_spec.name} does not define a review formula")
    write_review_subject(workspace.rig_dir)
    command = [
        gc_bin,
        "--city",
        str(workspace.city_dir),
        "--rig",
        workspace.rig_name,
        "sling",
        "gc.run-operator",
        pack_spec.review_formula,
        "--formula",
        "--title",
        review_title(pack_spec),
        "--var",
        f"subject_path={REVIEW_SUBJECT_PATH}",
        "--var",
        f"report_path={REVIEW_REPORT_PATH}",
        "--var",
        "interaction_mode=headless",
        "--var",
        "review_mode=report",
        "--nudge",
        "--json",
    ]
    output = run_checked(command, cwd=workspace.rig_dir, env=env, timeout=parse_duration("5m"), log_output=True)
    root_id = extract_sling_root_id(output)
    if root_id:
        return root_id
    bead = wait_for_root_by_title(gc_bin, workspace, env=env, title=review_title(pack_spec), timeout=parse_duration("30s"))
    if bead and bead.get("id"):
        return str(bead["id"])
    raise GateError(f"could not determine review workflow root from sling output:\n{output}")


def launch_build_formula(gc_bin: str, workspace: GateWorkspace, *, env: Mapping[str, str], pack_spec: PackSpec) -> str:
    if not pack_spec.build_formula:
        raise GateError(f"pack {pack_spec.name} does not define a build formula")
    command = [
        gc_bin,
        "--city",
        str(workspace.city_dir),
        "--rig",
        workspace.rig_name,
        "sling",
        "gc.run-operator",
        "--stdin",
        "--force",
        "--on",
        pack_spec.build_formula,
        "--title",
        build_title(pack_spec),
        "--var",
        f"artifact_root={build_artifact_root(pack_spec)}",
        "--var",
        "interaction_mode=headless",
        "--var",
        "review_mode=report",
        "--var",
        "drain_policy=separate",
        "--var",
        "push=false",
        "--var",
        "open_pr=false",
        "--var",
        "max_iterations=2",
        "--nudge",
        "--json",
    ]
    output = run_checked(
        command,
        cwd=workspace.rig_dir,
        env=env,
        timeout=parse_duration("5m"),
        log_output=True,
        input_text=build_basic_work_item(),
    )
    root_id = resolve_workflow_root_id(
        gc_bin,
        workspace,
        env=env,
        candidate_id=extract_sling_root_id(output),
        title=build_title(pack_spec),
        source_title=BUILD_SOURCE_TITLE,
        timeout=parse_duration("30s"),
    )
    if root_id:
        return root_id
    raise GateError(f"could not determine build-basic workflow root from sling output:\n{output}")


def extract_json_payload(text: str) -> Any | None:
    stripped = text.strip()
    if not stripped:
        return None
    for index, char in enumerate(stripped):
        if char not in "[{":
            continue
        candidate = stripped[index:]
        try:
            payload, _ = json.JSONDecoder().raw_decode(candidate)
            return payload
        except json.JSONDecodeError:
            continue
    return None


def extract_sling_root_id(output: str) -> str | None:
    payload = extract_json_payload(output)
    if payload is None:
        return None
    return find_first_key(payload, ("root_bead_id", "workflow_id", "root_id", "bead_id", "id"))


def find_first_key(value: Any, keys: Sequence[str]) -> str | None:
    if isinstance(value, dict):
        for key in keys:
            raw = value.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
        for raw in value.values():
            found = find_first_key(raw, keys)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = find_first_key(item, keys)
            if found:
                return found
    return None


def list_beads(gc_bin: str, workspace: GateWorkspace, *, env: Mapping[str, str]) -> list[dict[str, Any]]:
    try:
        output = run_checked(
            [
                gc_bin,
                "--city",
                str(workspace.city_dir),
                "--rig",
                workspace.rig_name,
                "bd",
                "list",
                "--all",
                "--json",
                "--limit",
                BD_LIST_LIMIT,
            ],
            env=env,
            timeout=parse_duration("30s"),
        )
        payload = extract_json_payload(output)
        if isinstance(payload, list):
            beads = [item for item in payload if isinstance(item, dict)]
            if beads:
                return append_event_route_history(beads, workspace)
            event_beads = list_beads_from_event_log(workspace)
            if event_beads:
                return event_beads
            return beads
        if payload is not None:
            raise GateError(f"unexpected gc bd list --json payload: {payload!r}")
    except Exception:
        event_beads = list_beads_from_event_log(workspace)
        if event_beads:
            return event_beads
        if not (workspace.rig_dir / ".gc" / "beads.json").exists():
            raise

    path = workspace.rig_dir / ".gc" / "beads.json"
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise GateError(f"{path} must contain a JSON object")
    beads = payload.get("beads")
    if not isinstance(beads, list):
        raise GateError(f"{path} missing beads array")
    return [item for item in beads if isinstance(item, dict)]


def list_beads_from_event_log(workspace: GateWorkspace) -> list[dict[str, Any]]:
    path = workspace.city_dir / ".gc" / "events.jsonl"
    if not path.is_file():
        return []

    beads: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        bead = event_payload_bead(event)
        if not isinstance(bead, dict):
            continue
        bead_id = bead.get("id")
        if isinstance(bead_id, str) and bead_id:
            beads[bead_id] = bead
    return list(beads.values())


def event_payload_bead(event: Mapping[str, Any]) -> dict[str, Any] | None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    bead = payload.get("bead")
    if isinstance(bead, dict):
        return bead
    if isinstance(payload.get("id"), str) and isinstance(payload.get("title"), str):
        return payload
    return None


def append_event_route_history(beads: Sequence[dict[str, Any]], workspace: GateWorkspace) -> list[dict[str, Any]]:
    routes = event_route_history_targets(workspace)
    if not routes:
        return list(beads)
    return [
        *beads,
        {
            "id": "__gc_event_route_history__",
            "title": "__gc_event_route_history__",
            "status": "closed",
            "metadata": {"gc.event.routed_to": routes},
        },
    ]


def event_route_history_targets(workspace: GateWorkspace) -> list[str]:
    path = workspace.city_dir / ".gc" / "events.jsonl"
    if not path.is_file():
        return []

    routes: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        bead = event_payload_bead(event)
        if isinstance(bead, dict):
            routes.extend(bead_route_targets(bead))
    return dedupe_strings(routes)


def find_unique_bead_by_title(beads: Sequence[Mapping[str, Any]], title: str) -> Mapping[str, Any] | None:
    matches = [bead for bead in beads if bead.get("title") == title]
    if len(matches) == 1:
        return matches[0]
    return None


def find_bead_by_id(beads: Sequence[Mapping[str, Any]], bead_id: str) -> Mapping[str, Any] | None:
    for bead in beads:
        if bead.get("id") == bead_id:
            return bead
    return None


def wait_for_root_by_title(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    title: str,
    timeout: float,
) -> Mapping[str, Any] | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        bead = find_unique_bead_by_title(list_beads(gc_bin, workspace, env=env), title)
        if bead is not None:
            return bead
        time.sleep(1)
    return None


def resolve_workflow_root_id(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    candidate_id: str | None,
    title: str,
    source_title: str | None = None,
    timeout: float,
) -> str | None:
    metadata_keys = (
        "gc.root_bead_id",
        "gc.workflow_root_id",
        "gc.attached_workflow_id",
        "root_bead_id",
        "workflow_id",
        "root_id",
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        beads = list_beads(gc_bin, workspace, env=env)
        root = find_unique_bead_by_title(beads, title)
        if root and root.get("id"):
            return str(root["id"])

        for bead in candidate_beads(beads, candidate_id, source_title):
            if bead.get("title") == title and bead.get("id"):
                return str(bead["id"])
            for key in metadata_keys:
                value = metadata_value(bead, key)
                if not value:
                    continue
                candidate_root = find_bead_by_id(beads, value)
                if candidate_root and candidate_root.get("title") == title:
                    return value
        time.sleep(1)
    return None


def candidate_beads(
    beads: Sequence[Mapping[str, Any]],
    candidate_id: str | None,
    source_title: str | None,
) -> list[Mapping[str, Any]]:
    candidates: list[Mapping[str, Any]] = []
    if candidate_id:
        bead = find_bead_by_id(beads, candidate_id)
        if bead is not None:
            candidates.append(bead)
    if source_title:
        bead = find_unique_bead_by_title(beads, source_title)
        if bead is not None and bead not in candidates:
            candidates.append(bead)
    return candidates


def show_bead(gc_bin: str, workspace: GateWorkspace, bead_id: str, *, env: Mapping[str, str]) -> dict[str, Any]:
    try:
        output = run_checked(
            [
                gc_bin,
                "--city",
                str(workspace.city_dir),
                "--rig",
                workspace.rig_name,
                "bd",
                "show",
                bead_id,
                "--json",
                "--include-comments",
            ],
            env=env,
            timeout=parse_duration("30s"),
        )
        payload = extract_json_payload(output)
        if isinstance(payload, dict):
            if payload.get("id") == bead_id:
                return payload
            raise GateError(f"unexpected gc bd show --json payload for {bead_id}: {payload!r}")
        if isinstance(payload, list):
            matches = [item for item in payload if isinstance(item, dict) and item.get("id") == bead_id]
            if len(matches) == 1:
                return matches[0]
            raise GateError(f"unexpected gc bd show --json payload for {bead_id}: {payload!r}")
        if payload is not None:
            raise GateError(f"unexpected gc bd show --json payload for {bead_id}: {payload!r}")
    except Exception:
        if not (workspace.rig_dir / ".gc" / "beads.json").exists():
            raise

    for bead in list_beads(gc_bin, workspace, env=env):
        if bead.get("id") == bead_id:
            return bead
    raise GateError(f"bead {bead_id} not found in gc bd show --json or gc bd list --json output")


def metadata_value(bead: Mapping[str, Any], key: str) -> str:
    metadata = bead.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    value = metadata.get(key)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def wait_for_workflow_pass(
    gc_bin: str,
    workspace: GateWorkspace,
    root_id: str,
    *,
    env: Mapping[str, str],
    timeout: float,
    poll_interval: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_bead: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last_bead = show_bead(gc_bin, workspace, root_id, env=env)
        status = str(last_bead.get("status") or "")
        outcome = metadata_value(last_bead, "gc.outcome")
        print(f"workflow {root_id}: status={status or '<unset>'} outcome={outcome or '<unset>'}", flush=True)
        if status == "closed":
            if outcome == "pass":
                return last_bead
            raise GateError(
                f"workflow {root_id} closed with gc.outcome={outcome!r}, want 'pass'\n"
                + collect_diagnostics(gc_bin, workspace, env=env)
            )
        time.sleep(poll_interval)
    raise GateError(
        f"timed out after {timeout:.0f}s waiting for workflow {root_id} to close; last bead={last_bead!r}\n"
        + collect_diagnostics(gc_bin, workspace, env=env)
    )


def collect_diagnostics(gc_bin: str, workspace: GateWorkspace, *, env: Mapping[str, str]) -> str:
    sections: list[str] = []
    commands = [
        ("sessions", [gc_bin, "--city", str(workspace.city_dir), "session", "list", "--state", "all"]),
        (
            "rig beads",
            [
                gc_bin,
                "--city",
                str(workspace.city_dir),
                "--rig",
                workspace.rig_name,
                "bd",
                "list",
                "--json",
                "--limit",
                BD_LIST_LIMIT,
            ],
        ),
    ]
    for label, command in commands:
        try:
            output = run_checked(command, env=env, timeout=parse_duration("30s"))
        except Exception as exc:  # pragma: no cover - diagnostic best effort
            output = f"{type(exc).__name__}: {exc}"
        sections.append(f"== {label} ==\n{output}")
    beads_path = workspace.rig_dir / ".gc" / "beads.json"
    if beads_path.exists():
        sections.append(f"== {beads_path} ==\n{beads_path.read_text(encoding='utf-8', errors='replace')}")
    for path in (
        workspace.city_dir / ".gc" / "runtime" / "control-dispatcher-trace.log",
        workspace.city_dir / "graph-workflow-trace.log",
        workspace.gc_home / "supervisor.log",
    ):
        if path.exists():
            sections.append(f"== {path} ==\n{path.read_text(encoding='utf-8', errors='replace')[-12000:]}")
    return "\n".join(sections)


def validate_review_report(
    root_bead: Mapping[str, Any],
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    pack_spec: PackSpec,
) -> None:
    validator = pack_spec.validator_source / "assets" / "scripts" / "validate_build_artifact.py"
    if not validator.is_file():
        raise GateError(f"review artifact validator was not found: {validator}")

    allow_approved = pack_spec.name != GASCITY_PACK
    failures: list[str] = []
    for report_path in review_report_candidates(root_bead, workspace.rig_dir, pack_spec):
        if not report_path.is_file():
            failures.append(f"{report_path}: missing")
            continue
        try:
            run_checked(
                [sys.executable, str(validator), "--schema", "gc.build.review.v1", "--path", str(report_path)],
                env=env,
                timeout=parse_duration("1m"),
                log_output=True,
            )
            require_expected_review_signal(report_path, allow_approved=allow_approved)
        except (GateError, subprocess.CalledProcessError) as exc:
            failures.append(f"{report_path}: {exc}")
            continue
        print(f"validated review report: {report_path}", flush=True)
        return

    detail = "\n".join(f"- {failure}" for failure in failures) if failures else "no candidate report paths"
    raise GateError(f"review gate did not produce a valid expected review artifact:\n{detail}")


def review_report_candidates(root_bead: Mapping[str, Any], rig_dir: Path, pack_spec: PackSpec) -> list[Path]:
    candidates: list[Path] = []
    for key in REVIEW_REPORT_METADATA_KEYS:
        raw_path = metadata_value(root_bead, key)
        if raw_path:
            candidates.append(resolve_artifact_path(raw_path, base=rig_dir))

    candidates.append(resolve_artifact_path(REVIEW_REPORT_PATH, base=rig_dir))
    if pack_spec.name != GASCITY_PACK:
        candidates.extend(resolve_artifact_path(path, base=rig_dir) for path in METHODOLOGY_REVIEW_REPORT_FALLBACKS)
        artifacts_dir = rig_dir / ".gc" / "inference-gate" / "artifacts"
        if artifacts_dir.is_dir():
            candidates.extend(sorted(artifacts_dir.glob("*.md")))

    unique: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    return unique


def require_expected_review_signal(report_path: Path, *, allow_approved: bool = False) -> None:
    text = report_path.read_text(encoding="utf-8", errors="replace")
    lower = text.lower()
    has_risk = "shell" in lower and "injection" in lower and "subprocess" in lower
    has_blocking_status = blocking_review_status(text)
    has_approved_status = re.search(r"(?m)^status:\s*approved\s*$", text) is not None
    has_resolution = any(
        marker in lower
        for marker in (
            "shell=false",
            "argument-vector",
            "argument vector",
            "argument list",
            "resolved",
            "covered",
            "fixed",
        )
    )
    has_status = has_blocking_status or (allow_approved and has_approved_status and has_resolution)
    if not has_status or not has_risk:
        raise GateError(
            "review report did not identify and handle the expected shell-injection risk. "
            f"status_ok={has_status} risk_ok={has_risk} resolution_ok={has_resolution} report={report_path}"
        )


def blocking_review_status(text: str) -> bool:
    if re.search(r"(?m)^status:\s*(changes_required|blocked)\s*$", text):
        return True
    if "cannot be approved" in text.lower():
        return True
    return re.search(
        r"(?ims)^#{1,6}\s*verdict\b[\s\S]{0,300}\b(iterate|changes_required|changes required|blocked)\b",
        text,
    ) is not None


def validate_build_basic_result(
    rig_dir: Path,
    beads: Sequence[Mapping[str, Any]],
    *,
    env: Mapping[str, str],
    timeout: float,
) -> Path:
    candidates = build_result_candidates(rig_dir, beads)
    failures: list[str] = []
    for candidate in candidates:
        slugger = candidate / "slugger.py"
        tests = candidate / "tests" / "test_slugger.py"
        if not slugger.is_file():
            failures.append(f"{candidate}: missing slugger.py")
            continue
        if not tests.is_file():
            failures.append(f"{candidate}: missing tests/test_slugger.py")
            continue

        source = slugger.read_text(encoding="utf-8", errors="replace")
        if "NotImplementedError" in source:
            failures.append(f"{candidate}: slugger.py still contains NotImplementedError")
            continue

        try:
            run_checked(
                [sys.executable, "-m", "pytest", "-q"],
                cwd=candidate,
                env=env,
                timeout=timeout,
                log_output=True,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            failures.append(f"{candidate}: pytest failed: {exc}")
            continue
        print(f"validated build-basic code result: {candidate}", flush=True)
        return candidate

    detail = "\n".join(f"- {failure}" for failure in failures) if failures else "no candidate directories found"
    raise GateError(f"build-basic did not produce a passing code result:\n{detail}")


def validate_build_basic_artifacts(
    root_bead: Mapping[str, Any],
    *,
    rig_dir: Path,
    env: Mapping[str, str],
    validator_source: Path,
) -> None:
    validator = validator_source / "assets" / "scripts" / "validate_build_artifact.py"
    if not validator.is_file():
        raise GateError(f"build artifact validator was not found: {validator}")

    for metadata_key, schema in BUILD_BASIC_ARTIFACT_CONTRACTS:
        raw_path = metadata_value(root_bead, metadata_key)
        if not raw_path:
            raise GateError(f"build-basic root missing required artifact metadata {metadata_key}")
        artifact_path = resolve_artifact_path(raw_path, base=rig_dir)
        if not artifact_path.is_file():
            raise GateError(f"build-basic artifact from {metadata_key} does not exist: {artifact_path}")
        try:
            run_checked(
                [sys.executable, str(validator), "--schema", schema, "--path", str(artifact_path)],
                env=env,
                timeout=parse_duration("1m"),
                log_output=True,
            )
        except subprocess.CalledProcessError as exc:
            raise GateError(
                f"build-basic artifact failed validation for {metadata_key} "
                f"(schema {schema}) at {artifact_path}: {exc}"
            ) from exc
        print(f"validated build-basic artifact: {metadata_key} schema={schema} path={artifact_path}", flush=True)


def resolve_artifact_path(value: str, *, base: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base / path).resolve()


def build_result_candidates(rig_dir: Path, beads: Sequence[Mapping[str, Any]]) -> list[Path]:
    candidates: list[Path] = []
    rig_root_from_implementation_summary = False
    for bead in beads:
        metadata = bead.get("metadata")
        if not isinstance(metadata, dict):
            continue
        for key in ("work_dir", "gc.work_dir", "gc.build.work_dir", "gc.implementation.work_dir"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(Path(value.strip()))
        for key in ("gc.implementation.summary_path", "gc.build.implementation_summary_path"):
            value = metadata.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            for ancestor in Path(value.strip()).parents:
                if ancestor.name == ".gc":
                    implementation_root = ancestor.parent
                    candidates.append(implementation_root)
                    try:
                        rig_root_from_implementation_summary = (
                            rig_root_from_implementation_summary
                            or implementation_root.resolve() == rig_dir.resolve()
                        )
                    except OSError:
                        pass
                    break
    worktrees_dir = rig_dir / "worktrees"
    if worktrees_dir.is_dir():
        candidates.extend(sorted(path for path in worktrees_dir.iterdir() if path.is_dir()))

    unique: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if resolved == rig_dir.resolve() and not rig_root_from_implementation_summary:
            continue
        if resolved in seen or not resolved.is_dir():
            continue
        unique.append(resolved)
        seen.add(resolved)
    return unique


def bead_route_targets(bead: Mapping[str, Any]) -> list[str]:
    targets: list[str] = []
    for key in ("assignee", "owner", "agent", "agent_id", "session", "target"):
        value = bead.get(key)
        if isinstance(value, str) and value.strip():
            targets.append(value.strip())

    metadata = bead.get("metadata")
    if isinstance(metadata, dict):
        for key, value in metadata.items():
            if not route_metadata_key(key):
                continue
            for target in string_values(value):
                targets.append(target)
    return dedupe_strings(targets)


def route_metadata_key(key: str) -> bool:
    return (
        key in {
            "gc.run_target",
            "gc.routed_to",
            "gc.target",
            "gc.assignee",
            "run_target",
            "routed_to",
            "target",
            "assignee",
        }
        or key.endswith(".run_target")
        or key.endswith(".routed_to")
        or key.endswith("_run_target")
        or key.endswith("_routed_to")
    )


def string_values(value: Any) -> list[str]:
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if isinstance(value, (int, float, bool)):
        return [str(value)]
    if isinstance(value, list):
        values: list[str] = []
        for item in value:
            values.extend(string_values(item))
        return values
    if isinstance(value, dict):
        values: list[str] = []
        for item in value.values():
            values.extend(string_values(item))
        return values
    return []


def dedupe_strings(values: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def route_matches(actual: str, expected: str) -> bool:
    actual = actual.strip()
    expected = expected.strip()
    if not actual or not expected:
        return False
    if actual == expected:
        return True
    return actual.endswith(f"/{expected}")


def validate_required_routes(
    beads: Sequence[Mapping[str, Any]],
    required_routes: Sequence[str],
    *,
    context: str,
) -> None:
    if not required_routes:
        return
    observed = sorted({target for bead in beads for target in bead_route_targets(bead)})
    missing = [
        expected
        for expected in required_routes
        if not any(route_matches(actual, expected) for actual in observed)
    ]
    if missing:
        raise GateError(
            f"{context} did not route through expected agent(s): {', '.join(missing)}. "
            f"Observed routes: {', '.join(observed) if observed else '<none>'}"
        )


def list_sessions(gc_bin: str, workspace: GateWorkspace, *, env: Mapping[str, str]) -> list[dict[str, Any]]:
    output = run_checked(
        [gc_bin, "--city", str(workspace.city_dir), "session", "list", "--state", "all", "--json"],
        env=env,
        timeout=parse_duration("30s"),
    )
    payload = extract_json_payload(output)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("sessions", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]
    if payload is None:
        return []
    raise GateError(f"unexpected gc session list --json payload: {payload!r}")


def session_identity_strings(session: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key in (
        "id",
        "name",
        "session",
        "session_id",
        "agent",
        "agent_id",
        "agent_name",
        "template",
        "template_name",
        "display_name",
    ):
        value = session.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    metadata = session.get("metadata")
    if isinstance(metadata, dict):
        for key in ("agent", "agent_id", "template", "name"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                values.append(value.strip())
    return dedupe_strings(values)


def identity_matches_agent(identity: str, agent: str) -> bool:
    identity = identity.strip()
    if identity == agent:
        return True
    base = identity.rsplit("/", 1)[-1].rsplit(".", 1)[-1]
    return base == agent


def wait_for_gastown_sessions(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    required_agents: Sequence[str],
    timeout: float,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout
    last_sessions: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_sessions = list_sessions(gc_bin, workspace, env=env)
        missing = missing_session_agents(last_sessions, required_agents)
        if not missing:
            print(
                "validated Gastown sessions: "
                + ", ".join(sorted(required_agents)),
                flush=True,
            )
            return last_sessions
        print(f"waiting for Gastown sessions: missing {', '.join(missing)}", flush=True)
        time.sleep(2)
    identities = sorted({value for session in last_sessions for value in session_identity_strings(session)})
    raise GateError(
        f"Gastown always-on sessions did not appear: {', '.join(missing_session_agents(last_sessions, required_agents))}. "
        f"Observed session identities: {', '.join(identities) if identities else '<none>'}"
    )


def missing_session_agents(sessions: Sequence[Mapping[str, Any]], required_agents: Sequence[str]) -> list[str]:
    identities = [value for session in sessions for value in session_identity_strings(session)]
    return [
        agent
        for agent in required_agents
        if not any(identity_matches_agent(identity, agent) for identity in identities)
    ]


def gastown_review_assignment_description() -> str:
    return """\
Run a bounded Gastown orchestration review-leg gate.

Review the following tiny release-gate plan as written. Do not execute the
plan, start another city, spawn sessions, or route extra work; the numbered
steps are the subject of the review.

1. Start a disposable Gastown city.
2. Require mayor, deacon, boot, and witness sessions to exist after startup.
3. Do not require refinery to be active at startup because refinery is configured on-demand.
4. Route a review assignment through the Gastown polecat pool and persist the report to bead notes.

Write the report in the bead notes with these sections:

## Summary
## Findings
## Recommendation

The expected finding is that the gate must check refinery availability by
configuration/formula surface rather than by active session presence.
"""


def create_gastown_review_assignment(gc_bin: str, workspace: GateWorkspace, *, env: Mapping[str, str]) -> str:
    metadata = {
        "coordinator": "mayor",
        "review_id": "gastown-inference-gate",
        "review_phase": "orchestration",
        "review_leg": "polecat-review-leg",
    }
    output = run_checked(
        [
            gc_bin,
            "--city",
            str(workspace.city_dir),
            "--rig",
            workspace.rig_name,
            "bd",
            "create",
            GASTOWN_REVIEW_ASSIGNMENT_TITLE,
            "--type",
            "task",
            "--description",
            gastown_review_assignment_description(),
            "--metadata",
            json.dumps(metadata, sort_keys=True),
            "--json",
        ],
        cwd=workspace.rig_dir,
        env=env,
        timeout=parse_duration("1m"),
        log_output=True,
    )
    bead_id = find_first_key(extract_json_payload(output), ("id", "bead_id"))
    if bead_id:
        return bead_id
    raise GateError(f"could not determine Gastown review assignment bead id from gc bd create output:\n{output}")


def launch_gastown_review_leg(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    pack_spec: PackSpec,
    assignment_id: str,
) -> None:
    target = f"{workspace.rig_name}/{pack_spec.binding}.polecat"
    run_checked(
        [
            gc_bin,
            "--city",
            str(workspace.city_dir),
            "--rig",
            workspace.rig_name,
            "sling",
            target,
            assignment_id,
            "--force",
            "--on",
            "mol-review-leg",
            "--title",
            GASTOWN_REVIEW_TITLE,
            "--var",
            f"binding_prefix={pack_spec.binding}.",
            "--nudge",
            "--json",
        ],
        cwd=workspace.rig_dir,
        env=env,
        timeout=parse_duration("5m"),
        log_output=True,
    )


def wait_for_bead_closed(
    gc_bin: str,
    workspace: GateWorkspace,
    bead_id: str,
    *,
    env: Mapping[str, str],
    timeout: float,
    poll_interval: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_bead: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last_bead = show_bead(gc_bin, workspace, bead_id, env=env)
        status = str(last_bead.get("status") or "")
        print(f"bead {bead_id}: status={status or '<unset>'}", flush=True)
        if status == "closed":
            return last_bead
        time.sleep(poll_interval)
    raise GateError(
        f"timed out after {timeout:.0f}s waiting for bead {bead_id} to close; last bead={last_bead!r}\n"
        + collect_diagnostics(gc_bin, workspace, env=env)
    )


def bead_notes_text(bead: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in ("notes", "note", "comments"):
        value = bead.get(key)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    parts.extend(string_values(item))
    return "\n".join(parts)


def require_gastown_review_report(bead: Mapping[str, Any]) -> None:
    notes = bead_notes_text(bead)
    lower = notes.lower()
    required = ("## summary", "## findings", "## recommendation")
    missing = [section for section in required if section not in lower]
    if missing:
        raise GateError(
            "Gastown review-leg assignment closed without the expected structured report notes. "
            f"Missing sections: {', '.join(missing)}"
        )
    if "refinery" not in lower or "on-demand" not in lower.replace("on demand", "on-demand"):
        raise GateError(
            "Gastown review-leg report did not identify the expected on-demand refinery assertion. "
            f"Notes: {notes[:1000]}"
        )


def all_gastown_formula_contracts() -> dict[str, tuple[str, ...]]:
    contracts: dict[str, list[str]] = {}
    for group in (GASTOWN_FORMULA_CONTRACTS, GASTOWN_BUILD_WORKFLOW_CONTRACTS):
        for formula_name, fragments in group.items():
            contracts.setdefault(formula_name, []).extend(fragments)
    return {formula_name: tuple(fragments) for formula_name, fragments in contracts.items()}


def validate_methodology_flow_contract(pack_spec: PackSpec) -> None:
    contract = METHODOLOGY_FLOW_CONTRACTS.get(pack_spec.name)
    if contract is None:
        return
    missing: list[str] = []

    build_document = load_methodology_formula(pack_spec.source, pack_spec.build_formula, missing)
    review_document = load_methodology_formula(pack_spec.source, pack_spec.review_formula, missing)
    if build_document:
        validate_methodology_build_formula(pack_spec, build_document, contract, missing)
    if review_document:
        validate_methodology_review_formula(pack_spec, review_document, contract, missing)

    expansion_routes = contract.get("expansion_routes", {})
    expansion_checks = contract.get("expansion_checks", {})
    if isinstance(expansion_routes, dict):
        for expansion_name, required_routes in expansion_routes.items():
            expansion_document = load_methodology_formula(pack_spec.source, str(expansion_name), missing)
            if expansion_document:
                validate_methodology_expansion(
                    pack_spec,
                    str(expansion_name),
                    expansion_document,
                    tuple(str(route) for route in required_routes),
                    str(expansion_checks.get(expansion_name, "")) if isinstance(expansion_checks, Mapping) else "",
                    missing,
                )

    if missing:
        raise GateError(
            f"{pack_spec.name} methodology flow contract drifted:\n"
            + "\n".join(f"- {item}" for item in missing)
        )


def load_methodology_formula(pack_source: Path, formula_name: str | None, missing: list[str]) -> dict[str, Any] | None:
    if not formula_name:
        missing.append("missing formula name in pack spec")
        return None
    path = pack_source / "formulas" / f"{formula_name}.formula.toml"
    if not path.is_file():
        missing.append(f"{formula_name}: missing formula file {path}")
        return None
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        missing.append(f"{formula_name}: invalid TOML: {exc}")
        return None
    if not isinstance(payload, dict):
        missing.append(f"{formula_name}: formula TOML did not parse to a table")
        return None
    if payload.get("formula") != formula_name:
        missing.append(f"{formula_name}: formula field is {payload.get('formula')!r}, want {formula_name!r}")
    if payload.get("contract") != "graph.v2":
        missing.append(f"{formula_name}: contract is {payload.get('contract')!r}, want 'graph.v2'")
    return payload


def validate_methodology_build_formula(
    pack_spec: PackSpec,
    document: Mapping[str, Any],
    contract: Mapping[str, Any],
    missing: list[str],
) -> None:
    if "build-base" not in list_values(document.get("extends")):
        missing.append(f"{pack_spec.build_formula}: must extend build-base")
    if document.get("target_required") is not True:
        missing.append(f"{pack_spec.build_formula}: target_required must be true")

    methodology = nested_mapping(document, "metadata", "gc", "methodology")
    for mode_key, expected in (
        ("allowed_drain_policies", ("separate", "same-session")),
        ("interaction_modes", ("headless",)),
        ("review_modes", ("report",)),
    ):
        observed = list_values(methodology.get(mode_key))
        for value in expected:
            if value not in observed:
                missing.append(f"{pack_spec.build_formula}: metadata.gc.methodology.{mode_key} missing {value!r}")

    steps = steps_by_id(document)
    build_steps = contract.get("build_steps", {})
    if not isinstance(build_steps, dict):
        missing.append(f"{pack_spec.name}: build_steps contract must be a table")
        return
    for step_id, expectations in build_steps.items():
        if not isinstance(expectations, Mapping):
            missing.append(f"{pack_spec.name}: invalid build step contract for {step_id}")
            continue
        step = steps.get(str(step_id))
        if not step:
            missing.append(f"{pack_spec.build_formula}: missing step {step_id}")
            continue
        validate_step_contract(pack_spec.build_formula or pack_spec.name, str(step_id), step, expectations, missing)


def validate_methodology_review_formula(
    pack_spec: PackSpec,
    document: Mapping[str, Any],
    contract: Mapping[str, Any],
    missing: list[str],
) -> None:
    if "code-review-base" not in list_values(document.get("extends")):
        missing.append(f"{pack_spec.review_formula}: must extend code-review-base")
    if document.get("mode") != "report":
        missing.append(f"{pack_spec.review_formula}: mode must be report")
    if document.get("internal") is not True:
        missing.append(f"{pack_spec.review_formula}: internal must be true")

    steps = steps_by_id(document)
    write_report = steps.get("write-report")
    if not write_report:
        missing.append(f"{pack_spec.review_formula}: missing step write-report")
        return
    validate_step_contract(
        pack_spec.review_formula or pack_spec.name,
        "write-report",
        write_report,
        {
            "artifact_schema": "gc.build.review.v1",
            "artifact_path_key": "gc.var.report_path",
            "expand": str(contract.get("review_expansion", "")),
        },
        missing,
    )


def validate_methodology_expansion(
    pack_spec: PackSpec,
    formula_name: str,
    document: Mapping[str, Any],
    required_routes: Sequence[str],
    required_check: str,
    missing: list[str],
) -> None:
    if document.get("type") != "expansion":
        missing.append(f"{formula_name}: type must be expansion")
    observed = expansion_route_targets(document)
    for expected in required_routes:
        if not any(route_matches(actual, expected) for actual in observed):
            missing.append(
                f"{formula_name}: missing expected route {expected!r}. "
                f"Observed routes: {', '.join(observed) if observed else '<none>'}"
            )

    loop_templates = [
        template
        for template in list_dicts(document.get("template"))
        if str(template.get("id") or "").endswith("-loop")
    ]
    if required_check and not any(step_check_path(template).endswith(required_check) for template in loop_templates):
        missing.append(f"{formula_name}: loop template missing {required_check}")


def validate_step_contract(
    formula_name: str,
    step_id: str,
    step: Mapping[str, Any],
    expectations: Mapping[str, Any],
    missing: list[str],
) -> None:
    metadata = mapping_value(step.get("metadata"))
    expected_run_target = expectations.get("run_target")
    if expected_run_target is not None and metadata.get("gc.run_target") != expected_run_target:
        missing.append(
            f"{formula_name}:{step_id}: gc.run_target is {metadata.get('gc.run_target')!r}, "
            f"want {expected_run_target!r}"
        )

    expected_schema = expectations.get("artifact_schema")
    if expected_schema is not None and metadata.get("gc.build.artifact_schema") != expected_schema:
        missing.append(
            f"{formula_name}:{step_id}: gc.build.artifact_schema is "
            f"{metadata.get('gc.build.artifact_schema')!r}, want {expected_schema!r}"
        )

    expected_path_key = expectations.get("artifact_path_key")
    if expected_path_key is not None:
        keys = [part.strip() for part in str(metadata.get("gc.build.artifact_path_keys") or "").split(",")]
        if expected_path_key not in keys:
            missing.append(f"{formula_name}:{step_id}: artifact path keys missing {expected_path_key!r}")

    expected_expand = expectations.get("expand")
    if expected_expand is not None and step.get("expand") != expected_expand:
        missing.append(f"{formula_name}:{step_id}: expand is {step.get('expand')!r}, want {expected_expand!r}")

    expected_drain_formula = expectations.get("drain_formula")
    drain = mapping_value(step.get("drain"))
    if expected_drain_formula is not None and drain.get("formula") != expected_drain_formula:
        missing.append(
            f"{formula_name}:{step_id}: drain formula is {drain.get('formula')!r}, "
            f"want {expected_drain_formula!r}"
        )

    expected_drain_context = expectations.get("drain_context")
    if expected_drain_context is not None and drain.get("context") != expected_drain_context:
        missing.append(
            f"{formula_name}:{step_id}: drain context is {drain.get('context')!r}, "
            f"want {expected_drain_context!r}"
        )

    if expectations.get("single_lane") is True and mapping_value(drain.get("item")).get("single_lane") is not True:
        missing.append(f"{formula_name}:{step_id}: drain.item.single_lane must be true")

    expected_needs = expectations.get("needs")
    if expected_needs is not None:
        observed_needs = list_values(step.get("needs"))
        for need in expected_needs:
            if need not in observed_needs:
                missing.append(f"{formula_name}:{step_id}: needs missing {need!r}")

    expected_check = expectations.get("check")
    if expected_check is not None and not step_check_path(step).endswith(str(expected_check)):
        missing.append(f"{formula_name}:{step_id}: check path missing {expected_check!r}")


def steps_by_id(document: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(step["id"]): step
        for step in list_dicts(document.get("steps"))
        if isinstance(step.get("id"), str)
    }


def expansion_route_targets(document: Mapping[str, Any]) -> list[str]:
    targets: list[str] = []
    for template in list_dicts(document.get("template")):
        targets.extend(metadata_route_targets(template))
        for child in list_dicts(template.get("children")):
            targets.extend(metadata_route_targets(child))
    return dedupe_strings(targets)


def metadata_route_targets(table: Mapping[str, Any]) -> list[str]:
    metadata = mapping_value(table.get("metadata"))
    target = metadata.get("gc.run_target")
    return [target] if isinstance(target, str) and target.strip() else []


def step_check_path(table: Mapping[str, Any]) -> str:
    check = mapping_value(mapping_value(table.get("check")).get("check"))
    path = check.get("path")
    return path if isinstance(path, str) else ""


def nested_mapping(value: Mapping[str, Any], *keys: str) -> Mapping[str, Any]:
    current: Any = value
    for key in keys:
        current = mapping_value(current).get(key)
    return mapping_value(current)


def mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def list_dicts(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def list_values(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def validate_gastown_orchestration_contract(pack_source: Path) -> None:
    missing: list[str] = []
    for formula_name, required_fragments in all_gastown_formula_contracts().items():
        path = pack_source / "formulas" / f"{formula_name}.toml"
        if not path.is_file():
            missing.append(f"{formula_name}: missing formula file {path}")
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for fragment in required_fragments:
            if fragment not in text:
                missing.append(f"{formula_name}: missing contract fragment {fragment!r}")
    if missing:
        raise GateError("Gastown orchestration contract drifted:\n" + "\n".join(f"- {item}" for item in missing))


def stop_city(gc_bin: str, workspace: GateWorkspace, *, env: Mapping[str, str]) -> None:
    for command in (
        [gc_bin, "stop", str(workspace.city_dir), "--force", "--timeout", "30s"],
        [gc_bin, "supervisor", "stop", "--wait", "--wait-timeout", "30s"],
    ):
        try:
            run_checked(command, env=env, timeout=parse_duration("1m"))
        except Exception as exc:  # pragma: no cover - cleanup best effort
            print(f"cleanup command failed ({shlex.join(command)}): {exc}", file=sys.stderr)


def require_smoke_ack(bead: Mapping[str, Any], pack_spec: PackSpec) -> None:
    expected = f"PACK_SMOKE_OK: {pack_spec.name}"
    if expected not in {line.strip() for line in bead_notes_text(bead).splitlines()}:
        raise GateError(f"{pack_spec.name} model smoke closed without required acknowledgment {expected!r}")


def run_smoke_gate(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    pack_spec: PackSpec,
    timeout: float,
    poll_interval: float,
) -> None:
    bead_id = launch_smoke_bead(gc_bin, workspace, env=env, pack_spec=pack_spec)
    closed = wait_for_bead_closed(
        gc_bin,
        workspace,
        bead_id,
        env=env,
        timeout=timeout,
        poll_interval=poll_interval,
    )
    require_smoke_ack(closed, pack_spec)
    target = smoke_target(workspace, pack_spec)
    validate_required_routes(
        [closed],
        [target],
        context=f"{pack_spec.name} model smoke",
    )


def run_review_gate(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    pack_spec: PackSpec,
    timeout: float,
    poll_interval: float,
) -> None:
    root_id = launch_review_formula(gc_bin, workspace, env=env, pack_spec=pack_spec)
    root_bead = wait_for_workflow_pass(
        gc_bin,
        workspace,
        root_id,
        env=env,
        timeout=timeout,
        poll_interval=poll_interval,
    )
    validate_review_report(root_bead, workspace, env=env, pack_spec=pack_spec)
    validate_required_routes(
        list_beads(gc_bin, workspace, env=env),
        pack_spec.required_review_routes,
        context=f"{pack_spec.name} review gate",
    )


def run_build_gate(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    pack_spec: PackSpec,
    timeout: float,
    poll_interval: float,
) -> None:
    root_id = launch_build_formula(gc_bin, workspace, env=env, pack_spec=pack_spec)
    root_bead = wait_for_workflow_pass(
        gc_bin,
        workspace,
        root_id,
        env=env,
        timeout=timeout,
        poll_interval=poll_interval,
    )
    validate_build_basic_artifacts(root_bead, rig_dir=workspace.rig_dir, env=env, validator_source=pack_spec.validator_source)
    workflow_beads = list_beads(gc_bin, workspace, env=env)
    validate_build_basic_result(
        workspace.rig_dir,
        [root_bead, *workflow_beads],
        env=env,
        timeout=parse_duration("2m"),
    )
    validate_required_routes(
        workflow_beads,
        pack_spec.required_build_routes,
        context=f"{pack_spec.name} build gate",
    )


def run_gastown_orchestration_gate(
    gc_bin: str,
    workspace: GateWorkspace,
    *,
    env: Mapping[str, str],
    pack_spec: PackSpec,
    timeout: float,
    poll_interval: float,
) -> None:
    if not pack_spec.gastown:
        raise GateError(f"pack {pack_spec.name} is not a Gastown orchestration pack")
    wait_for_gastown_sessions(
        gc_bin,
        workspace,
        env=env,
        required_agents=GASTOWN_ALWAYS_ON_AGENTS,
        timeout=min(timeout, parse_duration("5m")),
    )
    assignment_id = create_gastown_review_assignment(gc_bin, workspace, env=env)
    launch_gastown_review_leg(gc_bin, workspace, env=env, pack_spec=pack_spec, assignment_id=assignment_id)
    assignment = wait_for_bead_closed(
        gc_bin,
        workspace,
        assignment_id,
        env=env,
        timeout=timeout,
        poll_interval=poll_interval,
    )
    require_gastown_review_report(assignment)


def expand_pack_selection(selection: str) -> list[str]:
    if selection == "all-supported":
        return list(PACK_SPECS.keys())
    if selection == "methodology":
        return list(METHODOLOGY_PACKS)
    if selection == "model-smoke":
        return list(MODEL_SMOKE_PACKS)
    if selection in PACK_SPECS:
        return [selection]
    raise GateError(f"unknown pack selection: {selection}")


def resolve_pack_spec(args: argparse.Namespace, pack_name: str) -> PackSpec:
    base = PACK_SPECS[pack_name]
    pack_source = args.pack_source.resolve() if args.pack_source else base.source.resolve()
    roles_source = args.roles_source.resolve() if args.roles_source else base.roles_source.resolve()
    validator_source = args.validator_source.resolve() if args.validator_source else base.validator_source.resolve()
    pack_spec = replace(base, source=pack_source, roles_source=roles_source, validator_source=validator_source)
    if not (pack_spec.source / "pack.toml").is_file():
        raise GateError(f"pack source does not contain pack.toml for {pack_spec.name}: {pack_spec.source}")
    if not pack_spec.gastown and not (pack_spec.roles_source / "pack.toml").is_file():
        raise GateError(f"roles source does not contain pack.toml for {pack_spec.name}: {pack_spec.roles_source}")
    return pack_spec


def run_gate(args: argparse.Namespace, *, pack_name: str | None = None, workdir: Path | None = None) -> None:
    gc_bin = resolve_binary(args.gc_bin)
    bd_bin = resolve_binary(args.bd_bin)
    selected_pack = pack_name or args.pack
    pack_spec = resolve_pack_spec(args, selected_pack)

    gates = expand_gate_selection(args.gate, pack_spec)
    timeout = parse_duration(args.timeout)
    poll_interval = parse_duration(args.poll_interval)

    selected_workdir = workdir or args.workdir
    if selected_workdir:
        work_root = selected_workdir.resolve()
        if work_root.exists() and any(work_root.iterdir()):
            raise GateError(f"--workdir must be empty or absent: {work_root}")
        cleanup = False
    else:
        work_root = Path(tempfile.mkdtemp(prefix=f"{pack_spec.name}-pack-inference-"))
        cleanup = not args.keep_workdir

    workspace = write_gate_workspace(
        work_root,
        pack_source=pack_spec.source,
        roles_source=pack_spec.roles_source,
        validator_source=pack_spec.validator_source,
        pack_binding=pack_spec.binding,
        pack_name=pack_spec.name,
        gastown=pack_spec.gastown,
        include_pack_at_city_scope=not (pack_spec.gastown and SMOKE_GATE in gates),
        city_name=city_name_for_pack(args, pack_spec),
        rig_name=args.rig_name,
    )
    env = build_gate_env(gc_bin, workspace, bd_bin=bd_bin)
    should_stop = False
    try:
        validate_gc_bd_compatibility(gc_bin, bd_bin, env=env)
        if not args.skip_inference_env_check and not args.setup_only:
            expected_model = validate_inference_env(env)
            preflight_inference_model(expected_model, env=env)
        should_stop = True
        initialize_city(gc_bin, workspace, pack_spec=pack_spec, gates=gates, env=env)
        if args.setup_only:
            print(f"setup-only gate passed for {pack_spec.name}; workdir: {workspace.root}", flush=True)
            return
        start_city(gc_bin, workspace, env=env)
        for gate in gates:
            print(f"running {pack_spec.name} pack inference gate: {gate}", flush=True)
            if gate == SMOKE_GATE:
                run_smoke_gate(
                    gc_bin,
                    workspace,
                    env=env,
                    pack_spec=pack_spec,
                    timeout=timeout,
                    poll_interval=poll_interval,
                )
            elif gate == REVIEW_GATE:
                run_review_gate(
                    gc_bin,
                    workspace,
                    env=env,
                    pack_spec=pack_spec,
                    timeout=timeout,
                    poll_interval=poll_interval,
                )
            elif gate in (BUILD_GATE, BUILD_BASIC_GATE):
                run_build_gate(
                    gc_bin,
                    workspace,
                    env=env,
                    pack_spec=pack_spec,
                    timeout=timeout,
                    poll_interval=poll_interval,
                )
            elif gate == GASTOWN_ORCHESTRATION_GATE:
                run_gastown_orchestration_gate(
                    gc_bin,
                    workspace,
                    env=env,
                    pack_spec=pack_spec,
                    timeout=timeout,
                    poll_interval=poll_interval,
                )
            else:  # pragma: no cover - guarded by expand_gate_selection
                raise GateError(f"unsupported gate: {gate}")
        print(f"{pack_spec.name} pack inference gate passed: {', '.join(gates)}", flush=True)
        if args.keep_workdir or args.workdir:
            print(f"inference gate workdir: {workspace.root}", flush=True)
    finally:
        if should_stop:
            stop_city(gc_bin, workspace, env=env)
        if cleanup:
            shutil.rmtree(workspace.root, ignore_errors=True)


def city_name_for_pack(args: argparse.Namespace, pack_spec: PackSpec) -> str:
    if args.city_name != "gascity-pack-inference-gate" or pack_spec.name == GASCITY_PACK:
        return args.city_name
    return f"{pack_spec.name}-pack-inference-gate"


def resolve_binary(value: str) -> str:
    if os.path.sep in value:
        path = Path(value).resolve()
        if path.is_file():
            return str(path)
        raise GateError(f"binary not found: {value}")
    resolved = shutil.which(value)
    if not resolved:
        raise GateError(f"binary not found on PATH: {value}")
    return resolved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gc-bin", default=os.environ.get("GC_BIN", "gc"), help="gc binary to exercise")
    parser.add_argument(
        "--bd-bin",
        default=os.environ.get("GC_BEADS_BIN", "bd"),
        help="bd binary that must be schema-compatible with --gc-bin",
    )
    parser.add_argument(
        "--pack",
        choices=SUPPORTED_PACK_CHOICES,
        default=GASCITY_PACK,
        help="supported pack or pack group to exercise",
    )
    parser.add_argument("--pack-source", type=Path, help="override local pack root; valid only with a single --pack")
    parser.add_argument(
        "--roles-source",
        type=Path,
        help="override local gascity roles pack root used by non-Gastown packs",
    )
    parser.add_argument(
        "--validator-source",
        type=Path,
        help="override local pack root that provides build artifact validators and schemas",
    )
    parser.add_argument("--workdir", type=Path, help="directory for the disposable gate city and rig")
    parser.add_argument("--keep-workdir", action="store_true", help="keep the generated workdir after success")
    parser.add_argument("--city-name", default="gascity-pack-inference-gate", help="disposable city name")
    parser.add_argument("--rig-name", default="fixture", help="disposable rig name")
    parser.add_argument(
        "--gate",
        choices=(ALL_GATE, REVIEW_GATE, BUILD_GATE, BUILD_BASIC_GATE, GASTOWN_ORCHESTRATION_GATE, SMOKE_GATE),
        default=DEFAULT_GATE,
        help="which inference gate to run",
    )
    parser.add_argument("--timeout", default=DEFAULT_TIMEOUT, help="workflow completion timeout")
    parser.add_argument("--poll-interval", default=DEFAULT_POLL_INTERVAL, help="workflow polling interval")
    parser.add_argument("--setup-only", action="store_true", help="initialize imports/config without launching inference")
    parser.add_argument(
        "--skip-inference-env-check",
        action="store_true",
        help="skip Ollama/Claude env validation; intended only with --setup-only",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        pack_names = expand_pack_selection(args.pack)
        if len(pack_names) > 1 and args.pack_source:
            raise GateError("--pack-source can only be used when --pack selects a single pack")
        if len(pack_names) > 1 and args.workdir:
            args.workdir.mkdir(parents=True, exist_ok=True)
        for pack_name in pack_names:
            workdir = args.workdir / pack_name if len(pack_names) > 1 and args.workdir else None
            run_gate(args, pack_name=pack_name, workdir=workdir)
    except (GateError, subprocess.CalledProcessError, subprocess.TimeoutExpired, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
