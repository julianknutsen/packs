#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import validate_build_artifact
import validate_verdict_report

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


FRONT_MATTER_RE = re.compile(r"\A---\n(?P<body>.*?)\n---(?:\n|\Z)", re.DOTALL)
BODY_HASH_RE = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
UNSAFE_MARKER_RE = re.compile(r"<!--|--!?>|[\r\n]")
VALID_TRIAGE_VERDICTS = {
    "reproduced",
    "not_reproduced",
    "needs_info",
    "not_a_bug",
    "duplicate",
    "security_sensitive",
}
VALID_PRIORITIES = {"p0", "p1", "p2", "p3"}
VALID_ACTIONS = {"fix", "test_hardening", "close", "ask_reporter", "defer", "security_process"}
VALID_ACTIONS_BY_VERDICT = {
    "reproduced": {"fix", "defer"},
    "not_reproduced": {"test_hardening", "defer"},
    "needs_info": {"ask_reporter"},
    "not_a_bug": {"close"},
    "duplicate": {"close"},
    "security_sensitive": {"security_process"},
}

# gc.build.review.v1 `status` -> gc.verdict-report.v1 `verdict`. draft/questions/
# superseded have no terminal review outcome and are deliberately unmapped: the
# review must reach one of these before a PR comment can be rendered.
REVIEW_STATUS_TO_VERDICT = {
    "approved": "pass",
    "changes_required": "fail",
    "blocked": "fail",
}
FINDING_TABLE_COLUMNS = ("id", "severity", "title", "evidence", "required_fix")


class ValidationError(Exception):
    pass


YAML_ERROR_TYPES = (yaml.YAMLError,) if yaml is not None else ()
CLI_ERROR_TYPES = (
    OSError,
    UnicodeDecodeError,
    ValidationError,
    validate_verdict_report.ValidationError,
    validate_build_artifact.ValidationError,
) + YAML_ERROR_TYPES


@dataclass(frozen=True)
class TriageReport:
    schema: str
    repo: str
    issue_number: int
    body_hash: str
    verdict: str
    priority: str
    recommended_next_action: str
    reproduction_artifact_path: str
    reproduction_diff_path: str
    analysis_body: str


def validate_triage_report_text(
    text: str,
    *,
    expected_repo: str = "",
    expected_issue_number: int = 0,
    expected_body_hash: str = "",
) -> TriageReport:
    if yaml is None:
        raise ValidationError("PyYAML is required to parse triage reports")
    match = FRONT_MATTER_RE.match(text)
    if not match:
        raise ValidationError("triage report must start with YAML front matter")
    data = yaml.safe_load(match.group("body")) or {}
    if not isinstance(data, dict):
        raise ValidationError("triage report front matter must be a mapping")

    schema = required_string(data, "schema")
    repo = required_string(data, "repo")
    issue_number = required_int(data, "issue_number")
    body_hash = required_string(data, "body_hash")
    verdict = required_string(data, "verdict")
    priority = required_string(data, "priority")
    recommended_next_action = required_string(data, "recommended_next_action")
    reproduction_artifact_path = optional_string(data, "reproduction_artifact_path")
    reproduction_diff_path = optional_string(data, "reproduction_diff_path")

    if schema != "gc.github-issue-triage-report.v1":
        raise ValidationError(f"schema must be gc.github-issue-triage-report.v1, got {schema!r}")
    if issue_number <= 0:
        raise ValidationError("issue_number must be a positive integer")
    if not BODY_HASH_RE.match(body_hash):
        raise ValidationError("body_hash must be sha256:<64 lowercase hex chars>")
    if verdict not in VALID_TRIAGE_VERDICTS:
        raise ValidationError(f"verdict must be one of {sorted(VALID_TRIAGE_VERDICTS)}, got {verdict!r}")
    if priority not in VALID_PRIORITIES:
        raise ValidationError(f"priority must be one of {sorted(VALID_PRIORITIES)}, got {priority!r}")
    if recommended_next_action not in VALID_ACTIONS:
        raise ValidationError(f"recommended_next_action must be one of {sorted(VALID_ACTIONS)}")
    if recommended_next_action not in VALID_ACTIONS_BY_VERDICT[verdict]:
        raise ValidationError(f"recommended_next_action {recommended_next_action!r} is invalid for verdict {verdict!r}")
    if expected_repo and repo != expected_repo:
        raise ValidationError(f"repo must be {expected_repo!r}, got {repo!r}")
    if expected_issue_number and issue_number != expected_issue_number:
        raise ValidationError(f"issue_number must be {expected_issue_number}, got {issue_number}")
    if expected_body_hash and body_hash != expected_body_hash:
        raise ValidationError(f"body_hash must be {expected_body_hash!r}, got {body_hash!r}")
    analysis_body = text[match.end() :].strip()
    if not analysis_body:
        raise ValidationError("triage report analysis body must not be empty")
    return TriageReport(
        schema=schema,
        repo=repo,
        issue_number=issue_number,
        body_hash=body_hash,
        verdict=verdict,
        priority=priority,
        recommended_next_action=recommended_next_action,
        reproduction_artifact_path=reproduction_artifact_path,
        reproduction_diff_path=reproduction_diff_path,
        analysis_body=analysis_body,
    )


def review_outcome(verdict: str, severity: str) -> str:
    if verdict == "pass" and severity == "none":
        return "approve"
    if verdict == "fail" and severity == "minor":
        return "comment"
    if verdict == "fail" and severity == "major":
        return "request_changes"
    if verdict == "fail" and severity == "blocker":
        return "block"
    raise ValidationError(f"unsupported review verdict/severity combination: {verdict}/{severity}")


def translate_review_report(review_path: Path) -> str:
    """Derive a gc.verdict-report.v1 document from a gc.build.review.v1 report.

    The generic `review` formula's write-report step produces (and gates on)
    gc.build.review.v1: free-form Verdict/Findings/Verification prose behind a
    `status` front-matter field. The PR adapter's own gate expects
    gc.verdict-report.v1: a fixed verdict/severity/findings[] shape. The two
    schemas are immutable and cannot be unified, so this is the adapter
    boundary between them.
    """
    artifact = validate_build_artifact.validate_artifact_text(
        review_path.read_text(encoding="utf-8"), expected_schema="gc.build.review.v1"
    )
    status = artifact.front_matter["status"]
    verdict = REVIEW_STATUS_TO_VERDICT.get(status)
    if verdict is None:
        raise ValidationError(
            f"review status {status!r} has no verdict-report mapping; resolve the review to one of "
            f"{sorted(REVIEW_STATUS_TO_VERDICT)} before translating"
        )

    findings = parse_findings_table(extract_body_section(artifact.body, "Findings"))

    if verdict == "pass":
        if findings:
            raise ValidationError(f"{status!r} review report must not carry findings; resolve or downgrade status first")
        severity = "none"
    else:
        if not findings:
            raise ValidationError(f"{status!r} review report must include a Findings table with at least one row")
        severity = max(
            (finding["severity"] for finding in findings),
            key=lambda value: validate_verdict_report.SEVERITY_ORDER[value],
        )
        if status == "blocked" and severity != "blocker":
            raise ValidationError("a blocked review report must include at least one blocker-severity finding")

    document = {
        "schema": "gc.verdict-report.v1",
        "kind": "review",
        "verdict": verdict,
        "severity": severity,
        "findings": findings,
    }
    return "---\n" + yaml.safe_dump(document, sort_keys=False, allow_unicode=True) + "---\n"


def extract_body_section(body: str, section: str) -> str:
    match = re.search(rf"^##\s+{re.escape(section)}\s*$", body, re.MULTILINE)
    if not match:
        return ""
    start = match.end()
    next_heading = re.search(r"^##\s+\S", body[start:], re.MULTILINE)
    end = start + next_heading.start() if next_heading else len(body)
    return body[start:end]


def parse_findings_table(section_text: str) -> list[dict[str, str]]:
    lines = section_text.splitlines()
    index = 0
    while index < len(lines):
        cells = validate_build_artifact.split_table_row(lines[index])
        header = [cell.lower().replace(" ", "_") for cell in cells]
        if header and set(FINDING_TABLE_COLUMNS).issubset(header):
            positions = {name: header.index(name) for name in FINDING_TABLE_COLUMNS}
            index += 1
            if index < len(lines) and validate_build_artifact.is_separator_row(lines[index]):
                index += 1
            findings: list[dict[str, str]] = []
            while index < len(lines):
                row = validate_build_artifact.split_table_row(lines[index])
                if not row or len(row) <= max(positions.values()):
                    break
                finding = {name: row[position] for name, position in positions.items()}
                if not all(finding.values()):
                    raise ValidationError(f"findings table row {finding} is missing a required column value")
                if finding["severity"] not in validate_verdict_report.VALID_SEVERITIES - {"none"}:
                    raise ValidationError(
                        f"findings table row {finding['id']!r} has invalid severity {finding['severity']!r}; "
                        f"must be one of {sorted(validate_verdict_report.VALID_SEVERITIES - {'none'})}"
                    )
                findings.append(finding)
                index += 1
            return findings
        index += 1
    return []


def render_pr_review_comment(
    report_path: Path,
    *,
    outcome: str,
    head_sha: str = "",
    artifact_ref: str = "",
    human_approved: bool = False,
) -> str:
    report = validate_verdict_report.validate_report_text(report_path.read_text(encoding="utf-8"), expected_kind="review")
    if outcome != review_outcome(report.verdict, report.severity):
        raise ValidationError(f"outcome {outcome!r} does not match review report {report.verdict}/{report.severity}")
    head_sha = marker_value("head_sha", head_sha)
    artifact = public_line("artifact_ref", artifact_ref or str(report_path))
    gate_text = "human approved" if human_approved else "not human approved"
    return (
        f"<!-- gc:github-pr-review head_sha={head_sha} outcome={outcome} -->\n"
        "## GC PR Review\n\n"
        f"- outcome: {outcome}\n"
        f"- report: {report.verdict}/{report.severity}\n"
        f"- gate: {gate_text}\n"
        f"- artifact: {artifact}\n"
    )


def render_triage_comment(
    report_path: Path,
    *,
    artifact_ref: str = "",
    human_approved: bool = False,
) -> str:
    report = validate_triage_report_text(report_path.read_text(encoding="utf-8"))
    repo = marker_value("repo", report.repo, allow_empty=False)
    gate_text = "human approved" if human_approved else "not human approved"
    lines = [
        f"<!-- gc:github-issue-triage repo={repo} issue={report.issue_number} body_hash={report.body_hash} -->",
        "## GC Issue Triage",
        "",
        f"- verdict: {report.verdict}",
        f"- priority: {report.priority}",
        f"- next_action: {report.recommended_next_action}",
        f"- body_hash: {report.body_hash}",
        f"- gate: {gate_text}",
    ]
    if artifact_ref:
        lines.append(f"- artifact: {public_line('artifact_ref', artifact_ref)}")
    approval_notes = []
    if report.verdict == "security_sensitive":
        approval_notes.append("security-sensitive details require human approval before public posting")
    if report.priority == "p0":
        approval_notes.append("p0 details require human approval before public posting")
    if approval_notes and not human_approved:
        lines.append(f"- note: {'; '.join(approval_notes)}")
    elif report.analysis_body:
        lines.extend(["", "## Analysis", "", demote_markdown_headings(report.analysis_body)])
    return "\n".join(lines) + "\n"


def render_issue_fix_status(
    *,
    state: str,
    summary: str,
    run_id: str = "",
    pr_url: str = "",
    artifact_ref: str = "",
) -> str:
    state = marker_value("state", state, allow_empty=False)
    run_id = marker_value("run_id", run_id)
    summary = public_line("summary", summary)
    lines = [
        f"<!-- gc:github-issue-fix-status run_id={run_id} state={state} -->",
        "## GC Issue Fix",
        "",
        f"- state: {state}",
        f"- summary: {summary.strip()}",
    ]
    if pr_url:
        lines.append(f"- pr: {public_line('pr_url', pr_url)}")
    if artifact_ref:
        lines.append(f"- artifact: {public_line('artifact_ref', artifact_ref)}")
    return "\n".join(lines) + "\n"


def required_string(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{key} must be a non-empty string")
    return value.strip()


def optional_string(data: dict[str, Any], key: str) -> str:
    value = data.get(key, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValidationError(f"{key} must be a string")
    return value.strip()


def required_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, int):
        return value
    raise ValidationError(f"{key} must be an integer")


def marker_value(name: str, value: str, *, allow_empty: bool = True) -> str:
    value = value.strip()
    if not value:
        if allow_empty:
            return ""
        raise ValidationError(f"{name} must not be empty")
    if UNSAFE_MARKER_RE.search(value):
        raise ValidationError(f"{name} contains unsafe marker text")
    return value


def public_line(name: str, value: str) -> str:
    value = value.strip()
    if not value:
        raise ValidationError(f"{name} must not be empty")
    if UNSAFE_MARKER_RE.search(value):
        raise ValidationError(f"{name} contains unsafe public text")
    return value


def demote_markdown_headings(text: str) -> str:
    lines: list[str] = []
    for line in text.strip().splitlines():
        safe_line = line.replace("<!--", "&lt;!--").replace("-->", "--&gt;")
        hash_heading = re.match(r"^( {0,3})(#{1,5}\s.*)$", safe_line)
        max_hash_heading = re.match(r"^( {0,3})(#{6,}\s.*)$", safe_line)
        setext_underline = re.match(r"^( {0,3})(=+|-+)\s*$", safe_line)
        if hash_heading:
            lines.append(f"{hash_heading.group(1)}#{hash_heading.group(2)}")
        elif max_hash_heading:
            lines.append(f"{max_hash_heading.group(1)}\\{max_hash_heading.group(2)}")
        elif setext_underline:
            lines.append(f"{setext_underline.group(1)}\\{setext_underline.group(2)}")
        else:
            lines.append(safe_line)
    return "\n".join(lines).strip()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and render GitHub workflow reports")
    subparsers = parser.add_subparsers(dest="command", required=True)

    triage_parser = subparsers.add_parser("validate-triage")
    triage_parser.add_argument("path", type=Path)
    triage_parser.add_argument("--repo", default="")
    triage_parser.add_argument("--issue-number", type=int, default=0)
    triage_parser.add_argument("--body-hash", default="")

    outcome_parser = subparsers.add_parser("review-outcome")
    outcome_parser.add_argument("path", type=Path)

    translate_review_parser = subparsers.add_parser("translate-review-report")
    translate_review_parser.add_argument("report_path", type=Path)
    translate_review_parser.add_argument("--output", type=Path, required=True)

    review_comment_parser = subparsers.add_parser("render-review-comment")
    review_comment_parser.add_argument("report_path", type=Path)
    review_comment_parser.add_argument("--output", type=Path, required=True)
    review_comment_parser.add_argument("--head-sha", default="")
    review_comment_parser.add_argument("--artifact-ref", default="")
    review_comment_parser.add_argument("--human-approved", action="store_true")

    triage_comment_parser = subparsers.add_parser("render-triage-comment")
    triage_comment_parser.add_argument("report_path", type=Path)
    triage_comment_parser.add_argument("--output", type=Path, required=True)
    triage_comment_parser.add_argument("--artifact-ref", default="")
    triage_comment_parser.add_argument("--human-approved", action="store_true")

    fix_status_parser = subparsers.add_parser("render-issue-fix-status")
    fix_status_parser.add_argument("--state", required=True)
    fix_status_parser.add_argument("--summary", required=True)
    fix_status_parser.add_argument("--output", type=Path, required=True)
    fix_status_parser.add_argument("--run-id", default="")
    fix_status_parser.add_argument("--pr-url", default="")
    fix_status_parser.add_argument("--artifact-ref", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        if args.command == "validate-triage":
            report = validate_triage_report_text(
                args.path.read_text(encoding="utf-8"),
                expected_repo=args.repo,
                expected_issue_number=args.issue_number,
                expected_body_hash=args.body_hash,
            )
            output = {"ok": True, "verdict": report.verdict, "recommended_next_action": report.recommended_next_action}
        elif args.command == "review-outcome":
            report = validate_verdict_report.validate_report_text(args.path.read_text(encoding="utf-8"), expected_kind="review")
            output = {"ok": True, "outcome": review_outcome(report.verdict, report.severity)}
        elif args.command == "translate-review-report":
            translated = translate_review_report(args.report_path)
            args.output.write_text(translated, encoding="utf-8")
            report = validate_verdict_report.validate_report_text(translated, expected_kind="review")
            output = {"ok": True, "verdict": report.verdict, "severity": report.severity, "output": str(args.output)}
        elif args.command == "render-review-comment":
            report = validate_verdict_report.validate_report_text(args.report_path.read_text(encoding="utf-8"), expected_kind="review")
            outcome = review_outcome(report.verdict, report.severity)
            args.output.write_text(
                render_pr_review_comment(
                    args.report_path,
                    outcome=outcome,
                    head_sha=args.head_sha,
                    artifact_ref=args.artifact_ref,
                    human_approved=args.human_approved,
                ),
                encoding="utf-8",
            )
            output = {"ok": True, "outcome": outcome, "output": str(args.output)}
        elif args.command == "render-triage-comment":
            args.output.write_text(
                render_triage_comment(
                    args.report_path,
                    artifact_ref=args.artifact_ref,
                    human_approved=args.human_approved,
                ),
                encoding="utf-8",
            )
            output = {"ok": True, "output": str(args.output)}
        elif args.command == "render-issue-fix-status":
            args.output.write_text(
                render_issue_fix_status(
                    state=args.state,
                    summary=args.summary,
                    run_id=args.run_id,
                    pr_url=args.pr_url,
                    artifact_ref=args.artifact_ref,
                ),
                encoding="utf-8",
            )
            output = {"ok": True, "output": str(args.output)}
        else:  # pragma: no cover
            raise ValidationError(f"unsupported command {args.command}")
    except CLI_ERROR_TYPES as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
