from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock
from contextlib import redirect_stderr, redirect_stdout
import io

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "assets" / "scripts"))

import validate_context_bundle as context_validator
import validate_build_artifact as build_artifact_validator
import validate_verdict_report as verdict_validator


class ContextBundleValidatorTests(unittest.TestCase):
    def test_valid_context_bundle_accepts_only_name_path_description(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            subject = root / "requirements.md"
            subject.write_text("# Requirements\n", encoding="utf-8")
            bundle = root / "context.yaml"
            bundle.write_text(
                "items:\n"
                "  - name: Requirements\n"
                "    path: requirements.md\n"
                "    description: Product requirements.\n",
                encoding="utf-8",
            )

            result = context_validator.validate_bundle(bundle, allowed_roots=[root])

            self.assertEqual([item.name for item in result.items], ["Requirements"])
            self.assertEqual(result.items[0].resolved_path, subject.resolve())

    def test_context_bundle_rejects_unknown_item_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "requirements.md").write_text("# Requirements\n", encoding="utf-8")
            bundle = root / "context.yaml"
            bundle.write_text(
                "items:\n"
                "  - name: Requirements\n"
                "    path: requirements.md\n"
                "    description: Product requirements.\n"
                "    inline: no\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(context_validator.ValidationError, "unknown fields"):
                context_validator.validate_bundle(bundle, allowed_roots=[root])

    def test_context_bundle_rejects_missing_files_and_symlink_escapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = pathlib.Path(tmp) / "outside.md"
            outside.write_text("outside\n", encoding="utf-8")
            link = root / "link.md"
            link.symlink_to(outside)
            bundle = root / "context.yaml"
            bundle.write_text(
                "items:\n"
                "  - name: Link\n"
                "    path: link.md\n"
                "    description: Escaping symlink.\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(context_validator.ValidationError, "outside allowed roots"):
                context_validator.validate_bundle(bundle, allowed_roots=[root / "allowed"])

            bundle.write_text(
                "items:\n"
                "  - name: Missing\n"
                "    path: missing.md\n"
                "    description: Missing file.\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(context_validator.ValidationError, "does not exist"):
                context_validator.validate_bundle(bundle, allowed_roots=[root])

    def test_context_bundle_rejects_binary_and_secret_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            binary = root / "blob.bin"
            binary.write_bytes(b"abc\x00def")
            bundle = root / "context.yaml"
            bundle.write_text(
                "items:\n"
                "  - name: Blob\n"
                "    path: blob.bin\n"
                "    description: Binary file.\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(context_validator.ValidationError, "binary"):
                context_validator.validate_bundle(bundle, allowed_roots=[root])

            secret = root / ".env"
            secret.write_text("TOKEN=secret\n", encoding="utf-8")
            bundle.write_text(
                "items:\n"
                "  - name: Secret\n"
                "    path: .env\n"
                "    description: Secret file.\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(context_validator.ValidationError, "secret"):
                context_validator.validate_bundle(bundle, allowed_roots=[root])

            for filename in (".env.production", "private.pem", ".ssh/config", ".git/config", "cookies.txt"):
                secret = root / filename
                secret.parent.mkdir(parents=True, exist_ok=True)
                secret.write_text("secret\n", encoding="utf-8")
                bundle.write_text(
                    "items:\n"
                    "  - name: Secret\n"
                    f"    path: {filename}\n"
                    "    description: Secret file.\n",
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(context_validator.ValidationError, "secret"):
                    context_validator.validate_bundle(bundle, allowed_roots=[root])

    def test_context_bundle_accepts_benign_files_under_secret_named_ancestors(self) -> None:
        with tempfile.TemporaryDirectory(prefix="secret-project-") as tmp:
            root = pathlib.Path(tmp)
            subject = root / "requirements.md"
            subject.write_text("# Requirements\n", encoding="utf-8")
            bundle = root / "context.yaml"
            bundle.write_text(
                "items:\n"
                "  - name: Requirements\n"
                "    path: requirements.md\n"
                "    description: Product requirements.\n",
                encoding="utf-8",
            )

            result = context_validator.validate_bundle(bundle, allowed_roots=[root])

            self.assertEqual(result.items[0].resolved_path, subject.resolve())

    def test_context_bundle_rejects_secret_named_relative_directories(self) -> None:
        for dirname in (
            "secrets",
            "credentials",
            "tokens",
            "cookies",
            "vault-secrets",
            "aws-credentials",
            "oauth-tokens",
            "auth-cookies",
            "id_rsa_backup",
            "config/my-secret-store",
        ):
            with self.subTest(dirname=dirname), tempfile.TemporaryDirectory() as tmp:
                root = pathlib.Path(tmp)
                subject = root / dirname / "config.yaml"
                subject.parent.mkdir(parents=True)
                subject.write_text("token: value\n", encoding="utf-8")
                bundle = root / "context.yaml"
                bundle.write_text(
                    "items:\n"
                    "  - name: Secret config\n"
                    f"    path: {dirname}/config.yaml\n"
                    "    description: Secret material.\n",
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(context_validator.ValidationError, "secret"):
                    context_validator.validate_bundle(bundle, allowed_roots=[root])

    def test_context_bundle_rejects_symlink_to_secret_named_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            subject = root / "secrets" / "config.yaml"
            subject.parent.mkdir()
            subject.write_text("token: value\n", encoding="utf-8")
            link = root / "context.md"
            link.symlink_to(subject)
            bundle = root / "context.yaml"
            bundle.write_text(
                "items:\n"
                "  - name: Context\n"
                "    path: context.md\n"
                "    description: Benign-looking symlink.\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(context_validator.ValidationError, "secret"):
                context_validator.validate_bundle(bundle, allowed_roots=[root])

    def test_context_bundle_cli_reports_malformed_yaml_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bundle = pathlib.Path(tmp) / "context.yaml"
            bundle.write_text("items:\n  - name: [\n", encoding="utf-8")
            stderr = io.StringIO()

            with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                code = context_validator.main([str(bundle)])

            self.assertEqual(code, 1)
            self.assertIn("error:", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())


class BuildArtifactValidatorTests(unittest.TestCase):
    SCHEMA_SECTIONS = {
        "gc.build.requirements.v1": [
            "Problem Statement",
            "W6H",
            "User Stories",
            "Technical Stories",
            "Behavior Requirements",
            "Example Mapping",
            "Acceptance Criteria",
            "Out Of Scope",
            "Open Questions",
        ],
        "gc.build.plan.v1": [
            "Summary",
            "Current System",
            "Proposed Implementation",
            "Non-Goals",
            "Verification",
        ],
        "gc.build.decomposition.v1": [
            "Summary",
            "Selected Downstream Formulas",
            "Implementation Convoy",
            "Work Items",
        ],
        "gc.build.implementation-summary.v1": [
            "Summary",
            "Intended Behavior",
            "Changed Files",
            "Verification",
            "Remaining Risks",
        ],
        "gc.build.review.v1": [
            "Verdict",
            "Findings",
            "Verification",
        ],
        "gc.build.final-report.v1": [
            "Summary",
            "Outcome",
            "Artifacts",
            "Remaining Risks",
        ],
    }
    SCHEMA_STATUS = {
        "gc.build.requirements.v1": "approved",
        "gc.build.plan.v1": "approved",
        "gc.build.decomposition.v1": "approved",
        "gc.build.implementation-summary.v1": "approved",
        "gc.build.review.v1": "approved",
        "gc.build.final-report.v1": "approved",
    }
    SCHEMA_FILES = {
        "gc.build.requirements.v1": "requirements.v1.yaml",
        "gc.build.plan.v1": "plan.v1.yaml",
        "gc.build.decomposition.v1": "decomposition.v1.yaml",
        "gc.build.implementation-summary.v1": "implementation-summary.v1.yaml",
        "gc.build.review.v1": "review.v1.yaml",
        "gc.build.final-report.v1": "final-report.v1.yaml",
    }
    SCHEMA_ROOT = pathlib.Path(__file__).resolve().parents[1] / "schemas" / "build"
    VALIDATOR_SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "assets" / "scripts" / "validate_build_artifact.py"

    def valid_artifact(self, schema: str = "gc.build.requirements.v1") -> str:
        sections = []
        for section in self.SCHEMA_SECTIONS[schema]:
            content = f"{section} content."
            if section in {"Example Mapping", "Summary", "Verdict"}:
                content += (
                    "\n\n| ID | Status |\n"
                    "| --- | --- |\n"
                    "| GC-METH-001 | covered |\n"
                    "| GC-METH-012 | deferred |"
                )
            sections.append(f"## {section}\n\n{content}")
        body = "\n\n".join(sections)
        return f"""---
schema: {schema}
workflow:
  id: build-20260609-001
  formula: build-basic
methodology:
  pack: gascity
  name: build-basic
producer:
  formula: planning-base
  stage: requirements
  attempt: 1
status: {self.SCHEMA_STATUS[schema]}
trace:
  upstream:
    - path: requirements.after.md
      hash: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  coverage:
    - id: GC-METH-001
      status: covered
    - id: GC-METH-012
      status: deferred
      rationale: Derived-pack compatibility is verified by a later work item.
---

{body}
"""

    def test_build_artifact_accepts_valid_minimal_artifacts_for_all_base_schemas(self) -> None:
        for schema in self.SCHEMA_SECTIONS:
            with self.subTest(schema=schema):
                artifact = build_artifact_validator.validate_artifact_text(
                    self.valid_artifact(schema),
                    expected_schema=schema,
                )

                self.assertEqual(artifact.schema_id, schema)
                self.assertEqual([entry["id"] for entry in artifact.coverage], ["GC-METH-001", "GC-METH-012"])

    def test_build_artifact_rejects_missing_front_matter_and_wrong_schema(self) -> None:
        with self.assertRaisesRegex(build_artifact_validator.ValidationError, "front matter"):
            build_artifact_validator.validate_artifact_text("# Missing front matter\n", expected_schema="gc.build.requirements.v1")

        with self.assertRaisesRegex(build_artifact_validator.ValidationError, "schema"):
            build_artifact_validator.validate_artifact_text(
                self.valid_artifact("gc.build.plan.v1"),
                expected_schema="gc.build.requirements.v1",
            )

    def test_build_artifact_rejects_missing_upstream_hash(self) -> None:
        text = self.valid_artifact().replace("      hash: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n", "")

        with self.assertRaisesRegex(build_artifact_validator.ValidationError, "hash"):
            build_artifact_validator.validate_artifact_text(text, expected_schema="gc.build.requirements.v1")

    def test_build_artifact_rejects_invalid_coverage_status_and_missing_rationale(self) -> None:
        invalid_status = self.valid_artifact().replace("status: deferred", "status: waiting", 1)
        missing_rationale = self.valid_artifact().replace(
            "      rationale: Derived-pack compatibility is verified by a later work item.\n",
            "",
        )

        with self.assertRaisesRegex(build_artifact_validator.ValidationError, "coverage"):
            build_artifact_validator.validate_artifact_text(invalid_status, expected_schema="gc.build.requirements.v1")
        with self.assertRaisesRegex(build_artifact_validator.ValidationError, "rationale"):
            build_artifact_validator.validate_artifact_text(missing_rationale, expected_schema="gc.build.requirements.v1")

    def test_build_artifact_rejects_markdown_yaml_coverage_mismatch(self) -> None:
        text = self.valid_artifact().replace("| GC-METH-012 | deferred |", "| GC-METH-012 | covered |")

        with self.assertRaisesRegex(build_artifact_validator.ValidationError, "markdown coverage"):
            build_artifact_validator.validate_artifact_text(text, expected_schema="gc.build.requirements.v1")

    def test_build_artifact_validator_and_schema_files_are_present(self) -> None:
        self.assertTrue(self.VALIDATOR_SCRIPT.is_file(), f"missing {self.VALIDATOR_SCRIPT}")
        for schema_id, filename in self.SCHEMA_FILES.items():
            with self.subTest(schema=schema_id):
                self.assertTrue((self.SCHEMA_ROOT / filename).is_file(), f"missing {self.SCHEMA_ROOT / filename}")

    def test_build_artifact_schemas_keep_producer_metadata_neutral(self) -> None:
        for schema_id in self.SCHEMA_FILES:
            with self.subTest(schema=schema_id):
                schema = build_artifact_validator.load_schema(schema_id)

                leaves = {str(field).split(".")[-1].lower() for field in schema["required_front_matter"]}
                self.assertFalse(leaves & build_artifact_validator.FORBIDDEN_REQUIRED_FIELD_NAMES)

    def test_build_artifact_rejects_schema_requiring_role_fields(self) -> None:
        for field in ("owner", "stage-owner", "persona", "producer.role"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(build_artifact_validator.ValidationError, "must not require"):
                    build_artifact_validator.validate_schema_definition(
                        {"schema_id": "gc.build.requirements.v1", "required_front_matter": ["schema", field]}
                    )

    def test_build_artifact_statuses_follow_base_approval_states(self) -> None:
        review_questions = self.valid_artifact("gc.build.review.v1").replace(
            "\nstatus: approved\n", "\nstatus: questions\n"
        )
        summary_complete = self.valid_artifact("gc.build.implementation-summary.v1").replace(
            "\nstatus: approved\n", "\nstatus: complete\n"
        )

        artifact = build_artifact_validator.validate_artifact_text(
            review_questions, expected_schema="gc.build.review.v1"
        )
        self.assertEqual(artifact.front_matter["status"], "questions")
        with self.assertRaisesRegex(build_artifact_validator.ValidationError, "status"):
            build_artifact_validator.validate_artifact_text(
                summary_complete, expected_schema="gc.build.implementation-summary.v1"
            )

    def test_build_artifact_requires_coverage_for_declared_upstream_ids(self) -> None:
        declared = self.valid_artifact().replace(
            "      hash: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
            "      hash: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
            "      ids:\n"
            "        - GC-METH-001\n"
            "        - GC-METH-012\n",
        )
        missing = declared.replace("        - GC-METH-012\n", "        - GC-METH-012\n        - GC-METH-099\n")

        artifact = build_artifact_validator.validate_artifact_text(declared, expected_schema="gc.build.requirements.v1")
        self.assertEqual([entry["id"] for entry in artifact.coverage], ["GC-METH-001", "GC-METH-012"])
        with self.assertRaisesRegex(build_artifact_validator.ValidationError, "GC-METH-099"):
            build_artifact_validator.validate_artifact_text(missing, expected_schema="gc.build.requirements.v1")

    def test_build_artifact_cli_reports_errors_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = pathlib.Path(tmp) / "requirements.md"
            report.write_text("---\nschema: [\n---\n", encoding="utf-8")
            stderr = io.StringIO()

            with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                code = build_artifact_validator.main(
                    ["--schema", "gc.build.requirements.v1", "--path", str(report)]
                )

            self.assertEqual(code, 1)
            self.assertIn("error:", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())


class VerdictReportValidatorTests(unittest.TestCase):
    def test_verdict_report_accepts_pass_and_fail_reports(self) -> None:
        pass_report = """---
schema: gc.verdict-report.v1
kind: review
verdict: pass
severity: none
findings: []
---

No issues found.
"""
        fail_report = """---
schema: gc.verdict-report.v1
kind: gap-analysis
verdict: fail
severity: major
findings:
  - id: gap-001
    severity: major
    title: Missing restart test
    evidence: No test covers restart.
    required_fix: Add restart coverage.
---

Failure details.
"""

        self.assertEqual(verdict_validator.validate_report_text(pass_report).verdict, "pass")
        self.assertEqual(verdict_validator.validate_report_text(fail_report).severity, "major")

    def test_verdict_report_rejects_bad_schema_and_unstructured_failures(self) -> None:
        bad_schema = """---
schema: other
kind: review
verdict: pass
severity: none
findings: []
---
"""
        no_findings = """---
schema: gc.verdict-report.v1
kind: review
verdict: fail
severity: major
findings: []
---
"""

        with self.assertRaisesRegex(verdict_validator.ValidationError, "schema"):
            verdict_validator.validate_report_text(bad_schema)
        with self.assertRaisesRegex(verdict_validator.ValidationError, "findings"):
            verdict_validator.validate_report_text(no_findings)

    def test_verdict_report_rejects_severity_that_is_not_max_finding_severity(self) -> None:
        report = """---
schema: gc.verdict-report.v1
kind: review
verdict: fail
severity: minor
findings:
  - id: rev-001
    severity: blocker
    title: Unsafe publish
    evidence: Publish can mutate protected branch.
    required_fix: Block protected branches.
---
"""

        with self.assertRaisesRegex(verdict_validator.ValidationError, "maximum"):
            verdict_validator.validate_report_text(report)

    def test_verdict_report_rejects_non_string_finding_fields(self) -> None:
        field_values = {
            "id": "0",
            "severity": "0",
            "title": "{}",
            "evidence": "null",
            "required_fix": "[]",
        }
        for field, yaml_value in field_values.items():
            with self.subTest(field=field):
                report = f"""---
schema: gc.verdict-report.v1
kind: review
verdict: fail
severity: major
findings:
  - id: rev-001
    severity: major
    title: Missing test
    evidence: No test.
    required_fix: Add one.
    {field}: {yaml_value}
---
"""

                with self.assertRaisesRegex(verdict_validator.ValidationError, "missing fields"):
                    verdict_validator.validate_report_text(report)

    def test_verdict_report_cli_reports_malformed_yaml_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = pathlib.Path(tmp) / "review.md"
            report.write_text("---\nschema: [\n---\n", encoding="utf-8")
            stderr = io.StringIO()

            with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                code = verdict_validator.main([str(report), "--kind", "review"])

            self.assertEqual(code, 1)
            self.assertIn("error:", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())

    def test_verdict_report_cli_reports_invalid_utf8_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = pathlib.Path(tmp) / "review.md"
            report.write_bytes(b"\xff\xfe---\n")
            stderr = io.StringIO()

            with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                code = verdict_validator.main([str(report), "--kind", "review"])

            self.assertEqual(code, 1)
            self.assertIn("error:", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())


class BuildArtifactSchemaRootsTests(unittest.TestCase):
    def _write_schema(self, root: pathlib.Path, name: str, schema_id: str) -> None:
        (root / name).write_text(
            "schema_id: " + schema_id + "\n"
            "required_front_matter: [schema, status, trace]\n"
            "allowed_statuses: [approved]\n"
            "coverage_statuses: [covered]\n"
            "required_sections: []\n",
            encoding="utf-8",
        )

    def test_extra_root_resolves_new_schema_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self._write_schema(root, "custom.v1.yaml", "acme.build.custom.v1")
            with mock.patch.dict(os.environ, {"GC_BUILD_SCHEMA_ROOTS": str(root)}):
                schema = build_artifact_validator.load_schema("acme.build.custom.v1")
            self.assertEqual(schema["schema_id"], "acme.build.custom.v1")

    def test_extra_root_cannot_shadow_base_schema_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self._write_schema(root, "requirements.v1.yaml", "gc.build.requirements.v1")
            with mock.patch.dict(os.environ, {"GC_BUILD_SCHEMA_ROOTS": str(root)}):
                schema = build_artifact_validator.load_schema("gc.build.requirements.v1")
            # Base-first ordering: the published base definition wins; the
            # shadow attempt in the extra root is never consulted.
            self.assertIn("workflow.id", schema.get("required_front_matter", []))

    def test_unset_env_is_byte_identical_base_behavior(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "GC_BUILD_SCHEMA_ROOTS"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                build_artifact_validator.schema_roots(),
                [build_artifact_validator.SCHEMA_ROOT],
            )
            with self.assertRaises(build_artifact_validator.ValidationError):
                build_artifact_validator.load_schema("acme.build.custom.v1")

    def test_missing_or_blank_extra_roots_are_skipped(self) -> None:
        bogus = os.pathsep.join(["", "  ", "/nonexistent/schema/root"])
        with mock.patch.dict(os.environ, {"GC_BUILD_SCHEMA_ROOTS": bogus}):
            self.assertEqual(
                build_artifact_validator.schema_roots(),
                [build_artifact_validator.SCHEMA_ROOT],
            )


if __name__ == "__main__":
    unittest.main()
