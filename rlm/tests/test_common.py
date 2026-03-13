from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

if "rlm" not in sys.modules:
    fake_rlm = types.ModuleType("rlm")

    class _DummyRLMError(Exception):
        pass

    fake_rlm.BudgetExceededError = _DummyRLMError
    fake_rlm.CancellationError = _DummyRLMError
    fake_rlm.ErrorThresholdExceededError = _DummyRLMError
    fake_rlm.TimeoutExceededError = _DummyRLMError
    fake_rlm.TokenLimitExceededError = _DummyRLMError
    fake_rlm.RLM = object
    sys.modules["rlm"] = fake_rlm

    fake_logger = types.ModuleType("rlm.logger")
    fake_logger.RLMLogger = object
    sys.modules["rlm.logger"] = fake_logger

    fake_utils = types.ModuleType("rlm.utils")
    sys.modules["rlm.utils"] = fake_utils

    fake_prompts = types.ModuleType("rlm.utils.prompts")
    fake_prompts.RLM_SYSTEM_PROMPT = ""
    sys.modules["rlm.utils.prompts"] = fake_prompts

from rlm_cli import clamp_policy_override, create_runtime_config
from rlm_common import (
    CLIError,
    RuntimeConfig,
    backend_requires_network,
    cache_dir,
    ensure_runtime_layout,
    is_binary_blob,
    load_runtime_config,
    save_runtime_config,
    stage_corpus,
)
from rlm_runner import SourceTracker, parse_final_payload


class RuntimeConfigTests(unittest.TestCase):
    def test_runtime_config_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            city_root = Path(tmp)
            ensure_runtime_layout(city_root)
            cfg = RuntimeConfig(
                backend="openai",
                model="test-model",
                base_url="http://127.0.0.1:8000/v1",
                backend_api_key_env="",
                remote_backend_allowed=False,
                allowed_environments=["local"],
                default_environment="local",
                docker_image="",
                installed_at="2026-03-13T00:00:00+00:00",
            )
            save_runtime_config(city_root, cfg)
            loaded = load_runtime_config(city_root)
            self.assertEqual(loaded.model, "test-model")
            self.assertEqual(loaded.base_url, "http://127.0.0.1:8000/v1")
            self.assertEqual(loaded.allowed_environments, ["local"])
            self.assertEqual(loaded.default_environment, "local")

    def test_loopback_base_url_does_not_require_remote_ack(self) -> None:
        cfg = RuntimeConfig(
            backend="openai",
            model="test-model",
            base_url="http://127.0.0.1:8000/v1",
            backend_api_key_env="",
            remote_backend_allowed=False,
            allowed_environments=["local"],
            default_environment="local",
            docker_image="",
            installed_at="2026-03-13T00:00:00+00:00",
        )
        self.assertFalse(backend_requires_network(cfg))

    def test_call_overrides_can_reach_the_configured_ceiling(self) -> None:
        self.assertEqual(clamp_policy_override(3, 2, 3), 3)
        self.assertEqual(clamp_policy_override(4, 2, 3), 3)
        self.assertEqual(clamp_policy_override(None, 2, 3), 2)

    def test_invalid_install_policy_is_rejected_before_side_effects(self) -> None:
        args = SimpleNamespace(
            backend="openai",
            model="test-model",
            base_url="",
            backend_api_key_env=None,
            allow_remote_backend=False,
            environment="local",
            max_depth=5,
            max_depth_ceiling=3,
            max_iterations=16,
            max_iterations_ceiling=24,
            max_calls_per_hour=12,
            max_duration_seconds=300,
            max_tokens_per_call=120000,
            disable_logging=False,
            log_retention_days=7,
            ignore_gitignore=False,
        )
        with self.assertRaises(CLIError):
            create_runtime_config(args, Path("/tmp/fake-pack"))


class StageCorpusTests(unittest.TestCase):
    def test_stage_corpus_skips_gitignored_secrets_and_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            city_root = Path(tmp)
            ensure_runtime_layout(city_root)

            (city_root / "keep.txt").write_text("hello\nworld\n", encoding="utf-8")
            (city_root / "utf8.txt").write_text("é" * 200, encoding="utf-8")
            (city_root / ".env").write_text("SECRET=value\n", encoding="utf-8")
            (city_root / "ignored.log").write_text("ignore me\n", encoding="utf-8")
            (city_root / "binary.bin").write_bytes(b"\x00\xff\x00\xff")

            subprocess.run(["git", "init"], cwd=city_root, check=True, capture_output=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=city_root,
                check=True,
                capture_output=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Test User"],
                cwd=city_root,
                check=True,
                capture_output=True,
            )
            (city_root / ".gitignore").write_text("ignored.log\n", encoding="utf-8")

            bundle = stage_corpus(
                city_root=city_root,
                cwd=city_root,
                path_args=["."],
                glob_args=[],
                stdin_text=None,
                cfg=RuntimeConfig(default_environment="local", allowed_environments=["local"]),
            )

            staged_paths = {entry.display_path for entry in bundle.files}
            self.assertIn("keep.txt", staged_paths)
            self.assertIn("utf8.txt", staged_paths)
            self.assertIn(".gitignore", staged_paths)
            self.assertNotIn(".env", staged_paths)
            self.assertNotIn("ignored.log", staged_paths)
            self.assertNotIn("binary.bin", staged_paths)
            self.assertIn(".env", bundle.truncated_paths)
            self.assertIn(str((city_root / "ignored.log").as_posix()), bundle.truncated_paths)

    def test_unicode_text_is_not_classified_as_binary(self) -> None:
        self.assertFalse(is_binary_blob(("é" * 200).encode("utf-8")))

    def test_glob_symlink_outside_workspace_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            city_root = Path(tmp)
            ensure_runtime_layout(city_root)
            (city_root / "keep.txt").write_text("keep\n", encoding="utf-8")
            outside_dir = city_root.parent / "outside"
            outside_dir.mkdir()
            (outside_dir / "secret.txt").write_text("secret\n", encoding="utf-8")
            try:
                (city_root / "linked.txt").symlink_to(outside_dir / "secret.txt")
            except OSError as exc:
                self.skipTest(f"symlinks unavailable: {exc}")

            bundle = stage_corpus(
                city_root=city_root,
                cwd=city_root,
                path_args=[],
                glob_args=["*.txt"],
                stdin_text=None,
                cfg=RuntimeConfig(default_environment="local", allowed_environments=["local"]),
            )

            staged_paths = {entry.display_path for entry in bundle.files}
            self.assertIn("keep.txt", staged_paths)
            self.assertNotIn(str((outside_dir / "secret.txt").as_posix()), staged_paths)

    def test_stage_corpus_cleans_temp_dir_on_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            city_root = Path(tmp)
            ensure_runtime_layout(city_root)
            (city_root / "keep.txt").write_text("hello\n", encoding="utf-8")

            with mock.patch("rlm_common.read_text_file", side_effect=PermissionError("denied")):
                with self.assertRaises(PermissionError):
                    stage_corpus(
                        city_root=city_root,
                        cwd=city_root,
                        path_args=["keep.txt"],
                        glob_args=[],
                        stdin_text=None,
                        cfg=RuntimeConfig(default_environment="local", allowed_environments=["local"]),
                    )

            self.assertEqual(list(cache_dir(city_root).iterdir()), [])


class RunnerPayloadTests(unittest.TestCase):
    def test_parse_final_payload_prefers_tracked_sources(self) -> None:
        tracker = SourceTracker(
            [{"display_path": "keep.txt", "staged_relpath": "keep.txt", "line_count": 10}],
            Path("/tmp/context"),
        )
        tracker.record("keep.txt", 1, 2)
        payload = parse_final_payload(
            '{"answer":"ok","sources":[{"path":"fake.txt"}],"complete":true,"notes":[]}',
            tracker,
            [],
            complete_default=True,
            metadata=None,
            max_depth=2,
            max_iterations=4,
        )
        self.assertEqual(payload["sources"], [{"path": "keep.txt", "start_line": 1, "end_line": 2}])


if __name__ == "__main__":
    unittest.main()
