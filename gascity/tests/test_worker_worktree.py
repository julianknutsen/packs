"""Contract tests for gascity/assets/scripts/worker-worktree.sh.

Each test builds a throwaway "remote" (bare repo) plus a rig checkout, then
runs the script the way gc runs a pre_start command: cwd = the work dir,
GC_DIR / GC_RIG_ROOT / GC_TRIGGER_BEAD_ID in the environment. One row per
contract line in the script header; the rig root is checked untouched after
every case.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import tempfile
import time
import unittest


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "assets" / "scripts" / "worker-worktree.sh"


def git(cwd: pathlib.Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def commit(repo: pathlib.Path, name: str, content: str, message: str) -> str:
    (repo / name).write_text(content, encoding="utf-8")
    git(repo, "add", name)
    git(
        repo,
        "-c", "user.name=t", "-c", "user.email=t@example.invalid",
        "commit", "--quiet", "-m", message,
    )
    return git(repo, "rev-parse", "HEAD")


class Fixture:
    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self.remote = root / "remote.git"
        self.rig = root / "rig"
        self.city = root / "city"
        self.city.mkdir()
        subprocess.run(["git", "init", "--quiet", "--bare", "-b", "main", str(self.remote)], check=True)
        subprocess.run(["git", "init", "--quiet", "-b", "main", str(self.rig)], check=True)
        git(self.rig, "remote", "add", "origin", str(self.remote))
        self.main_sha = commit(self.rig, "README.md", "hello\n", "init")
        git(self.rig, "push", "--quiet", "-u", "origin", "main")
        git(self.rig, "remote", "set-head", "origin", "main")
        self.rig_status_before = self.rig_snapshot()

    def rig_snapshot(self) -> tuple[str, str, str]:
        return (
            git(self.rig, "rev-parse", "--abbrev-ref", "HEAD"),
            git(self.rig, "rev-parse", "HEAD"),
            git(self.rig, "status", "--porcelain"),
        )

    def push_branch(self, name: str, filename: str = "feature.txt") -> str:
        """Create a branch on the remote (not present locally in the rig)."""
        scratch = self.root / f"scratch-{name.replace('/', '_')}"
        subprocess.run(["git", "clone", "--quiet", str(self.remote), str(scratch)], check=True)
        git(scratch, "switch", "--quiet", "-c", name)
        sha = commit(scratch, filename, f"{name}\n", f"work on {name}")
        git(scratch, "push", "--quiet", "origin", name)
        return sha

    def push_branch_on(self, remote: str, name: str, filename: str) -> str:
        sha = self.push_branch(name, filename)
        git(self.rig, "fetch", "--quiet", remote)
        return sha

    def run(self, workdir: pathlib.Path, bead: str | None, *extra: str, check: bool = True, mkdir: bool = True, env_extra: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "GC_DIR": str(workdir),
            "GC_RIG_ROOT": str(self.rig),
            "GC_CITY": str(self.city),
            "WORKER_WORKTREE_LOCK_WAIT": "10",
            **(env_extra or {}),
        }
        env.pop("GC_TRIGGER_BEAD_ID", None)
        if bead is not None:
            env["GC_TRIGGER_BEAD_ID"] = bead
        if mkdir:
            workdir.mkdir(parents=True, exist_ok=True)  # gc MkdirAll's work_dir before pre_start
        proc = subprocess.run(
            ["sh", str(SCRIPT), *extra],
            cwd=str(workdir) if workdir.is_dir() else str(self.city),
            env=env,
            capture_output=True,
            text=True,
        )
        if check:
            assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
        return proc


class WorkerWorktreeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.fx = Fixture(pathlib.Path(self._tmp.name).resolve())
        self.lane = self.fx.city / ".worktrees" / "rig" / "lane-worker"

    def tearDown(self) -> None:
        # The rig root's working tree is never touched, whatever the case.
        self.assertEqual(self.fx.rig_snapshot(), self.fx.rig_status_before)
        self._tmp.cleanup()

    def branch_of(self, path: pathlib.Path) -> str:
        return git(path, "rev-parse", "--abbrev-ref", "HEAD")

    def assert_is_worktree(self, path: pathlib.Path) -> None:
        common = pathlib.Path(git(path, "rev-parse", "--git-common-dir"))
        if not common.is_absolute():
            common = path / common
        self.assertEqual(common.resolve(), (self.fx.rig / ".git").resolve())

    def asides(self, lane: pathlib.Path | None = None) -> list[pathlib.Path]:
        lane = lane or self.lane
        return sorted(lane.parent.glob(f"{lane.name}.aside-*"))

    # --- fresh work dir -----------------------------------------------------------

    def test_empty_workdir_new_bead_creates_branch_from_base(self) -> None:
        proc = self.fx.run(self.lane, "gp-abc1")
        self.assert_is_worktree(self.lane)
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), self.fx.main_sha)
        self.assertTrue((self.lane / "README.md").is_file())
        self.assertEqual(proc.stdout.split(), ["WORKTREE", str(self.lane), "gp-abc1", self.fx.main_sha[:7]])

    def test_workdir_that_does_not_exist_yet_is_created_when_its_parent_does(self) -> None:
        self.lane.parent.mkdir(parents=True)
        proc = self.fx.run(self.lane, "gp-abc1", mkdir=False)
        self.assert_is_worktree(self.lane)
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")
        self.assertEqual(proc.stdout.split()[1], str(self.lane))

    def test_rerun_is_idempotent(self) -> None:
        self.fx.run(self.lane, "gp-abc1")
        first = git(self.lane, "rev-parse", "HEAD")
        proc = self.fx.run(self.lane, "gp-abc1")
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), first)
        self.assertNotIn("WARN", proc.stderr)
        self.assertEqual(self.asides(), [])

    def test_new_branch_tracks_fresh_remote_tip_not_stale_local_main(self) -> None:
        # The rig checkout lags: origin/main moves on, local main does not.
        newer = self.fx.push_branch("main-advance", "newer.txt")
        scratch = self.fx.root / "scratch-main-advance"
        git(scratch, "push", "--quiet", "origin", "main-advance:main")
        self.fx.run(self.lane, "gp-abc1")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), newer)
        self.assertEqual(git(self.fx.rig, "rev-parse", "main"), self.fx.main_sha)

    # --- bead branch resolution -------------------------------------------------------

    def test_existing_remote_branch_named_for_bead_is_checked_out_and_tracked(self) -> None:
        sha = self.fx.push_branch("fix/gp-def2-resume-me")
        self.fx.run(self.lane, "gp-def2")
        self.assertEqual(self.branch_of(self.lane), "fix/gp-def2-resume-me")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), sha)
        self.assertEqual(git(self.lane, "rev-parse", "--abbrev-ref", "@{upstream}"), "origin/fix/gp-def2-resume-me")
        self.assertTrue((self.lane / "feature.txt").is_file())

    def test_bead_id_must_match_as_a_whole_token(self) -> None:
        # gp-abc10 and xgp-abc1 are other beads; gp-abc1 has no branch yet.
        self.fx.push_branch("fix/gp-abc10-unrelated", "a.txt")
        self.fx.push_branch("xgp-abc1-unrelated", "b.txt")
        self.fx.push_branch("gp-abc1x", "c.txt")
        self.fx.run(self.lane, "gp-abc1")
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), self.fx.main_sha)

    def test_bead_id_matches_with_slash_dash_and_dot_boundaries(self) -> None:
        for name, filename in (("feat/gp-tok1", "a.txt"),):
            sha = self.fx.push_branch(name, filename)
        self.fx.run(self.lane, "gp-tok1")
        self.assertEqual(self.branch_of(self.lane), "feat/gp-tok1")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), sha)

    def test_bead_id_with_regex_characters_is_matched_literally(self) -> None:
        self.fx.push_branch("gp-dotxx-other", "a.txt")  # would match 'gp-dot.x' as a regex
        self.fx.run(self.lane, "gp-dot.x")
        self.assertEqual(self.branch_of(self.lane), "gp-dot.x")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), self.fx.main_sha)

    def test_ambiguous_bead_branches_fail_closed(self) -> None:
        self.fx.push_branch("gp-amb1-first", "a.txt")
        self.fx.push_branch("gp-amb1-second", "b.txt")
        proc = self.fx.run(self.lane, "gp-amb1", check=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("several branches", proc.stderr)
        self.assertIn("gp-amb1-first", proc.stderr)
        self.assertIn("gp-amb1-second", proc.stderr)
        self.assertFalse((self.lane / ".git").exists())

    def test_bead_branch_checked_out_elsewhere_is_not_stolen(self) -> None:
        other = self.fx.root / "other worktree"  # a space: the porcelain parser must keep it
        git(self.fx.rig, "worktree", "add", "--quiet", "-b", "gp-ghi3-elsewhere", str(other), "origin/main")
        sha = commit(other, "elsewhere.txt", "x\n", "elsewhere work")
        proc = self.fx.run(self.lane, "gp-ghi3")
        self.assertEqual(self.branch_of(self.lane), "HEAD")  # detached
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), sha)
        self.assertIn("WARN", proc.stderr)
        self.assertIn(str(other), proc.stderr)
        self.assertNotIn("work in that worktree", proc.stderr)
        self.assertEqual(self.branch_of(other), "gp-ghi3-elsewhere")
        self.assertEqual(proc.stdout.split()[2], "detached")

    def test_rerun_under_a_path_with_spaces_keeps_its_own_branch(self) -> None:
        lane = self.fx.city / "lanes with spaces" / "lane worker"
        self.fx.run(lane, "gp-spc1")
        self.assertEqual(self.branch_of(lane), "gp-spc1")
        proc = self.fx.run(lane, "gp-spc1")
        self.assertEqual(self.branch_of(lane), "gp-spc1")  # not mis-detected as "elsewhere" and detached
        self.assertNotIn("WARN", proc.stderr)

    # --- existing lane ------------------------------------------------------------------

    def test_clean_lane_on_old_bead_switches_to_new_bead(self) -> None:
        self.fx.run(self.lane, "gp-old1")
        commit(self.lane, "old.txt", "old\n", "old bead work")
        # Untracked files (staged skills, hooks, node_modules) never count as dirt.
        (self.lane / ".agents").mkdir()
        (self.lane / ".agents" / "skills.txt").write_text("x", encoding="utf-8")
        proc = self.fx.run(self.lane, "gp-new2")
        self.assertEqual(self.branch_of(self.lane), "gp-new2")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), self.fx.main_sha)
        self.assertFalse((self.lane / "old.txt").exists())
        self.assertTrue((self.lane / ".agents" / "skills.txt").is_file())
        self.assertEqual(self.asides(), [])
        # The old bead's branch and commit survive.
        self.assertTrue(git(self.fx.rig, "rev-parse", "--verify", "gp-old1"))

    def test_dirty_lane_is_moved_aside_never_reset(self) -> None:
        self.fx.run(self.lane, "gp-old1")
        (self.lane / "README.md").write_text("uncommitted edit\n", encoding="utf-8")
        proc = self.fx.run(self.lane, "gp-new2")
        self.assertIn("moved aside", proc.stderr)
        asides = self.asides()
        self.assertEqual(len(asides), 1)
        self.assertEqual((asides[0] / "README.md").read_text(encoding="utf-8"), "uncommitted edit\n")
        self.assertEqual(self.branch_of(asides[0]), "gp-old1")
        self.assertEqual(self.branch_of(self.lane), "gp-new2")
        self.assertEqual((self.lane / "README.md").read_text(encoding="utf-8"), "hello\n")
        # git still knows both worktrees.
        listing = git(self.fx.rig, "worktree", "list", "--porcelain")
        self.assertIn(str(asides[0]), listing)
        self.assertIn(str(self.lane), listing)

    def test_switch_that_would_overwrite_an_ignored_file_moves_the_lane_aside(self) -> None:
        # The target branch tracks build/out.txt; the lane has an ignored local
        # file at that path. git switch would silently clobber it.
        sha = self.fx.push_branch("gp-ign2-tracks-build", "build-out.txt")
        scratch = self.fx.root / "scratch-gp-ign2-tracks-build"
        (scratch / "build").mkdir()
        (scratch / "build" / "out.txt").write_text("tracked\n", encoding="utf-8")
        git(scratch, "add", "build/out.txt")
        git(scratch, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "--quiet", "-m", "track build/out.txt")
        git(scratch, "push", "--quiet", "origin", "gp-ign2-tracks-build")
        self.fx.run(self.lane, "gp-old1")
        # per-worktree exclude lives in the worktree's git dir (.git here is a file)
        exclude = pathlib.Path(git(self.lane, "rev-parse", "--git-path", "info/exclude"))
        if not exclude.is_absolute():
            exclude = self.lane / exclude
        exclude.parent.mkdir(parents=True, exist_ok=True)
        exclude.write_text("build/\n", encoding="utf-8")
        (self.lane / "build").mkdir()
        (self.lane / "build" / "out.txt").write_text("local artifact, ignored\n", encoding="utf-8")
        self.assertEqual(git(self.lane, "status", "--porcelain"), "")
        proc = self.fx.run(self.lane, "gp-ign2")
        self.assertIn("moved aside", proc.stderr)
        asides = self.asides()
        self.assertEqual(len(asides), 1)
        self.assertEqual((asides[0] / "build" / "out.txt").read_text(encoding="utf-8"), "local artifact, ignored\n")
        self.assertEqual(self.branch_of(self.lane), "gp-ign2-tracks-build")
        self.assertEqual((self.lane / "build" / "out.txt").read_text(encoding="utf-8"), "tracked\n")
        self.assertNotEqual(sha, "")

    def test_locked_worktree_that_needs_moving_fails_closed(self) -> None:
        self.fx.run(self.lane, "gp-old1")
        (self.lane / "README.md").write_text("uncommitted edit\n", encoding="utf-8")
        git(self.fx.rig, "worktree", "lock", str(self.lane))
        proc = self.fx.run(self.lane, "gp-new2", check=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("refused to move", proc.stderr)
        self.assertEqual(self.asides(), [])
        self.assertEqual(self.branch_of(self.lane), "gp-old1")
        self.assertEqual((self.lane / "README.md").read_text(encoding="utf-8"), "uncommitted edit\n")

    def test_nonempty_non_worktree_dir_is_moved_aside(self) -> None:
        self.lane.mkdir(parents=True)
        (self.lane / "junk.txt").write_text("keep me\n", encoding="utf-8")
        proc = self.fx.run(self.lane, "gp-abc1")
        self.assertIn("moved aside", proc.stderr)
        asides = self.asides()
        self.assertEqual(len(asides), 1)
        self.assertEqual((asides[0] / "junk.txt").read_text(encoding="utf-8"), "keep me\n")
        self.assert_is_worktree(self.lane)
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")

    # --- no bead ----------------------------------------------------------------------------

    def test_no_bead_leaves_a_detached_worktree_at_base(self) -> None:
        proc = self.fx.run(self.lane, None)
        self.assert_is_worktree(self.lane)
        self.assertEqual(self.branch_of(self.lane), "HEAD")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), self.fx.main_sha)
        self.assertEqual(proc.stdout.split()[2], "detached")

    def test_no_bead_detaches_a_lane_that_was_on_a_branch_and_keeps_the_branch(self) -> None:
        self.fx.run(self.lane, "gp-old1")
        sha = commit(self.lane, "old.txt", "old\n", "old bead work")
        self.fx.run(self.lane, None)
        self.assertEqual(self.branch_of(self.lane), "HEAD")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), self.fx.main_sha)
        self.assertEqual(git(self.fx.rig, "rev-parse", "gp-old1"), sha)
        self.assertEqual(self.asides(), [])

    def test_no_bead_rerun_on_a_detached_lane_is_a_no_op(self) -> None:
        self.fx.run(self.lane, None)
        proc = self.fx.run(self.lane, None)
        self.assertEqual(self.branch_of(self.lane), "HEAD")
        self.assertNotIn("WARN", proc.stderr)

    # --- foreign checkouts and the lock -----------------------------------------------------------

    def test_checkout_of_another_repository_at_the_lane_is_refused_not_moved(self) -> None:
        self.lane.mkdir(parents=True)
        subprocess.run(["git", "init", "--quiet", str(self.lane)], check=True)
        (self.lane / "theirs.txt").write_text("someone else's repo\n", encoding="utf-8")
        proc = self.fx.run(self.lane, "gp-abc1", check=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("another repository", proc.stderr)
        self.assertEqual(self.asides(), [])
        self.assertEqual((self.lane / "theirs.txt").read_text(encoding="utf-8"), "someone else's repo\n")

    def lock_dir(self) -> pathlib.Path:
        return self.fx.rig / ".git" / "worker-worktree.lock"

    def test_lock_is_taken_and_released(self) -> None:
        self.fx.run(self.lane, "gp-abc1")
        self.assertFalse(self.lock_dir().exists())

    def test_stale_lock_from_a_dead_pid_is_cleared(self) -> None:
        self.lock_dir().mkdir()
        (self.lock_dir() / "pid").write_text("999999\n", encoding="utf-8")
        proc = self.fx.run(self.lane, "gp-abc1")
        self.assertIn("clearing stale lock", proc.stderr)
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")
        self.assertFalse(self.lock_dir().exists())

    def orphan_sleep(self, seconds: int) -> int:
        """Start `sleep` reparented to init so the OS reaps it when it exits."""
        out = subprocess.run(["sh", "-c", f"sleep {seconds} >/dev/null 2>&1 & echo $!"], check=True, capture_output=True, text=True).stdout
        return int(out.strip())

    def test_live_lock_is_waited_for(self) -> None:
        pid = self.orphan_sleep(3)
        self.lock_dir().mkdir()
        (self.lock_dir() / "pid").write_text(f"{pid}\n", encoding="utf-8")
        started = time.monotonic()
        proc = self.fx.run(self.lane, "gp-abc1")
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 2.0)
        self.assertIn("waiting for another run", proc.stderr)
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")
        self.assertFalse(self.lock_dir().exists())

    def test_live_lock_held_too_long_fails_closed(self) -> None:
        pid = self.orphan_sleep(8)
        self.lock_dir().mkdir()
        (self.lock_dir() / "pid").write_text(f"{pid}\n", encoding="utf-8")
        proc = self.fx.run(self.lane, "gp-abc1", check=False, env_extra={"WORKER_WORKTREE_LOCK_WAIT": "2"})
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("has held", proc.stderr)
        self.assertFalse((self.lane / ".git").exists())
        self.assertTrue(self.lock_dir().exists())  # not ours to clear while its owner lives
        os.kill(pid, 15)

    def test_zombie_lock_owner_counts_as_dead(self) -> None:
        holder = subprocess.Popen(["sleep", "0"])  # exits at once; not reaped until wait()
        time.sleep(0.5)
        self.lock_dir().mkdir()
        (self.lock_dir() / "pid").write_text(f"{holder.pid}\n", encoding="utf-8")
        try:
            proc = self.fx.run(self.lane, "gp-abc1")
        finally:
            holder.wait()
        self.assertIn("clearing stale lock", proc.stderr)
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")

    def test_dirty_lane_on_the_same_bead_restarts_detached_at_its_tip(self) -> None:
        self.fx.run(self.lane, "gp-old1")
        sha = commit(self.lane, "work.txt", "committed\n", "bead work")
        (self.lane / "README.md").write_text("uncommitted edit\n", encoding="utf-8")
        proc = self.fx.run(self.lane, "gp-old1")
        asides = self.asides()
        self.assertEqual(len(asides), 1)
        self.assertEqual(self.branch_of(asides[0]), "gp-old1")
        self.assertEqual((asides[0] / "README.md").read_text(encoding="utf-8"), "uncommitted edit\n")
        self.assertEqual(self.branch_of(self.lane), "HEAD")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), sha)
        self.assertIn("checked out at", proc.stderr)
        self.assertEqual(proc.stdout.split()[2], "detached")

    def test_base_head_fallback_means_the_rig_head_not_the_lane_head(self) -> None:
        # No usable remote: BASE falls back to HEAD, which must be the rig's.
        self.fx.run(self.lane, "gp-old1")
        commit(self.lane, "ahead.txt", "x\n", "lane moved ahead")
        proc = self.fx.run(self.lane, None, "--remote", "nosuch", "--no-fetch")
        self.assertIn("new branches start at the rig root's HEAD", proc.stderr)
        self.assertEqual(self.branch_of(self.lane), "HEAD")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), self.fx.main_sha)
        proc = self.fx.run(self.lane, "gp-new2", "--remote", "nosuch", "--no-fetch")
        self.assertEqual(self.branch_of(self.lane), "gp-new2")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), self.fx.main_sha)

    def test_ownerless_old_lock_is_reclaimed_but_a_fresh_one_is_respected(self) -> None:
        self.lock_dir().mkdir()  # no pid file: a run died between mkdir and publishing
        proc = self.fx.run(self.lane, "gp-abc1", check=False, env_extra={"WORKER_WORKTREE_LOCK_WAIT": "2"})
        self.assertNotEqual(proc.returncode, 0)  # fresh: respected
        os.utime(self.lock_dir(), (time.time() - 300, time.time() - 300))
        proc = self.fx.run(self.lane, "gp-abc1")
        self.assertIn("clearing stale lock", proc.stderr)
        self.assertEqual(self.branch_of(self.lane), "gp-abc1")
        self.assertFalse(self.lock_dir().exists())
        self.assertEqual(sorted(self.fx.rig.joinpath(".git").glob("worker-worktree.lock*")), [])

    def test_two_contenders_reclaiming_one_stale_lock_do_not_run_concurrently(self) -> None:
        """B observes the stale lock and pauses before taking the reclaim lock; A
        observes it, reclaims, acquires and holds. B must then find A's live lock
        under its own re-check, leave it alone, and wait for A to release."""
        self.lock_dir().mkdir()
        (self.lock_dir() / "pid").write_text("999999\n", encoding="utf-8")
        lane_a = self.fx.city / "lane-a"
        lane_b = self.fx.city / "lane-b"
        for lane in (lane_a, lane_b):
            lane.mkdir(parents=True)
        base_env = {**os.environ, "GC_RIG_ROOT": str(self.fx.rig), "WORKER_WORKTREE_LOCK_WAIT": "20"}
        base_env.pop("GC_TRIGGER_BEAD_ID", None)
        b = subprocess.Popen(
            ["sh", str(SCRIPT), "--no-fetch", "--bead", "gp-bbb1"],
            cwd=str(lane_b), env={**base_env, "GC_DIR": str(lane_b), "WORKER_WORKTREE_TEST_PAUSE_BEFORE_RECLAIM": "3"},
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        time.sleep(0.7)
        a_started = time.monotonic()
        a = subprocess.run(
            ["sh", str(SCRIPT), "--no-fetch", "--bead", "gp-aaa1"],
            cwd=str(lane_a), env={**base_env, "GC_DIR": str(lane_a), "WORKER_WORKTREE_TEST_PAUSE_AFTER_ACQUIRE": "4"},
            capture_output=True, text=True,
        )
        a_elapsed = time.monotonic() - a_started
        b_out, b_err = b.communicate(timeout=60)
        self.assertEqual(a.returncode, 0, a.stderr)
        self.assertEqual(b.returncode, 0, b_err)
        # A cleared the stale lock (B was paused before its reclaim); B never did.
        self.assertIn("clearing stale lock", a.stderr)
        self.assertNotIn("clearing stale lock", b_err)
        # B saw A's live lock under re-check and waited for it instead of removing it.
        self.assertIn("waiting for another run", b_err)
        self.assertGreater(a_elapsed, 3.5)  # A really held the lock across B's resume
        self.assertEqual(self.branch_of(lane_a), "gp-aaa1")
        self.assertEqual(self.branch_of(lane_b), "gp-bbb1")
        self.assertFalse(self.lock_dir().exists())
        self.assertFalse((self.fx.rig / ".git" / "worker-worktree.lock.reclaim").exists())

    def test_stuck_reclaim_lock_bounds_the_wait_then_fails_closed_when_old(self) -> None:
        self.lock_dir().mkdir()
        (self.lock_dir() / "pid").write_text("999999\n", encoding="utf-8")
        reclaim = self.fx.rig / ".git" / "worker-worktree.lock.reclaim"
        reclaim.mkdir()
        proc = self.fx.run(self.lane, "gp-abc1", check=False, env_extra={"WORKER_WORKTREE_LOCK_WAIT": "2"})
        self.assertNotEqual(proc.returncode, 0)  # fresh reclaim lock: bounded wait, not an infinite loop
        self.assertIn("has held", proc.stderr)
        os.utime(reclaim, (time.time() - 300, time.time() - 300))
        proc = self.fx.run(self.lane, "gp-abc1", check=False)
        self.assertNotEqual(proc.returncode, 0)  # old reclaim lock: fail closed, never auto-removed
        self.assertIn("stale reclaim lock", proc.stderr)
        self.assertTrue(reclaim.exists())
        self.assertFalse((self.lane / ".git").exists())

    def test_remote_name_with_metacharacters_still_matches_remote_bead_branches(self) -> None:
        git(self.fx.rig, "remote", "rename", "origin", "team|origin")
        git(self.fx.rig, "remote", "set-head", "team|origin", "main")
        sha = self.fx.push_branch_on("team|origin", "fix/gp-meta1-x", "m.txt")
        self.fx.run(self.lane, "gp-meta1", "--remote", "team|origin")
        self.assertEqual(self.branch_of(self.lane), "fix/gp-meta1-x")
        self.assertEqual(git(self.lane, "rev-parse", "HEAD"), sha)

    # --- refusals ----------------------------------------------------------------------------

    def test_workdir_inside_rig_root_is_refused_even_if_it_does_not_exist(self) -> None:
        inside = self.fx.rig / "nested"
        proc = self.fx.run(inside, "gp-abc1", check=False, mkdir=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("inside the rig root", proc.stderr)
        self.assertFalse(inside.exists())
        proc = self.fx.run(self.fx.rig, "gp-abc1", check=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("is the rig root", proc.stderr)

    def test_workdir_whose_parent_does_not_exist_is_refused(self) -> None:
        deep = self.fx.city / "missing" / "deeper" / "lane"
        proc = self.fx.run(deep, "gp-abc1", check=False, mkdir=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("parent of work dir does not exist", proc.stderr)
        self.assertFalse((self.fx.city / "missing").exists())

    def test_dot_dot_cannot_reach_into_the_rig_root(self) -> None:
        via_existing = self.fx.rig / ".." / "rig" / "nested"
        proc = self.fx.run(via_existing, "gp-abc1", check=False, mkdir=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("inside the rig root", proc.stderr)
        self.assertFalse((self.fx.rig / "nested").exists())
        via_missing = self.fx.root / "nope" / ".." / "rig" / "nested"
        proc = self.fx.run(via_missing, "gp-abc1", check=False, mkdir=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse((self.fx.rig / "nested").exists())
        trailing_dots = self.fx.city / "lane" / ".."
        proc = self.fx.run(trailing_dots, "gp-abc1", check=False, mkdir=False)
        self.assertNotEqual(proc.returncode, 0)

    def test_workdir_that_is_an_ancestor_of_the_rig_root_is_refused(self) -> None:
        proc = self.fx.run(self.fx.root, "gp-abc1", check=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("ancestor of the rig root", proc.stderr)
        self.assertEqual(self.asides(self.fx.root), [])
        self.assertTrue(self.fx.rig.is_dir())

    def test_symlink_alias_of_the_rig_root_is_refused(self) -> None:
        alias = self.fx.root / "rig-alias"
        alias.symlink_to(self.fx.rig)
        proc = self.fx.run(alias / "nested", "gp-abc1", check=False, mkdir=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("inside the rig root", proc.stderr)

    def test_symlink_then_dot_dot_resolves_physically(self) -> None:
        # city/tunnel -> rig/sub; "city/tunnel/../nested" is rig/nested on disk,
        # which is where the kernel (and gc's MkdirAll) would put the session.
        (self.fx.rig / "sub").mkdir()
        (self.fx.city / "tunnel").symlink_to(self.fx.rig / "sub")
        proc = self.fx.run(self.fx.city / "tunnel" / ".." / "nested", "gp-abc1", check=False, mkdir=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("inside the rig root", proc.stderr)
        self.assertFalse((self.fx.rig / "nested").exists())
        self.assertFalse((self.fx.city / "nested").exists())

    def test_flags_override_environment(self) -> None:
        lane = self.fx.city / "elsewhere"
        proc = self.fx.run(self.lane, "gp-env1", "--workdir", str(lane), "--bead", "gp-flag2", "--no-fetch")
        self.assertEqual(self.branch_of(lane), "gp-flag2")
        self.assertFalse((self.lane / ".git").exists())
        self.assertEqual(proc.stdout.split()[1], str(lane))


if __name__ == "__main__":
    unittest.main()
