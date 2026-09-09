"""Gate contract checks with a fake CLI; no network or model usage."""
import json
import os
from pathlib import Path
import subprocess

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / 'assets/scripts/codex-gate.sh'


@pytest.fixture
def gate(tmp_path):
    binary = tmp_path / 'codex'
    binary.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
pathlib.Path(os.environ['CAPTURE']).write_text(json.dumps({'args': args, 'stdin': sys.stdin.read()}))
pathlib.Path(args[args.index('--output-last-message')+1]).write_text(os.environ.get('ANSWER', 'VERDICT: CLEAN\\n'))
print('VERDICT: CLEAN')  # A transcript match must never pass the gate.
sys.exit(int(os.environ.get('CODEX_EXIT', '0')))
''')
    binary.chmod(0o755)
    repo = tmp_path / 'repo'
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    source = repo / 'source.txt'
    source.write_text('base\n')
    subprocess.run(['git', '-C', str(repo), 'add', 'source.txt'], check=True)
    subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Gate Test',
                    '-c', 'user.email=gate@example.invalid', 'commit',
                    '-qm', 'base'], check=True)
    subprocess.run(['git', '-C', str(repo), 'branch', 'review-base'], check=True)
    source.write_text('delta\n')
    subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Gate Test',
                    '-c', 'user.email=gate@example.invalid', 'commit',
                    '-qam', 'delta'], check=True)
    prompt = tmp_path / 'prompt file.txt'
    prompt.write_text('Review a literal `command` and $(touch nope).\nSecond line.')
    output = tmp_path / 'answer file.txt'
    capture = tmp_path / 'capture.json'

    def run(mode='exec', *, answer='VERDICT: CLEAN\n', exit_code=0, extra=(), dirty=None):
        if dirty == 'untracked':
            (repo / 'new.txt').write_text('unreviewed\n')
        elif dirty:
            source.write_text('unreviewed\n')
            if dirty == 'staged':
                subprocess.run(['git', '-C', str(repo), 'add', 'source.txt'], check=True)
        args = ['exec', str(prompt)] if mode == 'exec' else ['review', '--base', 'review-base']
        result = subprocess.run(
            ['bash', str(SCRIPT), *args, '-C', str(repo), '--output', str(output), *extra],
            input='inherited stdin must not reach Codex', text=True, capture_output=True,
            env={**os.environ, 'PATH': str(tmp_path) + os.pathsep + os.environ['PATH'],
                 'CAPTURE': str(capture), 'ANSWER': answer, 'CODEX_EXIT': str(exit_code)},
            timeout=10,
        )
        return result, json.loads(capture.read_text()) if capture.exists() else None, output
    return run


@pytest.mark.parametrize('mode', ['exec', 'review'])
def test_city_astra_invocation_and_stdin(gate, mode):
    result, call, output = gate(mode)
    assert result.returncode == 0, result.stderr
    args = call['args']
    assert args[args.index('-p') + 1] == 'city'
    assert args[args.index('-m') + 1] == 'gpt-6-astra'
    assert 'review_model="gpt-6-astra"' in args
    assert args[args.index('-s') + 1] == 'read-only'
    assert args[args.index('-a') + 1] == 'never'
    assert any(a.startswith('developer_instructions=') for a in args)
    assert '--skip-git-repo-check' in args and '-C' in args
    assert call['stdin'] == ''
    assert output.read_text() == 'VERDICT: CLEAN\n'
    if mode == 'review':
        assert 'QUICK code review of committed changes' in args[-1]
        repo = args[args.index('-C') + 1]
        base = subprocess.check_output(['git', '-C', repo, 'rev-parse', 'review-base'], text=True).strip()
        head = subprocess.check_output(['git', '-C', repo, 'rev-parse', 'HEAD'], text=True).strip()
        assert base != head
        assert f'git diff {base} {head}' in args[-1]
        assert 'review-base' not in args[-1]
    else:
        assert '$(touch nope).\nSecond line.' in args[-1]
        assert not (output.parent / 'nope').exists()


@pytest.mark.parametrize(('answer', 'expected'), [
    ('VERDICT: BLOCK\n', 1),
    ('No verdict\n', 2),
    ('quoted VERDICT: CLEAN\n', 2),
    ('VERDICT: CLEAN\nVERDICT: BLOCK\n', 2),
    ('VERDICT: CLEANISH\n', 2),
    ('VERDICT: CLEAN\nMore findings\n', 2),
    ('```\nVERDICT: CLEAN\n```\n', 2),
    ('```\nVERDICT: CLEAN\n', 2),
    ('   ~~~text\nVERDICT: CLEAN\n', 2),
    ('````\n```\nVERDICT: CLEAN\n', 2),
    ('```\n~~~\nVERDICT: CLEAN\n', 2),
    ('```text\nExample code\n```\nVERDICT: CLEAN\n', 0),
])
def test_only_one_exact_anchored_final_verdict_passes(gate, answer, expected):
    result, _, _ = gate(answer=answer)
    assert result.returncode == expected


def test_model_override_pins_review_model_too(gate):
    result, call, _ = gate('review', extra=('--model', 'test-model'))
    assert result.returncode == 0
    assert call['args'][call['args'].index('-m') + 1] == 'test-model'
    assert 'review_model="test-model"' in call['args']


def test_cli_failure_cannot_pass_or_reuse_stale_verdict(gate):
    result, _, output = gate()
    assert result.returncode == 0
    result, _, _ = gate(answer='VERDICT: CLEAN\n', exit_code=7)
    assert result.returncode == 7
    assert output.read_text() == ''


def test_review_rejects_empty_delta_before_calling_codex(gate):
    result, call, _ = gate('review', extra=('--base', 'HEAD'))
    assert result.returncode == 2
    assert 'nonempty committed delta' in result.stderr
    assert call is None


@pytest.mark.parametrize('dirty', ['untracked', 'modified', 'staged'])
def test_review_rejects_uncommitted_work_before_calling_codex(gate, dirty):
    result, call, _ = gate('review', dirty=dirty)
    assert result.returncode == 2
    assert 'uncommitted or untracked changes' in result.stderr
    assert call is None
