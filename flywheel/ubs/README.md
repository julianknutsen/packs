# UBS Pack

Pre-commit bug scanning via [ultimate_bug_scanner](https://github.com/Dicklesworthstone/ultimate_bug_scanner).

## What this pack provides

- **Hook**: `PreToolUse` — automatically scans staged files when the agent runs `git commit`
- **Skill**: `/scan-bugs` — manually scan files, staged changes, or full repo

## Prerequisites

Install the `ubs` binary:

```bash
pip install ultimate-bug-scanner
```

Or build from source:

```bash
git clone https://github.com/Dicklesworthstone/ultimate_bug_scanner.git
cd ultimate_bug_scanner && pip install -e .
```

## Usage

Add to your `city.toml`:

```toml
[workspace]
includes = [
    "https://github.com/julianknutsen/packs/tree/main/flywheel/ubs",
]
```
