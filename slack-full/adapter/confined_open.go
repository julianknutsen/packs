package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// openBeneath opens rel (a Clean, root-relative path containing no
// "..") beneath rootAbs via os.Root, the stdlib's openat(2)-backed
// traversal-confined API: every component resolves relative to the
// pinned root fd, so a directory swapped for a symlink mid-walk cannot
// redirect the open outside rootAbs — the portable equivalent of
// openat2(RESOLVE_BENEATH). The previous hand-rolled walk used raw
// syscall.Openat, which does not exist on darwin and made the adapter
// linux-only.
//
// O_NOFOLLOW on the leaf preserves the old walk's swap detection where
// it matters most: realPath is EvalSymlinks-resolved before it gets
// here, so a symlink at the leaf means the inode was swapped in the
// race window, and the open fails with ELOOP rather than silently
// reading the link target. Intermediate components differ from the old
// per-component O_NOFOLLOW in one way: a symlink whose target stays
// inside the root is followed rather than rejected. The caller's
// contract — confinement to rootAbs — holds either way; any link that
// would resolve outside the root fails the open.
//
// The root itself gets the same swap detection: os.OpenRoot follows a
// symlink at rootAbs (pinning the link target instead of rejecting),
// which would let a root swapped for a symlink in the race window
// re-anchor "confinement" at an attacker-chosen directory. rootAbs is
// therefore Lstat-checked (must be a real directory, not a link) and
// the pinned fd is fstat-compared against that Lstat via os.SameFile —
// a mismatch means the root moved between check and open, and the open
// is rejected.
func openBeneath(rootAbs, rel string) (*os.File, error) {
	if rel == "" || rel == "." || filepath.IsAbs(rel) {
		return nil, fmt.Errorf("openBeneath: invalid relative path %q", rel)
	}
	comps := strings.Split(filepath.Clean(rel), string(filepath.Separator))
	for _, c := range comps {
		if c == "" || c == "." || c == ".." {
			return nil, fmt.Errorf("openBeneath: invalid path component %q in %q", c, rel)
		}
	}
	lst, err := os.Lstat(rootAbs)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: lstat root %q: %w", rootAbs, err)
	}
	if lst.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("openBeneath: root %q is a symlink", rootAbs)
	}
	if !lst.IsDir() {
		return nil, fmt.Errorf("openBeneath: root %q is not a directory", rootAbs)
	}
	root, err := os.OpenRoot(rootAbs)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: open root %q: %w", rootAbs, err)
	}
	defer root.Close()
	rst, err := root.Stat(".")
	if err != nil {
		return nil, fmt.Errorf("openBeneath: stat pinned root %q: %w", rootAbs, err)
	}
	if !os.SameFile(lst, rst) {
		return nil, fmt.Errorf("openBeneath: root %q changed identity between check and open", rootAbs)
	}
	// Leaf no-follow (codex round 2): os.Root.OpenFile resolves an
	// in-root leaf symlink ITSELF even with O_NOFOLLOW (the flag only
	// stops kernel-side following; Root then follows the link within
	// the sandbox), so a leaf swapped for a symlink in the race window
	// would be silently read through. Restore the old walk's rejection
	// with the same Lstat + open + SameFile identity pattern used for
	// the root above — all via the pinned fd, so no path re-resolution
	// races.
	lleaf, err := root.Lstat(rel)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: lstat %q beneath %q: %w", rel, rootAbs, err)
	}
	if lleaf.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("openBeneath: leaf %q beneath %q is a symlink", rel, rootAbs)
	}
	f, err := root.OpenFile(rel, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: open %q beneath %q: %w", rel, rootAbs, err)
	}
	fst, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("openBeneath: stat opened leaf %q: %w", rel, err)
	}
	if !os.SameFile(lleaf, fst) {
		_ = f.Close()
		return nil, fmt.Errorf("openBeneath: leaf %q beneath %q changed identity between check and open", rel, rootAbs)
	}
	return f, nil
}
