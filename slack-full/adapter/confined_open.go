package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// openBeneath opens rel (a Clean, root-relative path containing no
// "..") beneath rootAbs by walking one path component at a time: each
// step is an openat(2) relative to the fd of the already-opened parent,
// with O_NOFOLLOW set. The parent fd pins the verified directory inode,
// so a directory swapped for a symlink mid-walk cannot redirect the
// traversal — the userspace equivalent of openat2(RESOLVE_BENEATH),
// which stdlib syscall does not expose.
//
// Uses golang.org/x/sys/unix rather than stdlib syscall: `syscall.Openat`
// exists only on Linux, so the stdlib version failed to COMPILE on darwin
// ("undefined: syscall.Openat"), taking the whole adapter with it. Neither
// stdlib escape hatch works here — darwin's syscall package is libSystem-
// based and does not export SYS_OPENAT either, and os.Root (Go 1.24+)
// confines correctly but silently IGNORES O_NOFOLLOW, so it would follow a
// symlink planted inside the file store and quietly relax the guarantee
// this function exists to provide. x/sys/unix keeps the semantics exact
// (verified on darwin/arm64: an in-root symlink still fails ELOOP under
// O_NOFOLLOW) at the cost of the one dependency this file's original
// comment was avoiding. That trade is deliberate: correctness of a security
// primitive over a dependency count.
//
// O_NOFOLLOW applies to every component, not just the leaf, so any
// symlink anywhere beneath root is a hard failure (ELOOP / ENOTDIR).
// That matches the caller's contract: realPath is EvalSymlinks-resolved
// before it gets here, so every component of a legitimate path is a
// real directory or file and the walk succeeds; only a mid-flight swap
// trips it.
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
	dirFlags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd, err := unix.Open(rootAbs, dirFlags, 0)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: open root %q: %w", rootAbs, err)
	}
	for i, c := range comps {
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if i < len(comps)-1 {
			flags |= unix.O_DIRECTORY
		}
		next, err := unix.Openat(fd, c, flags, 0)
		unix.Close(fd)
		if err != nil {
			return nil, fmt.Errorf("openBeneath: open component %q of %q: %w", c, rel, err)
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), filepath.Join(rootAbs, rel)), nil
}
