package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// openBeneath is the userspace RESOLVE_BENEATH walk backing
// readConfinedFile. These tests pin the mechanism: legitimate nested
// paths open, and a symlink at ANY component — the stand-in for a
// parent directory swapped mid-flight — is a hard failure.

func TestOpenBeneathReadsNestedFile(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a", "b"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "a", "b", "f.txt"), []byte("payload"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	f, err := openBeneath(root, filepath.Join("a", "b", "f.txt"))
	if err != nil {
		t.Fatalf("openBeneath: %v", err)
	}
	defer f.Close()
	buf := make([]byte, 16)
	n, _ := f.Read(buf)
	if got := string(buf[:n]); got != "payload" {
		t.Errorf("read %q, want %q", got, "payload")
	}
}

func TestOpenBeneathRefusesSymlinkComponents(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("loot"), 0o600); err != nil {
		t.Fatalf("write outside: %v", err)
	}

	// Intermediate component is a symlink to a directory outside root —
	// the post-swap shape of the parent-directory race.
	if err := os.Symlink(outside, filepath.Join(root, "swapped")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}
	if _, err := openBeneath(root, filepath.Join("swapped", "secret")); err == nil {
		t.Fatal("openBeneath followed a symlinked intermediate directory, want failure")
	}

	// Leaf component is a symlink — O_NOFOLLOW parity with the old code.
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "leaf")); err != nil {
		t.Fatalf("symlink leaf: %v", err)
	}
	if _, err := openBeneath(root, "leaf"); err == nil {
		t.Fatal("openBeneath followed a leaf symlink, want failure")
	}
}

func TestOpenBeneathRejectsInvalidRel(t *testing.T) {
	root := t.TempDir()
	// "a/../b" is absent: filepath.Clean collapses it to "b" before the
	// component check, which is correct — the cleaned form is beneath root.
	for _, rel := range []string{"", ".", "/etc/passwd", "..", filepath.Join("..", "x")} {
		if _, err := openBeneath(root, rel); err == nil {
			t.Errorf("openBeneath(%q) succeeded, want rejection", rel)
		}
	}
}

func TestReadConfinedFileStillReadsAndStillConfines(t *testing.T) {
	// Canonicalize: readConfinedFile's contract is an EvalSymlinks-resolved
	// path, and on darwin t.TempDir() sits under the /var -> /private/var
	// symlink.
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	sub := filepath.Join(root, "files")
	if err := os.MkdirAll(sub, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(sub, "report.txt")
	if err := os.WriteFile(path, []byte("contents"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := readConfinedFile(root, path)
	if err != nil {
		t.Fatalf("readConfinedFile: %v", err)
	}
	if string(got) != "contents" {
		t.Errorf("read %q, want %q", got, "contents")
	}

	if _, err := readConfinedFile(root, "/etc/passwd"); err == nil ||
		!strings.Contains(err.Error(), "outside root") {
		t.Errorf("escape attempt error = %v, want outside-root rejection", err)
	}
}

// A symlinked upload root must be rejected outright (hq-xizo P1):
// os.OpenRoot would follow it and re-anchor "confinement" at the link
// target, undoing the old walk's O_NOFOLLOW-on-root defense against a
// root swapped in the race window.
func TestOpenBeneathRejectsSymlinkedRoot(t *testing.T) {
	base := t.TempDir()
	realRoot := filepath.Join(base, "real-root")
	if err := os.MkdirAll(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(realRoot, "f.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkRoot := filepath.Join(base, "link-root")
	if err := os.Symlink(realRoot, linkRoot); err != nil {
		t.Fatal(err)
	}
	if f, err := openBeneath(linkRoot, "f.txt"); err == nil {
		f.Close()
		t.Fatal("openBeneath accepted a symlinked root; want rejection")
	}
}
