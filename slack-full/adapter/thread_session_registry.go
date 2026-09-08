package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// threadSessionRegistry maps (channelID, threadTS) → gc session id for
// the Slack thread-launcher mode (cby.5.a). When a `@@handle` post
// arrives in a thread, the adapter checks the registry: if a binding
// exists, deliver to that session id; otherwise spawn a new session
// and record the binding so subsequent posts in the same thread
// converge on the same agent.
//
// Concurrency contract:
//
//   - AcquireOrCreate is atomic per (channel, thread) key. N concurrent
//     posts in the same Slack thread invoke the create callback at
//     most once; the late arrivals receive the cached sessionID with
//     created=false. Distinct keys do not contend.
//   - The create callback is injected by the caller (cby.5.3 will pass
//     the spawn closure). The registry knows nothing about HTTP, the
//     /sessions endpoint, or what a "spawn" means — it only guarantees
//     single-flight semantics around an arbitrary user closure.
//   - A failed create callback is NOT cached. The next AcquireOrCreate
//     on the same key invokes the callback again. This matches the
//     real failure model: a transient spawn error must not perma-pin
//     the thread to "no agent."
//
// Persistence contract:
//
//   - Atomic temp-file + fsync + os.Rename, mirroring (and extending)
//     the rig_mappings.json pattern. Crash between write and rename
//     leaves a "<basename>-<random>.tmp" orphan that the cby.19
//     startup sweep cleans on next boot.
//   - Tolerant load: missing file → empty registry; zero-byte file →
//     empty registry; malformed JSON → empty registry + WARN log.
//     None of these abort adapter startup, because a corrupt thread
//     binding file at most loses the (recoverable) thread→session
//     cache; it is not a configuration source of truth like
//     rig_mappings.json.
type threadSessionRegistry struct {
	mu        sync.Mutex
	byKey     map[threadKey]string // (channel, thread) → sessionID
	bySession map[string]threadKey // sessionID → (channel, thread)
	gates     map[threadKey]*sync.Mutex
	diskPath  string
}

// threadKey is the composite (channelID, threadTS) lookup key. Both
// fields are required; AcquireOrCreate rejects an empty string in
// either so a missing thread_ts in a malformed Slack event can't
// collapse multiple threads into a single binding.
type threadKey struct {
	ChannelID string
	ThreadTS  string
}

// threadSessionDiskRecord is the on-disk JSON shape. Stored as a map
// of "<channel_id>:<thread_ts>" → record so a hand-edit is grep-able.
type threadSessionDiskRecord struct {
	ChannelID string `json:"channel_id"`
	ThreadTS  string `json:"thread_ts"`
	SessionID string `json:"session_id"`
}

// maxThreadSessionRegistryBytes caps the on-disk file size accepted at
// load. A healthy install holds a few thousand records of fixed shape;
// 10 MiB is several orders of magnitude over that. A larger file is
// presumed corrupt and refused. Mirrors rig_mappings.json's policy.
const maxThreadSessionRegistryBytes = 10 << 20 // 10 MiB

// newThreadSessionRegistry opens the registry at diskPath. A missing,
// empty, or malformed file yields an empty in-memory registry; only a
// genuine I/O error (permission denied on the dir, oversized file) is
// returned.
func newThreadSessionRegistry(diskPath string) (*threadSessionRegistry, error) {
	r := &threadSessionRegistry{
		byKey:     make(map[threadKey]string),
		bySession: make(map[string]threadKey),
		gates:     make(map[threadKey]*sync.Mutex),
		diskPath:  diskPath,
	}
	if err := r.load(); err != nil {
		return nil, fmt.Errorf("load thread session registry from %s: %w", diskPath, err)
	}
	return r, nil
}

// AcquireOrCreate returns the session id bound to (channelID,
// threadTS). If no binding exists, the create callback is invoked
// under a per-key mutex; the returned sessionID is cached and
// persisted. created reports whether this call was the one that ran
// the callback (true) versus served a cached value (false).
//
// channelID and threadTS must both be non-empty. An error from create
// is returned to the caller and NOT cached.
func (r *threadSessionRegistry) AcquireOrCreate(channelID, threadTS string, create func() (string, error)) (sessionID string, created bool, err error) {
	if channelID == "" || threadTS == "" {
		return "", false, fmt.Errorf("thread session registry: channelID and threadTS required")
	}
	if create == nil {
		return "", false, fmt.Errorf("thread session registry: create callback required")
	}
	key := threadKey{ChannelID: channelID, ThreadTS: threadTS}

	// Fast path: already cached.
	r.mu.Lock()
	if sid, ok := r.byKey[key]; ok {
		r.mu.Unlock()
		return sid, false, nil
	}
	// Acquire (or install) the per-key gate while holding the
	// registry lock so we publish a single gate to all racers on the
	// same key.
	gate, ok := r.gates[key]
	if !ok {
		gate = &sync.Mutex{}
		r.gates[key] = gate
	}
	r.mu.Unlock()

	// Serialize creates per-key. Distinct keys hold distinct gates so
	// they run in parallel.
	gate.Lock()
	defer gate.Unlock()

	// Re-check under the gate: an earlier racer may have already run
	// the callback and populated the cache.
	r.mu.Lock()
	if sid, ok := r.byKey[key]; ok {
		r.mu.Unlock()
		return sid, false, nil
	}
	r.mu.Unlock()

	sid, createErr := create()
	if createErr != nil {
		// Failed creates are NOT cached. Best-effort cleanup of the
		// per-key gate so a transient error doesn't leak gate
		// entries. Safe because we still hold the gate; any racer
		// queued behind us will re-check the cache on its turn,
		// miss, and either reuse this gate (if still in r.gates) or
		// install a fresh one.
		r.removeGateIfUnused(key, gate)
		return "", false, fmt.Errorf("thread session registry: create callback failed for channel=%q thread=%q: %w", channelID, threadTS, createErr)
	}
	if sid == "" {
		r.removeGateIfUnused(key, gate)
		return "", false, fmt.Errorf("thread session registry: create callback returned empty sessionID for channel=%q thread=%q", channelID, threadTS)
	}

	r.mu.Lock()
	r.byKey[key] = sid
	r.bySession[sid] = key
	saveErr := r.saveLocked()
	r.mu.Unlock()
	if saveErr != nil {
		return sid, true, fmt.Errorf("thread session registry: persist binding for channel=%q thread=%q: %w", channelID, threadTS, saveErr)
	}
	return sid, true, nil
}

// Lookup returns the session id for (channelID, threadTS) and a
// boolean indicating presence. Cheap locked lookup; does not contend
// with in-flight creates beyond the brief atomic map read.
func (r *threadSessionRegistry) Lookup(channelID, threadTS string) (sessionID string, ok bool) {
	if channelID == "" || threadTS == "" {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sid, ok := r.byKey[threadKey{ChannelID: channelID, ThreadTS: threadTS}]
	return sid, ok
}

// Remove drops the binding for (channelID, threadTS) and persists.
// Missing entries are not an error; callers may treat Remove as
// idempotent. Used by cby.5.4 teardown when a session ends.
func (r *threadSessionRegistry) Remove(channelID, threadTS string) error {
	if channelID == "" || threadTS == "" {
		return fmt.Errorf("thread session registry: channelID and threadTS required")
	}
	key := threadKey{ChannelID: channelID, ThreadTS: threadTS}
	r.mu.Lock()
	if sid, ok := r.byKey[key]; ok {
		delete(r.byKey, key)
		delete(r.bySession, sid)
	}
	delete(r.gates, key)
	err := r.saveLocked()
	r.mu.Unlock()
	if err != nil {
		return fmt.Errorf("thread session registry: persist remove for channel=%q thread=%q: %w", channelID, threadTS, err)
	}
	return nil
}

// RemoveBySessionID drops the binding given only a session id and
// returns the (channelID, threadTS) that was dropped. Used by the
// cby.5.4 session.stopped subscriber, which sees session-id-keyed
// events and must invalidate the corresponding thread cache. Missing
// session ids return ok=false (idempotent).
func (r *threadSessionRegistry) RemoveBySessionID(sessionID string) (channelID, threadTS string, ok bool) {
	if sessionID == "" {
		return "", "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key, found := r.bySession[sessionID]
	if !found {
		return "", "", false
	}
	delete(r.byKey, key)
	delete(r.bySession, sessionID)
	delete(r.gates, key)
	if err := r.saveLocked(); err != nil {
		// In-memory state has already been mutated; persistence
		// failure is logged but doesn't unwind the in-memory change.
		// The next successful save will catch the dropped binding;
		// in the meantime the live map is the source of truth.
		log.Printf("WARN: thread session registry: persist RemoveBySessionID for session=%q: %v", sessionID, err)
	}
	return key.ChannelID, key.ThreadTS, true
}

// removeGateIfUnused deletes the per-key gate from the gates map iff
// the gate matches the one passed in. Called from the failed-create
// path so a transient error doesn't leak gate entries forever.
func (r *threadSessionRegistry) removeGateIfUnused(key threadKey, gate *sync.Mutex) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.gates[key]; ok && cur == gate {
		delete(r.gates, key)
	}
}

func diskKeyFor(k threadKey) string {
	return k.ChannelID + ":" + k.ThreadTS
}

func (r *threadSessionRegistry) load() error {
	if r.diskPath == "" {
		return nil
	}
	f, err := os.Open(r.diskPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open %s: %w", r.diskPath, err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxThreadSessionRegistryBytes+1))
	if err != nil {
		return fmt.Errorf("read %s: %w", r.diskPath, err)
	}
	if int64(len(data)) > maxThreadSessionRegistryBytes {
		return fmt.Errorf("registry file %s exceeds %d bytes", r.diskPath, maxThreadSessionRegistryBytes)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var stored map[string]threadSessionDiskRecord
	if err := dec.Decode(&stored); err != nil {
		// Malformed JSON is recoverable: a corrupt thread cache at
		// most loses the (rebuildable) thread→session bindings, it
		// is not a config source of truth. Log and continue with an
		// empty registry; the next AcquireOrCreate overwrites the
		// corrupt file via the atomic-write helper.
		log.Printf("WARN: thread session registry: malformed file %q (%v); starting empty", r.diskPath, err)
		return nil
	}
	for storedKey, rec := range stored {
		if rec.ChannelID == "" || rec.ThreadTS == "" || rec.SessionID == "" {
			log.Printf("WARN: thread session registry: skipping record %q with missing fields", storedKey)
			continue
		}
		k := threadKey{ChannelID: rec.ChannelID, ThreadTS: rec.ThreadTS}
		if _, dup := r.byKey[k]; dup {
			log.Printf("WARN: thread session registry: duplicate key %q in store; last write wins", storedKey)
		}
		r.byKey[k] = rec.SessionID
		r.bySession[rec.SessionID] = k
	}
	return nil
}

func (r *threadSessionRegistry) saveLocked() error {
	if r.diskPath == "" {
		return nil
	}
	out := make(map[string]threadSessionDiskRecord, len(r.byKey))
	for k, sid := range r.byKey {
		out[diskKeyFor(k)] = threadSessionDiskRecord{
			ChannelID: k.ChannelID,
			ThreadTS:  k.ThreadTS,
			SessionID: sid,
		}
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("encode thread session store: %w", err)
	}
	return writeFile0600WithSync(r.diskPath, data)
}

// writeFile0600WithSync mirrors writeFile0600 in interactions.go but
// adds an explicit f.Sync() before close to guarantee the binding
// reaches stable storage before the rename returns. cby.5.a
// specifically calls out fsync-on-commit: a thread-session binding
// lost mid-write would silently double-spawn an agent on the next
// post in the same thread, which is a worse failure than the (small)
// fsync-per-write cost. The other registries (channel/rig/apps)
// write infrequently and tolerate the existing non-fsync behavior;
// we don't change their helper here to keep the blast radius scoped.
func writeFile0600WithSync(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %q: %w", dir, err)
	}
	f, err := os.CreateTemp(dir, filepath.Base(path)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp in %q: %w", dir, err)
	}
	tmpName := f.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		cleanup()
		return fmt.Errorf("chmod %q: %w", tmpName, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		cleanup()
		return fmt.Errorf("write %q: %w", tmpName, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		cleanup()
		return fmt.Errorf("fsync %q: %w", tmpName, err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close %q: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return fmt.Errorf("rename %q -> %q: %w", tmpName, path, err)
	}
	// Fsync the parent directory so the rename itself is durable
	// (codex round 3): the temp file's contents were synced above, but
	// on a host crash a filesystem may still lose the directory entry
	// — and a spool entry that vanishes on reboot silently loses an
	// already-acked Slack event. Same fsyncDir discipline as the
	// ingress receipt store.
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("fsync dir %q: %w", dir, err)
	}
	return nil
}
