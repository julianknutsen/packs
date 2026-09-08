// Tests for the inbound persist-and-retry spool (hq-xizo). Mirrors the
// slack-mini hq-1q1 test suite, adapted to slack-full's config and
// helper conventions (stubEnv/baseSlackEnv from main_test.go).
package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// compressRetries shrinks the inbound retry schedule so retry-path
// tests run in milliseconds instead of the production ~2 minutes. The
// swap holds inboundRetryDelaysMu: delivery goroutines leaked by other
// tests (their gc stub closes at t.Cleanup while the forward is still
// in flight, pushing them onto the retry path) may snapshot the
// schedule concurrently, and an unguarded write would trip -race.
func compressRetries(t *testing.T) {
	t.Helper()
	inboundRetryDelaysMu.Lock()
	saved := inboundRetryDelays
	inboundRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	inboundRetryDelaysMu.Unlock()
	t.Cleanup(func() {
		inboundRetryDelaysMu.Lock()
		inboundRetryDelays = saved
		inboundRetryDelaysMu.Unlock()
	})
}

func TestInboundSpoolDirFromEnv(t *testing.T) {
	t.Run("explicit override wins", func(t *testing.T) {
		env := baseSlackEnv()
		env["INBOUND_SPOOL_DIR"] = "/tmp/custom-spool"
		env["GC_SERVICE_STATE_ROOT"] = "/state/root"
		cfg, err := loadConfigFromEnv(stubEnv(env))
		if err != nil {
			t.Fatalf("loadConfigFromEnv: %v", err)
		}
		if cfg.inboundSpoolDir != "/tmp/custom-spool" {
			t.Errorf("inboundSpoolDir = %q, want /tmp/custom-spool", cfg.inboundSpoolDir)
		}
	})

	t.Run("derived from state root", func(t *testing.T) {
		env := baseSlackEnv()
		env["GC_SERVICE_STATE_ROOT"] = "/state/root"
		cfg, err := loadConfigFromEnv(stubEnv(env))
		if err != nil {
			t.Fatalf("loadConfigFromEnv: %v", err)
		}
		want := filepath.Join("/state/root", "data", "inbound-spool")
		if cfg.inboundSpoolDir != want {
			t.Errorf("inboundSpoolDir = %q, want %q", cfg.inboundSpoolDir, want)
		}
	})

	t.Run("unset disables spooling", func(t *testing.T) {
		cfg, err := loadConfigFromEnv(stubEnv(baseSlackEnv()))
		if err != nil {
			t.Fatalf("loadConfigFromEnv: %v", err)
		}
		if cfg.inboundSpoolDir != "" {
			t.Errorf("inboundSpoolDir = %q, want empty", cfg.inboundSpoolDir)
		}
	})
}

// TestDeliverInboundRetriesUntilSuccess confirms a transient gc failure
// is retried until the forward lands (hq-xizo: the adapter has already
// 200-acked Slack, so it must not drop the event). The spool entry
// survives the success — the CALLER retires it after the remaining
// durable work (alias dispatch), so a crash in between still replays.
// onFirstRetry must fire exactly once, before the first sleep, with
// exactly one forward attempt behind it — that hook releases the
// dispatch slot so retry sleeps cannot starve admission.
func TestDeliverInboundRetriesUntilSuccess(t *testing.T) {
	compressRetries(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	spool := t.TempDir()
	cfg := config{gcAPIBase: srv.URL, cityName: "test-city", inboundSpoolDir: spool}
	msg := externalInboundMessage{ProviderMessageID: "1700000000.0001", DedupKey: "slack-1700000000.0001"}
	path := spoolInbound(spool, msg)
	if path == "" {
		t.Fatal("spoolInbound returned no path")
	}
	// The atomic-write discipline must not leave tmp litter behind.
	if leftovers, _ := filepath.Glob(filepath.Join(spool, "*.tmp")); len(leftovers) != 0 {
		t.Errorf("spoolInbound left tmp files behind: %v", leftovers)
	}

	var hookFires, attemptsAtHook int32
	onFirstRetry := func() {
		atomic.AddInt32(&hookFires, 1)
		atomic.StoreInt32(&attemptsAtHook, calls.Load())
	}
	if !deliverInbound(cfg, msg, path, onFirstRetry) {
		t.Error("deliverInbound = false, want true (third attempt succeeds)")
	}

	if got := calls.Load(); got != 3 {
		t.Errorf("gc calls = %d, want 3 (two failures then success)", got)
	}
	if got := atomic.LoadInt32(&hookFires); got != 1 {
		t.Errorf("onFirstRetry fired %d times, want exactly 1", got)
	}
	if got := atomic.LoadInt32(&attemptsAtHook); got != 1 {
		t.Errorf("onFirstRetry fired after %d attempts, want 1 (before the first retry sleep)", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("spool entry must survive delivery for the caller to retire: %v", err)
	}
	removeSpoolEntry(path)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("removeSpoolEntry left the entry behind: %v", err)
	}
}

// TestDeliverInboundDeadLettersOnExhaustion confirms the spool entry
// survives (in the dead-letter dir) when every forward attempt fails.
// The final log line keeps the "inbound POST failed" marker external
// log-watchers key on — see deliverInbound.
func TestDeliverInboundDeadLettersOnExhaustion(t *testing.T) {
	compressRetries(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	spool := t.TempDir()
	cfg := config{gcAPIBase: srv.URL, cityName: "test-city", inboundSpoolDir: spool}
	msg := externalInboundMessage{
		ProviderMessageID: "1700000000.0002",
		DedupKey:          "slack-1700000000.0002",
		Text:              "lost mention",
	}
	path := spoolInbound(spool, msg)
	if path == "" {
		t.Fatal("spoolInbound returned no path")
	}

	if deliverInbound(cfg, msg, path, nil) {
		t.Error("deliverInbound = true, want false (every attempt fails)")
	}

	wantCalls := int32(len(snapshotInboundRetryDelays()) + 1)
	if got := calls.Load(); got != wantCalls {
		t.Errorf("gc calls = %d, want %d", got, wantCalls)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("spool entry should have moved to dead-letter: %v", err)
	}
	deadPath := filepath.Join(spool, "dead", filepath.Base(path))
	data, err := os.ReadFile(deadPath)
	if err != nil {
		t.Fatalf("dead-letter file: %v", err)
	}
	var got externalInboundMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("dead-letter decode: %v", err)
	}
	if got.Text != "lost mention" || got.DedupKey != msg.DedupKey {
		t.Errorf("dead-letter content = %+v", got)
	}
}

// TestReplaySpoolRedelivers confirms events left in the spool by a
// previous run (crash mid-retry) are re-forwarded at startup.
func TestReplaySpoolRedelivers(t *testing.T) {
	compressRetries(t)
	var gotText atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var wrap struct {
			Message externalInboundMessage `json:"message"`
		}
		_ = json.NewDecoder(r.Body).Decode(&wrap)
		gotText.Store(wrap.Message.Text)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	spool := t.TempDir()
	msg := externalInboundMessage{
		ProviderMessageID: "1700000000.0003",
		DedupKey:          "slack-1700000000.0003",
		Text:              "orphaned by crash",
	}
	path := spoolInbound(spool, msg)
	if path == "" {
		t.Fatal("spoolInbound returned no path")
	}

	cfg := config{gcAPIBase: srv.URL, cityName: "test-city", inboundSpoolDir: spool}
	replaySpool(cfg, nil)

	// Replay delivers asynchronously; wait for the spool entry to clear.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("spool entry not delivered within deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, _ := gotText.Load().(string); got != "orphaned by crash" {
		t.Errorf("replayed text = %q, want 'orphaned by crash'", got)
	}
}

// TestReplaySpoolDeadLettersCorruptEntries confirms an undecodable
// spool file is quarantined instead of blocking or crash-looping
// replay on every restart.
func TestReplaySpoolDeadLettersCorruptEntries(t *testing.T) {
	spool := t.TempDir()
	corrupt := filepath.Join(spool, "1-corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	replaySpool(config{gcAPIBase: "http://127.0.0.1:1", cityName: "test-city", inboundSpoolDir: spool}, nil)

	// Replay decodes in worker goroutines now; poll for the quarantine.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(corrupt); errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("corrupt entry should have moved to dead-letter")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(spool, "dead", "1-corrupt.json")); err != nil {
		t.Errorf("corrupt entry missing from dead-letter dir: %v", err)
	}
}

// TestReplaySpoolRedispatchesAliasTarget confirms replay redoes the
// targeted alias dispatch, not just the forward: a spool entry
// surviving a crash means the dispatch was never confirmed, so replay
// must re-resolve ExplicitTarget against the alias registry and POST
// the session message again before retiring the entry (P1: the old
// replay dropped the targeted copy silently).
func TestReplaySpoolRedispatchesAliasTarget(t *testing.T) {
	compressRetries(t)
	var inboundPosts, sessionPosts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/extmsg/inbound"):
			inboundPosts.Add(1)
		case strings.Contains(r.URL.Path, "/session/"):
			sessionPosts.Add(1)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	spool := t.TempDir()
	msg := externalInboundMessage{
		ProviderMessageID: "1700000000.0004",
		DedupKey:          "slack-1700000000.0004",
		Text:              "@mayor: crashed before dispatch",
		ExplicitTarget:    "mayor",
		Conversation:      conversationRef{ConversationID: "C1"},
	}
	path := spoolInbound(spool, msg)
	if path == "" {
		t.Fatal("spoolInbound returned no path")
	}

	aliasReg := newTestHandleAliasRegistry(t)
	if err := aliasReg.Set("mayor", "gc-42"); err != nil {
		t.Fatalf("aliasReg.Set: %v", err)
	}
	cfg := config{gcAPIBase: srv.URL, cityName: "test-city", inboundSpoolDir: spool}
	replaySpool(cfg, aliasReg)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("spool entry not retired within deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := inboundPosts.Load(); got != 1 {
		t.Errorf("inbound POSTs = %d, want 1", got)
	}
	if got := sessionPosts.Load(); got != 1 {
		t.Errorf("alias session POSTs = %d, want 1 (replay must redo the dispatch)", got)
	}
}

// TestReplaySpoolRetiresEntryWhenAliasGone: a spooled targeted entry
// whose handle no longer resolves has nothing left to dispatch to —
// replay forwards it and retires the entry instead of dead-lettering
// or looping on it.
func TestReplaySpoolRetiresEntryWhenAliasGone(t *testing.T) {
	compressRetries(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	spool := t.TempDir()
	msg := externalInboundMessage{
		ProviderMessageID: "1700000000.0005",
		DedupKey:          "slack-1700000000.0005",
		ExplicitTarget:    "ghost",
		Conversation:      conversationRef{ConversationID: "C1"},
	}
	path := spoolInbound(spool, msg)
	if path == "" {
		t.Fatal("spoolInbound returned no path")
	}

	cfg := config{gcAPIBase: srv.URL, cityName: "test-city", inboundSpoolDir: spool}
	replaySpool(cfg, newTestHandleAliasRegistry(t))

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("spool entry not retired within deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if entries, _ := os.ReadDir(filepath.Join(spool, "dead")); len(entries) != 0 {
		t.Errorf("entry dead-lettered on missing alias; want plain retirement")
	}
}

// codex round 2 P1: without a durable spool entry the dispatch slot
// must be HELD across the retry sleeps — the semaphore is then the
// only bound on sleeping retry goroutines.
func TestDeliverInboundHoldsSlotWhenUnspooled(t *testing.T) {
	compressRetries(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config{gcAPIBase: srv.URL, cityName: "test-city"}
	var hookFires int32
	ok := deliverInbound(cfg, externalInboundMessage{DedupKey: "slack-x"}, "", func() {
		atomic.AddInt32(&hookFires, 1)
	})
	if !ok {
		t.Fatal("deliverInbound = false, want true")
	}
	if got := atomic.LoadInt32(&hookFires); got != 0 {
		t.Errorf("onFirstRetry fired %d times with no spool entry, want 0 (slot must stay held)", got)
	}
}

// codex round 2: INBOUND_SPOOL_DIR= (set-but-empty) is the documented
// opt-out and must NOT be replaced by the state-root default.
func TestInboundSpoolDirExplicitEmptyDisables(t *testing.T) {
	env := baseSlackEnv()
	env["INBOUND_SPOOL_DIR"] = ""
	env["GC_SERVICE_STATE_ROOT"] = "/state/root"
	cfg, err := loadConfigFromLookup(func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	})
	if err != nil {
		t.Fatalf("loadConfigFromLookup: %v", err)
	}
	if cfg.inboundSpoolDir != "" {
		t.Errorf("inboundSpoolDir = %q with INBOUND_SPOOL_DIR= set-but-empty, want disabled", cfg.inboundSpoolDir)
	}
}
