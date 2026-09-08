// Inbound persist-and-retry spool (hq-xizo).
//
// handleSlackEvents 200-acks Slack BEFORE processSlackEvent forwards the
// event to gc's extmsg/inbound endpoint, so a failed forward used to
// lose the message silently — Slack never redelivers after a 200. Each
// decoded inbound is now:
//
//   - spooled to <spoolDir>/<unixnano>-<dedupkey>.json before the first
//     forward attempt (atomic tmp + fsync + rename, 0o600 file / 0o700
//     dir via writeFile0600WithSync; best-effort — a spool failure logs
//     and falls back to in-memory retries only)
//   - retried per inboundRetryDelays when the forward fails
//   - dead-lettered to <spoolDir>/dead on exhaustion; the final log
//     line keeps the "inbound POST failed" substring that external
//     log-watchers key on
//   - replayed at startup when a previous run crashed mid-retry
//
// Delivery is AT-LEAST-ONCE, and that is a deliberate tradeoff
// (codex round 2 P1, accepted): gc core does not yet consume the
// message DedupKey ("slack-"+ts) — see
// slack-full/docs/phase5-ledger-readiness.md — so a retry after an
// ambiguous failure (the POST was accepted but its response was
// lost) or a replay after a crash-before-retirement can duplicate
// the agent turn. The alternative is silently losing acked Slack
// messages, which this spool exists to prevent; once core honors
// DedupKey idempotently, these duplicates disappear with no adapter
// change.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// inboundRetryDelays spaces the forward retries after a failed inbound
// POST to gc: 5 attempts total over ~2 minutes, then dead-letter
// (hq-xizo). Package-level so tests can compress the schedule. Reads go
// through snapshotInboundRetryDelays and test-time swaps hold
// inboundRetryDelaysMu: delivery goroutines can outlive the test that
// spawned them (their gc stub closes at t.Cleanup, pushing them onto
// the retry path), so an unguarded swap would be a data race.
var (
	inboundRetryDelaysMu sync.RWMutex
	inboundRetryDelays   = []time.Duration{
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
	}
)

// snapshotInboundRetryDelays returns the current retry schedule. The
// slice contents are never mutated, only the package var is swapped
// (tests), so a snapshot of the header is safe to iterate lock-free.
func snapshotInboundRetryDelays() []time.Duration {
	inboundRetryDelaysMu.RLock()
	defer inboundRetryDelaysMu.RUnlock()
	return inboundRetryDelays
}

// spoolInbound persists a decoded inbound event before the first
// forward attempt, so a crash mid-retry cannot silently lose a
// Slack-acked message. Returns the spool file path, or "" when spooling
// is disabled (empty spoolDir) or the write fails — persistence is
// best-effort and never blocks the forward. The write routes through
// writeFile0600WithSync (tmp file in the same dir + fsync + close +
// rename, tmp removed on every error path) so a torn write can never
// leave a half-entry for startup replay to choke on.
func spoolInbound(spoolDir string, msg externalInboundMessage) string {
	if spoolDir == "" {
		return ""
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("spool: marshal: %v", err)
		return ""
	}
	name := fmt.Sprintf("%d-%s.json", time.Now().UnixNano(), sanitizeSpoolName(msg.DedupKey))
	path := filepath.Join(spoolDir, name)
	if err := writeFile0600WithSync(path, data); err != nil {
		log.Printf("spool: write %s: %v", path, err)
		return ""
	}
	return path
}

// spoolNameRE matches characters unsafe in a spool filename.
var spoolNameRE = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func sanitizeSpoolName(s string) string {
	return spoolNameRE.ReplaceAllString(s, "_")
}

// deliverInbound forwards one inbound message to gc, retrying failures
// per inboundRetryDelays. On success the canonical "inbound:" line is
// logged once and the CALLER owns the spool entry — it must remove it
// only after all remaining durable work (the targeted alias dispatch)
// has completed, so a crash between forward and dispatch replays the
// entry instead of silently losing the targeted copy. When every
// attempt fails the entry moves to the dead-letter directory here and
// deliverInbound returns false so processSlackEvent skips the
// post-forward work (busy reaction, alias dispatch). The final failure
// line deliberately contains "inbound POST failed" — external
// log-watchers key on that exact substring.
//
// onFirstRetry, when non-nil, fires once before the first retry sleep
// — but ONLY when the message is durably spooled. processSlackEvent
// passes its dispatch-slot release: the retry schedule sleeps ~2
// minutes, and a gc outage would otherwise pin every slot on sleeping
// goroutines and starve admission of fresh events (which then get
// dropped un-spooled — strictly worse than letting spooled stragglers
// retry outside the semaphore). Without a spool entry (disabled, or
// the write failed) the slot is deliberately HELD across the retries:
// the semaphore is then the only bound on sleeping retry goroutines,
// and releasing it would let a sustained outage grow one ~2-minute
// goroutine per inbound with no cap (codex round 2 P1).
func deliverInbound(cfg config, msg externalInboundMessage, spoolPath string, onFirstRetry func()) bool {
	delays := snapshotInboundRetryDelays()
	attempts := len(delays) + 1
	var lastErr error
	for i := range attempts {
		if i > 0 {
			if i == 1 && onFirstRetry != nil && spoolPath != "" {
				onFirstRetry()
			}
			time.Sleep(delays[i-1])
		}
		err := postInbound(cfg, msg)
		if err == nil {
			log.Printf("inbound: chan=%s user=%s ts=%s thread=%s target=%q files=%d text=%dch",
				msg.Conversation.ConversationID, msg.Actor.ID, msg.ProviderMessageID,
				msg.ReplyToMessageID, msg.ExplicitTarget, len(msg.Attachments), len(msg.Text))
			return true
		}
		lastErr = err
		if i < attempts-1 {
			log.Printf("inbound forward attempt %d/%d failed (retry in %s): %v",
				i+1, attempts, delays[i], err)
		}
	}
	log.Printf("inbound POST failed after %d attempts (dead-letter=%s) chan=%s ts=%s: %v",
		attempts, moveToDeadLetter(spoolPath), msg.Conversation.ConversationID,
		msg.ProviderMessageID, lastErr)
	return false
}

// removeSpoolEntry deletes a confirmed-done spool entry. "" (spooling
// disabled or the write failed) is a no-op.
func removeSpoolEntry(spoolPath string) {
	if spoolPath != "" {
		_ = os.Remove(spoolPath)
	}
}

// moveToDeadLetter quarantines an exhausted spool entry in the sibling
// "dead" directory so it can be replayed by hand; returns the new path,
// or "none" when spooling was disabled for this message. If the move
// fails the entry stays in the spool, where startup replay will retry
// it on the next adapter restart.
func moveToDeadLetter(spoolPath string) string {
	if spoolPath == "" {
		return "none"
	}
	deadDir := filepath.Join(filepath.Dir(spoolPath), "dead")
	if err := os.MkdirAll(deadDir, 0o700); err != nil {
		log.Printf("dead-letter: mkdir: %v", err)
		return spoolPath
	}
	dest := filepath.Join(deadDir, filepath.Base(spoolPath))
	if err := os.Rename(spoolPath, dest); err != nil {
		log.Printf("dead-letter: rename: %v", err)
		return spoolPath
	}
	return dest
}

// replaySpool re-delivers inbound events a previous run persisted but
// never confirmed done (crash mid-retry, or crash between the forward
// and the targeted alias dispatch). main() calls it before the
// listeners start serving. Each entry is re-forwarded and — when it
// carries an ExplicitTarget that still resolves to a registered alias
// — re-dispatched to the aliased session, because the spool entry
// outliving the crash means the dispatch was never confirmed.
// Delivery is at-least-once on both legs: the forward is deduped by
// the message DedupKey, and a crash after dispatch but before spool
// removal duplicates the targeted session message — preferred over
// losing it. Undecodable entries are quarantined to the dead-letter
// dir instead of crash-looping replay on every restart; dead-lettered
// entries are NOT replayed automatically — they stay under
// <spoolDir>/dead for manual replay. A missing spool dir is a no-op.
func replaySpool(cfg config, aliasReg *handleAliasRegistry) {
	if cfg.inboundSpoolDir == "" {
		return
	}
	entries, err := os.ReadDir(cfg.inboundSpoolDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("spool replay: %v", err)
		}
		return
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		paths = append(paths, filepath.Join(cfg.inboundSpoolDir, e.Name()))
	}
	if len(paths) == 0 {
		return
	}
	// Bounded worker pool (codex round 2): a crash during a busy gc
	// outage can leave thousands of live entries, and one goroutine per
	// entry would POST them all at once into a gc that just came back —
	// or exhaust sockets. Only PATHS are collected up front; each
	// worker reads and decodes its own entries, so payload memory is
	// bounded by the worker count too (entries can approach the
	// webhook size cap, and a big backlog decoded eagerly could hold
	// gigabytes at startup).
	work := make(chan string)
	for range replaySpoolWorkers {
		go func() {
			for path := range work {
				replayPath(cfg, aliasReg, path)
			}
		}()
	}
	go func() {
		for _, p := range paths {
			work <- p
		}
		close(work)
	}()
}

// replayPath reads and decodes one spool entry, quarantining
// undecodable files, then re-delivers it via replayOne.
func replayPath(cfg config, aliasReg *handleAliasRegistry, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("spool replay: read %s: %v", path, err)
		return
	}
	var msg externalInboundMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("spool replay: decode %s: %v (dead-letter=%s)", path, err, moveToDeadLetter(path))
		return
	}
	replayOne(cfg, aliasReg, path, msg)
}

// replaySpoolWorkers bounds concurrent startup replay deliveries.
const replaySpoolWorkers = 4

// replayOne re-delivers a single spooled entry: forward, then the
// targeted alias dispatch when the handle still resolves, then
// retirement. See replaySpool for the at-least-once semantics.
func replayOne(cfg config, aliasReg *handleAliasRegistry, path string, msg externalInboundMessage) {
	log.Printf("spool replay: re-delivering %s (chan=%s ts=%s target=%q)",
		filepath.Base(path), msg.Conversation.ConversationID, msg.ProviderMessageID, msg.ExplicitTarget)
	if !deliverInbound(cfg, msg, path, nil) {
		return // dead-lettered by deliverInbound
	}
	if msg.ExplicitTarget != "" && aliasReg != nil {
		if sessionID, ok := aliasReg.Get(msg.ExplicitTarget); ok {
			if !dispatchToAliasedSession(cfg, sessionID, msg, msg.ExplicitTarget) {
				reactAliasDispatchFailure(cfg.slackBotToken,
					msg.Conversation.ConversationID, msg.ProviderMessageID)
				log.Printf("spool replay: alias dispatch failed chan=%s ts=%s target=%q (dead-letter=%s)",
					msg.Conversation.ConversationID, msg.ProviderMessageID,
					msg.ExplicitTarget, moveToDeadLetter(path))
				return
			}
		}
		// Target no longer registered: nothing left to dispatch to —
		// fall through and retire the entry.
	}
	removeSpoolEntry(path)
}
