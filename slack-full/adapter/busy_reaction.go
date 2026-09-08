package main

import (
	"sync"
	"time"
)

// Busy-reaction lifecycle (hq-xizo).
//
// Multi-party channel threads are the primary surface for talking to
// agents through this adapter, and Slack Assistant mode — whose
// assistant.threads.setStatus would normally render a "working on it"
// status — is deliberately not used (its event feed is the reason the
// DM privacy gate in dm_gate.go exists). The channel-native
// replacement: when a targeted inbound is dispatched,
// processSlackEvent adds a busy reaction (BUSY_REACTION, default
// "hourglass") to the inbound Slack message and records the pending
// mark here; when the agent's reply is published back into the same
// conversation/thread, handlePublish looks the mark up and removes
// the reaction via reactions.remove — add on dispatch, remove on
// reply.
//
// Entries are keyed by (conversation id, thread key). A thread-reply
// inbound registers under BOTH its thread_ts (the root the reply
// publish normally threads under) and its own ts (the key a publish
// carries when the responder threads under the targeted message
// itself), so either reply shape clears the mark; a channel-root
// inbound has one key — its own ts. The two sibling entries point at
// the same messageTS and consuming either deletes both.
//
// Two races are handled explicitly:
//
//   - add still in flight when the reply lands: take() sees the
//     pending add (the adds map) and defers — it flags the add state
//     "cleared" instead of returning the ts, and the add goroutine
//     removes the reaction itself right after the add lands
//     (confirmAdd reports cleared). Without this the remove would
//     no-op before the add landed and the emoji would stick forever.
//   - re-mark of the same thread key with a NEW targeted message
//     before the previous reply arrived: the displaced mark's
//     reaction is removed (returned to the caller for an async
//     reactions.remove, or delegated to its in-flight add completer)
//     instead of being silently forgotten with its emoji stranded.
//
// The registry is memory-only and best-effort by design: a mark whose
// reply never arrives expires after busyReactionTTL (the entry is
// dropped and the reaction simply stops being removable), and an
// adapter restart forgets pending marks. Nothing here may block or
// fail the dispatch or publish paths.

// busyReactionDefault is the emoji added when BUSY_REACTION is unset.
const busyReactionDefault = "hourglass"

// busyReactionTTL bounds how long a pending busy mark stays
// removable. A reply landing later than this is either a very slow
// agent or a session that died mid-task; in both cases silently
// keeping the map entry forever is worse than leaving a stale
// hourglass on one old message.
const busyReactionTTL = 30 * time.Minute

// busyReactionMaxEntries hard-caps the registry so a pathological
// event stream (many distinct targeted inbounds, no replies) cannot
// grow it without bound. Mirrors dmGateMaxEntries: on overflow,
// expired entries are already swept and the oldest surviving mark is
// evicted — that mark's reaction just stops being removable.
const busyReactionMaxEntries = 4096

// busyReactionKey identifies one conversation/thread with a pending
// busy mark.
type busyReactionKey struct {
	channel   string
	threadKey string
}

// busyReactionMark is one pending busy reaction: the ts of the Slack
// message the reaction was added to, the sibling registry key
// registered for the same inbound ("" when the inbound had only one
// key), and when it was added (for TTL).
type busyReactionMark struct {
	messageTS  string
	siblingKey string
	addedAt    time.Time
}

// busyAddKey identifies one asynchronous reactions.add in flight.
type busyAddKey struct {
	channel   string
	messageTS string
}

// busyAddState tracks an in-flight reactions.add. cleared is set when
// the mark was consumed (reply landed, or the mark was displaced by a
// re-mark) before the add finished; the add goroutine then owns the
// compensating reactions.remove.
type busyAddState struct {
	cleared bool
	startAt time.Time
}

// busyThreadKey derives the primary registry thread key for an
// inbound message: its thread_ts when it is a thread reply, its own
// ts when it is a channel-root message (a reply to it will thread
// under that same ts).
func busyThreadKey(threadTS, messageTS string) string {
	if threadTS != "" {
		return threadTS
	}
	return messageTS
}

// busyReactionRegistry tracks pending busy marks. Safe for concurrent
// callers; the mutex guards the maps only — Slack API calls never run
// under it.
//
// A nil *busyReactionRegistry is inert: mark is a no-op and take
// reports no pending mark, so tests (and a misordered main) degrade
// to "no lifecycle" rather than panicking.
type busyReactionRegistry struct {
	mu      sync.Mutex
	entries map[busyReactionKey]busyReactionMark
	adds    map[busyAddKey]*busyAddState
	// cleared records (channel, messageTS) pairs whose busy mark a
	// reply recently consumed. A Slack redelivery of the same event
	// (webhook 200 lost) would otherwise re-mark and re-add the emoji
	// on a message gc deduped away — no second reply is coming, so
	// the re-added hourglass would stick until TTL. Entries expire
	// after busyClearedTTL and are swept opportunistically.
	cleared map[busyAddKey]time.Time
	// now is the clock; nil means time.Now. Injectable so tests can
	// drive TTL expiry without sleeping.
	now func() time.Time
}

// busyClearedTTL bounds how long a consumed message blocks re-marking.
// Slack retries a lost webhook ack within minutes; five minutes
// comfortably covers the redelivery window while keeping the map
// small.
const busyClearedTTL = 5 * time.Minute

func newBusyReactionRegistry() *busyReactionRegistry {
	return &busyReactionRegistry{
		entries: make(map[busyReactionKey]busyReactionMark),
		adds:    make(map[busyAddKey]*busyAddState),
		cleared: make(map[busyAddKey]time.Time),
	}
}

func (r *busyReactionRegistry) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// mark records a pending busy reaction on messageTS under
// (channel, threadKey) — and, for a thread reply (threadKey !=
// messageTS), under (channel, messageTS) as well, so a reply publish
// carrying either the thread root or the targeted message's own ts
// clears it. It also registers the reactions.add as in flight; the
// add goroutine MUST later call confirmAdd (landed) or abandonAdd
// (failed).
//
// A re-mark of the same key (human re-tags the agent in the same
// thread before the first reply lands) overwrites — the busy emoji
// sits on the newest targeted message and the reply clears that one.
// The displaced mark's emoji is not stranded: its ts is returned so
// the caller can fire reactions.remove for it, unless its add is
// still in flight, in which case the removal is delegated to that
// add's completer.
// admitted=false means the caller must skip the whole add lifecycle:
// either a reply already consumed this message's mark within
// busyClearedTTL (a Slack redelivery of a finished event — the
// tombstone check and the insert are one locked operation, codex
// round 2), or the message is already live in the registry (a
// concurrent copy of the same event admitted it first — a second
// reactions.add would leave a second in-flight add racing the single
// removal).
func (r *busyReactionRegistry) mark(channel, threadKey, messageTS string) (displaced []string, admitted bool) {
	if r == nil || channel == "" || threadKey == "" || messageTS == "" {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	r.sweepLocked(now)
	if at, ok := r.cleared[busyAddKey{channel: channel, messageTS: messageTS}]; ok && now.Sub(at) <= busyClearedTTL {
		return nil, false
	}
	keys := []string{threadKey}
	sibling := map[string]string{threadKey: ""}
	if messageTS != threadKey {
		keys = append(keys, messageTS)
		sibling[threadKey] = messageTS
		sibling[messageTS] = threadKey
	}
	for _, k := range keys {
		if cur, ok := r.entries[busyReactionKey{channel: channel, threadKey: k}]; ok && cur.messageTS == messageTS {
			// Already live: coalesce with the first copy's mark and
			// its single in-flight add.
			return nil, false
		}
	}
	for _, k := range keys {
		key := busyReactionKey{channel: channel, threadKey: k}
		if old, ok := r.entries[key]; ok && old.messageTS != messageTS {
			displaced = append(displaced, r.displaceLocked(channel, old)...)
		}
		r.entries[key] = busyReactionMark{
			messageTS:  messageTS,
			siblingKey: sibling[k],
			addedAt:    now,
		}
	}
	r.adds[busyAddKey{channel: channel, messageTS: messageTS}] = &busyAddState{startAt: now}
	for len(r.entries) > busyReactionMaxEntries {
		r.evictOldestLocked()
	}
	return displaced, true
}

// cancelMark retires a mark whose inbound never reached gc: no reply
// will ever come to clear it, and its reactions.add never launched
// (the add fires only after delivery succeeds), so entries and the
// in-flight add state are simply dropped. Entries are removed only
// while they still point at messageTS — a newer re-target's mark is
// never touched.
func (r *busyReactionRegistry) cancelMark(channel, threadKey, messageTS string) {
	if r == nil || channel == "" || messageTS == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := []string{threadKey}
	if messageTS != threadKey {
		keys = append(keys, messageTS)
	}
	for _, k := range keys {
		key := busyReactionKey{channel: channel, threadKey: k}
		if m, ok := r.entries[key]; ok && m.messageTS == messageTS {
			delete(r.entries, key)
		}
	}
	delete(r.adds, busyAddKey{channel: channel, messageTS: messageTS})
}

// displaceLocked retires an overwritten mark: its sibling entry (if
// still pointing at the same message) is dropped, and its reaction is
// either handed back for removal (add already landed) or delegated to
// the in-flight add's completer via the cleared flag. Called with
// r.mu held.
func (r *busyReactionRegistry) displaceLocked(channel string, old busyReactionMark) []string {
	if old.siblingKey != "" {
		sk := busyReactionKey{channel: channel, threadKey: old.siblingKey}
		if sib, ok := r.entries[sk]; ok && sib.messageTS == old.messageTS {
			delete(r.entries, sk)
		}
	}
	if st, ok := r.adds[busyAddKey{channel: channel, messageTS: old.messageTS}]; ok {
		st.cleared = true
		return nil
	}
	return []string{old.messageTS}
}

// take consumes the pending mark for (channel, threadKey), deleting
// its sibling entry too. ok=true means the caller owns firing
// reactions.remove for the returned ts. ok=false covers: no mark,
// expired mark, and — the fast-reply race — a mark whose
// reactions.add is still in flight; in that last case the add
// completer fires the remove itself (take flags the add state and
// confirmAdd reports it), because a remove issued before the add
// lands would no-op and strand the emoji.
func (r *busyReactionRegistry) take(channel, threadKey string) (messageTS string, ok bool) {
	if r == nil {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := busyReactionKey{channel: channel, threadKey: threadKey}
	m, present := r.entries[key]
	if !present {
		return "", false
	}
	delete(r.entries, key)
	if m.siblingKey != "" {
		sk := busyReactionKey{channel: channel, threadKey: m.siblingKey}
		if sib, ok := r.entries[sk]; ok && sib.messageTS == m.messageTS {
			delete(r.entries, sk)
		}
	}
	if r.clock().Sub(m.addedAt) > busyReactionTTL {
		return "", false
	}
	r.cleared[busyAddKey{channel: channel, messageTS: m.messageTS}] = r.clock()
	if st, ok := r.adds[busyAddKey{channel: channel, messageTS: m.messageTS}]; ok {
		st.cleared = true
		return "", false
	}
	return m.messageTS, true
}

// takeExact consumes the mark for messageTS under its possible keys
// (thread root and own ts) ONLY while the entries still point at that
// exact message — a newer re-target's mark is never touched. Backs
// the alias-dispatch-failure cleanup: the addressed session never got
// the message, no reply is coming, and TTL expiry would only forget
// the mark without removing the Slack-side emoji. removeNow reports
// whether the caller owes the reactions.remove (the add already
// landed); when the add is still in flight the removal is delegated
// to its completer exactly like take.
func (r *busyReactionRegistry) takeExact(channel, threadTS, messageTS string) (removeNow bool) {
	if r == nil || channel == "" || messageTS == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := []string{busyThreadKey(threadTS, messageTS)}
	if threadTS != "" && threadTS != messageTS {
		keys = append(keys, messageTS)
	}
	found := false
	for _, k := range keys {
		key := busyReactionKey{channel: channel, threadKey: k}
		if m, ok := r.entries[key]; ok && m.messageTS == messageTS {
			delete(r.entries, key)
			found = true
		}
	}
	if !found {
		return false
	}
	if st, ok := r.adds[busyAddKey{channel: channel, messageTS: messageTS}]; ok {
		st.cleared = true
		return false
	}
	return true
}

// confirmAdd records that the asynchronous reactions.add for
// (channel, messageTS) landed. removeNow reports whether the mark was
// consumed while the add was in flight — the caller must then fire
// reactions.remove itself, completing the deferred removal.
func (r *busyReactionRegistry) confirmAdd(channel, messageTS string) (removeNow bool) {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := busyAddKey{channel: channel, messageTS: messageTS}
	st, ok := r.adds[key]
	if !ok {
		return false
	}
	delete(r.adds, key)
	return st.cleared
}

// abandonAdd drops the in-flight add state after a failed
// reactions.add. There is no emoji on the message, so a cleared flag
// is discarded; any surviving entries for the message become inert —
// a later take returns the ts and the remove no-ops benignly
// ("no_reaction").
func (r *busyReactionRegistry) abandonAdd(channel, messageTS string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.adds, busyAddKey{channel: channel, messageTS: messageTS})
}

// pending reports the recorded message ts for (channel, threadKey)
// without consuming or TTL-checking the entry. Test/observability
// helper.
func (r *busyReactionRegistry) pending(channel, threadKey string) (messageTS string, ok bool) {
	if r == nil {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, present := r.entries[busyReactionKey{channel: channel, threadKey: threadKey}]
	if !present {
		return "", false
	}
	return m.messageTS, true
}

// size reports the number of pending mark entries (sibling entries
// count individually). Test helper.
func (r *busyReactionRegistry) size() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// sweepLocked drops expired marks and stale add states. Called with
// r.mu held. Add states are covered by confirmAdd/abandonAdd on every
// goroutine exit path; the sweep is insurance against a leak keeping
// the map growing forever.
func (r *busyReactionRegistry) sweepLocked(now time.Time) {
	for k, m := range r.entries {
		if now.Sub(m.addedAt) > busyReactionTTL {
			delete(r.entries, k)
		}
	}
	for k, st := range r.adds {
		if now.Sub(st.startAt) > busyReactionTTL {
			delete(r.adds, k)
		}
	}
	for k, at := range r.cleared {
		if now.Sub(at) > busyClearedTTL {
			delete(r.cleared, k)
		}
	}
}

// evictOldestLocked drops the single oldest mark. Called with r.mu
// held, only on the insert that pushed the map past the cap (expired
// entries were already swept by mark). The evicted mark's sibling
// entry (if any) survives, so the reaction stays removable through
// the other key.
func (r *busyReactionRegistry) evictOldestLocked() {
	var oldestKey busyReactionKey
	var oldestAt time.Time
	first := true
	for k, m := range r.entries {
		if first || m.addedAt.Before(oldestAt) {
			oldestKey, oldestAt, first = k, m.addedAt, false
		}
	}
	if !first {
		delete(r.entries, oldestKey)
	}
}
