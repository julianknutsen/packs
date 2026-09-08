package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// DM privacy gate (hq-xizo, ported from the gastown Slack daemon).
//
// Slack can deliver message.im events for DM conversations the bot is
// not actually a member of — notably when Assistant/Agent mode is
// enabled on the app, the events feed includes DMs between two humans.
// Processing those forwards a private human↔human conversation to
// agents. Before processSlackEvent does any real work on a kind=="dm"
// inbound, it consults this gate, which verifies the bot can access
// the conversation via Slack's conversations.info API and drops the
// event silently (never revealing the bot's presence) when it cannot.
//
// Interpretation of the conversations.info response:
//
//   - ok:true                          → allowed (bot can access the DM)
//   - ok:false, error=channel_not_found → definitively NOT allowed
//   - anything else (transport error,
//     non-2xx, rate limit, unknown
//     error string)                    → NOT allowed for this event
//     (fail closed), but the verdict is NOT cached, so the very next
//     event for the same channel retries the API instead of being
//     poisoned by a transient failure.

// dmGatePositiveTTL is how long an "allowed" verdict is cached. DMs
// the bot belongs to stay that way in practice; five minutes keeps a
// steady-state conversation at one conversations.info call per
// channel per five minutes.
const dmGatePositiveTTL = 5 * time.Minute

// dmGateNegativeTTL is how long a channel_not_found verdict is
// cached. Short, so a human who *just* invited the bot (or an app
// whose membership was just fixed) is only shadow-dropped for at most
// 30 seconds while still absorbing event bursts for foreign DMs.
const dmGateNegativeTTL = 30 * time.Second

// dmGateCheckTimeout bounds the conversations.info round-trip so a
// stuck Slack API cannot pin the dispatch goroutine (and its
// dispatchSem slot) indefinitely.
const dmGateCheckTimeout = 10 * time.Second

// dmGateMaxEntries hard-caps the cache map so a pathological event
// stream (many distinct foreign DM channel ids) cannot grow it
// without bound. On overflow, expired entries are pruned; if the map
// is still over cap the entry closest to expiry is evicted — a benign
// re-check on that channel's next event.
const dmGateMaxEntries = 4096

// dmGateEntry is one cached verdict for a channel id.
type dmGateEntry struct {
	allowed bool
	expires time.Time
}

// dmGate caches per-DM-channel membership verdicts. Safe for
// concurrent callers; the mutex guards only the maps — the
// conversations.info round-trip runs unlocked. Concurrent misses on
// the same channel coalesce onto one in-flight probe (the inflight
// map): the first caller performs the API call, later callers block
// on its done channel and adopt the same verdict. The wait is bounded
// by the winner's dmGateCheckTimeout-scoped request.
//
// A nil *dmGate fails closed: allow reports not-allowed. The gate is
// a privacy control, so an unwired gate must drop DMs rather than
// leak them; only main() (and tests) construct one.
type dmGate struct {
	mu       sync.Mutex
	entries  map[string]dmGateEntry
	inflight map[string]*dmGateProbe
	// now is the clock; nil means time.Now. Injectable so tests can
	// drive TTL expiry without sleeping.
	now func() time.Time
}

// dmGateProbe is one in-flight conversations.info round-trip. done is
// closed after allowed/reason are populated; waiters read them only
// after <-done.
type dmGateProbe struct {
	done    chan struct{}
	allowed bool
	reason  string
}

func newDMGate() *dmGate {
	return &dmGate{
		entries:  make(map[string]dmGateEntry),
		inflight: make(map[string]*dmGateProbe),
	}
}

func (g *dmGate) clock() time.Time {
	if g.now != nil {
		return g.now()
	}
	return time.Now()
}

// allow reports whether a DM inbound on channel may be processed.
// reason is meaningful only when allowed is false; it carries a short
// tag safe for logs (channel disposition only — never message
// content).
func (g *dmGate) allow(token, channel string) (allowed bool, reason string) {
	if g == nil {
		return false, "gate_unconfigured"
	}
	now := g.clock()
	g.mu.Lock()
	if e, ok := g.entries[channel]; ok {
		if now.Before(e.expires) {
			g.mu.Unlock()
			if e.allowed {
				return true, ""
			}
			return false, "channel_not_found_cached"
		}
		delete(g.entries, channel)
	}
	// Cache miss: coalesce onto an in-flight probe for this channel if
	// one exists, otherwise become the probe owner. A DM burst (Slack
	// delivers message.im events per message) would otherwise fan one
	// miss into N identical conversations.info calls.
	if p, ok := g.inflight[channel]; ok {
		g.mu.Unlock()
		<-p.done
		return p.allowed, p.reason
	}
	probe := &dmGateProbe{done: make(chan struct{})}
	g.inflight[channel] = probe
	g.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), dmGateCheckTimeout)
	defer cancel()
	verdict, detail := queryConversationsInfo(ctx, token, channel)
	switch verdict {
	case dmGateVerdictAllowed:
		g.store(channel, true, now.Add(dmGatePositiveTTL))
		allowed, reason = true, ""
	case dmGateVerdictNotFound:
		g.store(channel, false, now.Add(dmGateNegativeTTL))
		allowed, reason = false, "channel_not_found"
	default:
		// Fail closed on any API-level failure, but do NOT cache the
		// verdict: the next event for this channel retries.
		allowed, reason = false, "api_error: "+detail
	}
	probe.allowed, probe.reason = allowed, reason
	g.mu.Lock()
	delete(g.inflight, channel)
	g.mu.Unlock()
	close(probe.done)
	return allowed, reason
}

// store records a verdict, pruning on overflow. Called only for the
// two definitive verdicts — API errors are never stored.
func (g *dmGate) store(channel string, allowed bool, expires time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.entries[channel] = dmGateEntry{allowed: allowed, expires: expires}
	if len(g.entries) > dmGateMaxEntries {
		g.evictLocked()
	}
}

// evictLocked drops expired entries and, if the map is still over
// cap, the single entry closest to expiry. Called with g.mu held,
// only on the insert that pushed the map past the cap.
func (g *dmGate) evictLocked() {
	now := g.clock()
	var soonestKey string
	var soonestExpiry time.Time
	for key, e := range g.entries {
		if !now.Before(e.expires) {
			delete(g.entries, key)
			continue
		}
		if soonestKey == "" || e.expires.Before(soonestExpiry) {
			soonestKey, soonestExpiry = key, e.expires
		}
	}
	if len(g.entries) > dmGateMaxEntries && soonestKey != "" {
		delete(g.entries, soonestKey)
	}
}

// dmGateVerdict is the tri-state outcome of one conversations.info
// probe. Allowed and NotFound are definitive and cacheable; Error is
// transient and must not be cached.
type dmGateVerdict int

const (
	dmGateVerdictAllowed dmGateVerdict = iota
	dmGateVerdictNotFound
	dmGateVerdictError
)

// queryConversationsInfo asks Slack whether the bot token can access
// channel. detail is a short log-safe tag describing the failure when
// the verdict is dmGateVerdictError or dmGateVerdictNotFound.
func queryConversationsInfo(ctx context.Context, token, channel string) (verdict dmGateVerdict, detail string) {
	if token == "" {
		return dmGateVerdictError, "slack token empty"
	}
	if channel == "" {
		return dmGateVerdictError, "channel empty"
	}
	q := url.Values{}
	q.Set("channel", channel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		slackAPIBase+"/conversations.info?"+q.Encode(), nil)
	if err != nil {
		return dmGateVerdictError, fmt.Sprintf("build conversations.info request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return dmGateVerdictError, fmt.Sprintf("conversations.info: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return dmGateVerdictError, fmt.Sprintf("read conversations.info body: %v", err)
	}
	if resp.StatusCode >= 300 {
		return dmGateVerdictError, fmt.Sprintf("conversations.info HTTP %d", resp.StatusCode)
	}
	var sr slackConversationsInfoResp
	if err := json.Unmarshal(body, &sr); err != nil {
		return dmGateVerdictError, fmt.Sprintf("decode conversations.info: %v", err)
	}
	if sr.OK {
		return dmGateVerdictAllowed, ""
	}
	if sr.Error == "channel_not_found" {
		return dmGateVerdictNotFound, "channel_not_found"
	}
	if sr.Error == "missing_scope" {
		// Operator-actionable: the gate NEEDS im:read (manifest
		// oauth_config.scopes.bot) to probe DM membership. Without it
		// every legitimate DM fails closed here — say so explicitly
		// instead of leaving a bare error string.
		return dmGateVerdictError, "conversations.info not ok: missing_scope (bot token lacks im:read — add it to the app manifest and reinstall the app)"
	}
	return dmGateVerdictError, "conversations.info not ok: " + sr.Error
}
