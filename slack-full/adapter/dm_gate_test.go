package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for the DM privacy gate (hq-xizo) — see dm_gate.go. The
// processSlackEvent path is exercised end-to-end against an httptest
// stub for Slack's conversations.info endpoint (overriding
// slackAPIBase, same pattern as thread_context_test.go) and an
// inboundCapture gc stub. The assertion surfaces are (1) whether the
// gc stub received the forwarded inbound and (2) how many times
// conversations.info was consulted.

// fakeConversationsInfo serves /conversations.info with the
// currently-configured status+body, counting calls. The response is
// swappable mid-test via set() so cache-retry behavior can be
// asserted against a single server.
type fakeConversationsInfo struct {
	mu     sync.Mutex
	status int
	body   string
	calls  int32
}

func newFakeConversationsInfo(t *testing.T, status int, body string) (*fakeConversationsInfo, *httptest.Server) {
	t.Helper()
	f := &fakeConversationsInfo{status: status, body: body}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/conversations.info") {
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
			return
		}
		atomic.AddInt32(&f.calls, 1)
		f.mu.Lock()
		status, body := f.status, f.body
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeConversationsInfo) set(status int, body string) {
	f.mu.Lock()
	f.status = status
	f.body = body
	f.mu.Unlock()
}

func (f *fakeConversationsInfo) callCount() int32 {
	return atomic.LoadInt32(&f.calls)
}

// dmGateTestConfig mirrors the cfg shape the thread-context tests
// use, with the gate under test wired in.
func dmGateTestConfig(gcURL string, gate *dmGate) config {
	return config{
		gcAPIBase:     gcURL,
		cityName:      "test-city",
		provider:      "slack",
		accountID:     "T1",
		handlePrefix:  "@",
		slackBotToken: "xoxb-fake",
		dmGate:        gate,
		dispatchSem:   defaultTestDispatchSem,
	}
}

// dmGateEventEnvelope builds an event_callback envelope for a plain
// (non-thread, no-prefix) message so the gate is the only Slack API
// consumer on the path.
func dmGateEventEnvelope(t *testing.T, channel, channelType, ts string) slackEventEnvelope {
	t.Helper()
	raw, err := json.Marshal(slackMessageEvent{
		Type:        "message",
		Channel:     channel,
		ChannelType: channelType,
		User:        "U_HUMAN",
		TS:          ts,
		Text:        "hello from a human",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return slackEventEnvelope{Type: "event_callback", Event: raw}
}

func TestDMGate_MemberDMForwards(t *testing.T) {
	info, slackStub := newFakeConversationsInfo(t, http.StatusOK,
		`{"ok":true,"channel":{"id":"D0MEMBER1"}}`)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := dmGateTestConfig(gcStub.URL, newDMGate())
	env := dmGateEventEnvelope(t, "D0MEMBER1", "im", "300.000001")

	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := info.callCount(); got != 1 {
		t.Errorf("conversations.info calls = %d, want 1", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("gc received %d inbound POSTs, want 1", len(msgs))
	}
	if msgs[0].Conversation.Kind != "dm" {
		t.Errorf("forwarded kind = %q, want %q", msgs[0].Conversation.Kind, "dm")
	}
	if msgs[0].Conversation.ConversationID != "D0MEMBER1" {
		t.Errorf("forwarded conversation id = %q, want %q",
			msgs[0].Conversation.ConversationID, "D0MEMBER1")
	}
}

func TestDMGate_NonMemberDMDropped(t *testing.T) {
	info, slackStub := newFakeConversationsInfo(t, http.StatusOK,
		`{"ok":false,"error":"channel_not_found"}`)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := dmGateTestConfig(gcStub.URL, newDMGate())
	env := dmGateEventEnvelope(t, "D0FOREIGN1", "im", "300.000002")

	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := info.callCount(); got != 1 {
		t.Errorf("conversations.info calls = %d, want 1", got)
	}
	if msgs := capture.snapshot(); len(msgs) != 0 {
		t.Fatalf("gc received %d inbound POSTs, want 0 (non-member DM must be dropped)", len(msgs))
	}
}

// TestDMGate_APIErrorFailsClosedAndNotCached pins the two halves of
// the fail-closed contract: a transient API failure drops the event
// (never forward on uncertainty), but the verdict is NOT cached, so
// the next event re-probes and goes through once Slack recovers.
func TestDMGate_APIErrorFailsClosedAndNotCached(t *testing.T) {
	info, slackStub := newFakeConversationsInfo(t, http.StatusInternalServerError,
		`{"ok":false,"error":"internal_error"}`)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := dmGateTestConfig(gcStub.URL, newDMGate())
	aliasReg := newTestHandleAliasRegistry(t)

	// Event 1: Slack API is failing — fail closed, no forward.
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil,
		dmGateEventEnvelope(t, "D0FLAKY1", "im", "300.000003"), func() {})
	if msgs := capture.snapshot(); len(msgs) != 0 {
		t.Fatalf("gc received %d inbound POSTs during API failure, want 0", len(msgs))
	}
	if got := info.callCount(); got != 1 {
		t.Fatalf("conversations.info calls = %d, want 1", got)
	}

	// Event 2: Slack recovered. The error verdict must not have been
	// cached, so this event re-probes and forwards.
	info.set(http.StatusOK, `{"ok":true,"channel":{"id":"D0FLAKY1"}}`)
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil,
		dmGateEventEnvelope(t, "D0FLAKY1", "im", "300.000004"), func() {})
	if got := info.callCount(); got != 2 {
		t.Errorf("conversations.info calls = %d, want 2 (API-error verdicts must not be cached)", got)
	}
	if msgs := capture.snapshot(); len(msgs) != 1 {
		t.Fatalf("gc received %d inbound POSTs after recovery, want 1", len(msgs))
	}
}

func TestDMGate_PositiveVerdictCached(t *testing.T) {
	info, slackStub := newFakeConversationsInfo(t, http.StatusOK,
		`{"ok":true,"channel":{"id":"D0MEMBER2"}}`)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := dmGateTestConfig(gcStub.URL, newDMGate())
	aliasReg := newTestHandleAliasRegistry(t)

	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil,
		dmGateEventEnvelope(t, "D0MEMBER2", "im", "300.000005"), func() {})
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil,
		dmGateEventEnvelope(t, "D0MEMBER2", "im", "300.000006"), func() {})

	if got := info.callCount(); got != 1 {
		t.Errorf("conversations.info calls = %d, want 1 (positive verdict must be cached)", got)
	}
	if msgs := capture.snapshot(); len(msgs) != 2 {
		t.Fatalf("gc received %d inbound POSTs, want 2", len(msgs))
	}
}

// TestDMGate_NegativeVerdictExpiresAfter30s drives the injected clock
// past dmGateNegativeTTL and asserts the gate re-probes — a human who
// just added the bot to the conversation is only shadow-dropped for
// the negative-TTL window, not forever.
func TestDMGate_NegativeVerdictExpiresAfter30s(t *testing.T) {
	info, slackStub := newFakeConversationsInfo(t, http.StatusOK,
		`{"ok":false,"error":"channel_not_found"}`)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	gate := newDMGate()
	now := time.Unix(1_000_000, 0)
	gate.now = func() time.Time { return now }

	cfg := dmGateTestConfig(gcStub.URL, gate)
	aliasReg := newTestHandleAliasRegistry(t)

	// t+0: not a member — probe once, drop, cache negative.
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil,
		dmGateEventEnvelope(t, "D0LATER1", "im", "300.000007"), func() {})
	if got := info.callCount(); got != 1 {
		t.Fatalf("conversations.info calls = %d, want 1", got)
	}

	// t+29s: still inside the negative TTL — no re-probe, still dropped.
	now = now.Add(29 * time.Second)
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil,
		dmGateEventEnvelope(t, "D0LATER1", "im", "300.000008"), func() {})
	if got := info.callCount(); got != 1 {
		t.Errorf("conversations.info calls = %d, want 1 (negative verdict cached inside TTL)", got)
	}
	if msgs := capture.snapshot(); len(msgs) != 0 {
		t.Fatalf("gc received %d inbound POSTs, want 0 so far", len(msgs))
	}

	// t+31s: negative TTL elapsed AND the bot is now a member — the
	// gate must re-probe and let the event through.
	now = now.Add(2 * time.Second)
	info.set(http.StatusOK, `{"ok":true,"channel":{"id":"D0LATER1"}}`)
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil,
		dmGateEventEnvelope(t, "D0LATER1", "im", "300.000009"), func() {})
	if got := info.callCount(); got != 2 {
		t.Errorf("conversations.info calls = %d, want 2 (negative verdict must expire after 30s)", got)
	}
	if msgs := capture.snapshot(); len(msgs) != 1 {
		t.Fatalf("gc received %d inbound POSTs after membership fixed, want 1", len(msgs))
	}
}

// TestDMGate_ChannelMessageSkipsGate pins that non-DM kinds never
// consult the gate: channel/group/mpim inbounds are governed by
// explicit operator bindings, and burning a conversations.info call
// per channel message would be pure rate-limit waste.
func TestDMGate_ChannelMessageSkipsGate(t *testing.T) {
	info, slackStub := newFakeConversationsInfo(t, http.StatusOK,
		`{"ok":false,"error":"channel_not_found"}`)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := dmGateTestConfig(gcStub.URL, newDMGate())
	env := dmGateEventEnvelope(t, "C1", "channel", "300.000010")

	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := info.callCount(); got != 0 {
		t.Errorf("conversations.info calls = %d, want 0 (channel messages must not consult the gate)", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("gc received %d inbound POSTs, want 1 (channel message forwards without the gate)", len(msgs))
	}
	if msgs[0].Conversation.Kind != "room" {
		t.Errorf("forwarded kind = %q, want %q", msgs[0].Conversation.Kind, "room")
	}
}

// TestDMGateAllow_NilGateFailsClosed pins the nil-receiver contract:
// an unwired gate is a privacy control that failed to initialize, so
// it must drop, not leak.
func TestDMGateAllow_NilGateFailsClosed(t *testing.T) {
	var g *dmGate
	allowed, reason := g.allow("xoxb-fake", "D0ANY")
	if allowed {
		t.Fatal("nil gate allowed a DM; must fail closed")
	}
	if reason == "" {
		t.Error("nil gate returned empty reason; want a log-safe tag")
	}
}

// TestDMGateAllow_PositiveVerdictExpiresAfter5m is the unit-level
// companion to TestDMGate_PositiveVerdictCached: the allowed verdict
// is not forever — after dmGatePositiveTTL the gate re-probes, so a
// bot removed from a conversation stops processing its events within
// five minutes.
func TestDMGateAllow_PositiveVerdictExpiresAfter5m(t *testing.T) {
	info, slackStub := newFakeConversationsInfo(t, http.StatusOK,
		`{"ok":true,"channel":{"id":"D0TTL1"}}`)
	withSlackAPIStub(t, slackStub)

	gate := newDMGate()
	now := time.Unix(2_000_000, 0)
	gate.now = func() time.Time { return now }

	if allowed, _ := gate.allow("xoxb-fake", "D0TTL1"); !allowed {
		t.Fatal("first probe: allowed = false, want true")
	}
	now = now.Add(4*time.Minute + 59*time.Second)
	if allowed, _ := gate.allow("xoxb-fake", "D0TTL1"); !allowed {
		t.Fatal("inside TTL: allowed = false, want true (cached)")
	}
	if got := info.callCount(); got != 1 {
		t.Fatalf("conversations.info calls = %d, want 1 inside positive TTL", got)
	}
	now = now.Add(2 * time.Second)
	if allowed, _ := gate.allow("xoxb-fake", "D0TTL1"); !allowed {
		t.Fatal("after TTL: allowed = false, want true (re-probed)")
	}
	if got := info.callCount(); got != 2 {
		t.Errorf("conversations.info calls = %d, want 2 (positive verdict must expire after 5m)", got)
	}
}

// Concurrent cache misses for the same channel must coalesce onto one
// conversations.info probe (hq-xizo P2): Slack delivers message.im
// per message, so a DM burst would otherwise fan one miss into N
// identical API calls. The stub delays its answer long enough that
// every goroutine reaches the gate before the first probe resolves;
// with the singleflight in place exactly one call may reach Slack.
func TestDMGate_ConcurrentMissesCoalesce(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(srv.Close)
	withSlackAPIStub(t, srv)

	gate := newDMGate()
	const workers = 8
	var wg sync.WaitGroup
	results := make([]bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			allowed, _ := gate.allow("xoxb-fake", "D_COALESCE")
			results[i] = allowed
		}(i)
	}
	wg.Wait()
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("conversations.info calls = %d, want 1 (coalesced)", got)
	}
	for i, allowed := range results {
		if !allowed {
			t.Errorf("worker %d: allowed = false, want true (winner's verdict shared)", i)
		}
	}
}

// A missing_scope failure must name the fix (im:read in the app
// manifest): the gate fails closed on it, and a bare error string
// cost a live workspace every DM until someone read the Slack docs.
func TestDMGate_MissingScopeDetailActionable(t *testing.T) {
	_, srv := newFakeConversationsInfo(t, http.StatusOK, `{"ok":false,"error":"missing_scope"}`)
	withSlackAPIStub(t, srv)

	gate := newDMGate()
	allowed, reason := gate.allow("xoxb-fake", "D_SCOPE")
	if allowed {
		t.Fatal("allowed = true on missing_scope, want fail-closed")
	}
	if !strings.Contains(reason, "im:read") {
		t.Errorf("reason = %q, want the im:read manifest hint", reason)
	}
}
