package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withTimeout returns a context bounded by d, with a Cleanup-style
// cancel that the caller can defer. Test-only helper.
func withTimeout(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), d)
}

// Tests for gc-px8.5 — first-mention thread-context forwarding.
//
// The processSlackEvent paths are exercised end-to-end against an
// httptest stub for both Slack's conversations.replies endpoint
// (overriding slackAPIBase) and gc's inbound endpoint
// (cfg.gcAPIBase). The captured POST body to gc is the assertion
// surface — the bridge-mail Text either carries the preamble or
// doesn't, depending on the cache state.

// withSlackAPIStub installs a slackAPIBase override pointing at the
// supplied test server and returns a restore closure. Pattern matches
// what other slack-API tests in this package do.
func withSlackAPIStub(t *testing.T, srv *httptest.Server) {
	t.Helper()
	prev := slackAPIBase
	slackAPIBase = srv.URL
	t.Cleanup(func() { slackAPIBase = prev })
}

// inboundCapture is a thin gc-stub that records each posted inbound
// message body so tests can assert on Text content without parsing
// raw bytes inline.
type inboundCapture struct {
	mu       sync.Mutex
	messages []externalInboundMessage
}

func (c *inboundCapture) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var env struct {
			Message externalInboundMessage `json:"message"`
		}
		if err := json.Unmarshal(body, &env); err == nil {
			c.mu.Lock()
			c.messages = append(c.messages, env.Message)
			c.mu.Unlock()
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func (c *inboundCapture) snapshot() []externalInboundMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]externalInboundMessage, len(c.messages))
	copy(out, c.messages)
	return out
}

// fakeSlackRepliesServer returns an httptest server that replies to
// /conversations.replies with the supplied messages, counting how
// many times it was called.
func fakeSlackRepliesServer(t *testing.T, messages []slackThreadMessage) (*httptest.Server, *int32) {
	t.Helper()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/conversations.replies") {
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
			return
		}
		atomic.AddInt32(&calls, 1)
		resp := slackConversationsRepliesResp{OK: true, Messages: messages}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestThreadContext_FirstMentionPrependsPreamble(t *testing.T) {
	prior := []slackThreadMessage{
		{User: "U_ALICE", Text: "should we ship this?", TS: "100.000001"},
		{User: "U_BOB", Text: "lgtm — open the PR", TS: "100.000002"},
	}
	slackStub, calls := fakeSlackRepliesServer(t, prior)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := config{
		gcAPIBase:               gcStub.URL,
		cityName:                "test-city",
		provider:                "slack",
		accountID:               "T1",
		handlePrefix:            "@",
		slackBotToken:           "xoxb-fake",
		slackThreadContextLimit: 20,
		threadContextCache:      newThreadContextCache(),
		dispatchSem:             defaultTestDispatchSem,
	}
	rawMsg, _ := json.Marshal(slackMessageEvent{
		Type:     "message",
		Channel:  "C1",
		User:     "U_ALICE",
		TS:       "100.000003",
		ThreadTS: "100.000001",
		Text:     "@mayor please weigh in",
	})
	env := slackEventEnvelope{Type: "event_callback", Event: rawMsg}

	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := atomic.LoadInt32(calls); got != 1 {
		t.Fatalf("conversations.replies calls = %d, want 1", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1", len(msgs))
	}
	body := msgs[0].Text
	if !strings.HasPrefix(body, "Thread context (2 earlier messages):\n") {
		t.Errorf("body missing preamble; got %q", body)
	}
	if !strings.Contains(body, "@U_ALICE: should we ship this?") {
		t.Errorf("body missing U_ALICE prior message; got %q", body)
	}
	if !strings.Contains(body, "@U_BOB: lgtm — open the PR") {
		t.Errorf("body missing U_BOB prior message; got %q", body)
	}
	// The "@mayor" handle prefix is stripped by parseHandlePrefix
	// before the preamble is prepended; the literal mention text
	// "please weigh in" must still appear after the preamble.
	if !strings.Contains(body, "please weigh in") {
		t.Errorf("body missing original text after preamble; got %q", body)
	}
	if msgs[0].ExplicitTarget != "mayor" {
		t.Errorf("ExplicitTarget = %q, want %q", msgs[0].ExplicitTarget, "mayor")
	}
}

// TestThreadContext_SecondMentionWithoutNewActivityNoPreamble — the
// gc-px8.6 cache stores per-target last-delivered ts. A second
// mention of the same target with no peer activity in between
// fetches Slack again (option B) but the formatter filter sees no
// messages newer than the cached cutoff, so no preamble is emitted.
// gc-px8.5's user-visible "no redundant context paste" guarantee
// is preserved even though the API call count is now per-inbound.
func TestThreadContext_SecondMentionWithoutNewActivityNoPreamble(t *testing.T) {
	// Shared replies list mutated between calls so the second
	// inbound sees only itself and the parent (no new peer activity).
	var serverMu sync.Mutex
	replies := []slackThreadMessage{
		{User: "U_ALICE", Text: "context line", TS: "200.000001"},
	}
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only count conversations.replies; the adapter's busy-reaction
		// path posts to /reactions.add and is out of scope here.
		if strings.HasSuffix(r.URL.Path, "/conversations.replies") {
			atomic.AddInt32(&calls, 1)
		}
		serverMu.Lock()
		resp := slackConversationsRepliesResp{OK: true, Messages: append([]slackThreadMessage(nil), replies...)}
		serverMu.Unlock()
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	prev := slackAPIBase
	slackAPIBase = srv.URL
	t.Cleanup(func() { slackAPIBase = prev })

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := config{
		gcAPIBase:               gcStub.URL,
		cityName:                "test-city",
		provider:                "slack",
		accountID:               "T1",
		handlePrefix:            "@",
		slackBotToken:           "xoxb-fake",
		slackThreadContextLimit: 20,
		threadContextCache:      newThreadContextCache(),
		dispatchSem:             defaultTestDispatchSem,
	}

	first, _ := json.Marshal(slackMessageEvent{
		Type:     "message",
		Channel:  "C1",
		User:     "U_BOB",
		TS:       "200.000002",
		ThreadTS: "200.000001",
		Text:     "@mayor first mention",
	})
	second, _ := json.Marshal(slackMessageEvent{
		Type:     "message",
		Channel:  "C1",
		User:     "U_BOB",
		TS:       "200.000003",
		ThreadTS: "200.000001",
		Text:     "@mayor follow-up question",
	})

	aliasReg := newTestHandleAliasRegistry(t)
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: first}, func() {})
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: second}, func() {})

	// Option B: each inbound fetches. The cache filters preamble.
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("conversations.replies calls = %d, want 2 (option B: fetch per inbound)", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 2 {
		t.Fatalf("captured %d inbound messages, want 2", len(msgs))
	}
	if !strings.HasPrefix(msgs[0].Text, "Thread context (1 earlier message):\n") {
		t.Errorf("first inbound missing preamble; got %q", msgs[0].Text)
	}
	if strings.Contains(msgs[1].Text, "Thread context") {
		t.Errorf("second inbound carried preamble despite no new peer activity; got %q", msgs[1].Text)
	}
	if !strings.HasPrefix(msgs[1].Text, "follow-up question") {
		t.Errorf("second inbound text unexpected; got %q", msgs[1].Text)
	}
}

func TestThreadContext_NonThreadInboundSkipsFetch(t *testing.T) {
	slackStub, calls := fakeSlackRepliesServer(t, nil)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := config{
		gcAPIBase:               gcStub.URL,
		cityName:                "test-city",
		provider:                "slack",
		accountID:               "T1",
		handlePrefix:            "@",
		slackBotToken:           "xoxb-fake",
		slackThreadContextLimit: 20,
		threadContextCache:      newThreadContextCache(),
		dispatchSem:             defaultTestDispatchSem,
	}
	// thread_ts empty: not a reply; no preamble path.
	rawMsg, _ := json.Marshal(slackMessageEvent{
		Type:    "message",
		Channel: "C1",
		User:    "U_ALICE",
		TS:      "300.000001",
		Text:    "standalone message",
	})
	env := slackEventEnvelope{Type: "event_callback", Event: rawMsg}
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := atomic.LoadInt32(calls); got != 0 {
		t.Errorf("conversations.replies calls = %d, want 0", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1", len(msgs))
	}
	if strings.Contains(msgs[0].Text, "Thread context") {
		t.Errorf("non-thread inbound carried preamble; got %q", msgs[0].Text)
	}
}

func TestThreadContext_ThreadParentSkipsFetch(t *testing.T) {
	// thread_ts == ts: this IS the thread parent. No priors exist.
	slackStub, calls := fakeSlackRepliesServer(t, nil)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := config{
		gcAPIBase:               gcStub.URL,
		cityName:                "test-city",
		provider:                "slack",
		accountID:               "T1",
		handlePrefix:            "@",
		slackBotToken:           "xoxb-fake",
		slackThreadContextLimit: 20,
		threadContextCache:      newThreadContextCache(),
		dispatchSem:             defaultTestDispatchSem,
	}
	rawMsg, _ := json.Marshal(slackMessageEvent{
		Type:     "message",
		Channel:  "C1",
		User:     "U_ALICE",
		TS:       "400.000001",
		ThreadTS: "400.000001",
		Text:     "kicking off a new thread",
	})
	env := slackEventEnvelope{Type: "event_callback", Event: rawMsg}
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := atomic.LoadInt32(calls); got != 0 {
		t.Errorf("conversations.replies calls = %d, want 0", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1", len(msgs))
	}
	if strings.Contains(msgs[0].Text, "Thread context") {
		t.Errorf("thread-parent inbound carried preamble; got %q", msgs[0].Text)
	}
}

func TestThreadContext_NoPriorsAfterFilteringEmitsNoPreamble(t *testing.T) {
	// Slack returns only the current message and a bot-authored
	// reply. Both are filtered; no priors → no preamble.
	currentTS := "500.000005"
	prior := []slackThreadMessage{
		{User: "U_ALICE", Text: "first", TS: "500.000001"},
		{BotID: "B0", User: "", Text: "bot reply", TS: "500.000002"},
		// The current message itself; conversations.replies includes it.
		{User: "U_BOB", Text: "@mayor question", TS: currentTS},
	}
	// First inbound has TS = "500.000001" — making U_ALICE's message
	// not "prior" (it's the current one). Other entries are bot or
	// later. So preamble should NOT be emitted.
	slackStub, calls := fakeSlackRepliesServer(t, prior)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := config{
		gcAPIBase:               gcStub.URL,
		cityName:                "test-city",
		provider:                "slack",
		accountID:               "T1",
		handlePrefix:            "@",
		slackBotToken:           "xoxb-fake",
		slackThreadContextLimit: 20,
		threadContextCache:      newThreadContextCache(),
		dispatchSem:             defaultTestDispatchSem,
	}
	rawMsg, _ := json.Marshal(slackMessageEvent{
		Type:     "message",
		Channel:  "C1",
		User:     "U_ALICE",
		TS:       "500.000001",
		ThreadTS: "500.000000",
		Text:     "@mayor first mention",
	})
	env := slackEventEnvelope{Type: "event_callback", Event: rawMsg}
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := atomic.LoadInt32(calls); got != 1 {
		t.Errorf("conversations.replies calls = %d, want 1 (one fetch attempted, no preamble emitted)", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1", len(msgs))
	}
	if strings.Contains(msgs[0].Text, "Thread context") {
		t.Errorf("inbound carried preamble despite no priors after filter; got %q", msgs[0].Text)
	}
}

// TestThreadContext_FetchFailureRetriesNextInbound — gc-px8.6
// trade-off: errors do NOT advance the cached ts, so a transient
// Slack 5xx on inbound 1 is retried on inbound 2 (rather than
// permanently losing context for the thread the way the gc-px8.5
// "mark before fetch" policy did). Persistent failures pay one
// log line per inbound; that's the right operator signal.
func TestThreadContext_FetchFailureRetriesNextInbound(t *testing.T) {
	var calls int32
	failingSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only count conversations.replies; the adapter's busy-reaction
		// path posts to /reactions.add and is out of scope here.
		if strings.HasSuffix(r.URL.Path, "/conversations.replies") {
			atomic.AddInt32(&calls, 1)
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(failingSrv.Close)
	withSlackAPIStub(t, failingSrv)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := config{
		gcAPIBase:               gcStub.URL,
		cityName:                "test-city",
		provider:                "slack",
		accountID:               "T1",
		handlePrefix:            "@",
		slackBotToken:           "xoxb-fake",
		slackThreadContextLimit: 20,
		threadContextCache:      newThreadContextCache(),
		dispatchSem:             defaultTestDispatchSem,
	}
	mk := func(ts string) []byte {
		raw, _ := json.Marshal(slackMessageEvent{
			Type: "message", Channel: "C1", User: "U_ALICE",
			TS: ts, ThreadTS: "600.000001", Text: "@mayor x",
		})
		return raw
	}
	aliasReg := newTestHandleAliasRegistry(t)
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: mk("600.000002")}, func() {})
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: mk("600.000003")}, func() {})

	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("conversations.replies calls = %d, want 2 (errors must retry per inbound, not permanently suppress)", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 2 {
		t.Fatalf("captured %d inbound messages, want 2", len(msgs))
	}
	for i, m := range msgs {
		if strings.Contains(m.Text, "Thread context") {
			t.Errorf("inbound %d carried preamble despite fetch failure; got %q", i, m.Text)
		}
	}
}

// TestThreadContext_CrossAgentDeltaVisibility — gc-px8.6's payoff.
// Two agents (mayor, PL) bound to the same thread. mayor is
// mentioned first, sees U_ALICE's prior. PL is mentioned next,
// sees U_ALICE's prior PLUS the human reply that landed between
// (which Slack's conversations.replies returns once posted). mayor
// is then mentioned again — sees ONLY the messages posted since
// its last visit (peer activity it hasn't seen yet), not redundant
// re-paste of U_ALICE's prior already conveyed at step 1.
//
// Note on self-exclusion: the implementation does not currently
// resolve target-handle (e.g. "mayor") to the agent's underlying
// Slack User ID, so a later message authored by the same agent
// will appear in that agent's own subsequent preamble (provided it
// landed after the cached last-visit cutoff). Filtering self by
// identity is a future enhancement; for now redundancy is bounded
// by the per-target delta and is the smaller cost than missing
// genuine peer activity. The bead's acceptance criteria don't
// require self-exclusion.
func TestThreadContext_CrossAgentDeltaVisibility(t *testing.T) {
	thread := "700.000001"
	var serverMu sync.Mutex
	replies := []slackThreadMessage{
		{User: "U_ALICE", Text: "should we ship?", TS: "700.000001"},
	}
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only count conversations.replies; the adapter's busy-reaction
		// path posts to /reactions.add and is out of scope here.
		if strings.HasSuffix(r.URL.Path, "/conversations.replies") {
			atomic.AddInt32(&calls, 1)
		}
		serverMu.Lock()
		resp := slackConversationsRepliesResp{OK: true, Messages: append([]slackThreadMessage(nil), replies...)}
		serverMu.Unlock()
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	prev := slackAPIBase
	slackAPIBase = srv.URL
	t.Cleanup(func() { slackAPIBase = prev })

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := config{
		gcAPIBase:               gcStub.URL,
		cityName:                "test-city",
		provider:                "slack",
		accountID:               "T1",
		handlePrefix:            "@",
		slackBotToken:           "xoxb-fake",
		slackThreadContextLimit: 20,
		threadContextCache:      newThreadContextCache(),
		dispatchSem:             defaultTestDispatchSem,
	}
	aliasReg := newTestHandleAliasRegistry(t)

	mk := func(ts, text string) []byte {
		raw, _ := json.Marshal(slackMessageEvent{
			Type: "message", Channel: "C1", User: "U_ALICE",
			TS: ts, ThreadTS: thread, Text: text,
		})
		return raw
	}

	// Step 1: mayor mentioned. Should see U_ALICE's prior only.
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: mk("700.000002", "@mayor weigh in?")}, func() {})

	// Step 2: a peer human posts a reply. Slack returns it on
	// subsequent fetches.
	serverMu.Lock()
	replies = append(replies, slackThreadMessage{User: "U_PEER", Text: "I think yes, with caveats X and Y", TS: "700.000003"})
	serverMu.Unlock()

	// Step 3: PL mentioned. PL has no cache entry yet, so PL sees
	// EVERYTHING posted before this inbound: U_ALICE's prior AND
	// the U_PEER reply. This is the cross-agent visibility payoff.
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: mk("700.000004", "@PL your read?")}, func() {})

	// Step 4: another peer reply lands.
	serverMu.Lock()
	replies = append(replies, slackThreadMessage{User: "U_PEER2", Text: "agree; suggest staging first", TS: "700.000005"})
	serverMu.Unlock()

	// Step 5: mayor mentioned AGAIN. Mayor's cached last-delivered
	// ts is "700.000002" (set at step 1). The delta filter
	// includes only messages with ts > 700.000002 AND ts < current
	// (700.000006): U_PEER@700.000003 and U_PEER2@700.000005.
	// U_ALICE@700.000001 is filtered out — already delivered to
	// mayor at step 1.
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: mk("700.000006", "@mayor counter?")}, func() {})

	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("conversations.replies calls = %d, want 3 (one per inbound in thread)", got)
	}
	msgs := capture.snapshot()
	if len(msgs) != 3 {
		t.Fatalf("captured %d inbound messages, want 3", len(msgs))
	}

	// Step 1: mayor sees U_ALICE only.
	if !strings.Contains(msgs[0].Text, "Thread context (1 earlier message):") {
		t.Errorf("step 1 (mayor first) missing single-prior preamble; got %q", msgs[0].Text)
	}
	if !strings.Contains(msgs[0].Text, "@U_ALICE: should we ship?") {
		t.Errorf("step 1 missing U_ALICE prior; got %q", msgs[0].Text)
	}

	// Step 3: PL sees U_ALICE AND U_PEER (2 priors). This is the
	// cross-agent visibility payoff: PL gets context that landed
	// between mayor's mention and PL's mention.
	if !strings.Contains(msgs[1].Text, "Thread context (2 earlier messages):") {
		t.Errorf("step 3 (PL first) missing two-prior preamble; got %q", msgs[1].Text)
	}
	if !strings.Contains(msgs[1].Text, "@U_ALICE: should we ship?") {
		t.Errorf("step 3 missing U_ALICE prior; got %q", msgs[1].Text)
	}
	if !strings.Contains(msgs[1].Text, "@U_PEER: I think yes") {
		t.Errorf("step 3 missing peer's intervening reply; got %q", msgs[1].Text)
	}

	// Step 5: mayor sees the delta since step 1 — U_PEER and
	// U_PEER2. NOT U_ALICE (already delivered to mayor).
	if !strings.Contains(msgs[2].Text, "Thread context (2 earlier messages):") {
		t.Errorf("step 5 (mayor second) expected delta of 2 messages; got %q", msgs[2].Text)
	}
	if strings.Contains(msgs[2].Text, "@U_ALICE: should we ship?") {
		t.Errorf("step 5 carried U_ALICE prior again (cache should exclude already-delivered to mayor); got %q", msgs[2].Text)
	}
	if !strings.Contains(msgs[2].Text, "@U_PEER: I think yes") {
		t.Errorf("step 5 missing U_PEER reply (delta since mayor's last visit); got %q", msgs[2].Text)
	}
	if !strings.Contains(msgs[2].Text, "@U_PEER2: agree; suggest staging first") {
		t.Errorf("step 5 missing U_PEER2 reply (delta since mayor's last visit); got %q", msgs[2].Text)
	}
}

// TestThreadContext_IsolationAcrossThreads — the cache must not leak
// activity from thread A into thread B even when both are in the
// same channel, and not leak from channel C1 into channel C2 even
// when both have a thread_ts of the same value (rare in practice
// but easy to assert).
func TestThreadContext_IsolationAcrossThreads(t *testing.T) {
	prior := []slackThreadMessage{
		{User: "U_ALICE", Text: "thread-A-only", TS: "800.000001"},
	}
	slackStub, _ := fakeSlackRepliesServer(t, prior)
	withSlackAPIStub(t, slackStub)

	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := config{
		gcAPIBase:               gcStub.URL,
		cityName:                "test-city",
		provider:                "slack",
		accountID:               "T1",
		handlePrefix:            "@",
		slackBotToken:           "xoxb-fake",
		slackThreadContextLimit: 20,
		threadContextCache:      newThreadContextCache(),
		dispatchSem:             defaultTestDispatchSem,
	}

	// Mayor is mentioned in thread A → cache pulls the prior.
	threadA, _ := json.Marshal(slackMessageEvent{
		Type: "message", Channel: "C1", User: "U_BOB",
		TS: "800.000002", ThreadTS: "800.000001",
		Text: "@mayor in A",
	})
	// Mayor mentioned in thread B (different ts root, same channel)
	// — the cache key (target, channel, thread_ts) differs, so this
	// is a fresh first-mention and gets the same priors view.
	threadB, _ := json.Marshal(slackMessageEvent{
		Type: "message", Channel: "C1", User: "U_BOB",
		TS: "900.000002", ThreadTS: "900.000001",
		Text: "@mayor in B",
	})
	aliasReg := newTestHandleAliasRegistry(t)
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: threadA}, func() {})
	processSlackEvent(cfg, aliasReg, nil, nil, nil, nil, slackEventEnvelope{Type: "event_callback", Event: threadB}, func() {})

	msgs := capture.snapshot()
	if len(msgs) != 2 {
		t.Fatalf("captured %d, want 2", len(msgs))
	}
	// Thread B's preamble should not reference thread A's ts.
	// (The stub returns the same reply set for both, but since
	// they're treated as independent threads the second still
	// gets a fresh first-mention preamble. The isolation point is
	// that thread B's CACHE entry didn't pre-exist from thread A.)
	if !strings.Contains(msgs[1].Text, "Thread context") {
		t.Errorf("thread B should get its own first-mention preamble; got %q", msgs[1].Text)
	}
}

// Direct unit tests for the helpers without going through processSlackEvent.

func TestThreadContextCache_LastDeliveredRoundTrip(t *testing.T) {
	t.Parallel()
	c := newThreadContextCache()

	// Empty key returns "" — never delivered.
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "" {
		t.Errorf("initial lastDeliveredFor = %q, want \"\"", got)
	}
	c.markDelivered("mayor", "C1", "T1", "100.000001")
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000001" {
		t.Errorf("after first markDelivered = %q, want %q", got, "100.000001")
	}
	// Newer ts advances.
	c.markDelivered("mayor", "C1", "T1", "100.000005")
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000005" {
		t.Errorf("after newer markDelivered = %q, want %q", got, "100.000005")
	}
	// Older ts does not regress (race protection).
	c.markDelivered("mayor", "C1", "T1", "100.000003")
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000005" {
		t.Errorf("after older markDelivered = %q, want %q (no regression)", got, "100.000005")
	}

	// Per-target isolation: PL on the same thread is independent.
	if got := c.lastDeliveredFor("PL", "C1", "T1"); got != "" {
		t.Errorf("PL on same thread should start empty; got %q", got)
	}
	c.markDelivered("PL", "C1", "T1", "100.000004")
	if got := c.lastDeliveredFor("PL", "C1", "T1"); got != "100.000004" {
		t.Errorf("PL after mark = %q, want %q", got, "100.000004")
	}
	// Mayor's value unchanged by PL's mark.
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000005" {
		t.Errorf("mayor after PL mark = %q, want %q (per-target isolation)", got, "100.000005")
	}

	// Per-thread isolation.
	c.markDelivered("mayor", "C1", "T2", "200.0")
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000005" {
		t.Errorf("mayor T1 after marking T2 = %q, want %q (per-thread isolation)", got, "100.000005")
	}

	// Empty target — channel-bound default routing.
	c.markDelivered("", "C1", "T1", "100.000010")
	if got := c.lastDeliveredFor("", "C1", "T1"); got != "100.000010" {
		t.Errorf("empty-target mark/get = %q, want %q", got, "100.000010")
	}
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000005" {
		t.Errorf("mayor after empty-target mark = %q, want %q", got, "100.000005")
	}

	// Nil receiver is safe.
	var nilCache *threadContextCache
	nilCache.markDelivered("mayor", "C1", "T1", "999.0")
	if got := nilCache.lastDeliveredFor("mayor", "C1", "T1"); got != "" {
		t.Errorf("nil cache lastDeliveredFor = %q, want \"\"", got)
	}

	// Empty channel/thread short-circuits.
	if got := c.lastDeliveredFor("mayor", "", "T1"); got != "" {
		t.Errorf("empty channel = %q, want \"\"", got)
	}
	if got := c.lastDeliveredFor("mayor", "C1", ""); got != "" {
		t.Errorf("empty thread = %q, want \"\"", got)
	}
	c.markDelivered("mayor", "", "T1", "999.0")
	c.markDelivered("mayor", "C1", "", "999.0")
	c.markDelivered("mayor", "C1", "T1", "")
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000005" {
		t.Errorf("invalid markDelivered calls leaked into store; got %q want %q", got, "100.000005")
	}
}

func TestThreadContextCache_TTLEviction(t *testing.T) {
	t.Parallel()
	c := newThreadContextCache()
	clock := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return clock }

	c.markDelivered("mayor", "C1", "T1", "100.000001")
	c.markDelivered("PL", "C1", "T2", "100.000002")

	// Within TTL: both entries live; the read refreshes mayor's clock.
	clock = clock.Add(threadContextCacheTTL - time.Hour)
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000001" {
		t.Errorf("within TTL = %q, want %q", got, "100.000001")
	}

	// Past PL's TTL but inside mayor's refreshed window: PL ages out,
	// mayor survives because the read above re-touched it.
	clock = clock.Add(2 * time.Hour)
	if got := c.lastDeliveredFor("PL", "C1", "T2"); got != "" {
		t.Errorf("expired entry = %q, want \"\" (TTL eviction)", got)
	}
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000001" {
		t.Errorf("read-refreshed entry = %q, want %q (touch on read keeps live threads warm)", got, "100.000001")
	}

	// The expired entry was deleted, not just hidden.
	c.mu.Lock()
	_, stillThere := c.lastDelivered[threadCacheKey("PL", "C1", "T2")]
	c.mu.Unlock()
	if stillThere {
		t.Error("expired entry still present in map after read")
	}

	// A write to a fully-expired cache starts fresh.
	clock = clock.Add(2 * threadContextCacheTTL)
	c.markDelivered("mayor", "C1", "T1", "200.000001")
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "200.000001" {
		t.Errorf("after re-mark = %q, want %q", got, "200.000001")
	}
}

func TestThreadContextCache_CapEvictsOldestTouched(t *testing.T) {
	t.Parallel()
	c := newThreadContextCache()
	clock := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return clock }

	// Fill to cap; entry i is touched at base+i seconds, so T0 is the
	// oldest when the overflow insert arrives.
	for i := 0; i < threadContextCacheMaxEntries; i++ {
		clock = clock.Add(time.Second)
		c.markDelivered("mayor", "C1", fmt.Sprintf("T%d", i), "100.000001")
	}
	clock = clock.Add(time.Second)
	c.markDelivered("mayor", "C1", "T-overflow", "100.000001")

	c.mu.Lock()
	size := len(c.lastDelivered)
	c.mu.Unlock()
	if size != threadContextCacheMaxEntries {
		t.Errorf("cache size after overflow = %d, want %d (cap enforced)", size, threadContextCacheMaxEntries)
	}
	if got := c.lastDeliveredFor("mayor", "C1", "T0"); got != "" {
		t.Errorf("oldest entry = %q, want \"\" (evicted on overflow)", got)
	}
	if got := c.lastDeliveredFor("mayor", "C1", "T-overflow"); got != "100.000001" {
		t.Errorf("overflow entry = %q, want %q (newest survives)", got, "100.000001")
	}
	if got := c.lastDeliveredFor("mayor", "C1", "T1"); got != "100.000001" {
		t.Errorf("second-oldest entry = %q, want %q (only one eviction needed)", got, "100.000001")
	}
}

func TestThreadContextCache_MarkDeliveredConcurrent(t *testing.T) {
	t.Parallel()
	c := newThreadContextCache()
	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		ts := fmt.Sprintf("100.%06d", i)
		go func(ts string) {
			defer wg.Done()
			c.markDelivered("mayor", "C1", "T1", ts)
		}(ts)
	}
	wg.Wait()
	got := c.lastDeliveredFor("mayor", "C1", "T1")
	want := "100.000015" // largest of 0..15 in 6-digit form
	if got != want {
		t.Errorf("after concurrent marks lastDelivered = %q, want %q (highest must win)", got, want)
	}
}

func TestFormatThreadContextPreamble_FiltersAndFormats(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		replies  []slackThreadMessage
		current  string
		since    string // gc-px8.6 lower bound; "" = first delivery
		want     string
		wantNoOp bool
	}{
		// All ts strings are 17-char Slack format
		// "<10-digit-seconds>.<6-digit-microseconds>". Lexical order
		// matches numeric order at this fixed length, which is what
		// formatThreadContextPreamble's filter relies on.
		{
			name:     "no replies",
			replies:  nil,
			current:  "1700000100.000000",
			wantNoOp: true,
		},
		{
			name: "only current message",
			replies: []slackThreadMessage{
				{User: "U1", Text: "hi", TS: "1700000100.000000"},
			},
			current:  "1700000100.000000",
			wantNoOp: true,
		},
		{
			name: "only later replies after current",
			replies: []slackThreadMessage{
				{User: "U1", Text: "future", TS: "1700000200.000000"},
			},
			current:  "1700000100.000000",
			wantNoOp: true,
		},
		{
			name: "only bot replies",
			replies: []slackThreadMessage{
				{BotID: "B0", Text: "bot noise", TS: "1700000050.000000"},
			},
			current:  "1700000100.000000",
			wantNoOp: true,
		},
		{
			name: "only whitespace replies",
			replies: []slackThreadMessage{
				{User: "U1", Text: "   ", TS: "1700000050.000000"},
			},
			current:  "1700000100.000000",
			wantNoOp: true,
		},
		{
			name: "single prior",
			replies: []slackThreadMessage{
				{User: "U1", Text: "earlier", TS: "1700000050.000000"},
			},
			current: "1700000100.000000",
			want:    "Thread context (1 earlier message):\n@U1: earlier\n\n---\n\n",
		},
		{
			name: "two priors with newline collapse",
			replies: []slackThreadMessage{
				{User: "U1", Text: "line1\nline2", TS: "1700000050.000000"},
				{User: "U2", Text: "alright", TS: "1700000060.000000"},
			},
			current: "1700000100.000000",
			want:    "Thread context (2 earlier messages):\n@U1: line1 | line2\n@U2: alright\n\n---\n\n",
		},
		{
			name: "empty user falls back to ?",
			replies: []slackThreadMessage{
				{User: "", Text: "anon", TS: "1700000050.000000"},
			},
			current: "1700000100.000000",
			want:    "Thread context (1 earlier message):\n@?: anon\n\n---\n\n",
		},
		{
			// gc-px8.6: when sinceTS is set, messages at-or-before
			// it are filtered out — they were already delivered to
			// this target on a previous visit.
			name: "since filters out already-delivered priors",
			replies: []slackThreadMessage{
				{User: "U1", Text: "old", TS: "1700000050.000000"},
				{User: "U2", Text: "new", TS: "1700000080.000000"},
			},
			current: "1700000100.000000",
			since:   "1700000050.000000",
			want:    "Thread context (1 earlier message):\n@U2: new\n\n---\n\n",
		},
		{
			name: "since equals ts is treated as already-delivered (boundary)",
			replies: []slackThreadMessage{
				{User: "U1", Text: "boundary", TS: "1700000050.000000"},
			},
			current:  "1700000100.000000",
			since:    "1700000050.000000",
			wantNoOp: true,
		},
		{
			name: "since strictly greater than all priors yields nothing",
			replies: []slackThreadMessage{
				{User: "U1", Text: "early", TS: "1700000050.000000"},
			},
			current:  "1700000100.000000",
			since:    "1700000080.000000",
			wantNoOp: true,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatThreadContextPreamble(tc.replies, tc.current, tc.since)
			if tc.wantNoOp {
				if got != "" {
					t.Errorf("expected empty preamble, got %q", got)
				}
				return
			}
			if got != tc.want {
				t.Errorf("preamble:\ngot:  %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// TestFetchThreadReplies_QueryAndAuth — cannot t.Parallel because
// it overwrites the package-level slackAPIBase var.
func TestFetchThreadReplies_QueryAndAuth(t *testing.T) {
	var (
		mu            sync.Mutex
		capturedAuth  string
		capturedQuery string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		capturedAuth = r.Header.Get("Authorization")
		capturedQuery = r.URL.RawQuery
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(slackConversationsRepliesResp{OK: true})
	}))
	t.Cleanup(srv.Close)
	prev := slackAPIBase
	slackAPIBase = srv.URL
	t.Cleanup(func() { slackAPIBase = prev })

	ctx, cancel := withTimeout(t, 5*time.Second)
	defer cancel()
	if _, err := fetchThreadReplies(ctx, "xoxb-test", "C1", "1700000100.000000", 5); err != nil {
		t.Fatalf("fetchThreadReplies: %v", err)
	}
	mu.Lock()
	gotAuth, gotQuery := capturedAuth, capturedQuery
	mu.Unlock()
	if gotAuth != "Bearer xoxb-test" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer xoxb-test")
	}
	if !strings.Contains(gotQuery, "channel=C1") ||
		!strings.Contains(gotQuery, "ts=1700000100.000000") ||
		!strings.Contains(gotQuery, "limit=5") {
		t.Errorf("query string %q missing expected fields", gotQuery)
	}
}

func TestFetchThreadReplies_RejectsEmptyArgs(t *testing.T) {
	t.Parallel()
	ctx, cancel := withTimeout(t, time.Second)
	defer cancel()
	cases := []struct {
		name              string
		token, ch, thread string
	}{
		{"empty token", "", "C1", "T1"},
		{"empty channel", "xoxb", "", "T1"},
		{"empty thread", "xoxb", "C1", ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := fetchThreadReplies(ctx, tc.token, tc.ch, tc.thread, 5); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

// TestFetchThreadReplies_NotOK — cannot t.Parallel because it
// overwrites the package-level slackAPIBase var.
func TestFetchThreadReplies_NotOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(slackConversationsRepliesResp{OK: false, Error: "missing_scope"})
	}))
	t.Cleanup(srv.Close)
	prev := slackAPIBase
	slackAPIBase = srv.URL
	t.Cleanup(func() { slackAPIBase = prev })

	ctx, cancel := withTimeout(t, time.Second)
	defer cancel()
	_, err := fetchThreadReplies(ctx, "xoxb", "C1", "1700000100.000000", 5)
	if err == nil {
		t.Fatal("expected error on ok=false")
	}
	if !strings.Contains(err.Error(), "missing_scope") {
		t.Errorf("error %v missing slack error code", err)
	}
}
