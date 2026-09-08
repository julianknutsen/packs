package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Tests for the busy-reaction lifecycle (hq-xizo): a targeted inbound
// gets the BUSY_REACTION emoji (default "hourglass") added to the
// inbound Slack message and a pending mark recorded; the agent's
// reply publishing back into the same conversation/thread removes the
// reaction and consumes the mark. The lifecycle replaces the previous
// unconditional "eyes" reaction on targeted inbounds and is the
// channel-native stand-in for Slack Assistant-mode setStatus.

// recordedReaction is one captured reactions.add / reactions.remove
// call, decoded from the stub Slack server's request body.
type recordedReaction struct {
	op        string // "add" | "remove"
	channel   string
	name      string
	timestamp string
}

// reactionRecorder collects reaction calls hitting the stub Slack
// server. Each call is also pushed onto ch so tests can await the
// async reaction goroutines without polling.
type reactionRecorder struct {
	mu    sync.Mutex
	calls []recordedReaction
	ch    chan recordedReaction
}

func (r *reactionRecorder) record(rec recordedReaction) {
	r.mu.Lock()
	r.calls = append(r.calls, rec)
	r.mu.Unlock()
	select {
	case r.ch <- rec:
	default:
	}
}

// await blocks until one reaction call arrives or d elapses.
func (r *reactionRecorder) await(t *testing.T, d time.Duration) recordedReaction {
	t.Helper()
	select {
	case rec := <-r.ch:
		return rec
	case <-time.After(d):
		t.Fatalf("no reaction call reached the Slack stub within %v", d)
		return recordedReaction{}
	}
}

// assertNoCall asserts that no (further) reaction call arrives within d.
func (r *reactionRecorder) assertNoCall(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case rec := <-r.ch:
		t.Fatalf("unexpected reaction call: op=%s chan=%s name=%s ts=%s",
			rec.op, rec.channel, rec.name, rec.timestamp)
	case <-time.After(d):
		// expected: silence
	}
}

// newReactionRecordingSlackStub returns an httptest Slack API stub
// that records reactions.add / reactions.remove calls (answering
// ok:true), answers chat.postMessage with a fixed ts, and answers
// every other method with a bare ok:true. Callers pair it with
// withSlackAPIStub.
func newReactionRecordingSlackStub(t *testing.T) (*httptest.Server, *reactionRecorder) {
	t.Helper()
	rr := &reactionRecorder{ch: make(chan recordedReaction, 16)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/reactions.add"), strings.HasSuffix(r.URL.Path, "/reactions.remove"):
			var body slackReactionsAddReq
			_ = json.NewDecoder(r.Body).Decode(&body)
			op := "add"
			if strings.HasSuffix(r.URL.Path, "/reactions.remove") {
				op = "remove"
			}
			rr.record(recordedReaction{op: op, channel: body.Channel, name: body.Name, timestamp: body.Timestamp})
			_, _ = fmt.Fprint(w, `{"ok":true}`)
		case strings.HasSuffix(r.URL.Path, "/chat.postMessage"):
			_, _ = fmt.Fprint(w, `{"ok":true,"ts":"999.000001"}`)
		default:
			_, _ = fmt.Fprint(w, `{"ok":true}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, rr
}

// busyTestConfig builds the minimal cfg for processSlackEvent tests of
// the busy lifecycle, targeting the supplied gc stub.
func busyTestConfig(gcURL string) config {
	return config{
		gcAPIBase:     gcURL,
		cityName:      "test-city",
		provider:      "slack",
		accountID:     "T1",
		handlePrefix:  "@",
		slackBotToken: "xoxb-fake",
		dispatchSem:   defaultTestDispatchSem,
		busyReaction:  busyReactionDefault,
		busyMarks:     newBusyReactionRegistry(),
	}
}

// targetedInboundEnvelope builds an event_callback envelope for a
// channel message explicitly addressing @mayor.
func targetedInboundEnvelope(t *testing.T, channel, ts, threadTS string) slackEventEnvelope {
	t.Helper()
	rawMsg, err := json.Marshal(slackMessageEvent{
		Type:     "message",
		Channel:  channel,
		User:     "U_ALICE",
		TS:       ts,
		ThreadTS: threadTS,
		Text:     "@mayor please deploy the thing",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return slackEventEnvelope{Type: "event_callback", Event: rawMsg}
}

// publishBody builds a /publish request body targeting conversation
// with an optional thread ts.
func publishBody(conversationID, replyTo string) string {
	b, _ := json.Marshal(publishRequest{
		SessionID:        "gc-1",
		Conversation:     conversationRef{ConversationID: conversationID},
		Text:             "done — deployed",
		ReplyToMessageID: replyTo,
	})
	return string(b)
}

// (a) A targeted inbound adds exactly one busy reaction — the
// configured default "hourglass", NOT the legacy "eyes" — on the
// inbound ts, and records the pending mark under (channel, own-ts)
// for a channel-root message.
func TestBusyReaction_TargetedInboundAddsBusyAndRecordsMark(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	env := targetedInboundEnvelope(t, "C1", "100.000010", "")
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	got := reactions.await(t, 2*time.Second)
	if got.op != "add" {
		t.Errorf("reaction op = %q, want %q", got.op, "add")
	}
	if got.name != "hourglass" {
		t.Errorf("reaction name = %q, want %q (busy emoji replaces the legacy eyes)", got.name, "hourglass")
	}
	if got.channel != "C1" || got.timestamp != "100.000010" {
		t.Errorf("reaction target = (%s, %s), want (C1, 100.000010)", got.channel, got.timestamp)
	}
	// Exactly one reaction call: no second add, and in particular no
	// legacy "eyes".
	reactions.assertNoCall(t, 300*time.Millisecond)

	if ts, ok := cfg.busyMarks.pending("C1", "100.000010"); !ok || ts != "100.000010" {
		t.Errorf("busy mark = (%q, %v), want (100.000010, true)", ts, ok)
	}
	if msgs := capture.snapshot(); len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1", len(msgs))
	}
}

// A targeted inbound that is itself a thread reply records its mark
// under the thread's root ts (thread_ts), still reacting on the
// inbound message's own ts.
func TestBusyReaction_ThreadReplyInboundKeyedByThreadTS(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	env := targetedInboundEnvelope(t, "C1", "100.000020", "100.000001")
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	got := reactions.await(t, 2*time.Second)
	if got.op != "add" || got.timestamp != "100.000020" {
		t.Errorf("reaction = (%s on %s), want add on 100.000020", got.op, got.timestamp)
	}
	if ts, ok := cfg.busyMarks.pending("C1", "100.000001"); !ok || ts != "100.000020" {
		t.Errorf("busy mark under thread key = (%q, %v), want (100.000020, true)", ts, ok)
	}
}

// (b) A subsequent /publish into the same conversation+thread removes
// the busy emoji from the recorded message ts and consumes the
// registry entry. Covers both inbound shapes: a channel-root inbound
// (marked under its own ts) and a thread-reply inbound (marked under
// its thread_ts, reaction on its own ts).
func TestBusyReaction_PublishToSameThreadRemovesBusy(t *testing.T) {
	cases := []struct {
		name      string
		threadKey string // registry key = publish reply_to_message_id
		markedTS  string // ts the reaction sits on
	}{
		{name: "root inbound, reply threads under its ts", threadKey: "100.000010", markedTS: "100.000010"},
		{name: "thread-reply inbound, keyed by thread root", threadKey: "100.000001", markedTS: "100.000020"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slackStub, reactions := newReactionRecordingSlackStub(t)
			withSlackAPIStub(t, slackStub)

			marks := newBusyReactionRegistry()
			marks.mark("C1", tc.threadKey, tc.markedTS)
			marks.confirmAdd("C1", tc.markedTS) // simulate the landed reactions.add
			cfg := config{slackBotToken: "xoxb-fake", busyReaction: "hourglass", busyMarks: marks}

			req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(publishBody("C1", tc.threadKey)))
			rec := httptest.NewRecorder()
			handlePublish(cfg, nil, nil, newPublishDedupCache(publishDedupTTL))(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("publish status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
			}
			var receipt publishReceipt
			if err := json.Unmarshal(rec.Body.Bytes(), &receipt); err != nil || !receipt.Delivered {
				t.Fatalf("publish receipt delivered = %v (err=%v, body=%s)", receipt.Delivered, err, rec.Body.String())
			}

			got := reactions.await(t, 2*time.Second)
			if got.op != "remove" {
				t.Errorf("reaction op = %q, want %q", got.op, "remove")
			}
			if got.name != "hourglass" {
				t.Errorf("reaction name = %q, want %q", got.name, "hourglass")
			}
			if got.channel != "C1" || got.timestamp != tc.markedTS {
				t.Errorf("remove target = (%s, %s), want (C1, %s)", got.channel, got.timestamp, tc.markedTS)
			}
			if _, ok := marks.pending("C1", tc.threadKey); ok {
				t.Error("registry entry survived the publish; want consumed")
			}
		})
	}
}

// (c) A /publish that matches no pending mark — unrelated
// conversation, or unrelated thread in the same conversation, or no
// thread ts at all — fires no reactions.remove and leaves existing
// marks untouched.
func TestBusyReaction_UnrelatedPublishDoesNotRemove(t *testing.T) {
	for _, tc := range []struct {
		name         string
		conversation string
		replyTo      string
	}{
		{name: "different conversation", conversation: "C_OTHER", replyTo: "100.000010"},
		{name: "different thread", conversation: "C1", replyTo: "555.000001"},
		{name: "no thread ts", conversation: "C1", replyTo: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slackStub, reactions := newReactionRecordingSlackStub(t)
			withSlackAPIStub(t, slackStub)

			marks := newBusyReactionRegistry()
			marks.mark("C1", "100.000010", "100.000010")
			marks.confirmAdd("C1", "100.000010")
			cfg := config{slackBotToken: "xoxb-fake", busyReaction: "hourglass", busyMarks: marks}

			req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(publishBody(tc.conversation, tc.replyTo)))
			rec := httptest.NewRecorder()
			handlePublish(cfg, nil, nil, newPublishDedupCache(publishDedupTTL))(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("publish status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
			}

			reactions.assertNoCall(t, 300*time.Millisecond)
			if _, ok := marks.pending("C1", "100.000010"); !ok {
				t.Error("unrelated publish consumed the pending mark; want untouched")
			}
		})
	}
}

// (d) BUSY_REACTION= (set-but-empty) disables the lifecycle: the
// config loads as empty, a targeted inbound adds NO reaction (the
// legacy eyes included — the lifecycle replaced it) and records no
// mark, while the inbound still forwards to gc.
func TestBusyReaction_EmptyEnvDisablesLifecycle(t *testing.T) {
	// Config level: present-but-empty must NOT fall back to the
	// default. Requires the lookup entry point — a plain getenv cannot
	// express "set but empty".
	env := baseSlackEnv()
	env["BUSY_REACTION"] = ""
	cfg0, err := loadConfigFromLookup(func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	})
	if err != nil {
		t.Fatalf("loadConfigFromLookup: %v", err)
	}
	if cfg0.busyReaction != "" {
		t.Fatalf("busyReaction = %q with BUSY_REACTION= set-but-empty, want disabled (empty)", cfg0.busyReaction)
	}

	// Behavior level: no reaction of any kind, no mark, inbound still
	// forwarded.
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	cfg.busyReaction = ""
	env2 := targetedInboundEnvelope(t, "C1", "100.000010", "")
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env2, func() {})

	reactions.assertNoCall(t, 400*time.Millisecond)
	if n := cfg.busyMarks.size(); n != 0 {
		t.Errorf("registry has %d entries with the lifecycle disabled, want 0", n)
	}
	if msgs := capture.snapshot(); len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1 (disable must not drop the forward)", len(msgs))
	}
}

// BUSY_REACTION config defaults and overrides through the standard
// env entry points.
func TestBusyReaction_ConfigDefaultsAndOverrides(t *testing.T) {
	cases := []struct {
		name  string
		set   bool
		value string
		want  string
	}{
		{name: "unset defaults to hourglass", set: false, want: "hourglass"},
		{name: "custom emoji honored", set: true, value: "hourglass_flowing_sand", want: "hourglass_flowing_sand"},
		{name: "surrounding colons stripped", set: true, value: ":hourglass_flowing_sand:", want: "hourglass_flowing_sand"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseSlackEnv()
			if tc.set {
				env["BUSY_REACTION"] = tc.value
			}
			cfg, err := loadConfigFromEnv(stubEnv(env))
			if err != nil {
				t.Fatalf("loadConfigFromEnv: %v", err)
			}
			if cfg.busyReaction != tc.want {
				t.Errorf("busyReaction = %q, want %q", cfg.busyReaction, tc.want)
			}
		})
	}
}

// (e) A custom BUSY_REACTION emoji is used on both sides of the
// lifecycle: the add on dispatch and the remove on reply.
func TestBusyReaction_CustomEmojiHonored(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	cfg.busyReaction = "hourglass_flowing_sand"
	env := targetedInboundEnvelope(t, "C1", "100.000010", "")
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	added := reactions.await(t, 2*time.Second)
	if added.op != "add" || added.name != "hourglass_flowing_sand" {
		t.Errorf("add = (%s, %s), want (add, hourglass_flowing_sand)", added.op, added.name)
	}

	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(publishBody("C1", "100.000010")))
	rec := httptest.NewRecorder()
	handlePublish(cfg, nil, nil, newPublishDedupCache(publishDedupTTL))(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	removed := reactions.await(t, 2*time.Second)
	if removed.op != "remove" || removed.name != "hourglass_flowing_sand" || removed.timestamp != "100.000010" {
		t.Errorf("remove = (%s, %s on %s), want (remove, hourglass_flowing_sand on 100.000010)",
			removed.op, removed.name, removed.timestamp)
	}
}

// (g) A mark past busyReactionTTL is dead: the publish consumes the
// stale entry but fires no reactions.remove.
func TestBusyReaction_ExpiredMarkDoesNotRemove(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)

	var mu sync.Mutex
	current := time.Now()
	marks := newBusyReactionRegistry()
	marks.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	marks.mark("C1", "100.000010", "100.000010")
	marks.confirmAdd("C1", "100.000010")
	mu.Lock()
	current = current.Add(busyReactionTTL + time.Minute)
	mu.Unlock()

	cfg := config{slackBotToken: "xoxb-fake", busyReaction: "hourglass", busyMarks: marks}
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(publishBody("C1", "100.000010")))
	rec := httptest.NewRecorder()
	handlePublish(cfg, nil, nil, newPublishDedupCache(publishDedupTTL))(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	var receipt publishReceipt
	if err := json.Unmarshal(rec.Body.Bytes(), &receipt); err != nil || !receipt.Delivered {
		t.Fatalf("publish receipt delivered = %v (err=%v)", receipt.Delivered, err)
	}

	reactions.assertNoCall(t, 300*time.Millisecond)
	if n := marks.size(); n != 0 {
		t.Errorf("registry has %d entries after expired-take, want 0 (stale entry dropped)", n)
	}
}

// Registry semantics: take consumes, re-take misses, expired entries
// are swept opportunistically on mark.
func TestBusyReactionRegistry_TakeAndSweep(t *testing.T) {
	var mu sync.Mutex
	current := time.Now()
	r := newBusyReactionRegistry()
	r.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}

	r.mark("C1", "1.0", "1.0")
	r.confirmAdd("C1", "1.0")
	if ts, ok := r.take("C1", "1.0"); !ok || ts != "1.0" {
		t.Fatalf("take = (%q, %v), want (1.0, true)", ts, ok)
	}
	if _, ok := r.take("C1", "1.0"); ok {
		t.Fatal("second take succeeded; want consumed on first take")
	}

	// Opportunistic sweep: an expired entry disappears on the next mark.
	r.mark("C1", "2.0", "2.0")
	mu.Lock()
	current = current.Add(busyReactionTTL + time.Minute)
	mu.Unlock()
	r.mark("C1", "3.0", "3.0")
	if n := r.size(); n != 1 {
		t.Errorf("size after sweep = %d, want 1 (expired 2.0 swept, fresh 3.0 kept)", n)
	}
	if _, ok := r.pending("C1", "2.0"); ok {
		t.Error("expired entry 2.0 survived the sweep")
	}
}

// Registry semantics: the size cap evicts the oldest surviving mark
// (mirroring the dmGateMaxEntries pattern), keeping the map bounded.
func TestBusyReactionRegistry_CapEvictsOldest(t *testing.T) {
	var mu sync.Mutex
	current := time.Now()
	r := newBusyReactionRegistry()
	r.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}

	for i := 0; i <= busyReactionMaxEntries; i++ {
		mu.Lock()
		current = current.Add(time.Millisecond)
		mu.Unlock()
		key := fmt.Sprintf("%d.0", i)
		r.mark("C1", key, key)
	}
	if n := r.size(); n != busyReactionMaxEntries {
		t.Errorf("size = %d, want cap %d", n, busyReactionMaxEntries)
	}
	if _, ok := r.pending("C1", "0.0"); ok {
		t.Error("oldest entry survived cap eviction; want evicted")
	}
	last := fmt.Sprintf("%d.0", busyReactionMaxEntries)
	if _, ok := r.pending("C1", last); !ok {
		t.Error("newest entry missing after cap eviction; want kept")
	}
}

// A nil registry is inert on every method — the lifecycle degrades to
// "no busy affordance" instead of panicking (old tests construct cfg
// without busyMarks).
func TestBusyReactionRegistry_NilSafe(t *testing.T) {
	var r *busyReactionRegistry
	if displaced, admitted := r.mark("C1", "1.0", "1.0"); displaced != nil || admitted {
		t.Error("mark on nil registry reported displaced marks or admission")
	}
	if r.confirmAdd("C1", "1.0") {
		t.Error("confirmAdd on nil registry reported a deferred removal")
	}
	r.abandonAdd("C1", "1.0")
	if _, ok := r.take("C1", "1.0"); ok {
		t.Error("take on nil registry reported a mark")
	}
	if _, ok := r.pending("C1", "1.0"); ok {
		t.Error("pending on nil registry reported a mark")
	}
	if n := r.size(); n != 0 {
		t.Errorf("size on nil registry = %d, want 0", n)
	}
}

// Fast-reply race (hq-xizo P2): the reply's take() arrives while the
// async reactions.add is still in flight. take must NOT hand the ts
// to the caller (its remove would no-op before the add lands and
// strand the emoji); instead the add's completer learns via
// confirmAdd that it owes the removal.
func TestBusyReactionRegistry_TakeDuringInflightAddDefersToCompleter(t *testing.T) {
	r := newBusyReactionRegistry()
	r.mark("C1", "1.0", "1.0")
	if ts, ok := r.take("C1", "1.0"); ok {
		t.Fatalf("take during in-flight add returned (%q, true); want deferred", ts)
	}
	if !r.confirmAdd("C1", "1.0") {
		t.Fatal("confirmAdd = false after deferred take; want removeNow=true")
	}
	// The deferral consumed the mark: nothing left to take.
	if _, ok := r.take("C1", "1.0"); ok {
		t.Fatal("mark survived the deferred take")
	}
}

// A thread-reply inbound registers under BOTH the thread root and its
// own ts, so a reply publish carrying either key clears the reaction
// (hq-xizo P2: reply shapes differ between gc reply-current and
// thread-root threading).
func TestBusyReactionRegistry_SiblingKeysBothClear(t *testing.T) {
	for _, key := range []string{"root.1", "msg.2"} {
		r := newBusyReactionRegistry()
		r.mark("C1", "root.1", "msg.2")
		r.confirmAdd("C1", "msg.2")
		ts, ok := r.take("C1", key)
		if !ok || ts != "msg.2" {
			t.Fatalf("take(%q) = (%q, %v), want (msg.2, true)", key, ts, ok)
		}
		// Consuming either key retires the sibling too.
		other := "msg.2"
		if key == "msg.2" {
			other = "root.1"
		}
		if _, ok := r.take("C1", other); ok {
			t.Fatalf("sibling key %q survived take(%q)", other, key)
		}
	}
}

// Re-marking the same thread with a new targeted message must not
// strand the displaced mark's emoji (hq-xizo P2). Landed add: the old
// ts comes back from mark() for the caller to remove. In-flight add:
// removal is delegated to that add's completer.
func TestBusyReactionRegistry_RemarkDisplacesOldReaction(t *testing.T) {
	t.Run("landed add returns displaced ts", func(t *testing.T) {
		r := newBusyReactionRegistry()
		r.mark("C1", "root.1", "msg.1")
		r.confirmAdd("C1", "msg.1")
		displaced, admitted := r.mark("C1", "root.1", "msg.2")
		if len(displaced) != 1 || displaced[0] != "msg.1" || !admitted {
			t.Fatalf("mark = (%v, %v), want ([msg.1], true)", displaced, admitted)
		}
		// The new mark is live for the eventual reply.
		r.confirmAdd("C1", "msg.2")
		if ts, ok := r.take("C1", "root.1"); !ok || ts != "msg.2" {
			t.Fatalf("take = (%q, %v), want (msg.2, true)", ts, ok)
		}
	})
	t.Run("in-flight add delegates removal to completer", func(t *testing.T) {
		r := newBusyReactionRegistry()
		r.mark("C1", "root.1", "msg.1")
		displaced, admitted := r.mark("C1", "root.1", "msg.2")
		if len(displaced) != 0 || !admitted {
			t.Fatalf("mark = (%v, %v), want ([], true) (add still in flight)", displaced, admitted)
		}
		if !r.confirmAdd("C1", "msg.1") {
			t.Fatal("confirmAdd(msg.1) = false; want removeNow=true for the displaced in-flight add")
		}
	})
}

// abandonAdd after a failed reactions.add leaves the surviving
// entries inert: a later take returns the ts (the remove will no-op
// benignly) instead of deferring to a completer that will never run.
func TestBusyReactionRegistry_AbandonAddLeavesTakeUsable(t *testing.T) {
	r := newBusyReactionRegistry()
	r.mark("C1", "1.0", "1.0")
	r.abandonAdd("C1", "1.0")
	if ts, ok := r.take("C1", "1.0"); !ok || ts != "1.0" {
		t.Fatalf("take after abandonAdd = (%q, %v), want (1.0, true)", ts, ok)
	}
}

// codex round 2: a Slack redelivery of an event whose reply already
// consumed the busy mark must not re-add the emoji — gc dedups the
// inbound, so no second reply would ever clear it.
func TestBusyReaction_RedeliveryAfterClearAddsNothing(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	env := targetedInboundEnvelope(t, "C1", "100.000010", "")
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})
	if got := reactions.await(t, 2*time.Second); got.op != "add" {
		t.Fatalf("first delivery: op = %q, want add", got.op)
	}

	// The agent's reply consumes the mark.
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(publishBody("C1", "100.000010")))
	rec := httptest.NewRecorder()
	handlePublish(cfg, nil, nil, newPublishDedupCache(publishDedupTTL))(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("publish status = %d", rec.Code)
	}
	if got := reactions.await(t, 2*time.Second); got.op != "remove" {
		t.Fatalf("reply: op = %q, want remove", got.op)
	}

	// Redelivery of the same event: no re-mark, no reactions.add.
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})
	reactions.assertNoCall(t, 400*time.Millisecond)
	if n := cfg.busyMarks.size(); n != 0 {
		t.Errorf("registry has %d entries after redelivery, want 0", n)
	}
}

// codex round 2: takeExact consumes only entries still pointing at
// the exact message — the alias-failure cleanup must never strip a
// newer re-target's affordance.
func TestBusyReactionRegistry_TakeExact(t *testing.T) {
	r := newBusyReactionRegistry()
	r.mark("C1", "root.1", "msg.1")
	r.confirmAdd("C1", "msg.1")
	if !r.takeExact("C1", "root.1", "msg.1") {
		t.Fatal("takeExact = false for a landed mark; want removeNow=true")
	}
	if _, ok := r.pending("C1", "root.1"); ok {
		t.Fatal("entry survived takeExact")
	}

	// Newer re-target owns the keys: takeExact for the OLD message
	// must not touch it.
	r.mark("C1", "root.1", "msg.2")
	r.confirmAdd("C1", "msg.2")
	if r.takeExact("C1", "root.1", "msg.1") {
		t.Fatal("takeExact consumed a newer re-target's mark")
	}
	if ts, ok := r.pending("C1", "root.1"); !ok || ts != "msg.2" {
		t.Fatalf("newer mark = (%q, %v), want (msg.2, true)", ts, ok)
	}

	// In-flight add: removal defers to the completer.
	r2 := newBusyReactionRegistry()
	r2.mark("C1", "root.1", "msg.1")
	if r2.takeExact("C1", "root.1", "msg.1") {
		t.Fatal("takeExact = true during in-flight add; want deferred")
	}
	if !r2.confirmAdd("C1", "msg.1") {
		t.Fatal("confirmAdd = false after deferred takeExact; want removeNow=true")
	}
}
