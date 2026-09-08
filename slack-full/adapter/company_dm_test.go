package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// company_dm_test.go — Slack company-rooms Phase 4 (per-agent DM) coverage:
// the app-bound verification order, DM admission (self-echo, dedup), routing
// (allowed-human policy, unbound → failed_dm_unbound + redrive), the advisory
// session guard, receipt-store-failure 503, owner-token custody (missing token
// degrades; switchboard token never touches a DM channel), and the golden
// fixtures. Maps to acceptance rule 12 / spec §Test plan items 1-4, 6, 9-12.

const (
	testDMChannel = "D0OLLIEDM"
	ollieAppID    = "A0AAAAAA1"
	rileyAppID    = "A0AAAAAA2"
	switchAppID   = "A0SWITCH"
)

// ---- DM harness ------------------------------------------------------------

type dmSpy struct {
	mu            sync.Mutex
	hydrateTokens []string
	reactTokens   []string
	replyTokens   []string
}

func (s *dmSpy) tokens() (hydrate, react, reply []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.hydrateTokens...),
		append([]string(nil), s.reactTokens...),
		append([]string(nil), s.replyTokens...)
}

func loadAgentAppsStore(t *testing.T, h *companyHarness, f agentAppsFile) *agentAppsStore {
	t.Helper()
	p := filepath.Join(t.TempDir(), "agent_apps.json")
	if err := os.WriteFile(p, marshalAgentApps(t, f), 0o600); err != nil {
		t.Fatalf("write agent apps: %v", err)
	}
	s := &agentAppsStore{}
	_ = s.Load(p, h.dirStore.Snapshot())
	return s
}

func loadDMBindingsStore(t *testing.T, h *companyHarness, f dmBindingsFile) *dmBindingsStore {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dm_bindings.json")
	if err := os.WriteFile(p, marshalDMBindings(t, f), 0o600); err != nil {
		t.Fatalf("write dm bindings: %v", err)
	}
	s := &dmBindingsStore{}
	_ = s.Load(p, h.dirStore.Snapshot())
	return s
}

// setupDM builds a DM-ready harness: ollie + riley registered agent apps, an
// ollie→ollie DM binding (riley intentionally unbound), an ollie owner token
// present in a 0700 secrets dir (riley's token absent), and recording token
// hooks. The switchboard token is a sentinel so a test can assert it never
// touches a DM channel.
func setupDM(t *testing.T) (*companyHarness, *fakeGC, *dmSpy) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 8)
	setFixedClock(h)
	h.gw.slackToken = "xoxb-SWITCHBOARD"
	h.gw.agentApps = loadAgentAppsStore(t, h, baseAgentAppsFile())
	h.gw.dmBindStore = loadDMBindingsStore(t, h, dmBindingsFile{
		SchemaVersion: 1,
		DMBindings:    []DMBinding{{Agent: "ollie", Session: "ollie"}},
	})
	h.gw.secretsDir = writeTokenFile(t, "ollie", "xoxb-ollie", 0o700, 0o600)

	spy := &dmSpy{}
	// DM membership gate (codex round 3): default the probe to "member"
	// so existing DM tests exercise their own concern without touching a
	// real conversations.info endpoint; membership-gate tests override.
	h.gw.mpimMemberProbe = func(token, channel string) mpimProbeOutcome {
		return mpimProbeMember
	}
	h.gw.hydrateDM = func(token string, msg CompanyMessage) companyHydration {
		spy.mu.Lock()
		spy.hydrateTokens = append(spy.hydrateTokens, token)
		spy.mu.Unlock()
		return companyHydration{RootProvenance: companyRootProvenanceVerified, ContextStatus: companyContextAvailable}
	}
	h.gw.reactHookTok = func(token, method, channel, ts, name string) ackOutcome {
		spy.mu.Lock()
		spy.reactTokens = append(spy.reactTokens, token)
		spy.mu.Unlock()
		return ackSuccess
	}
	h.gw.replyHookTok = func(token, channel, threadTS, text string) bool {
		spy.mu.Lock()
		spy.replyTokens = append(spy.replyTokens, token)
		spy.mu.Unlock()
		return true
	}
	h.openBarrier()
	return h, gc, spy
}

func dmMessage(user, ts, text string) slackMessageEvent {
	return slackMessageEvent{User: user, Channel: testDMChannel, TS: ts, Text: text, ChannelType: "im"}
}

func admitDMViaHandler(t *testing.T, h *companyHarness, ev slackMessageEvent, apiAppID string, retryNum int) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	ev.Type = "message"
	ev.ChannelType = "im"
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal dm event: %v", err)
	}
	env := slackEventEnvelope{Type: "event_callback", TeamID: testTeamID, APIAppID: apiAppID, EventID: "Ev-" + ev.TS, Event: raw}
	req := httptest.NewRequest(http.MethodPost, "/slack/events", nil)
	if retryNum > 0 {
		req.Header.Set("X-Slack-Retry-Num", strconv.Itoa(retryNum))
		req.Header.Set("X-Slack-Retry-Reason", "http_timeout")
	}
	w := httptest.NewRecorder()
	handled := h.gw.tryHandleEvent(w, req, env, h.gw.agentAppsSnapshot())
	return w, handled
}

func dmOrigin(ts string) ReceiptOrigin {
	return ReceiptOrigin{TeamID: testTeamID, ChannelID: testDMChannel, TS: ts}
}

func getReceipt(t *testing.T, h *companyHarness, ts string) *IngressReceipt {
	t.Helper()
	r, err := h.gw.store().Get(dmOrigin(ts))
	if err != nil {
		t.Fatalf("get receipt: %v", err)
	}
	return r
}

// ---- verification order (items 1, 2) ---------------------------------------

func dmVerifyCfg(t *testing.T) config {
	t.Helper()
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	h := newCompanyHarness(t, gc.server.URL, &df, nil, 4)
	h.gw.agentApps = loadAgentAppsStore(t, h, agentAppsFile{
		SchemaVersion: 1,
		AgentApps: []AgentApp{
			{TeamID: testTeamID, APIAppID: ollieAppID, SigningSecret: "ollie-secret"},
			{TeamID: testTeamID, APIAppID: rileyAppID, SigningSecret: "riley-secret"},
		},
	})
	return config{slackSigningKey: "env-secret", companyGateway: h.gw}
}

func signedBody(t *testing.T, secret string, obj any) (body []byte, ts, sig string) {
	t.Helper()
	body, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	ts = strconv.FormatInt(time.Now().Unix(), 10)
	sig = signFor(secret, ts, body)
	return body, ts, sig
}

func TestDMVerificationOrder(t *testing.T) {
	eventBody := map[string]any{
		"type":       "event_callback",
		"api_app_id": ollieAppID,
		"team_id":    testTeamID,
		"event":      map[string]any{"type": "message", "channel_type": "im", "channel": testDMChannel},
	}
	urlBody := map[string]any{"type": "url_verification", "challenge": "c0"}

	cases := []struct {
		name       string
		obj        any
		secret     string
		want       bool
		wantReject bool // dmSigRejects must increment
	}{
		{"url_verification env secret", urlBody, "env-secret", true, false},
		{"url_verification registered secret", urlBody, "ollie-secret", true, false},
		{"url_verification unknown secret", urlBody, "nope", false, false},
		{"registered app own secret", eventBody, "ollie-secret", true, false},
		{"registered app cross-app spoof", eventBody, "riley-secret", false, true},
		{"registered app env secret rejected", eventBody, "env-secret", false, true},
		{"unknown app legacy env secret", withApp(eventBody, switchAppID), "env-secret", true, false},
		{"unknown app legacy registered secret rejected", withApp(eventBody, switchAppID), "ollie-secret", false, true},
		{"unknown app bogus secret", withApp(eventBody, switchAppID), "bogus", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := dmVerifyCfg(t)
			before := cfg.companyGateway.dmSigRejects.Load()
			body, ts, sig := signedBody(t, tc.secret, tc.obj)
			got := verifyInboundEvent(cfg, cfg.companyGateway.agentAppsSnapshot(), parseEventHead(body), body, ts, sig)
			if got != tc.want {
				t.Fatalf("verifyInboundEvent = %v, want %v", got, tc.want)
			}
			after := cfg.companyGateway.dmSigRejects.Load()
			if (after > before) != tc.wantReject {
				t.Errorf("dmSigRejects delta=%d, wantReject=%v", after-before, tc.wantReject)
			}
		})
	}
}

// TestDMVerificationSwitchboardAppIDPinsEnvSecret pins spec verification rule 2:
// an event_callback whose api_app_id == SLACK_APP_ID verifies against the env
// secret ONLY, and that pin takes precedence over a registered agent record
// sharing the same api_app_id (rule 2 before rule 3). A rule-2 mismatch is the
// plain rooms 401, never a company_dm_sig_reject.
func TestDMVerificationSwitchboardAppIDPinsEnvSecret(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	h := newCompanyHarness(t, gc.server.URL, &df, nil, 4)
	// A registered agent record COLLIDING with the switchboard's api_app_id,
	// carrying a DIFFERENT secret: rule 2 must never fall through to it.
	h.gw.agentApps = loadAgentAppsStore(t, h, agentAppsFile{
		SchemaVersion: 1,
		AgentApps: []AgentApp{
			{TeamID: testTeamID, APIAppID: switchAppID, SigningSecret: "switch-registered-secret"},
			{TeamID: testTeamID, APIAppID: ollieAppID, SigningSecret: "ollie-secret"},
		},
	})
	cfg := config{slackSigningKey: "env-secret", slackAppID: switchAppID, companyGateway: h.gw}

	eventBody := withApp(map[string]any{
		"type":    "event_callback",
		"team_id": testTeamID,
		"event":   map[string]any{"type": "message", "channel_type": "im", "channel": testDMChannel},
	}, switchAppID)

	cases := []struct {
		name       string
		secret     string
		want       bool
		wantReject bool
	}{
		{"switchboard env secret accepted", "env-secret", true, false},
		{"registered-secret shadow rejected (rule 2 precedence)", "switch-registered-secret", false, false},
		{"bogus secret rejected", "bogus", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := cfg.companyGateway.dmSigRejects.Load()
			body, ts, sig := signedBody(t, tc.secret, eventBody)
			got := verifyInboundEvent(cfg, cfg.companyGateway.agentAppsSnapshot(), parseEventHead(body), body, ts, sig)
			if got != tc.want {
				t.Fatalf("verifyInboundEvent = %v, want %v", got, tc.want)
			}
			if delta := cfg.companyGateway.dmSigRejects.Load() - before; (delta > 0) != tc.wantReject {
				t.Errorf("dmSigRejects delta=%d, wantReject=%v", delta, tc.wantReject)
			}
		})
	}
}

func withApp(base map[string]any, appID string) map[string]any {
	out := make(map[string]any, len(base))
	for k, v := range base {
		out[k] = v
	}
	out["api_app_id"] = appID
	return out
}

// ---- admission (items 3, 4) ------------------------------------------------

func TestDMHumanAdmittedOnceRedeliveryAbsorbed(t *testing.T) {
	h, gc, _ := setupDM(t)
	ts := "1700000000.000700"
	ev := dmMessage("U0HUMAN01", ts, "hey ollie")

	w, handled := admitDMViaHandler(t, h, ev, ollieAppID, 0)
	if !handled || w.Code != http.StatusOK {
		t.Fatalf("first admit: handled=%v code=%d", handled, w.Code)
	}
	h.wait()

	if r := getReceipt(t, h, ts); r == nil || r.Kind != receiptKindDM || r.OwnerAppID != ollieAppID {
		t.Fatalf("receipt = %+v, want dm/owner", r)
	}
	if got := len(gc.sessionCalls()); got != 1 {
		t.Fatalf("session calls = %d, want 1", got)
	}

	// Redelivery (x-slack-retry-num >= 1) of the same origin: 200, no second
	// receipt, no second delivery.
	w, handled = admitDMViaHandler(t, h, ev, ollieAppID, 1)
	if !handled || w.Code != http.StatusOK {
		t.Fatalf("redelivery: handled=%v code=%d", handled, w.Code)
	}
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("session calls after redelivery = %d, want still 1", got)
	}
}

func TestDMHappyPathDeliversWithOwnerToken(t *testing.T) {
	h, gc, spy := setupDM(t)
	ts := "1700000000.000710"
	h.gw.visibleAcks = true // exercise the owner-token ack actor too

	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "check deploy"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 || !strings.Contains(calls[0].path, "/session/ollie/messages") {
		t.Fatalf("session calls = %+v, want one to ollie", calls)
	}
	if !strings.Contains(calls[0].body, "direct message to agent \"ollie\"") {
		t.Errorf("reminder body missing DM framing: %q", calls[0].body)
	}
	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	// DM pointer written to dm/<session>.json with kind dm + owner_app_id.
	p := readDMPointer(t, h, "ollie")
	if p.Kind != "dm" || p.OwnerAppID != ollieAppID || p.Room != "" {
		t.Errorf("dm pointer = %+v, want kind dm / owner / empty room", p)
	}

	// Owner-token custody: hydration + acks used ollie's token, NEVER the
	// switchboard sentinel.
	hydrate, react, _ := spy.tokens()
	for _, tok := range append(append([]string{}, hydrate...), react...) {
		if tok == "xoxb-SWITCHBOARD" {
			t.Fatalf("switchboard token used on a DM channel: %v", tok)
		}
		if tok != "xoxb-ollie" {
			t.Errorf("unexpected DM actor token %q, want xoxb-ollie", tok)
		}
	}
	if len(hydrate) == 0 {
		t.Error("hydrateDM never called with the owner token")
	}
}

func TestDMSelfEchoTerminalNoDelivery(t *testing.T) {
	h, gc, _ := setupDM(t)
	ts := "1700000000.000720"
	// A bot-authored self-echo: the owner app's own post echoed back into the im.
	ev := dmMessage("U0AAAAAA1", ts, "on it")
	ev.BotID = "B0AAAAAA1"
	ev.AppID = ollieAppID
	ev.Metadata = json.RawMessage(`{"event_type":"gc_dm_reply","event_payload":{"nonce":"gcs-1"}}`)

	if _, handled := admitDMViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("self-echo not handled")
	}
	h.wait()

	r := getReceipt(t, h, ts)
	if r == nil {
		t.Fatal("self-echo produced no receipt (needed as dedup/reconciliation memory)")
	}
	if r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonDMSelfEcho {
		t.Fatalf("status/reason = %q/%q, want no_delivery/dm_self_echo", r.Status, r.Reason)
	}
	if len(r.Targets) != 0 {
		t.Errorf("self-echo recorded targets %+v, want none", r.Targets)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("self-echo woke %d sessions, want 0", got)
	}
	// The stored event keeps its metadata (the reconciler scans receipts). It
	// now lives in the body sidecar; read it through the accessor.
	if !strings.Contains(string(h.gw.store().receiptBody(r)), "gc_dm_reply") {
		t.Error("self-echo receipt dropped its metadata")
	}
}

// TestDMSelfEchoMissingTokenCountsAckDegrade pins m1: a DM receipt with no
// pending bound target (a self-echo) whose owner token is missing degrades its
// visible ack — and that degradation is COUNTED on company_dm_token_missing
// (spec §Acks), where before it was a silent no-op because the hydration path
// (the only counter site) never runs for a no-pending-target receipt.
func TestDMSelfEchoMissingTokenCountsAckDegrade(t *testing.T) {
	h, gc, spy := setupDM(t)
	h.gw.visibleAcks = true
	if err := os.Remove(filepath.Join(h.gw.secretsDir, "bot-token-ollie.txt")); err != nil {
		t.Fatalf("remove token: %v", err)
	}
	before := h.gw.dmTokenMissing.Load()

	ts := "1700000000.000785"
	ev := dmMessage("U0AAAAAA1", ts, "on it")
	ev.BotID = "B0AAAAAA1"
	ev.AppID = ollieAppID
	ev.Metadata = json.RawMessage(`{"event_type":"gc_dm_reply","event_payload":{"v":1,"nonce":"gcs-1"}}`)
	if _, handled := admitDMViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("self-echo not handled")
	}
	h.wait()

	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonDMSelfEcho {
		t.Fatalf("status/reason = %q/%q, want no_delivery/dm_self_echo", r.Status, r.Reason)
	}
	// Counted exactly once for this receipt (deduped via the degraded cursor);
	// the hydration path never ran, so the ack path is the sole counter site.
	if got := h.gw.dmTokenMissing.Load() - before; got != 1 {
		t.Fatalf("dmTokenMissing delta = %d, want exactly 1 (ack-degrade counted, deduped)", got)
	}
	if r.AckState != ackStateDegraded {
		t.Errorf("AckState = %q, want degraded", r.AckState)
	}
	_, react, reply := spy.tokens()
	if len(react) != 0 || len(reply) != 0 {
		t.Errorf("degraded ack still called react/reply: react=%v reply=%v", react, reply)
	}
	if len(gc.sessionCalls()) != 0 {
		t.Errorf("self-echo woke %d sessions, want 0", len(gc.sessionCalls()))
	}
}

// ---- routing (item 6 + allowed-human policy) -------------------------------

func TestDMUnboundFailsThenRedriveDelivers(t *testing.T) {
	h, gc, _ := setupDM(t)
	// riley IS a registered agent app + directory agent but has NO dm binding.
	ts := "1700000000.000730"
	ev := dmMessage("U0HUMAN01", ts, "riley?")
	ev.Channel = "D0RILEYDM"

	if _, handled := admitDMViaHandler(t, h, ev, rileyAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: "D0RILEYDM", TS: ts}
	r, _ := h.gw.store().Get(origin)
	if r.Status != ingressStatusFailed {
		t.Fatalf("status = %q, want failed", r.Status)
	}
	var unbound *TargetDelivery
	for k := range r.Targets {
		td := r.Targets[k]
		if td.Agent == "riley" {
			unbound = &td
		}
	}
	if unbound == nil || unbound.Detail != companyReasonFailedDMUnbound {
		t.Fatalf("unbound target = %+v, want failed_dm_unbound", unbound)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Fatalf("unbound woke %d sessions, want 0", got)
	}

	// Import a riley dm binding, then company-redrive re-resolves the unbound
	// target against dm_bindings and delivers.
	h.gw.dmBindStore = loadDMBindingsStore(t, h, dmBindingsFile{
		SchemaVersion: 1,
		DMBindings:    []DMBinding{{Agent: "ollie", Session: "ollie"}, {Agent: "riley", Session: "riley-dm"}},
	})
	// riley needs a token for hydration; drop one into the same secrets dir.
	if err := os.WriteFile(filepath.Join(h.gw.secretsDir, "bot-token-riley.txt"), []byte("xoxb-riley"), 0o600); err != nil {
		t.Fatalf("write riley token: %v", err)
	}
	_, cerr := h.gw.applyRedrive(r, companyRedriveRequest{Origin: &origin})
	if cerr != nil {
		t.Fatalf("applyRedrive: %v", cerr)
	}
	h.gw.triggerDelivery(origin)
	h.wait()

	r, _ = h.gw.store().Get(origin)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("post-redrive status = %q, want delivered", r.Status)
	}
	found := false
	for _, c := range gc.sessionCalls() {
		if strings.Contains(c.path, "/session/riley-dm/messages") {
			found = true
		}
	}
	if !found {
		t.Errorf("no delivery to riley-dm session after redrive: %+v", gc.sessionCalls())
	}
}

func TestDMAllowedHumanPolicy(t *testing.T) {
	// present-but-empty allowlist → every human denied; absent → allowed.
	cases := []struct {
		name         string
		allow        *[]string
		wantDelivery bool
		wantReason   string
	}{
		{"absent allows workspace humans", nil, true, ""},
		{"present empty denies everyone", &[]string{}, false, wakeReasonDMAuthorNotAllowed},
		{"present with author allowed", &[]string{"U0HUMAN01"}, true, ""},
		{"present without author denied", &[]string{"U0SOMEONE"}, false, wakeReasonDMAuthorNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gc := newFakeGC(t)
			df := baseDirectoryFile()
			df.DMAllowedHumans = tc.allow
			bf := baseBindingsFile()
			h := newCompanyHarness(t, gc.server.URL, &df, &bf, 8)
			setFixedClock(h)
			h.gw.slackToken = "xoxb-SWITCHBOARD"
			h.gw.agentApps = loadAgentAppsStore(t, h, baseAgentAppsFile())
			h.gw.dmBindStore = loadDMBindingsStore(t, h, dmBindingsFile{
				SchemaVersion: 1, DMBindings: []DMBinding{{Agent: "ollie", Session: "ollie"}},
			})
			h.gw.secretsDir = writeTokenFile(t, "ollie", "xoxb-ollie", 0o700, 0o600)
			h.gw.hydrateDM = func(string, CompanyMessage) companyHydration {
				return companyHydration{ContextStatus: companyContextUnavailable}
			}
			h.gw.mpimMemberProbe = func(string, string) mpimProbeOutcome {
				return mpimProbeMember
			}
			h.openBarrier()

			ts := "1700000000.000740"
			if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
				t.Fatal("not handled")
			}
			h.wait()
			r := getReceipt(t, h, ts)
			if tc.wantDelivery {
				if r.Status != ingressStatusDelivered {
					t.Fatalf("status = %q, want delivered", r.Status)
				}
				if len(gc.sessionCalls()) != 1 {
					t.Errorf("session calls = %d, want 1", len(gc.sessionCalls()))
				}
			} else {
				if r.Status != ingressStatusNoDelivery || r.Reason != tc.wantReason {
					t.Fatalf("status/reason = %q/%q, want no_delivery/%s", r.Status, r.Reason, tc.wantReason)
				}
				if len(gc.sessionCalls()) != 0 {
					t.Errorf("denied author woke %d sessions", len(gc.sessionCalls()))
				}
			}
		})
	}
}

// TestDMBodyMissingPendingReceiptParks pins C5/m8 for the DM path: a live DM
// receipt whose body sidecar is missing PARKS (parked_body_integrity) instead of
// terminalizing dm_author_not_allowed off an empty (null-body) message, and
// delivers once the body is restored.
func TestDMBodyMissingPendingReceiptParks(t *testing.T) {
	h, gc, _ := setupDM(t)
	ts := "1700000000.004100"
	ev := dmMessage("U0HUMAN01", ts, "hey ollie")
	ev.Type = "message"
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	origin := dmOrigin(ts)
	receipt := &IngressReceipt{
		Origin: origin, EventID: "Ev-" + ts, APIAppID: ollieAppID,
		Kind: receiptKindDM, OwnerAppID: ollieAppID,
		Status: ingressStatusReceived, Event: raw,
		ThreadRootTS: deriveHumanRootTS(decodeCompanyMessage(origin, raw)),
	}
	if created, _, aerr := h.gw.store().Admit(receipt); aerr != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, aerr)
	}
	if err := os.Remove(h.gw.store().bodyPathForID(receiptID(origin))); err != nil {
		t.Fatalf("remove body: %v", err)
	}

	if out := h.gw.deliverReceipt(origin); out != deliverParkedPreclaim {
		t.Fatalf("deliverReceipt outcome = %v, want parked", out)
	}
	r := getReceipt(t, h, ts)
	if r == nil || isTerminalStatus(r.Status) {
		t.Fatalf("body-missing DM receipt terminalized: status=%q reason=%q", statusOf(r), reasonOf(r))
	}
	if r.Status != ingressStatusReceived || r.Reason != companyReasonBodyIntegrity {
		t.Fatalf("status/reason = %q/%q, want received/parked_body_integrity", statusOf(r), reasonOf(r))
	}
	if len(gc.sessionCalls()) != 0 {
		t.Fatalf("parked DM receipt woke %d sessions, want 0", len(gc.sessionCalls()))
	}

	// Restore body → redrive delivers once.
	if err := h.gw.store().writeBodyOnce(receiptID(origin), raw); err != nil {
		t.Fatalf("restore body: %v", err)
	}
	h.gw.triggerDelivery(origin)
	h.wait()
	r = getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("post-restore status = %q, want delivered", r.Status)
	}
	if len(gc.sessionCalls()) != 1 {
		t.Errorf("post-restore deliveries = %d, want 1", len(gc.sessionCalls()))
	}
}

// TestDMRegistryUnavailableParksNotTerminal pins C3: a DM receipt admitted
// before a crash, then routed while the agent-apps registry is unavailable (nil
// snapshot), PARKS (sweep-recoverable) rather than terminalizing as an allowlist
// denial — and delivers once the registry recovers. Contrast the terminal
// dm_author_not_allowed reserved for a LIVE registry that answers the policy.
func TestDMRegistryUnavailableParksNotTerminal(t *testing.T) {
	h, gc, _ := setupDM(t)
	ts := "1700000000.000790"

	// Admit a human DM receipt directly in status "received" (crash-before-
	// routing), bypassing the immediate delivery trigger.
	ev := dmMessage("U0HUMAN01", ts, "hey ollie")
	ev.Type = "message"
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	origin := dmOrigin(ts)
	receipt := &IngressReceipt{
		Origin: origin, EventID: "Ev-" + ts, APIAppID: ollieAppID,
		Kind: receiptKindDM, OwnerAppID: ollieAppID,
		Status: ingressStatusReceived, Event: raw,
	}
	if created, _, aerr := h.gw.store().Admit(receipt); aerr != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, aerr)
	}

	// Registry goes dark: a corrupt/unreadable agent_apps.json installs a nil
	// snapshot (Snapshot() == nil).
	h.gw.agentApps = &agentAppsStore{}

	if out := h.gw.deliverReceipt(origin); out != deliverParkedPreclaim {
		t.Fatalf("deliverReceipt outcome = %v, want parked", out)
	}
	r := getReceipt(t, h, ts)
	if isTerminalStatus(r.Status) {
		t.Fatalf("receipt terminalized on registry-unavailable: status=%q reason=%q", r.Status, r.Reason)
	}
	if r.Status != ingressStatusReceived || r.Reason != wakeReasonDMAppUnregistered {
		t.Fatalf("status/reason = %q/%q, want received/dm_app_unregistered", r.Status, r.Reason)
	}
	if len(gc.sessionCalls()) != 0 {
		t.Fatalf("parked receipt woke %d sessions, want 0", len(gc.sessionCalls()))
	}

	// Registry recovers; the same receipt delivers on the next drive.
	h.gw.agentApps = loadAgentAppsStore(t, h, baseAgentAppsFile())
	h.gw.triggerDelivery(origin)
	h.wait()
	r = getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("post-recovery status = %q, want delivered", r.Status)
	}
	if len(gc.sessionCalls()) != 1 {
		t.Errorf("session calls = %d, want 1 after recovery", len(gc.sessionCalls()))
	}
}

// TestDMAdmissionUsesVerifyTimeSnapshot pins m7: admission consumes the SAME
// registration snapshot the HMAC verification used, so a SIGHUP that deregisters
// the app between verify and admit cannot route a verified agent-app DM into the
// legacy dispatcher. The DM gateway still owns the event (200 + receipt), and
// delivery then re-resolves against the live (now-empty) registry — parking
// sweep-recoverably rather than mis-dispatching.
func TestDMAdmissionUsesVerifyTimeSnapshot(t *testing.T) {
	h, gc, _ := setupDM(t)
	ts := "1700000000.000795"

	verifySnap := h.gw.agentAppsSnapshot() // ollie registered at verify time
	if _, ok := verifySnap.Get(ollieAppID); !ok {
		t.Fatal("precondition: ollie registered in the verify snapshot")
	}
	// A SIGHUP deregisters every app AFTER verification.
	h.gw.agentApps = &agentAppsStore{} // Snapshot() == nil

	ev := dmMessage("U0HUMAN01", ts, "hey ollie")
	ev.Type = "message"
	ev.ChannelType = "im"
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	env := slackEventEnvelope{Type: "event_callback", TeamID: testTeamID, APIAppID: ollieAppID, EventID: "Ev-" + ts, Event: raw}
	w := httptest.NewRecorder()
	// Admission is handed the verify-time snapshot: the DM gateway owns the event.
	handled := h.gw.tryHandleEvent(w, httptest.NewRequest(http.MethodPost, "/slack/events", nil), env, verifySnap)
	if !handled {
		t.Fatal("verified agent-app DM fell through to legacy after a mid-request deregister")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("admit code = %d, want 200", w.Code)
	}
	h.wait()
	r := getReceipt(t, h, ts)
	if r == nil || r.Kind != receiptKindDM || r.OwnerAppID != ollieAppID {
		t.Fatalf("expected a DM receipt admitted via the shared snapshot, got %+v", r)
	}
	// Delivery re-resolved against the now-empty registry → parked (C3), never
	// woke a session and never leaked to legacy.
	if isTerminalStatus(r.Status) {
		t.Errorf("receipt terminalized; want non-terminal park, status=%q", r.Status)
	}
	if len(gc.sessionCalls()) != 0 {
		t.Errorf("parked receipt woke %d sessions, want 0", len(gc.sessionCalls()))
	}
}

// ---- session-existence guard (item 9) --------------------------------------

func TestDMSessionGuardHoldsThenProceeds(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true

	// A custom gc: 404 on the session-existence GET while missing.Load()==true,
	// 200 once the session materializes; 200 on wake and delivery POSTs.
	var sessionMissing atomic.Bool
	sessionMissing.Store(true)
	var deliveryPosts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && !strings.HasSuffix(r.URL.Path, "/messages") {
			if sessionMissing.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages") {
			deliveryPosts.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.000750"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusRouting {
		t.Fatalf("status = %q, want routing (guard held, not terminal)", r.Status)
	}
	if deliveryPosts.Load() != 0 {
		t.Fatalf("guard did not prevent the delivery POST: posts=%d", deliveryPosts.Load())
	}
	var detail string
	var attempts int
	for _, td := range r.Targets {
		detail, attempts = td.Detail, td.Attempts
	}
	if detail != companyDetailMaterializing || attempts < 1 {
		t.Fatalf("target detail/attempts = %q/%d, want materializing/>=1", detail, attempts)
	}

	// Session materializes; a re-drive re-checks (negatives are not cached: the
	// prior 404 was never remembered) and delivers.
	sessionMissing.Store(false)
	h.gw.triggerDelivery(dmOrigin(ts))
	h.wait()
	r = getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status after materialize = %q, want delivered", r.Status)
	}
	if deliveryPosts.Load() != 1 {
		t.Errorf("delivery posts = %d, want 1", deliveryPosts.Load())
	}
}

func TestDMSessionGuardNetworkErrorProceeds(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true
	// A server whose session-existence GET errors (hijack + close) but whose
	// delivery POST succeeds: a guard network error must not reduce availability
	// below flag-off.
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && !strings.HasSuffix(r.URL.Path, "/messages") {
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, _ := hj.Hijack()
				_ = conn.Close()
				return
			}
		}
		posts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.000760"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered (guard error must proceed)", r.Status)
	}
	if posts.Load() != 1 {
		t.Errorf("delivery posts = %d, want 1 (guard error proceeded)", posts.Load())
	}
}

// ---- receipt-store failure 503 (item 10) -----------------------------------

func TestDMStoreWriteFailure503(t *testing.T) {
	h, _, _ := setupDM(t)
	ts := "1700000000.000770"
	// Make the ingress dir unwritable so Admit's temp write fails.
	if err := os.Chmod(h.ingressDir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	w, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0)
	if !handled {
		t.Fatal("not handled")
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", w.Code)
	}
	if w.Header().Get("x-slack-no-retry") != "" {
		t.Error("503 must not carry x-slack-no-retry (Slack must redeliver)")
	}
	if h.gw.store().WriteFailures() == 0 {
		t.Error("write-failure counter did not increment")
	}
	// Restore perms; retry admits once.
	if err := os.Chmod(h.ingressDir, 0o700); err != nil {
		t.Fatalf("chmod restore: %v", err)
	}
	w, _ = admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 1)
	if w.Code != http.StatusOK {
		t.Fatalf("retry code = %d, want 200", w.Code)
	}
	h.wait()
	if getReceipt(t, h, ts) == nil {
		t.Error("retry did not admit a receipt")
	}
}

// ---- missing owner token (item 11) -----------------------------------------

func TestDMMissingOwnerTokenDegrades(t *testing.T) {
	h, gc, spy := setupDM(t)
	h.gw.visibleAcks = true
	// Remove ollie's token so the owner token is missing.
	if err := os.Remove(filepath.Join(h.gw.secretsDir, "bot-token-ollie.txt")); err != nil {
		t.Fatalf("remove token: %v", err)
	}
	before := h.gw.dmTokenMissing.Load()

	ts := "1700000000.000780"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered (wake still happens without context)", r.Status)
	}
	if len(gc.sessionCalls()) != 1 {
		t.Fatalf("session calls = %d, want 1", len(gc.sessionCalls()))
	}
	// Frozen hydration degraded to context_unavailable.
	var hy companyHydration
	_ = json.Unmarshal(r.Hydration, &hy)
	if hy.ContextStatus != companyContextUnavailable {
		t.Errorf("context_status = %q, want context_unavailable", hy.ContextStatus)
	}
	if h.gw.dmTokenMissing.Load() <= before {
		t.Error("company_dm_token_missing did not increment")
	}
	// The switchboard token must NEVER be used on the DM channel: with no owner
	// token, hydration and acks are skipped entirely (no token recorded).
	hydrate, react, reply := spy.tokens()
	all := append(append(append([]string{}, hydrate...), react...), reply...)
	for _, tok := range all {
		if tok == "xoxb-SWITCHBOARD" {
			t.Fatalf("switchboard token used on a DM channel: %v", all)
		}
	}
}

// ---- golden fixtures (item 12) ---------------------------------------------

const dmFixtureDir = "../tests/fixtures/company"

func TestDMFixturesParse(t *testing.T) {
	dir := testDirectory(t)

	agentAppsData, err := os.ReadFile(filepath.Join(dmFixtureDir, "agent_apps.json"))
	if err != nil {
		t.Fatalf("read agent_apps fixture: %v", err)
	}
	apps, err := ParseAgentApps(agentAppsData)
	if err != nil {
		t.Fatalf("parse agent_apps fixture: %v", err)
	}
	if _, ok := apps.Get(ollieAppID); !ok {
		t.Error("agent_apps fixture missing ollie app")
	}
	if w := apps.JoinWarnings(dir); len(w) != 0 {
		t.Errorf("agent_apps fixture has join warnings against base directory: %v", w)
	}

	dmbData, err := os.ReadFile(filepath.Join(dmFixtureDir, "dm_bindings.json"))
	if err != nil {
		t.Fatalf("read dm_bindings fixture: %v", err)
	}
	dmb, warnings, err := ParseDMBindings(dmbData, dir)
	if err != nil {
		t.Fatalf("parse dm_bindings fixture: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("dm_bindings fixture warnings: %v", warnings)
	}
	if bd, ok := dmb.BindingFor("ollie"); !ok || bd.Session != "ollie" || bd.City != "" {
		t.Errorf("dm_bindings fixture ollie = (%+v, %v)", bd, ok)
	}
	// The writer omits an empty city and keeps a city-qualified one; the fixture
	// (regenerated from cmd_bind_dm) must round-trip both shapes.
	if bd, ok := dmb.BindingFor("riley"); !ok || bd.Session != "riley-main" || bd.City != "riley-city" {
		t.Errorf("dm_bindings fixture riley = (%+v, %v)", bd, ok)
	}

	// message.im human + self-echo wire shapes.
	humanEnv := readEnvFixture(t, "message_im_human.json")
	if humanEnv.APIAppID != ollieAppID {
		t.Errorf("human fixture api_app_id = %q", humanEnv.APIAppID)
	}
	var hev slackMessageEvent
	_ = json.Unmarshal(humanEnv.Event, &hev)
	if hev.ChannelType != "im" || hev.BotID != "" || hev.User == "" {
		t.Errorf("human fixture inner shape = %+v", hev)
	}
	selfEnv := readEnvFixture(t, "message_im_self_echo.json")
	var sev slackMessageEvent
	_ = json.Unmarshal(selfEnv.Event, &sev)
	if sev.ChannelType != "im" || sev.BotID == "" || len(sev.Metadata) == 0 {
		t.Errorf("self-echo fixture must be bot-authored with metadata: %+v", sev)
	}
	if !isBotAuthored(decodeCompanyMessage(ReceiptOrigin{}, selfEnv.Event)) {
		t.Error("self-echo fixture must classify as bot-authored")
	}
	// The self-echo metadata MUST carry the {"v":1,"nonce":...} event_payload the
	// Python reconciler (_scan_receipt_for_nonce) actually scans for — the shape
	// dm_metadata produces. A drift back to a receipt_id/other shape breaks here
	// (C4 / m3): pin event_type gc_dm_reply + a non-empty nonce that decodes.
	var meta struct {
		EventType    string `json:"event_type"`
		EventPayload struct {
			V     int    `json:"v"`
			Nonce string `json:"nonce"`
		} `json:"event_payload"`
	}
	if err := json.Unmarshal(sev.Metadata, &meta); err != nil {
		t.Fatalf("self-echo metadata does not decode: %v", err)
	}
	if meta.EventType != "gc_dm_reply" {
		t.Errorf("self-echo event_type = %q, want gc_dm_reply", meta.EventType)
	}
	if meta.EventPayload.V != 1 || meta.EventPayload.Nonce == "" {
		t.Errorf("self-echo event_payload = %+v, want {v:1, nonce:non-empty}", meta.EventPayload)
	}
}

func TestDMPointerFixtureByteShape(t *testing.T) {
	p := companyCurrentTurn{
		SchemaVersion: companyCurrentTurnSchemaV,
		Session:       "ollie",
		ReceiptID:     "in-example-dm",
		TeamID:        testTeamID,
		ChannelID:     "D0AAAAAAA",
		TS:            "1700000000.000700",
		Room:          "",
		Kind:          "dm",
		ThreadRootTS:  "1700000000.000700",
		Agent:         "ollie",
		OwnerAppID:    ollieAppID,
		DeliveredAt:   "2026-07-18T00:00:06Z",
	}
	got, err := marshalCurrentTurn(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want, err := os.ReadFile(filepath.Join(dmFixtureDir, "dm_current_turn.json"))
	if err != nil {
		t.Fatalf("read pointer fixture: %v", err)
	}
	if strings.TrimSpace(string(got)) != strings.TrimSpace(string(want)) {
		t.Errorf("DM pointer bytes diverge from golden fixture:\n got: %s\nwant: %s", got, want)
	}
}

// TestDMPointerNoCollisionWithDottedRoomSession pins m2 / m13: a DM pointer for
// session "ollie" and a ROOM pointer for a session literally named "ollie.dm"
// land in disjoint namespaces (turnsDir/dm/ollie.json vs turnsDir/ollie.dm.json)
// and cannot clobber each other, closing the C6 cross-pointer hazard the shared
// sanitizer's interior-dot pass-through would otherwise reopen.
func TestDMPointerNoCollisionWithDottedRoomSession(t *testing.T) {
	dir := t.TempDir()
	roomPtr := companyCurrentTurn{
		SchemaVersion: companyCurrentTurnSchemaV, Session: "ollie.dm", Kind: "targeted",
		Room: "orchestrator-team", Agent: "ollie", DeliveredAt: "2026-07-18T00:00:00Z",
	}
	if err := writeCurrentTurnPointer(dir, roomPtr); err != nil {
		t.Fatalf("write room pointer: %v", err)
	}
	dmPtr := companyCurrentTurn{
		SchemaVersion: companyCurrentTurnSchemaV, Session: "ollie", Kind: "dm",
		Room: "", Agent: "ollie", OwnerAppID: ollieAppID, DeliveredAt: "2026-07-18T00:00:06Z",
	}
	if err := writeCurrentTurnPointer(dir, dmPtr); err != nil {
		t.Fatalf("write dm pointer: %v", err)
	}

	var rp companyCurrentTurn
	roomData, err := os.ReadFile(filepath.Join(dir, "ollie.dm.json"))
	if err != nil {
		t.Fatalf("room pointer file: %v", err)
	}
	if err := json.Unmarshal(roomData, &rp); err != nil {
		t.Fatalf("decode room pointer: %v", err)
	}
	if rp.Kind != "targeted" || rp.Session != "ollie.dm" {
		t.Errorf("room pointer clobbered by DM write: %+v", rp)
	}

	var dp companyCurrentTurn
	dmData, err := os.ReadFile(filepath.Join(dir, "dm", "ollie.json"))
	if err != nil {
		t.Fatalf("dm pointer file: %v", err)
	}
	if err := json.Unmarshal(dmData, &dp); err != nil {
		t.Fatalf("decode dm pointer: %v", err)
	}
	if dp.Kind != "dm" || dp.Session != "ollie" {
		t.Errorf("dm pointer clobbered by room write: %+v", dp)
	}
}

// ---- helpers ---------------------------------------------------------------

func readDMPointer(t *testing.T, h *companyHarness, session string) companyCurrentTurn {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(h.turnsDir, "dm", session+".json"))
	if err != nil {
		t.Fatalf("read dm pointer %s: %v", session, err)
	}
	var p companyCurrentTurn
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("decode dm pointer: %v", err)
	}
	return p
}

func readEnvFixture(t *testing.T, name string) slackEventEnvelope {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dmFixtureDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var env slackEventEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return env
}

// DM membership gate (codex round 3): a message.im for a conversation
// the owner app cannot access — the Assistant/Agent-mode foreign
// human↔human DM — must terminalize with no session delivery, while a
// probe transport error parks (sweep-recoverable) instead of leaking
// or losing the DM.
func TestDMMembershipGate(t *testing.T) {
	t.Run("not a member terminalizes without delivery", func(t *testing.T) {
		h, gc, _ := setupDM(t)
		ts := "1700000000.000801"
		h.gw.mpimMemberProbe = func(token, channel string) mpimProbeOutcome {
			return mpimProbeNotMember
		}
		if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "private things"), ollieAppID, 0); !handled {
			t.Fatal("not handled")
		}
		h.wait()

		r := getReceipt(t, h, ts)
		if r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonDMNotMember {
			t.Fatalf("receipt = (%s, %s), want (no_delivery, %s)", r.Status, r.Reason, wakeReasonDMNotMember)
		}
		if calls := gc.sessionCalls(); len(calls) != 0 {
			t.Fatalf("session calls = %+v, want none (foreign DM must not reach the agent)", calls)
		}
	})
	t.Run("probe error parks recoverably", func(t *testing.T) {
		h, gc, _ := setupDM(t)
		ts := "1700000000.000802"
		h.gw.mpimMemberProbe = func(token, channel string) mpimProbeOutcome {
			return mpimProbeError
		}
		if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hello"), ollieAppID, 0); !handled {
			t.Fatal("not handled")
		}
		h.wait()

		r := getReceipt(t, h, ts)
		if isTerminalStatus(r.Status) {
			t.Fatalf("receipt terminalized on probe error: status=%q reason=%q", r.Status, r.Reason)
		}
		if r.Status != ingressStatusReceived || r.Reason != wakeReasonDMMemberUnknown {
			t.Fatalf("status/reason = %q/%q, want received/%s", r.Status, r.Reason, wakeReasonDMMemberUnknown)
		}
		if calls := gc.sessionCalls(); len(calls) != 0 {
			t.Fatalf("session calls = %+v, want none while parked", calls)
		}

		// Probe recovers; the same receipt delivers on the next drive.
		h.gw.mpimMemberProbe = func(token, channel string) mpimProbeOutcome {
			return mpimProbeMember
		}
		h.gw.triggerDelivery(dmOrigin(ts))
		h.wait()
		if calls := gc.sessionCalls(); len(calls) != 1 {
			t.Fatalf("session calls after recovery = %+v, want exactly 1", calls)
		}
	})
}
