package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// signSlack produces a valid X-Slack-Signature header for the given body,
// timestamp, and secret — the inverse of verifySlackSignature.
func signSlack(secret, ts string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + ts + ":"))
	_, _ = mac.Write(body)
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifySlackSignature(t *testing.T) {
	const secret = "shhh"
	body := []byte(`{"type":"event_callback"}`)
	now := strconv.FormatInt(time.Now().Unix(), 10)
	stale := strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10)
	future := strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10)
	valid := signSlack(secret, now, body)

	tests := []struct {
		name   string
		secret string
		ts     string
		sig    string
		want   bool
	}{
		{"valid", secret, now, valid, true},
		{"wrong secret", "nope", now, valid, false},
		{"tampered body sig", secret, now, signSlack(secret, now, []byte("other")), false},
		{"stale timestamp", secret, stale, signSlack(secret, stale, body), false},
		{"far-future timestamp", secret, future, signSlack(secret, future, body), false},
		{"non-numeric timestamp", secret, "abc", valid, false},
		{"empty secret", "", now, valid, false},
		{"empty ts", secret, "", valid, false},
		{"empty sig", secret, now, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := verifySlackSignature(tc.secret, tc.ts, body, tc.sig); got != tc.want {
				t.Fatalf("verifySlackSignature = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStripLeadingMention(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"<@U0BOT> status please", "status please"},
		{"  <@U0BOT>   hello", "hello"},
		{"<@U0BOT> <@U1OPS> deploy", "deploy"},
		{"no mention here", "no mention here"},
		{"<@U0BOT>", ""},
		{"   ", ""},
		{"text then <@U0BOT>", "text then <@U0BOT>"}, // only leading mentions stripped
	}
	for _, tc := range tests {
		if got := stripLeadingMention(tc.in); got != tc.want {
			t.Errorf("stripLeadingMention(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSlackKindFromChannelType(t *testing.T) {
	tests := []struct {
		ctype, cid, want string
	}{
		{"channel", "C123", "room"},
		{"group", "G123", "room"},
		{"mpim", "C123", "room"},
		{"im", "D123", "dm"},
		{"", "C123", "room"},
		{"", "G123", "room"},
		{"", "D123", "dm"},
		{"", "", "dm"},
	}
	for _, tc := range tests {
		if got := slackKindFromChannelType(tc.ctype, tc.cid); got != tc.want {
			t.Errorf("slackKindFromChannelType(%q,%q) = %q, want %q", tc.ctype, tc.cid, got, tc.want)
		}
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	base := map[string]string{
		"SLACK_BOT_TOKEN":      "xoxb-1",
		"SLACK_SIGNING_SECRET": "secret",
		"SLACK_WORKSPACE_ID":   "T123",
		"GC_CITY_NAME":         "mycity",
	}
	clone := func(extra map[string]string) func(string) string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return func(k string) string { return m[k] }
	}

	t.Run("defaults", func(t *testing.T) {
		cfg, err := loadConfigFromEnv(clone(nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.publicListen != defaultPublicListen {
			t.Errorf("publicListen = %q, want default", cfg.publicListen)
		}
		if cfg.inboundTarget != defaultInboundTarget {
			t.Errorf("inboundTarget = %q, want %q", cfg.inboundTarget, defaultInboundTarget)
		}
		if !cfg.registerOnStart {
			t.Error("registerOnStart should default true")
		}
		if cfg.slackAPIBase != defaultSlackAPIBase {
			t.Errorf("slackAPIBase = %q, want default", cfg.slackAPIBase)
		}
	})

	t.Run("slack api base override", func(t *testing.T) {
		cfg, err := loadConfigFromEnv(clone(map[string]string{"SLACK_API_BASE": "https://relay.example/api/"}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.slackAPIBase != "https://relay.example/api" {
			t.Errorf("slackAPIBase = %q, want trimmed override", cfg.slackAPIBase)
		}
	})

	t.Run("missing required", func(t *testing.T) {
		getenv := func(k string) string {
			if k == "GC_CITY_NAME" {
				return ""
			}
			return base[k]
		}
		_, err := loadConfigFromEnv(getenv)
		if err == nil || !strings.Contains(err.Error(), "GC_CITY_NAME") {
			t.Fatalf("expected missing GC_CITY_NAME error, got %v", err)
		}
	})

	t.Run("city name with slash rejected", func(t *testing.T) {
		_, err := loadConfigFromEnv(clone(map[string]string{"GC_CITY_NAME": "a/b"}))
		if err == nil || !strings.Contains(err.Error(), "must not contain") {
			t.Fatalf("expected city-name rejection, got %v", err)
		}
	})

	t.Run("proxy_process requires url prefix", func(t *testing.T) {
		_, err := loadConfigFromEnv(clone(map[string]string{"GC_SERVICE_SOCKET": "/tmp/s.sock"}))
		if err == nil || !strings.Contains(err.Error(), "GC_SERVICE_URL_PREFIX") {
			t.Fatalf("expected url-prefix error, got %v", err)
		}
	})

	t.Run("proxy_process callback url", func(t *testing.T) {
		cfg, err := loadConfigFromEnv(clone(map[string]string{
			"GC_SERVICE_SOCKET":     "/tmp/s.sock",
			"GC_SERVICE_URL_PREFIX": "/v0/city/mycity/svc/slack-mini/",
			"GC_API_BASE_URL":       "http://127.0.0.1:8372",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := "http://127.0.0.1:8372/v0/city/mycity/svc/slack-mini"
		if cfg.internalCallbackURL != want {
			t.Errorf("internalCallbackURL = %q, want %q", cfg.internalCallbackURL, want)
		}
	})

	t.Run("tcp mode derives callback url from internal listener", func(t *testing.T) {
		// No GC_SERVICE_SOCKET → standalone TCP mode. The callback must not be
		// empty (an empty callback_url breaks gc→adapter callbacks).
		cfg, err := loadConfigFromEnv(clone(nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := "http://" + defaultInternalListen
		if cfg.internalCallbackURL != want {
			t.Errorf("internalCallbackURL = %q, want %q", cfg.internalCallbackURL, want)
		}
	})

	t.Run("tcp mode normalizes wildcard internal host to loopback", func(t *testing.T) {
		cfg, err := loadConfigFromEnv(clone(map[string]string{"LISTEN_INTERNAL": "0.0.0.0:9001"}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.internalCallbackURL != "http://127.0.0.1:9001" {
			t.Errorf("internalCallbackURL = %q, want loopback-normalized", cfg.internalCallbackURL)
		}
	})
}

func TestHandleSlackEventsURLVerification(t *testing.T) {
	cfg := config{signingSecret: "secret"}
	body := []byte(`{"type":"url_verification","challenge":"c4tt0ken"}`)
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	req := httptest.NewRequest(http.MethodPost, "/slack/events", strings.NewReader(string(body)))
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", signSlack(cfg.signingSecret, ts, body))
	rec := httptest.NewRecorder()

	handleSlackEvents(cfg)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "c4tt0ken" {
		t.Fatalf("challenge echo = %q, want c4tt0ken", got)
	}
}

func TestHandleSlackEventsBadSignature(t *testing.T) {
	cfg := config{signingSecret: "secret"}
	body := []byte(`{"type":"event_callback"}`)
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	req := httptest.NewRequest(http.MethodPost, "/slack/events", strings.NewReader(string(body)))
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", "v0=deadbeef")
	rec := httptest.NewRecorder()

	handleSlackEvents(cfg)(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// TestBridgeEventPostsInbound drives an app_mention through bridgeEvent and
// asserts the extmsg inbound payload shape.
func TestBridgeEventPostsInbound(t *testing.T) {
	var got externalInboundMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/extmsg/inbound") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		var wrap struct {
			Message externalInboundMessage `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&wrap); err != nil {
			t.Errorf("decode body: %v", err)
		}
		got = wrap.Message
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config{
		gcAPIBase:     srv.URL,
		cityName:      "mycity",
		provider:      "slack",
		workspaceID:   "T123",
		inboundTarget: "mayor",
	}
	event := slackMessageEvent{
		Type:        "app_mention",
		User:        "U99",
		Text:        "<@U0BOT> deploy please",
		Channel:     "C42",
		TS:          "1700000000.0001",
		ThreadTS:    "1700000000.0000",
		ChannelType: "channel",
	}
	raw, _ := json.Marshal(event)
	bridgeEvent(cfg, slackEventEnvelope{Type: "event_callback", Event: raw})

	if got.Text != "deploy please" {
		t.Errorf("Text = %q, want stripped 'deploy please'", got.Text)
	}
	if got.ExplicitTarget != "mayor" {
		t.Errorf("ExplicitTarget = %q, want mayor", got.ExplicitTarget)
	}
	if got.Conversation.ConversationID != "C42" || got.Conversation.Kind != "room" {
		t.Errorf("conversation = %+v, want channel C42 room", got.Conversation)
	}
	if got.DedupKey != "slack-1700000000.0001" {
		t.Errorf("DedupKey = %q", got.DedupKey)
	}
	if got.ReplyToMessageID != "1700000000.0000" {
		t.Errorf("ReplyToMessageID = %q", got.ReplyToMessageID)
	}
}

// TestBridgeEventIgnoresNonMentions confirms Tier 1 drops everything that
// is not a clean human app_mention.
func TestBridgeEventIgnoresNonMentions(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	cfg := config{gcAPIBase: srv.URL, cityName: "c", inboundTarget: "mayor"}

	drop := func(name string, ev slackMessageEvent) {
		raw, _ := json.Marshal(ev)
		bridgeEvent(cfg, slackEventEnvelope{Type: "event_callback", Event: raw})
		if called {
			t.Errorf("%s: expected event dropped, but inbound was posted", name)
			called = false
		}
	}
	drop("plain message", slackMessageEvent{Type: "message", User: "U1", Text: "hi", Channel: "C1", TS: "1"})
	drop("bot message", slackMessageEvent{Type: "app_mention", BotID: "B1", Text: "hi", Channel: "C1", TS: "1"})
	drop("subtype", slackMessageEvent{Type: "app_mention", Subtype: "message_changed", User: "U1", Text: "hi", TS: "1"})
	drop("empty after strip", slackMessageEvent{Type: "app_mention", User: "U1", Text: "<@U0BOT>", Channel: "C1", TS: "1"})
}

func TestHandlePostMessage(t *testing.T) {
	var gotBody slackPostMessageReq
	var gotAuth string
	slack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(slackPostMessageResp{OK: true, TS: "1700000000.0002", Channel: "C42"})
	}))
	defer slack.Close()

	cfg := config{botToken: "xoxb-tok", slackAPIBase: slack.URL}
	reqBody := `{"channel":"C42","text":"build green","thread_ts":"1700000000.0000"}`
	req := httptest.NewRequest(http.MethodPost, "/post-message", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()

	handlePostMessage(cfg)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if gotAuth != "Bearer xoxb-tok" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if gotBody.Channel != "C42" || gotBody.Text != "build green" || gotBody.ThreadTS != "1700000000.0000" {
		t.Errorf("forwarded body = %+v", gotBody)
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out["ok"] != true || out["ts"] != "1700000000.0002" {
		t.Errorf("response = %v", out)
	}
}

func TestHandlePostMessageValidation(t *testing.T) {
	cfg := config{botToken: "xoxb"}
	cases := map[string]string{
		"missing channel": `{"text":"hi"}`,
		"missing text":    `{"channel":"C1"}`,
		"bad json":        `{`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/post-message", strings.NewReader(body))
			rec := httptest.NewRecorder()
			handlePostMessage(cfg)(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
		})
	}
}

func TestHandlePostMessageSlackError(t *testing.T) {
	slack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(slackPostMessageResp{OK: false, Error: "channel_not_found"})
	}))
	defer slack.Close()

	cfg := config{botToken: "xoxb", slackAPIBase: slack.URL}
	req := httptest.NewRequest(http.MethodPost, "/post-message", strings.NewReader(`{"channel":"C1","text":"hi"}`))
	rec := httptest.NewRecorder()
	handlePostMessage(cfg)(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "channel_not_found") {
		t.Errorf("error not surfaced: %s", rec.Body.String())
	}
}

// TestRegisterAdapter confirms the self-registration payload shape.
func TestRegisterAdapter(t *testing.T) {
	var got adapterRegisterRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/extmsg/adapters") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config{
		gcAPIBase:           srv.URL,
		cityName:            "mycity",
		provider:            "slack",
		workspaceID:         "T123",
		internalCallbackURL: "http://127.0.0.1:8372/v0/city/mycity/svc/slack-mini",
	}
	if err := registerAdapter(context.Background(), cfg); err != nil {
		t.Fatalf("registerAdapter: %v", err)
	}
	if got.Provider != "slack" || got.AccountID != "T123" {
		t.Errorf("register payload = %+v", got)
	}
	if got.Capabilities.SupportsChildConversations {
		t.Error("Tier 1 must not advertise child conversations")
	}
}

func TestHandleHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	handleHealthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("body = %q, want ok", rec.Body.String())
	}
}

// TestListenUDS confirms the socket binds, is owner-only, and that a stale
// socket file from a prior run is replaced rather than blocking startup.
func TestListenUDS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapter.sock")

	lis, err := listenUDS(path)
	if err != nil {
		t.Fatalf("listenUDS: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket perm = %o, want 600", perm)
	}
	_ = lis.Close()

	// A leftover socket file at the path must not block a restart.
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale: %v", err)
	}
	lis2, err := listenUDS(path)
	if err != nil {
		t.Fatalf("listenUDS over stale socket: %v", err)
	}
	defer func() { _ = lis2.Close() }()
	if _, ok := lis2.(*net.UnixListener); !ok {
		t.Errorf("listener type = %T, want *net.UnixListener", lis2)
	}
}

// compressRetries shrinks the inbound retry schedule so retry-path tests
// run in milliseconds instead of the production ~2 minutes.
func compressRetries(t *testing.T) {
	t.Helper()
	saved := inboundRetryDelays
	inboundRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { inboundRetryDelays = saved })
}

func TestSpoolDirFromEnv(t *testing.T) {
	base := map[string]string{
		"SLACK_BOT_TOKEN":      "xoxb-1",
		"SLACK_SIGNING_SECRET": "secret",
		"SLACK_WORKSPACE_ID":   "T123",
		"GC_CITY_NAME":         "mycity",
	}
	getenv := func(extra map[string]string) func(string) string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return func(k string) string { return m[k] }
	}

	t.Run("explicit override wins", func(t *testing.T) {
		cfg, err := loadConfigFromEnv(getenv(map[string]string{
			"SLACK_MINI_SPOOL_DIR":  "/tmp/custom-spool",
			"GC_SERVICE_STATE_ROOT": "/state/root",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.spoolDir != "/tmp/custom-spool" {
			t.Errorf("spoolDir = %q, want /tmp/custom-spool", cfg.spoolDir)
		}
	})

	t.Run("derived from state root", func(t *testing.T) {
		cfg, err := loadConfigFromEnv(getenv(map[string]string{
			"GC_SERVICE_STATE_ROOT": "/state/root",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := filepath.Join("/state/root", "data", "inbound-spool")
		if cfg.spoolDir != want {
			t.Errorf("spoolDir = %q, want %q", cfg.spoolDir, want)
		}
	})

	t.Run("unset disables spooling", func(t *testing.T) {
		cfg, err := loadConfigFromEnv(getenv(nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.spoolDir != "" {
			t.Errorf("spoolDir = %q, want empty", cfg.spoolDir)
		}
	})
}

// TestDeliverInboundRetriesUntilSuccess confirms a transient gc failure is
// retried and the spool entry is removed once the forward lands (bug hq-1q1:
// the adapter has already 200-acked Slack, so it must not drop the event).
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
	cfg := config{gcAPIBase: srv.URL, cityName: "c", spoolDir: spool}
	msg := externalInboundMessage{ProviderMessageID: "1700000000.0001", DedupKey: "slack-1700000000.0001"}
	path, _ := spoolInbound(spool, msg)
	if path == "" {
		t.Fatal("spoolInbound returned no path")
	}

	deliverInbound(cfg, msg, path)

	if got := calls.Load(); got != 3 {
		t.Errorf("gc calls = %d, want 3 (two failures then success)", got)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("spool entry not removed after successful delivery: %v", err)
	}
}

// TestDeliverInboundDeadLettersOnExhaustion confirms the spool entry
// survives (in the dead-letter dir) when every forward attempt fails, and
// that the final log line keeps the "inbound POST failed" marker external
// log-watchers key on.
func TestDeliverInboundDeadLettersOnExhaustion(t *testing.T) {
	compressRetries(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	spool := t.TempDir()
	cfg := config{gcAPIBase: srv.URL, cityName: "c", spoolDir: spool}
	msg := externalInboundMessage{
		ProviderMessageID: "1700000000.0002",
		DedupKey:          "slack-1700000000.0002",
		Text:              "lost mention",
	}
	path, _ := spoolInbound(spool, msg)
	if path == "" {
		t.Fatal("spoolInbound returned no path")
	}

	deliverInbound(cfg, msg, path)

	wantCalls := int32(len(inboundRetryDelays) + 1)
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

// TestReplaySpoolRedelivers confirms events left in the spool by a previous
// run (crash mid-retry) are re-forwarded at startup.
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
	path, _ := spoolInbound(spool, msg)
	if path == "" {
		t.Fatal("spoolInbound returned no path")
	}

	cfg := config{gcAPIBase: srv.URL, cityName: "c", spoolDir: spool}
	replaySpool(cfg)

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

// TestReplaySpoolDeadLettersCorruptEntries confirms an undecodable spool
// file is quarantined instead of blocking or crash-looping replay.
func TestReplaySpoolDeadLettersCorruptEntries(t *testing.T) {
	spool := t.TempDir()
	corrupt := filepath.Join(spool, "1-corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	replaySpool(config{gcAPIBase: "http://127.0.0.1:1", cityName: "c", spoolDir: spool})

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

// TestPostJSONSurfacesErrorStatus confirms a >=400 from gc is an error.
func TestPostJSONSurfacesErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "nope")
	}))
	defer srv.Close()
	err := postJSON(context.Background(), srv.URL, []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("expected error surfacing body, got %v", err)
	}
}

func TestSlackifyMarkdown(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"bold stars", "deploy **now** please", "deploy *now* please"},
		{"bold underscores", "deploy __now__ please", "deploy *now* please"},
		{"multiple bolds", "**a** and **b**", "*a* and *b*"},
		{"strike", "~~gone~~", "~gone~"},
		{"link", "see [the PR](https://example.com/pr/42) here", "see <https://example.com/pr/42|the PR> here"},
		{"heading", "# Status\nall green", "*Status*\nall green"},
		{"mrkdwn bold untouched", "*mayor:* on it", "*mayor:* on it"},
		{"italic untouched", "_soon_", "_soon_"},
		{"inline code protected", "run `gc status --json **not bold**`", "run `gc status --json **not bold**`"},
		{"fenced code protected", "```\n**not bold**\n# not a heading\n```", "```\n**not bold**\n# not a heading\n```"},
		{"unterminated fence protected", "before **bold**\n```\n**raw**", "before *bold*\n```\n**raw**"},
		{"handle prefix example", "**mayor:** build is green", "*mayor:* build is green"},
		{"empty", "", ""},
		{"plain multiplication untouched", "2 * 3 * 4 = 24", "2 * 3 * 4 = 24"},
		// codex P2: link destinations must survive the emphasis passes.
		{"underscores in link destination", "[init](https://host/pkg/__init__.py)", "<https://host/pkg/__init__.py|init>"},
		{"stars in link destination", "[x](https://host/a**b**c)", "<https://host/a**b**c|x>"},
		{"underscores in bare url", "see https://host/pkg/__init__.py now", "see https://host/pkg/__init__.py now"},
		{"tildes in bare url", "https://host/~~archive~~/x", "https://host/~~archive~~/x"},
		// codex P2: balanced parentheses in link destinations.
		{"parens in link destination", "[docs](https://host/Function_(mathematics))", "<https://host/Function_(mathematics)|docs>"},
		{"nested parens with trailing text", "[d](https://h/a_(b_(c))) end", "<https://h/a_(b_(c))|d> end"},
		// codex P2: multi-backtick code spans protect their contents.
		{"double backtick span protected", "x ``has `tick` and **raw**`` y", "x ``has `tick` and **raw**`` y"},
		{"unbalanced backtick run passes through", "a `` b **bold**", "a `` b *bold*"},
		// codex round 2: embedded ``` inside a code line is not a closer.
		{"fence with embedded triple backticks", "```\nfmt.Println(\"```\")\n**raw**\n```\nafter **b**", "```\nfmt.Println(\"```\")\n**raw**\n```\nafter *b*"},
		// codex round 2: escaped parens are URL data.
		{"escaped paren in link destination", `[x](https://h/a\)b)`, "<https://h/a)b|x>"},
		// codex round 2: heading hash glued to text is content.
		{"heading ending in hash", "# C#\nbody", "*C#*\nbody"},
		{"heading with spaced closer", "## Title ##\nbody", "*Title*\nbody"},
		// codex round 2: intraword underscores stay literal.
		{"intraword underscores untouched", "foo__bar__baz", "foo__bar__baz"},
		{"boundary underscore bold still converts", "say __hi__ now", "say *hi* now"},
		// codex round 2: trailing emphasis delimiters are not URL bytes.
		{"bold around bare url", "**see https://example.com**", "*see https://example.com*"},
	}
	for _, tc := range cases {
		if got := slackifyMarkdown(tc.in); got != tc.want {
			t.Errorf("%s: slackifyMarkdown(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}

func TestHandlePostMessageConvertsMarkdown(t *testing.T) {
	var gotBody slackPostMessageReq
	slack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"ts":"1","channel":"C1"}`))
	}))
	defer slack.Close()

	cfg := config{slackAPIBase: slack.URL, botToken: "xoxb-test"}
	reqBody := `{"channel":"C1","text":"**mayor:** deploy [PR](https://example.com/42) done"}`
	req := httptest.NewRequest(http.MethodPost, "/post-message", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	handlePostMessage(cfg)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	want := "*mayor:* deploy <https://example.com/42|PR> done"
	if gotBody.Text != want {
		t.Errorf("posted text = %q, want %q", gotBody.Text, want)
	}
}

func TestResolveUserDisplayNameCachesSuccesses(t *testing.T) {
	calls := 0
	slack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/users.info" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("user"); got != "U123" {
			t.Errorf("user param = %q", got)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer xoxb-test" {
			t.Errorf("auth = %q", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"user":{"name":"afik","real_name":"Afik Cohen","profile":{"display_name":"Afik","real_name":"Afik Cohen"}}}`))
	}))
	defer slack.Close()

	cfg := config{slackAPIBase: slack.URL, botToken: "xoxb-test"}
	cache := newUserNameCache()
	if got := resolveUserDisplayName(context.Background(), cache, cfg, "U123"); got != "Afik" {
		t.Fatalf("resolved = %q, want Afik (profile.display_name preferred)", got)
	}
	if got := resolveUserDisplayName(context.Background(), cache, cfg, "U123"); got != "Afik" {
		t.Fatalf("second resolve = %q", got)
	}
	if calls != 1 {
		t.Errorf("users.info calls = %d, want 1 (cached)", calls)
	}
}

func TestResolveUserDisplayNameFallsBackToRawID(t *testing.T) {
	calls := 0
	slack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":false,"error":"missing_scope"}`))
	}))
	defer slack.Close()

	cfg := config{slackAPIBase: slack.URL, botToken: "xoxb-test"}
	cache := newUserNameCache()
	if got := resolveUserDisplayName(context.Background(), cache, cfg, "U404"); got != "U404" {
		t.Fatalf("resolved = %q, want raw id fallback", got)
	}
	// Failures are negative-cached (a token without users:read fails on
	// every mention) but expire on the shorter failure TTL, so the fix
	// heals without a restart once the scope is granted.
	if got := resolveUserDisplayName(context.Background(), cache, cfg, "U404"); got != "U404" {
		t.Fatalf("second resolve = %q, want raw id fallback", got)
	}
	if calls != 1 {
		t.Errorf("users.info calls = %d, want 1 (failure negative-cached)", calls)
	}
	if _, ok := cache.get("U404", time.Now().Add(userNameFailureTTL+time.Second)); ok {
		t.Error("negative cache entry should expire after userNameFailureTTL")
	}
}

func TestBridgeEventResolvesDisplayName(t *testing.T) {
	var got externalInboundMessage
	gc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var wrap struct {
			Message externalInboundMessage `json:"message"`
		}
		_ = json.NewDecoder(r.Body).Decode(&wrap)
		got = wrap.Message
		w.WriteHeader(http.StatusOK)
	}))
	defer gc.Close()
	slack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"user":{"name":"afik","profile":{"display_name":"Afik"}}}`))
	}))
	defer slack.Close()

	cfg := config{
		gcAPIBase:     gc.URL,
		slackAPIBase:  slack.URL,
		botToken:      "xoxb-test",
		cityName:      "mycity",
		provider:      "slack",
		workspaceID:   "T123",
		inboundTarget: "mayor",
	}
	event := slackMessageEvent{
		Type: "app_mention", User: "U777", Text: "<@U0BOT> hello",
		Channel: "C42", TS: "1700000000.0001", ChannelType: "channel",
	}
	raw, _ := json.Marshal(event)
	bridgeEvent(cfg, slackEventEnvelope{Type: "event_callback", Event: raw})

	if got.Actor.DisplayName != "Afik" {
		t.Errorf("DisplayName = %q, want resolved 'Afik'", got.Actor.DisplayName)
	}
	if got.Actor.ID != "U777" {
		t.Errorf("Actor.ID = %q, want raw id preserved", got.Actor.ID)
	}
}

func TestRegisterAdapterSendsReplyInstructions(t *testing.T) {
	var got adapterRegisterRequest
	gc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/extmsg/adapters") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer gc.Close()

	cfg := config{
		gcAPIBase:           gc.URL,
		cityName:            "mycity",
		provider:            "slack",
		workspaceID:         "T123",
		internalCallbackURL: "http://127.0.0.1:1/cb",
	}
	if err := registerAdapter(context.Background(), cfg); err != nil {
		t.Fatalf("registerAdapter: %v", err)
	}
	if !strings.Contains(got.ReplyInstructions, "gc slack-mini post-message --channel {conversation_id}[ --thread-ts {thread_ts}]") {
		t.Errorf("reply_instructions missing Tier-1 reply command: %q", got.ReplyInstructions)
	}
	if strings.Contains(strings.ToLower(got.ReplyInstructions), "prefix") {
		t.Errorf("reply_instructions must not mandate a handle prefix (hq-dy6): %q", got.ReplyInstructions)
	}
}

// codex P1: the decoded mention must be durably spooled BEFORE the
// handler writes the 200 ack — Slack never redelivers after a 200, so
// an ack-then-async-spool order loses the message to a crash in the
// enrichment window. The gc stub never answers successfully, so the
// entry present right after the handler returns proves the write was
// synchronous with the request, not the delivery goroutine.
func TestHandleSlackEventsSpoolsBeforeAck(t *testing.T) {
	spool := t.TempDir()
	cfg := config{
		signingSecret: "s3cr3t",
		gcAPIBase:     "http://127.0.0.1:1", // refused: delivery cannot win the race
		cityName:      "test-city",
		provider:      "slack",
		workspaceID:   "T1",
		inboundTarget: "mayor",
		spoolDir:      spool,
	}
	raw, _ := json.Marshal(slackMessageEvent{
		Type: "app_mention", Channel: "C1", User: "U1", TS: "1.0",
		Text: "<@BOT> hello",
	})
	env, _ := json.Marshal(slackEventEnvelope{Type: "event_callback", Event: raw})
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/slack/events", strings.NewReader(string(env)))
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", signSlack(cfg.signingSecret, ts, env))
	rec := httptest.NewRecorder()
	handleSlackEvents(cfg)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	entries, err := os.ReadDir(spool)
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	var jsons, tmps int
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".json"):
			jsons++
		case strings.Contains(e.Name(), ".tmp"):
			tmps++
		}
	}
	if jsons != 1 {
		t.Errorf("spool entries at ack time = %d, want 1 (spool must precede the 200)", jsons)
	}
	if tmps != 0 {
		t.Errorf("atomic spool write left %d tmp files behind", tmps)
	}
}
