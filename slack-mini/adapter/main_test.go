package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// TestBridgeEventWorkspaceGuard pins the workspace check at the funnel both
// transports feed. Neither transport establishes which workspace an event
// belongs to — Socket Mode has no signature, and the HTTP path's HMAC only
// proves Slack sent it for this app — while every bridged message is stamped
// with cfg.workspaceID, so an app installed twice would misfile the second
// workspace's mentions. Guarding here means neither transport can miss it.
func TestBridgeEventWorkspaceGuard(t *testing.T) {
	bridged := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		bridged = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	cfg := config{gcAPIBase: srv.URL, cityName: "c", workspaceID: "T123", inboundTarget: "mayor"}

	raw, err := json.Marshal(slackMessageEvent{
		Type: "app_mention", User: "U1", Text: "<@U0BOT> hi", Channel: "C1", TS: "1",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	for _, tc := range []struct {
		name     string
		teamID   string
		wantSent bool
	}{
		{"foreign workspace", "T_OTHER", false},
		{"configured workspace", "T123", true},
		// Slack sends team_id on every event_callback, so an absent one means a
		// hand-built payload rather than a foreign workspace; the guard must not
		// silently swallow those.
		{"absent team_id", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bridged = false
			bridgeEvent(cfg, slackEventEnvelope{Type: "event_callback", TeamID: tc.teamID, Event: raw})
			if bridged != tc.wantSent {
				t.Errorf("bridged = %v, want %v (team_id=%q, workspace=%q)", bridged, tc.wantSent, tc.teamID, cfg.workspaceID)
			}
		})
	}
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
