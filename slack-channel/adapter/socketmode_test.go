package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// scriptedConn is a socketConn that replays a fixed sequence of frames and
// records everything written back, so the envelope loop can be exercised
// without a server.
type scriptedConn struct {
	mu     sync.Mutex
	frames [][]byte
	next   int
	writes [][]byte
	closed bool
	// blockAfterScript, when set, makes Read block on ctx instead of
	// returning io.EOF once the script is exhausted — used to model a
	// connection that stays open.
	blockAfterScript bool
	// pingErr, when set, makes Ping fail — a wedged connection.
	pingErr error
	pings   int
}

func (s *scriptedConn) Read(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	if s.next < len(s.frames) {
		frame := s.frames[s.next]
		s.next++
		s.mu.Unlock()
		return frame, nil
	}
	s.mu.Unlock()
	if s.blockAfterScript {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, io.EOF
}

func (s *scriptedConn) Write(_ context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, append([]byte(nil), data...))
	return nil
}

func (s *scriptedConn) Ping(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pings++
	return s.pingErr
}

func (s *scriptedConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *scriptedConn) written() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.writes...)
}

// gcStub stands in for the gc API, signalling each bridged inbound message.
func gcStub(t *testing.T) (*httptest.Server, chan externalInboundMessage) {
	t.Helper()
	got := make(chan externalInboundMessage, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var wrap struct {
			Message externalInboundMessage `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&wrap); err != nil {
			t.Errorf("decode inbound: %v", err)
		}
		got <- wrap.Message
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, got
}

func appMentionEnvelope(t *testing.T, envelopeID, teamID, text string) []byte {
	t.Helper()
	event, err := json.Marshal(slackMessageEvent{
		Type:        "app_mention",
		User:        "U99",
		Text:        text,
		Channel:     "C42",
		TS:          "1700000000.0001",
		ChannelType: "channel",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	payload, err := json.Marshal(slackEventEnvelope{
		Type:   "event_callback",
		TeamID: teamID,
		Event:  event,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	frame, err := json.Marshal(socketEnvelope{
		Type:       "events_api",
		EnvelopeID: envelopeID,
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return frame
}

func TestConfigSocketModeSelector(t *testing.T) {
	regDir := t.TempDir()
	// Socket Mode does not consult the signing secret, so it must not be
	// required — that is the whole point of the ingress-free path.
	cfg, err := loadConfigFromEnv(func(key string) string {
		switch key {
		case "SLACK_APP_TOKEN":
			return "xapp-1-A-abc"
		case "SLACK_BOT_TOKEN":
			return "xoxb-abc"
		case "SLACK_WORKSPACE_ID":
			return "T123"
		case "GC_CITY_NAME":
			return "mycity"
		case "SLACK_CHANNEL_REGISTRY_DIR":
			return regDir
		}
		return ""
	})
	if err != nil {
		t.Fatalf("socket-mode config rejected: %v", err)
	}
	if !cfg.socketMode() {
		t.Error("socketMode() = false with SLACK_APP_TOKEN set")
	}

	// Without an app token the HTTP transport still demands its secret.
	_, err = loadConfigFromEnv(func(key string) string {
		switch key {
		case "SLACK_BOT_TOKEN":
			return "xoxb-abc"
		case "SLACK_WORKSPACE_ID":
			return "T123"
		case "GC_CITY_NAME":
			return "mycity"
		case "SLACK_CHANNEL_REGISTRY_DIR":
			return regDir
		}
		return ""
	})
	if err == nil || !strings.Contains(err.Error(), "SLACK_SIGNING_SECRET") {
		t.Errorf("http mode without signing secret: err = %v, want SLACK_SIGNING_SECRET required", err)
	}
}

func TestConfigRejectsBotTokenAsAppToken(t *testing.T) {
	regDir := t.TempDir()
	_, err := loadConfigFromEnv(func(key string) string {
		switch key {
		case "SLACK_APP_TOKEN":
			return "xoxb-not-an-app-token"
		case "SLACK_BOT_TOKEN":
			return "xoxb-abc"
		case "SLACK_WORKSPACE_ID":
			return "T123"
		case "GC_CITY_NAME":
			return "mycity"
		case "SLACK_CHANNEL_REGISTRY_DIR":
			return regDir
		}
		return ""
	})
	if err == nil || !strings.Contains(err.Error(), "xapp-") {
		t.Errorf("err = %v, want a message naming the xapp- prefix", err)
	}
}

func TestOpenSocketConnectionURL(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"url":"wss://wss-primary.slack.com/link/?ticket=abc"}`))
	}))
	defer srv.Close()

	url, err := openSocketConnectionURL(context.Background(), config{
		slackAPIBase: srv.URL,
		appToken:     "xapp-1-A-abc",
	})
	if err != nil {
		t.Fatalf("openSocketConnectionURL: %v", err)
	}
	if url != "wss://wss-primary.slack.com/link/?ticket=abc" {
		t.Errorf("url = %q", url)
	}
	// The app-level token, not the bot token, authenticates this call.
	if gotAuth != "Bearer xapp-1-A-abc" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if gotPath != "/apps.connections.open" {
		t.Errorf("path = %q", gotPath)
	}
}

func TestOpenSocketConnectionURLSurfacesScopeHint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"invalid_auth"}`))
	}))
	defer srv.Close()

	_, err := openSocketConnectionURL(context.Background(), config{
		slackAPIBase: srv.URL,
		appToken:     "xapp-1-A-abc",
	})
	if err == nil {
		t.Fatal("want error on ok:false")
	}
	// The overwhelmingly common cause is a missing scope; the message should
	// say so rather than leaving the operator with a bare invalid_auth.
	if !strings.Contains(err.Error(), "invalid_auth") || !strings.Contains(err.Error(), "connections:write") {
		t.Errorf("err = %v, want invalid_auth plus the connections:write hint", err)
	}
}

func TestPumpSocketAcksThenBridges(t *testing.T) {
	gc, inbound := gcStub(t)
	srv := newTestServer(t)
	srv.cfg.gcAPIBase = gc.URL
	conn := &scriptedConn{frames: [][]byte{
		[]byte(`{"type":"hello","num_connections":1}`),
		appMentionEnvelope(t, "env-1", "T123", "<@U0BOT> deploy please"),
	}}

	if err := srv.pumpSocket(context.Background(), conn, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("pumpSocket err = %v, want io.EOF at end of script", err)
	}

	writes := conn.written()
	if len(writes) != 1 {
		t.Fatalf("wrote %d frames, want exactly 1 ack (hello is not acked)", len(writes))
	}
	var ack map[string]string
	if err := json.Unmarshal(writes[0], &ack); err != nil {
		t.Fatalf("ack is not JSON: %v (%s)", err, writes[0])
	}
	if ack["envelope_id"] != "env-1" {
		t.Errorf("ack = %v, want envelope_id env-1", ack)
	}

	select {
	case msg := <-inbound:
		if msg.Text != "deploy please" {
			t.Errorf("bridged Text = %q, want the mention stripped", msg.Text)
		}
		if msg.ExplicitTarget != "mayor" {
			t.Errorf("bridged ExplicitTarget = %q", msg.ExplicitTarget)
		}
		// Tier 2 scopes the dedup key per delivery target, because one
		// message fans out to every session bound to the channel.
		if msg.DedupKey != "slack-1700000000.0001-mayor" {
			t.Errorf("bridged DedupKey = %q", msg.DedupKey)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event was acked but never bridged to gc")
	}
	if !conn.closed {
		t.Error("pumpSocket returned without closing the connection")
	}
}

func TestPumpSocketDrainsAfterDisconnect(t *testing.T) {
	gc, inbound := gcStub(t)
	srv := newTestServer(t)
	srv.cfg.gcAPIBase = gc.URL
	conn := &scriptedConn{frames: [][]byte{
		[]byte(`{"type":"disconnect","reason":"refresh_requested"}`),
		appMentionEnvelope(t, "env-2", "T123", "<@U0BOT> still here"),
	}}

	// A warned-about close is orderly, not an error: envelopes already in
	// flight are still acked, and the supervisor reconnects without logging
	// a failure.
	if err := srv.pumpSocket(context.Background(), conn, nil); err != nil {
		t.Fatalf("pumpSocket err = %v, want nil after a disconnect warning", err)
	}
	if writes := conn.written(); len(writes) != 1 {
		t.Errorf("wrote %d frames, want the in-flight envelope acked during drain", len(writes))
	}
	select {
	case <-inbound:
	case <-time.After(5 * time.Second):
		t.Fatal("envelope received during drain was never bridged")
	}
}

func TestPumpSocketSkipsUndecodableFrame(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.gcAPIBase = "http://127.0.0.1:1"
	conn := &scriptedConn{frames: [][]byte{[]byte(`{not json`)}}
	// A malformed frame must not kill the connection — the next envelope on
	// the same socket should still be read.
	if err := srv.pumpSocket(context.Background(), conn, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("pumpSocket err = %v, want the loop to continue to EOF", err)
	}
	if len(conn.written()) != 0 {
		t.Error("acked an undecodable frame")
	}
}

func TestPumpSocketStopsOnContextCancel(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.gcAPIBase = "http://127.0.0.1:1"
	conn := &scriptedConn{blockAfterScript: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.pumpSocket(ctx, conn, nil) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pumpSocket ignored context cancellation")
	}
}

func TestHandleSocketEnvelopeDropsForeignWorkspace(t *testing.T) {
	gc, inbound := gcStub(t)
	srv := newTestServer(t)
	srv.cfg.gcAPIBase = gc.URL

	var env socketEnvelope
	if err := json.Unmarshal(appMentionEnvelope(t, "env-3", "T_OTHER", "<@U0BOT> hi"), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Every bridged message is stamped with cfg.workspaceID as its account
	// id, so an event from another workspace would be filed under the wrong
	// account. Socket Mode has no signature to catch it — this check does.
	srv.handleSocketEnvelope(env)

	select {
	case msg := <-inbound:
		t.Fatalf("bridged an event from a foreign workspace: %+v", msg)
	case <-time.After(250 * time.Millisecond):
	}
}

func TestHandleSocketEnvelopeIgnoresNonEventTypes(t *testing.T) {
	gc, inbound := gcStub(t)
	srv := newTestServer(t)
	srv.cfg.gcAPIBase = gc.URL
	// Interactivity and slash commands belong to the larger tiers; Tier 1
	// must ignore them rather than misroute them as mentions.
	srv.handleSocketEnvelope(socketEnvelope{Type: "interactive", EnvelopeID: "e", Payload: json.RawMessage(`{}`)})
	select {
	case msg := <-inbound:
		t.Fatalf("bridged a non-events_api envelope: %+v", msg)
	case <-time.After(250 * time.Millisecond):
	}
}

func TestSocketAckEscapesEnvelopeID(t *testing.T) {
	// Envelope ids are Slack-supplied; a hand-concatenated ack would let a
	// crafted id break out of the JSON.
	ack := socketAck(`abc","injected":"x`)
	var decoded map[string]string
	if err := json.Unmarshal(ack, &decoded); err != nil {
		t.Fatalf("ack is not valid JSON: %v (%s)", err, ack)
	}
	if _, injected := decoded["injected"]; injected {
		t.Errorf("envelope id escaped its field: %s", ack)
	}
	if decoded["envelope_id"] != `abc","injected":"x` {
		t.Errorf("envelope_id = %q", decoded["envelope_id"])
	}
}

// TestSocketModeEndToEnd drives the real transport: a WebSocket server stands
// in for Slack, and the adapter negotiates, connects, acks, and bridges over
// an actual connection rather than the scripted stub.
func TestSocketModeEndToEnd(t *testing.T) {
	gc, inbound := gcStub(t)

	acked := make(chan string, 1)
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = c.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"hello"}`)); err != nil {
			return
		}
		if err := c.Write(ctx, websocket.MessageText, appMentionEnvelope(t, "env-e2e", "T123", "<@U0BOT> ship it")); err != nil {
			return
		}
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var ack map[string]string
		_ = json.Unmarshal(data, &ack)
		acked <- ack["envelope_id"]
	}))
	defer wsSrv.Close()

	// apps.connections.open hands back the test server's ws:// URL.
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		wsURL := "ws" + strings.TrimPrefix(wsSrv.URL, "http")
		_, _ = w.Write([]byte(`{"ok":true,"url":"` + wsURL + `"}`))
	}))
	defer apiSrv.Close()

	srv := newTestServer(t)
	srv.cfg.gcAPIBase = gc.URL
	srv.cfg.slackAPIBase = apiSrv.URL
	srv.cfg.appToken = "xapp-1-A-abc"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := srv.runSocketConnection(ctx, nil); err != nil {
		t.Fatalf("runSocketConnection: %v", err)
	}

	select {
	case id := <-acked:
		if id != "env-e2e" {
			t.Errorf("ack envelope_id = %q, want env-e2e", id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no ack reached the server over a real connection")
	}
	select {
	case msg := <-inbound:
		if msg.Text != "ship it" {
			t.Errorf("bridged Text = %q", msg.Text)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("event never bridged to gc")
	}
}

// TestRunSocketModeReconnectsAfterDisconnect exercises the supervisor over a
// real transport: the first connection is told to go away, and the adapter
// must dial a second one and keep bridging. Reconnect is the steady state in
// Socket Mode — Slack recycles connections routinely — so this is the path
// that matters most in production.
func TestRunSocketModeReconnectsAfterDisconnect(t *testing.T) {
	gc, inbound := gcStub(t)

	var mu sync.Mutex
	connections := 0
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		mu.Lock()
		connections++
		n := connections
		mu.Unlock()

		if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"hello"}`)); err != nil {
			return
		}
		if n == 1 {
			// Warn, then go away as Slack does after its grace period.
			_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"disconnect","reason":"refresh_requested"}`))
			return
		}
		// The replacement connection carries the event that must not be lost.
		_ = c.Write(ctx, websocket.MessageText, appMentionEnvelope(t, "env-after-reconnect", "T123", "<@U0BOT> after reconnect"))
		<-ctx.Done()
	}))
	defer wsSrv.Close()

	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"url":"ws` + strings.TrimPrefix(wsSrv.URL, "http") + `"}`))
	}))
	defer apiSrv.Close()

	srv := newTestServer(t)
	srv.cfg.gcAPIBase = gc.URL
	srv.cfg.slackAPIBase = apiSrv.URL
	srv.cfg.appToken = "xapp-1-A-abc"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.runSocketMode(ctx) }()

	select {
	case msg := <-inbound:
		if msg.Text != "after reconnect" {
			t.Errorf("bridged Text = %q", msg.Text)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("adapter never reconnected after the disconnect")
	}
}

// TestPumpSocketSurvivesIdleConnection is a regression test. An earlier
// revision bounded each Read with a 90s deadline as an idle watchdog, which
// tore down healthy connections on any quiet workspace: Slack's keepalives
// are WebSocket pings, answered inside the library without ever waking a
// read, so silence is the normal state of a working connection.
func TestPumpSocketSurvivesIdleConnection(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.gcAPIBase = "http://127.0.0.1:1"
	conn := &scriptedConn{blockAfterScript: true} // never sends anything
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- srv.pumpSocket(ctx, conn, nil) }()

	select {
	case err := <-done:
		t.Fatalf("pumpSocket gave up on an idle but healthy connection: %v", err)
	case <-time.After(500 * time.Millisecond):
		// Still reading, which is correct.
	}
}

func TestKeepaliveClosesWedgedConnection(t *testing.T) {
	conn := &scriptedConn{blockAfterScript: true, pingErr: errors.New("no pong")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closed := make(chan struct{})
	var once sync.Once
	go keepaliveSocket(ctx, conn, time.Millisecond, func() { once.Do(func() { close(closed) }) })

	// A connection Slack has stopped answering must be torn down rather than
	// read from forever — the failure mode a NAT or proxy timeout produces.
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("unanswered ping did not close the connection")
	}
}

func TestKeepaliveStopsWithContext(t *testing.T) {
	conn := &scriptedConn{blockAfterScript: true}
	ctx, cancel := context.WithCancel(context.Background())
	closedCount := 0
	done := make(chan struct{})
	go func() {
		keepaliveSocket(ctx, conn, time.Millisecond, func() { closedCount++ })
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("keepalive ignored context cancellation")
	}
	// Healthy pings must never trip the close path.
	if closedCount != 0 {
		t.Errorf("closeConn called %d times on a healthy connection", closedCount)
	}
	if conn.pings == 0 {
		t.Error("keepalive never pinged")
	}
}

func TestPumpSocketWarnsBeforeDraining(t *testing.T) {
	gc, _ := gcStub(t)
	srv := newTestServer(t)
	srv.cfg.gcAPIBase = gc.URL
	conn := &scriptedConn{frames: [][]byte{
		[]byte(`{"type":"disconnect","reason":"warning"}`),
		[]byte(`{"type":"disconnect","reason":"warning"}`),
	}}

	warnings := 0
	if err := srv.pumpSocket(context.Background(), conn, func() { warnings++ }); err != nil {
		t.Fatalf("pumpSocket err = %v", err)
	}
	// The warning must fire so the supervisor can dial a replacement while
	// this connection is still draining — that overlap is the point of
	// Slack's early warning. A repeat warning is not a second reconnect.
	if warnings != 1 {
		t.Errorf("onWarning fired %d times, want exactly 1", warnings)
	}
}
