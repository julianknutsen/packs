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
		}
		return ""
	})
	if err == nil || !strings.Contains(err.Error(), "SLACK_SIGNING_SECRET") {
		t.Errorf("http mode without signing secret: err = %v, want SLACK_SIGNING_SECRET required", err)
	}
}

func TestConfigRejectsBotTokenAsAppToken(t *testing.T) {
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
	cfg := config{
		gcAPIBase:     gc.URL,
		cityName:      "mycity",
		provider:      "slack",
		workspaceID:   "T123",
		inboundTarget: "mayor",
	}
	conn := &scriptedConn{frames: [][]byte{
		[]byte(`{"type":"hello","num_connections":1}`),
		appMentionEnvelope(t, "env-1", "T123", "<@U0BOT> deploy please"),
	}}

	if err := pumpSocket(context.Background(), cfg, defaultSocketTimings(), conn, nil); !errors.Is(err, io.EOF) {
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
		if msg.DedupKey != "slack-1700000000.0001" {
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
	cfg := config{gcAPIBase: gc.URL, cityName: "c", workspaceID: "T123", inboundTarget: "mayor"}
	conn := &scriptedConn{frames: [][]byte{
		[]byte(`{"type":"disconnect","reason":"refresh_requested"}`),
		appMentionEnvelope(t, "env-2", "T123", "<@U0BOT> still here"),
	}}

	// A warned-about close is orderly, not an error: envelopes already in
	// flight are still acked, and the supervisor reconnects without logging
	// a failure.
	if err := pumpSocket(context.Background(), cfg, defaultSocketTimings(), conn, nil); err != nil {
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
	cfg := config{gcAPIBase: "http://127.0.0.1:1", cityName: "c", workspaceID: "T123"}
	conn := &scriptedConn{frames: [][]byte{[]byte(`{not json`)}}
	// A malformed frame must not kill the connection — the next envelope on
	// the same socket should still be read.
	if err := pumpSocket(context.Background(), cfg, defaultSocketTimings(), conn, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("pumpSocket err = %v, want the loop to continue to EOF", err)
	}
	if len(conn.written()) != 0 {
		t.Error("acked an undecodable frame")
	}
}

func TestPumpSocketStopsOnContextCancel(t *testing.T) {
	cfg := config{gcAPIBase: "http://127.0.0.1:1", cityName: "c", workspaceID: "T123"}
	conn := &scriptedConn{blockAfterScript: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pumpSocket(ctx, cfg, defaultSocketTimings(), conn, nil) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pumpSocket ignored context cancellation")
	}
}

func TestHandleSocketEnvelopeDropsForeignWorkspace(t *testing.T) {
	gc, inbound := gcStub(t)
	cfg := config{gcAPIBase: gc.URL, cityName: "c", workspaceID: "T123", inboundTarget: "mayor"}

	var env socketEnvelope
	if err := json.Unmarshal(appMentionEnvelope(t, "env-3", "T_OTHER", "<@U0BOT> hi"), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// The guard itself lives in bridgeEvent, shared with the HTTP path (see
	// TestBridgeEventWorkspaceGuard); this pins that the socket path actually
	// reaches it, since Socket Mode has no signature to fall back on.
	handleSocketEnvelope(cfg, env)

	select {
	case msg := <-inbound:
		t.Fatalf("bridged an event from a foreign workspace: %+v", msg)
	case <-time.After(250 * time.Millisecond):
	}
}

func TestHandleSocketEnvelopeIgnoresNonEventTypes(t *testing.T) {
	gc, inbound := gcStub(t)
	cfg := config{gcAPIBase: gc.URL, cityName: "c", workspaceID: "T123", inboundTarget: "mayor"}
	// Interactivity and slash commands belong to the larger tiers; Tier 1
	// must ignore them rather than misroute them as mentions.
	handleSocketEnvelope(cfg, socketEnvelope{Type: "interactive", EnvelopeID: "e", Payload: json.RawMessage(`{}`)})
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

	cfg := config{
		gcAPIBase:     gc.URL,
		cityName:      "mycity",
		provider:      "slack",
		workspaceID:   "T123",
		inboundTarget: "mayor",
		slackAPIBase:  apiSrv.URL,
		appToken:      "xapp-1-A-abc",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := runSocketConnection(ctx, cfg, defaultSocketTimings(), nil); err != nil {
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

	cfg := config{
		gcAPIBase:     gc.URL,
		cityName:      "mycity",
		workspaceID:   "T123",
		inboundTarget: "mayor",
		slackAPIBase:  apiSrv.URL,
		appToken:      "xapp-1-A-abc",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runSocketMode(ctx, cfg) }()

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
	cfg := config{gcAPIBase: "http://127.0.0.1:1", cityName: "c", workspaceID: "T123"}
	conn := &scriptedConn{blockAfterScript: true} // never sends anything
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- pumpSocket(ctx, cfg, defaultSocketTimings(), conn, nil) }()

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
	cfg := config{gcAPIBase: gc.URL, cityName: "c", workspaceID: "T123", inboundTarget: "mayor"}
	conn := &scriptedConn{frames: [][]byte{
		[]byte(`{"type":"disconnect","reason":"warning"}`),
		[]byte(`{"type":"disconnect","reason":"warning"}`),
	}}

	warnings := 0
	if err := pumpSocket(context.Background(), cfg, defaultSocketTimings(), conn, func() { warnings++ }); err != nil {
		t.Fatalf("pumpSocket err = %v", err)
	}
	// The warning must fire so the supervisor can dial a replacement while
	// this connection is still draining — that overlap is the point of
	// Slack's early warning. A repeat warning is not a second reconnect.
	if warnings != 1 {
		t.Errorf("onWarning fired %d times, want exactly 1", warnings)
	}
}

// --- the timing layer ------------------------------------------------------
//
// The tests below reach the connection-lifecycle timers through socketTimings.
// They exist because wall-clock waiting cannot express these properties: an
// earlier idle test waited 500ms against a 90s deadline and so passed on the
// very bug it was written for, and nothing pinned that a connection runs a
// keepalive, that a drain is bounded, or that reconnects are paced.

// deadlineProbeConn records whether the context pumpSocket reads with carries
// a deadline. It never yields a frame, modelling a healthy but idle Slack
// connection.
type deadlineProbeConn struct {
	mu       sync.Mutex
	hasDL    bool
	pings    int
	readOnce chan struct{}
}

func (d *deadlineProbeConn) Read(ctx context.Context) ([]byte, error) {
	_, ok := ctx.Deadline()
	d.mu.Lock()
	d.hasDL = ok
	d.mu.Unlock()
	select {
	case d.readOnce <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (d *deadlineProbeConn) Write(context.Context, []byte) error { return nil }

func (d *deadlineProbeConn) Ping(context.Context) error {
	d.mu.Lock()
	d.pings++
	d.mu.Unlock()
	return nil
}

func (d *deadlineProbeConn) Close() error { return nil }

// TestPumpSocketReadsWithoutDeadline pins the mechanism behind the idle fix:
// an idle Socket Mode connection is healthy, so reads must block on a context
// no timer will expire. Waiting a wall-clock interval cannot express this —
// the deadline that caused the outage was 90s — whereas the absence of any
// deadline is checkable in one read.
func TestPumpSocketReadsWithoutDeadline(t *testing.T) {
	cfg := config{gcAPIBase: "http://127.0.0.1:1", cityName: "c", workspaceID: "T123"}
	conn := &deadlineProbeConn{readOnce: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = pumpSocket(ctx, cfg, defaultSocketTimings(), conn, nil) }()

	select {
	case <-conn.readOnce:
	case <-time.After(5 * time.Second):
		t.Fatal("pumpSocket never issued a read")
	}
	conn.mu.Lock()
	hasDL := conn.hasDL
	conn.mu.Unlock()
	if hasDL {
		t.Fatal("pumpSocket bounded its read with a deadline; idle connections are torn down on that timer")
	}
}

// TestPumpSocketTearsDownWedgedConnection is the other half of the liveness
// design, and the half that needed the injectable interval: a connection Slack
// has stopped answering must be abandoned. TestKeepaliveClosesWedgedConnection
// calls keepaliveSocket directly, so it stays green if pumpSocket never starts
// one; this drives the production wiring instead.
func TestPumpSocketTearsDownWedgedConnection(t *testing.T) {
	tm := defaultSocketTimings()
	tm.pingInterval = time.Millisecond

	cfg := config{gcAPIBase: "http://127.0.0.1:1", cityName: "c", workspaceID: "T123"}
	// Reads block forever, as a wedged connection does: only the keepalive can
	// end this pump.
	conn := &scriptedConn{blockAfterScript: true, pingErr: errors.New("no pong")}

	done := make(chan error, 1)
	go func() { done <- pumpSocket(context.Background(), cfg, tm, conn, nil) }()

	select {
	case err := <-done:
		if err == nil {
			t.Error("pumpSocket reported a clean end for a wedged connection")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pumpSocket read forever from a connection Slack had stopped answering")
	}
	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if !closed {
		t.Error("wedged connection was left open")
	}
}

// TestPumpSocketBoundsDrain pins the drain bound. The existing drain test ends
// because its scripted frames run out and Read returns io.EOF, so it never
// exercises the timer; here Read never returns on its own, which leaves the
// AfterFunc as the only way out.
func TestPumpSocketBoundsDrain(t *testing.T) {
	tm := defaultSocketTimings()
	tm.drainGrace = 25 * time.Millisecond

	gc, _ := gcStub(t)
	cfg := config{gcAPIBase: gc.URL, cityName: "c", workspaceID: "T123", inboundTarget: "mayor"}
	conn := &scriptedConn{
		frames:           [][]byte{[]byte(`{"type":"disconnect","reason":"warning"}`)},
		blockAfterScript: true,
	}

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- pumpSocket(context.Background(), cfg, tm, conn, nil) }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("drain ended with err = %v, want nil (an elapsed drain is expected, not a failure)", err)
		}
		if elapsed := time.Since(start); elapsed < tm.drainGrace {
			t.Errorf("drain ended after %s, before the %s grace — in-flight envelopes lose their ack window", elapsed, tm.drainGrace)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("a warned connection whose close never landed drained past %s and would linger beside its replacement", tm.drainGrace)
	}
}

// TestDefaultSocketTimingsMatchConstants pins the production timer set to
// literal durations rather than to the constants it is built from. Every other
// test in this section injects scaled-down timings, so without this pin a
// production interval could drift — or be zeroed — with the whole suite green.
// Changing a timer here is fine; it just has to be visible in the diff.
func TestDefaultSocketTimingsMatchConstants(t *testing.T) {
	want := socketTimings{
		pingInterval: 30 * time.Second,
		drainGrace:   10 * time.Second,
		backoffMin:   1 * time.Second,
		backoffMax:   30 * time.Second,
		minLifetime:  30 * time.Second,
	}
	if got := defaultSocketTimings(); got != want {
		t.Errorf("defaultSocketTimings() = %+v, want %+v", got, want)
	}
}

func TestSettleBackoffResetsOnlyAfterMinLifetime(t *testing.T) {
	tm := socketTimings{backoffMin: time.Second, backoffMax: 30 * time.Second, minLifetime: 30 * time.Second}
	for _, tc := range []struct {
		name     string
		backoff  time.Duration
		lifetime time.Duration
		want     time.Duration
	}{
		// A connection that proved the endpoint healthy earns its successor
		// the floor again — this is the ordinary Slack refresh.
		{"outlived the minimum", 8 * time.Second, 30 * time.Second, time.Second},
		{"long-lived", 30 * time.Second, time.Hour, time.Second},
		// Dial-OK then instant death is the case a reset-on-establishment
		// misreads as health, redialling at the floor forever.
		{"died immediately", 8 * time.Second, 0, 8 * time.Second},
		{"just short of the minimum", 8 * time.Second, 29 * time.Second, 8 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := settleBackoff(tc.backoff, tc.lifetime, tm); got != tc.want {
				t.Errorf("settleBackoff(%s, %s) = %s, want %s", tc.backoff, tc.lifetime, got, tc.want)
			}
		})
	}
}

func TestNextBackoffDoublesToCap(t *testing.T) {
	tm := socketTimings{backoffMin: time.Second, backoffMax: 30 * time.Second}
	for _, tc := range []struct{ in, want time.Duration }{
		{time.Second, 2 * time.Second},
		{8 * time.Second, 16 * time.Second},
		{16 * time.Second, 30 * time.Second}, // 32s clamped
		{30 * time.Second, 30 * time.Second},
	} {
		if got := nextBackoff(tc.in, tm); got != tc.want {
			t.Errorf("nextBackoff(%s) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// TestRunSocketModePacesWarnedReconnect drives the supervisor over a real
// transport against a server that warns every connection the moment it opens —
// the shape two adapters sharing one app token produce as they displace each
// other. That path used to redial at network speed, one apps.connections.open
// plus handshake per cycle, because it was the only reconnect arm with no
// delay and because backoff reset on every successful dial.
func TestRunSocketModePacesWarnedReconnect(t *testing.T) {
	tm := socketTimings{
		backoffMin: 40 * time.Millisecond,
		backoffMax: 5 * time.Second,
		// No connection here reaches minLifetime, so each successor must carry
		// the escalating delay: this is what distinguishes pacing from a reset
		// on every dial.
		minLifetime: time.Hour,
		// Neither timer should fire during the test; the connections are held
		// open by the server, not ended by a keepalive or an elapsed drain.
		pingInterval: time.Hour,
		drainGrace:   time.Hour,
	}

	var mu sync.Mutex
	var dials []time.Time
	stop := make(chan struct{})
	dialed := make(chan struct{}, 16)

	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		dials = append(dials, time.Now())
		mu.Unlock()
		select {
		case dialed <- struct{}{}:
		default:
		}

		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"hello"}`)); err != nil {
			return
		}
		if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"disconnect","reason":"link_disabled"}`)); err != nil {
			return
		}
		// Hold the connection open. If it ended here, the supervisor could take
		// its `done` arm instead of the warned arm — both are paced now, so the
		// test would pass without ever measuring the path it is about.
		select {
		case <-stop:
		case <-ctx.Done():
		}
	}))
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"url":"ws` + strings.TrimPrefix(wsSrv.URL, "http") + `"}`))
	}))

	cfg := config{
		gcAPIBase:     "http://127.0.0.1:1",
		cityName:      "mycity",
		workspaceID:   "T123",
		inboundTarget: "mayor",
		slackAPIBase:  apiSrv.URL,
		appToken:      "xapp-1-A-abc",
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Unwind inside out: release the handlers, stop the supervisor redialling,
	// then close the servers, so nothing touches t after this test returns.
	defer wsSrv.Close()
	defer apiSrv.Close()
	defer cancel()
	defer close(stop)

	go func() { _ = runSocketModeWithTimings(ctx, cfg, tm) }()

	const wantDials = 4
	for i := 0; i < wantDials; i++ {
		select {
		case <-dialed:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d dials arrived", i, wantDials)
		}
	}

	mu.Lock()
	got := append([]time.Time(nil), dials...)
	mu.Unlock()
	if len(got) < wantDials {
		t.Fatalf("recorded %d dials, want %d", len(got), wantDials)
	}

	gaps := make([]time.Duration, 0, len(got)-1)
	for i := 1; i < len(got); i++ {
		gaps = append(gaps, got[i].Sub(got[i-1]))
	}
	for i, gap := range gaps {
		if gap < tm.backoffMin {
			t.Errorf("redial %d came %s after the previous one, inside the %s floor: gaps=%v", i+1, gap, tm.backoffMin, gaps)
		}
	}
	// Doubling from a 40ms floor gives ~40ms, ~80ms, ~160ms. A backoff that
	// reset on every dial would hold every gap near the floor, so requiring
	// the last one to exceed 3x it is what pins the escalation.
	if last := gaps[len(gaps)-1]; last < 3*tm.backoffMin {
		t.Errorf("last redial gap %s did not escalate past 3x the %s floor: gaps=%v", last, tm.backoffMin, gaps)
	}
}
