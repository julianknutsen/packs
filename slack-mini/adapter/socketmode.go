// Socket Mode transport — the ingress-free alternative to the public
// /slack/events listener.
//
// Socket Mode replaces Slack's inbound HTTP webhook with an outbound
// WebSocket the adapter opens to Slack. Nothing listens on a public port and
// no TLS terminator or tunnel is needed, which is the only way to run this
// pack on a network that cannot accept inbound connections.
//
// The transport is selected by SLACK_APP_TOKEN: when it is set the adapter
// runs Socket Mode and never binds LISTEN_PUBLIC; when it is empty the
// adapter keeps the original Events-API-over-HTTP behaviour unchanged. The
// two are alternatives, never both at once — Slack itself refuses to deliver
// events to a Request URL while Socket Mode is enabled on the app.
//
// Wire protocol (https://api.slack.com/apis/socket-mode):
//
//  1. POST apps.connections.open with the app-level token → a single-use
//     wss:// URL, valid for ~30s.
//  2. Dial it. Slack sends {"type":"hello"} on success.
//  3. Each event arrives as an envelope: {"envelope_id":…,"type":"events_api",
//     "payload":{…}}. The payload is byte-identical to the HTTP webhook body,
//     which is why this file reuses bridgeEvent unchanged.
//  4. Ack every envelope with {"envelope_id":…} within 3s or Slack redelivers.
//  5. {"type":"disconnect"} warns the connection is going away, ~10s before
//     it closes, so a replacement can be opened before events are missed.
//
// Authenticity differs from the HTTP path and is worth being explicit about:
// there is no signing secret and no HMAC. The trust boundary is the TLS
// connection the adapter itself opened using its app-level token — an
// attacker cannot inject envelopes without already holding that token. This
// is Slack's designed model for Socket Mode, not a relaxation of the HTTP
// path, which keeps verifying signatures exactly as before.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

const (
	// socketAckTimeout bounds the ack write. Slack redelivers an envelope
	// that is not acked within 3s, so a write that cannot complete well
	// inside that window is better abandoned than left blocking the reader.
	socketAckTimeout = 2 * time.Second

	// Liveness is checked by pinging Slack, not by bounding reads. A healthy
	// Socket Mode connection can sit idle indefinitely — no mentions means no
	// data messages — and Slack's own keepalive pings are answered inside the
	// WebSocket library without ever waking a read. A read deadline would
	// therefore tear down perfectly good connections on any quiet workspace.
	// A ping that goes unanswered is the real signal that a connection is
	// wedged in a way TCP has not noticed yet.
	socketPingInterval = 30 * time.Second
	socketPingTimeout  = 10 * time.Second

	// socketOpenTimeout bounds apps.connections.open plus the WebSocket
	// handshake. The URL Slack returns expires in ~30s.
	socketOpenTimeout = 30 * time.Second

	// Reconnect backoff after a failed or lost connection. Reset once a
	// connection has lasted socketMinLifetime (see settleBackoff).
	socketBackoffMin = 1 * time.Second
	socketBackoffMax = 30 * time.Second

	// socketMinLifetime is how long a connection must survive before its
	// successor is allowed to start from socketBackoffMin again. Resetting on
	// establishment alone lets a server that accepts a connection and drops or
	// disowns it immediately be redialled at the floor forever; making a
	// connection prove itself first is what escalates the delay in exactly that
	// case. Slack recycles healthy connections on a far longer cadence, so
	// ordinary refreshes still reset the backoff.
	socketMinLifetime = 30 * time.Second

	// socketDrainGrace is how long a connection that announced a disconnect
	// keeps reading after its replacement is live, so in-flight envelopes
	// still get acked instead of being redelivered.
	socketDrainGrace = 10 * time.Second

	// maxSocketMessage caps a single inbound WebSocket message, mirroring
	// maxInboundBody on the HTTP path.
	maxSocketMessage = 1 << 20 // 1 MiB
)

// socketTimings groups the connection-lifecycle timers. Production passes
// defaultSocketTimings(); tests pass a scaled-down copy so the same code paths
// can be driven in milliseconds.
//
// The seam exists because these timers are the layer no test could otherwise
// reach: read at their call sites, they made it impossible to pin that
// pumpSocket actually runs a keepalive, that a drain is bounded, or that
// reconnects are paced — each of which stayed green through the mutation that
// removed it. socketPingTimeout, socketAckTimeout and socketOpenTimeout stay
// constants deliberately: nothing drives them, and a knob no test turns is
// only a wider surface.
type socketTimings struct {
	pingInterval time.Duration
	drainGrace   time.Duration
	backoffMin   time.Duration
	backoffMax   time.Duration
	// minLifetime is how long a connection must last before its successor is
	// allowed to start from backoffMin again.
	minLifetime time.Duration
}

// defaultSocketTimings is the production timer set.
func defaultSocketTimings() socketTimings {
	return socketTimings{
		pingInterval: socketPingInterval,
		drainGrace:   socketDrainGrace,
		backoffMin:   socketBackoffMin,
		backoffMax:   socketBackoffMax,
		minLifetime:  socketMinLifetime,
	}
}

// socketEnvelope is the Socket Mode frame wrapping each delivery. Payload is
// the same JSON body the Events API would have POSTed to /slack/events.
type socketEnvelope struct {
	Type         string          `json:"type"`
	EnvelopeID   string          `json:"envelope_id,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	RetryAttempt int             `json:"retry_attempt,omitempty"`
}

// socketConn is the WebSocket surface this file needs, extracted so the
// envelope loop can be tested against a scripted connection without standing
// up a server. realSocketConn is the only production implementation.
type socketConn interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, data []byte) error
	// Ping sends a WebSocket ping and waits for the matching pong, which is
	// how liveness is established on a connection that may legitimately carry
	// no data for hours.
	Ping(ctx context.Context) error
	Close() error
}

type realSocketConn struct{ c *websocket.Conn }

func (r realSocketConn) Read(ctx context.Context) ([]byte, error) {
	_, data, err := r.c.Read(ctx)
	return data, err
}

func (r realSocketConn) Write(ctx context.Context, data []byte) error {
	return r.c.Write(ctx, websocket.MessageText, data)
}

func (r realSocketConn) Ping(ctx context.Context) error { return r.c.Ping(ctx) }

func (r realSocketConn) Close() error {
	// CloseNow over a handshaked close: the peer is Slack and the connection
	// is being discarded either way, so waiting on a close reply only delays
	// the redial.
	return r.c.CloseNow()
}

// runSocketMode supervises the Socket Mode connection until ctx is done,
// redialing with capped exponential backoff. It returns only when ctx is
// cancelled; every other failure is a reconnect, because losing the socket
// is the expected steady state (Slack recycles connections routinely) rather
// than a fatal condition.
func runSocketMode(ctx context.Context, cfg config) error {
	return runSocketModeWithTimings(ctx, cfg, defaultSocketTimings())
}

// runSocketModeWithTimings is runSocketMode with its timer set injected, so
// the reconnect lifecycle can be driven in a test without waiting on
// production intervals.
func runSocketModeWithTimings(ctx context.Context, cfg config, tm socketTimings) error {
	backoff := tm.backoffMin
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		next, err := superviseConnection(ctx, cfg, tm, backoff)
		if err != nil {
			return err
		}
		backoff = next
	}
}

// superviseConnection runs exactly one connection's lifecycle: dial it, wait
// for it to end (or to be warned about), pace the delay before the next dial,
// and return the backoff that dial should start from. A non-nil error means the
// supervisor should stop — only shutdown produces one, which is what makes
// runSocketModeWithTimings' loop return solely on cancellation.
func superviseConnection(ctx context.Context, cfg config, tm socketTimings, backoff time.Duration) (time.Duration, error) {
	warned := make(chan struct{}, 1)
	done, err := runSocketConnection(ctx, cfg, tm, func() {
		select {
		case warned <- struct{}{}:
		default: // already warned; one signal is enough
		}
	})
	if err != nil {
		if ctx.Err() != nil {
			return backoff, ctx.Err()
		}
		log.Printf("socket mode: connect failed: %v (retrying in %s)", err, backoff)
		return paceRedial(ctx, backoff, tm)
	}
	connectedAt := time.Now()

	select {
	case <-ctx.Done():
		return backoff, ctx.Err()

	case <-warned:
		// Slack warned this socket is closing, ~10s ahead. The replacement
		// is dialled while the old connection keeps draining and acking in
		// its own goroutine — that overlap is the entire reason Slack sends
		// the warning early. The drained connection closes itself; nothing
		// here needs to wait for it.
		//
		// The dial is paced all the same. A connection that lasted a normal
		// lifetime resets the delay to backoffMin first, which fits inside
		// the warning's ~10s lead and keeps the overlap; only a connection
		// warned moments after it opened — the connection-limit churn two
		// adapters sharing one app token produce — carries an escalating
		// delay here, and that is precisely the case this path used to
		// redial at network speed.
		backoff = settleBackoff(backoff, time.Since(connectedAt), tm)
		log.Printf("socket mode: opening replacement connection in %s", backoff)
		return paceRedial(ctx, backoff, tm)

	case connErr := <-done:
		if ctx.Err() != nil {
			return backoff, ctx.Err()
		}
		backoff = settleBackoff(backoff, time.Since(connectedAt), tm)
		if connErr != nil {
			log.Printf("socket mode: connection ended: %v (reconnecting in %s)", connErr, backoff)
		} else {
			log.Printf("socket mode: connection closed (reconnecting in %s)", backoff)
		}
		return paceRedial(ctx, backoff, tm)
	}
}

// paceRedial waits out the current delay and returns the delay the dial after
// it should use. Shutdown during the wait is reported as an error, which stops
// the supervisor rather than redialling into a cancelled context.
func paceRedial(ctx context.Context, backoff time.Duration, tm socketTimings) (time.Duration, error) {
	if waitErr := sleepFor(ctx, backoff); waitErr != nil {
		return backoff, waitErr
	}
	return nextBackoff(backoff, tm), nil
}

// settleBackoff picks the delay before the next dial from how long the
// connection that just ended lasted. A connection that survived
// tm.minLifetime proved the endpoint healthy, so its successor starts from the
// floor again; one that died sooner keeps the escalating delay, which is what
// stops a dial-OK → instant-death cycle from redialling at the floor forever.
func settleBackoff(backoff, lifetime time.Duration, tm socketTimings) time.Duration {
	if lifetime >= tm.minLifetime {
		return tm.backoffMin
	}
	return backoff
}

// nextBackoff doubles the reconnect delay, capped.
func nextBackoff(backoff time.Duration, tm socketTimings) time.Duration {
	backoff *= 2
	if backoff > tm.backoffMax {
		return tm.backoffMax
	}
	return backoff
}

// sleepFor waits for d, returning early with ctx.Err() if the context is
// cancelled first.
func sleepFor(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// runSocketConnection dials one connection and pumps it in the background,
// returning a channel that yields the pump's outcome. Dialing is synchronous
// so the caller can distinguish "could not connect" (back off) from "was
// connected and then ended" (reconnect promptly); pumping is not, so the
// caller can react to a disconnect warning without waiting for the drain.
// onWarning fires when Slack announces an impending close.
func runSocketConnection(ctx context.Context, cfg config, tm socketTimings, onWarning func()) (<-chan error, error) {
	conn, err := dialSocketMode(ctx, cfg)
	if err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- pumpSocket(ctx, cfg, tm, conn, onWarning) }()
	return done, nil
}

// dialSocketMode negotiates a connection URL and opens it.
func dialSocketMode(ctx context.Context, cfg config) (socketConn, error) {
	openCtx, cancel := context.WithTimeout(ctx, socketOpenTimeout)
	defer cancel()

	wsURL, err := openSocketConnectionURL(openCtx, cfg)
	if err != nil {
		return nil, err
	}
	// Dial through http.DefaultClient rather than a bare TLS dial so
	// HTTP_PROXY/HTTPS_PROXY are honoured — the adapter is expected to run
	// on exactly the kind of restricted network that needs an egress proxy.
	c, _, err := websocket.Dial(openCtx, wsURL, &websocket.DialOptions{
		HTTPClient: http.DefaultClient,
	})
	if err != nil {
		return nil, fmt.Errorf("dial socket mode: %w", err)
	}
	c.SetReadLimit(maxSocketMessage)
	log.Printf("socket mode: connected")
	return realSocketConn{c: c}, nil
}

// appsConnectionsOpenResp is the apps.connections.open reply.
type appsConnectionsOpenResp struct {
	OK    bool   `json:"ok"`
	URL   string `json:"url,omitempty"`
	Error string `json:"error,omitempty"`
}

// openSocketConnectionURL calls apps.connections.open, which is the only
// endpoint that accepts the app-level (xapp-) token rather than the bot
// token.
func openSocketConnectionURL(ctx context.Context, cfg config) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.slackAPIBase+"/apps.connections.open", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.appToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("apps.connections.open: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSocketMessage))
	if err != nil {
		return "", fmt.Errorf("read apps.connections.open response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("apps.connections.open http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out appsConnectionsOpenResp
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("decode apps.connections.open response: %w", err)
	}
	if !out.OK {
		// invalid_auth here almost always means the token is a bot token or
		// is missing the connections:write scope; say so rather than making
		// the operator look it up.
		return "", fmt.Errorf("apps.connections.open: %s (app-level token needs the connections:write scope)", out.Error)
	}
	if out.URL == "" {
		return "", errors.New("apps.connections.open returned no url")
	}
	return out.URL, nil
}

// pumpSocket reads envelopes until the connection ends, acking each one and
// bridging events through the same path the HTTP listener uses.
func pumpSocket(ctx context.Context, cfg config, tm socketTimings, conn socketConn, onWarning func()) error {
	defer func() { _ = conn.Close() }()

	// connCtx bounds this connection specifically: the keepalive cancels it
	// when Slack stops answering pings, and the drain timer cancels it when a
	// warned-about connection has had long enough to finish. Reads block on
	// it with no deadline of their own — see socketPingInterval.
	connCtx, closeConn := context.WithCancel(ctx)
	defer closeConn()
	go keepaliveSocket(connCtx, conn, tm.pingInterval, closeConn)

	// A disconnect warning arrives ~10s before Slack drops the socket. Rather
	// than tear down immediately (which loses whatever is in flight), the
	// caller is signalled to open a replacement while this loop keeps
	// draining and acking on the doomed connection.
	draining := false

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		data, err := conn.Read(connCtx)
		if err != nil {
			return pumpReadErr(ctx, err, draining)
		}

		var env socketEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			log.Printf("socket mode: undecodable frame: %v", err)
			continue
		}

		switch env.Type {
		case "hello":
			continue
		case "disconnect":
			log.Printf("socket mode: disconnect requested (reason=%s), draining", env.Reason)
			if !draining {
				draining = true
				startDrain(tm, closeConn, onWarning)
			}
			continue
		}

		if ackErr := ackEnvelope(ctx, conn, env.EnvelopeID); ackErr != nil {
			return ackErr
		}

		// Bridge off the read loop, exactly as the HTTP path does after its
		// ack: postInbound is bounded by gcCallTimeout, and a slow gc must
		// not stall acks for the envelopes queued behind this one.
		go handleSocketEnvelope(cfg, env)
	}
}

// pumpReadErr maps a failed connection read to the pump's return value. A read
// that fails while draining is the expected end of a warned-about connection,
// not a fault; a read that fails during shutdown reports the shutdown.
func pumpReadErr(ctx context.Context, readErr error, draining bool) error {
	if draining {
		// Expected: the warned-about close finally landed, or the drain
		// window elapsed.
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return readErr
}

// startDrain begins the drain of a connection Slack has announced it will
// close: the window is bounded, and the supervisor is signalled to dial the
// replacement that overlaps it.
func startDrain(tm socketTimings, closeConn func(), onWarning func()) {
	// Bound the drain: if Slack never actually closes, this connection must
	// not linger next to its replacement.
	time.AfterFunc(tm.drainGrace, closeConn)
	if onWarning != nil {
		onWarning()
	}
}

// ackEnvelope acknowledges one envelope. Envelopes that carry no id (hello,
// disconnect) are not acked, matching Slack's protocol.
func ackEnvelope(ctx context.Context, conn socketConn, envelopeID string) error {
	if envelopeID == "" {
		return nil
	}
	ackCtx, ackCancel := context.WithTimeout(ctx, socketAckTimeout)
	ackErr := conn.Write(ackCtx, socketAck(envelopeID))
	ackCancel()
	if ackErr != nil {
		// Acking is what stops redelivery, so a failed ack means the
		// connection is no longer usable — redial and let Slack redeliver
		// rather than processing on a dead socket.
		return fmt.Errorf("ack envelope %s: %w", envelopeID, ackErr)
	}
	return nil
}

// keepaliveSocket pings Slack on a fixed interval and closes the connection
// when a ping goes unanswered. This is the liveness check for a transport
// whose healthy state is silence: without it, a connection dropped in a way
// that never reaches TCP (a NAT timeout, a proxy reaping an idle tunnel)
// would leave the adapter reading forever from a socket Slack has forgotten.
func keepaliveSocket(ctx context.Context, conn socketConn, interval time.Duration, closeConn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, socketPingTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return // shutting down; not a ping failure
				}
				log.Printf("socket mode: keepalive ping failed: %v", err)
				closeConn()
				return
			}
		}
	}
}

// socketAck builds the ack frame for an envelope.
func socketAck(envelopeID string) []byte {
	// Marshal rather than concatenate: envelope ids are Slack-supplied and
	// must not be able to break out of the JSON they are embedded in.
	b, err := json.Marshal(map[string]string{"envelope_id": envelopeID})
	if err != nil {
		// Unreachable for a map[string]string, but never emit a malformed ack.
		return []byte(`{}`)
	}
	return b
}

// handleSocketEnvelope routes one non-control envelope. Only events_api is
// meaningful at Tier 1 — interactivity and slash commands belong to the
// larger tiers — and the payload is the same shape the HTTP listener decodes,
// so the bridge itself is shared verbatim with the webhook path.
func handleSocketEnvelope(cfg config, env socketEnvelope) {
	// Log every delivery on arrival. Tier 1 subscribes to app_mention alone,
	// so this is low volume, and without it a dropped event is indis-
	// tinguishable from Slack never having sent one — which is exactly the
	// question an operator debugging a silent bot needs answered.
	log.Printf("socket mode: envelope received type=%s", env.Type)
	if env.Type != "events_api" || len(env.Payload) == 0 {
		return
	}
	var payload slackEventEnvelope
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		log.Printf("socket mode: decode payload: %v", err)
		return
	}
	// The workspace-identity check lives in bridgeEvent, shared with the HTTP
	// path: neither transport proves which workspace an event belongs to (the
	// HTTP signature proves it came from Slack for this app, not from this
	// team), and both stamp cfg.workspaceID as the account id, so the guard
	// belongs at the funnel rather than here.
	if env.RetryAttempt > 0 {
		// gc dedupes on DedupKey, so a redelivery is harmless; log it because
		// a persistent retry count means acks are not landing in time.
		log.Printf("socket mode: redelivered envelope (attempt %d)", env.RetryAttempt)
	}
	bridgeEvent(cfg, payload)
}
