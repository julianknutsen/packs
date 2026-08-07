// gc-slack-mini-adapter — the Tier-1 ("slack-mini") Slack ↔ gc bridge.
//
// The minimal viable Slack→mayor surface. This file is the whole adapter
// bar one thing: the Socket Mode transport lives in socketmode.go, kept
// separate because it is an alternative to the HTTP receiver below rather
// than an addition to it.
//
//   - Inbound: either a public HTTPS receiver for the Slack Events API, or —
//     when SLACK_APP_TOKEN is set — a Socket Mode WebSocket the adapter opens
//     outbound to Slack, which needs no public ingress at all (see
//     socketmode.go). Only `app_mention` is handled either way. Each mention
//     is bridged to gc by POSTing /v0/city/{city}/extmsg/inbound, addressed
//     to the mayor session (override with SLACK_MINI_INBOUND_TARGET).
//
//   - Outbound: a UDS endpoint (/post-message) that posts plain text to a
//     Slack channel via chat.postMessage using the workspace bot token.
//     The pack's commands/post-message.sh wrapper reaches it through gc's
//     /svc/slack-mini reverse proxy. This is the only outbound verb at Tier 1.
//
// Tier 1 keeps NO on-disk registries: no channel bindings, no per-session
// identity, no apps registry, no rig/room state. Those belong to
// slack-channel (Tier 2) and slack-full (Tier 3). See
// docs/design/slack-pack-tiering.md.
//
// Required env:
//
//	SLACK_BOT_TOKEN        Bot token (xoxb-...) for outbound chat.postMessage.
//	                       Not used on the inbound path (which only verifies
//	                       the signing secret and POSTs to gc).
//	SLACK_SIGNING_SECRET   HMAC secret for verifying Slack request signatures
//	                       on the HTTP inbound bridge. Required at Tier 1 when
//	                       running the HTTP transport — there is no
//	                       apps-registry fallback, so without it every inbound
//	                       is rejected. Not used (and not required) in Socket
//	                       Mode, which has no request signatures.
//	SLACK_WORKSPACE_ID     Slack workspace (team) id; the extmsg account id.
//	GC_CITY_NAME           gc city the adapter bridges into.
//
// Controller-injected env (proxy_process mode):
//
//	GC_SERVICE_SOCKET      UDS path the internal listener binds. When set, the
//	                       adapter runs as a gc proxy_process service.
//	GC_SERVICE_URL_PREFIX  Reverse-proxy prefix gc routes to this service;
//	                       used to compute the self-registration callback URL.
//	GC_API_BASE_URL        gc API base (default http://127.0.0.1:9443).
//
// Optional env:
//
//	SLACK_APP_TOKEN            App-level token (xapp-...) with the
//	                           connections:write scope. When set, the adapter
//	                           runs Socket Mode instead of the HTTP receiver:
//	                           LISTEN_PUBLIC is never bound and
//	                           SLACK_SIGNING_SECRET is not required.
//	LISTEN_PUBLIC              Public bind for /slack/events (default 0.0.0.0:8775).
//	                           Ignored in Socket Mode.
//	LISTEN_INTERNAL            TCP bind for the internal mux when GC_SERVICE_SOCKET
//	                           is unset (default 127.0.0.1:8776).
//	REGISTER_ON_START          "true" (default) self-registers as an extmsg adapter.
//	ADAPTER_PROVIDER           extmsg provider name (default "slack").
//	SLACK_MINI_INBOUND_TARGET  Session handle inbound mentions address (default "mayor").
//	SLACK_API_BASE             Slack web API base (default https://slack.com/api).
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultPublicListen   = "0.0.0.0:8775"
	defaultInternalListen = "127.0.0.1:8776"
	defaultGCAPIBase      = "http://127.0.0.1:9443"
	defaultProvider       = "slack"
	defaultInboundTarget  = "mayor"

	// maxInboundBody caps the /slack/events body read. The body is
	// unsigned until HMAC-verified, so bounding it pre-verify limits a
	// memory-amplification vector. Slack event payloads are small.
	maxInboundBody = 1 << 20 // 1 MiB

	// slackReplayWindow rejects requests whose signed timestamp is older
	// than this, mitigating replay of a captured signature.
	slackReplayWindow = 5 * time.Minute

	slackPostTimeout = 15 * time.Second
)

// defaultSlackAPIBase is the production Slack web API origin. Overridable
// via SLACK_API_BASE (and via cfg.slackAPIBase in tests).
const defaultSlackAPIBase = "https://slack.com/api"

// gcCallTimeout bounds outbound calls to the gc API so a stalled gc cannot
// pin an inbound-bridge goroutine (or block startup registration) forever.
const gcCallTimeout = 15 * time.Second

type config struct {
	publicListen        string
	internalListen      string
	serviceSocket       string
	gcAPIBase           string
	internalCallbackURL string
	cityName            string
	provider            string
	workspaceID         string
	botToken            string
	signingSecret       string
	appToken            string
	inboundTarget       string
	slackAPIBase        string
	registerOnStart     bool
}

// socketMode reports whether the adapter takes inbound over a Socket Mode
// WebSocket instead of the public HTTP listener. The app-level token is the
// selector: it is required for Socket Mode and useless without it, so its
// presence is the whole contract — there is no second toggle to keep in sync.
func (c config) socketMode() bool { return c.appToken != "" }

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	internalDescr := cfg.internalListen
	if cfg.serviceSocket != "" {
		internalDescr = "uds:" + cfg.serviceSocket
	}
	inboundDescr := "http:" + cfg.publicListen
	if cfg.socketMode() {
		inboundDescr = "socket-mode (no public listener)"
	}
	log.Printf("starting gc-slack-mini-adapter inbound=%s internal=%s gc=%s city=%s target=%s",
		inboundDescr, internalDescr, cfg.gcAPIBase, cfg.cityName, cfg.inboundTarget)

	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/slack/events", handleSlackEvents(cfg))
	publicMux.HandleFunc("/healthz", handleHealthz)
	publicMux.HandleFunc("/", http.NotFound)

	internalMux := http.NewServeMux()
	internalMux.HandleFunc("POST /post-message", handlePostMessage(cfg))
	internalMux.HandleFunc("/healthz", handleHealthz)

	publicSrv := &http.Server{
		Addr:              cfg.publicListen,
		Handler:           publicMux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	internalSrv := &http.Server{
		Handler:           internalMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	if cfg.registerOnStart {
		regCtx, cancel := context.WithTimeout(context.Background(), gcCallTimeout)
		err := registerAdapter(regCtx, cfg)
		cancel()
		if err != nil {
			log.Fatalf("register adapter: %v", err)
		}
		log.Printf("registered with gc as provider=%s account=%s callback=%s/post-message",
			cfg.provider, cfg.workspaceID, cfg.internalCallbackURL)
	}

	// Socket Mode owns the inbound path when enabled; the public listener is
	// not merely unused but never bound, which is the point on a network that
	// cannot accept inbound connections.
	socketCtx, socketCancel := context.WithCancel(context.Background())
	defer socketCancel()

	errCh := make(chan error, 2)
	if cfg.socketMode() {
		go func() {
			// runSocketMode reconnects internally and returns only when its
			// context is cancelled, so a return here means shutdown.
			if err := runSocketMode(socketCtx, cfg); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	} else {
		go func() {
			log.Printf("public listener serving on %s (Slack events)", cfg.publicListen)
			errCh <- publicSrv.ListenAndServe()
		}()
	}
	go func() {
		if cfg.serviceSocket != "" {
			log.Printf("internal listener serving on UDS %s (gc proxy_process)", cfg.serviceSocket)
			lis, err := listenUDS(cfg.serviceSocket)
			if err != nil {
				errCh <- err
				return
			}
			errCh <- internalSrv.Serve(lis)
			return
		}
		internalSrv.Addr = cfg.internalListen
		log.Printf("internal listener serving on %s (post-message only)", cfg.internalListen)
		errCh <- internalSrv.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case <-stop:
		log.Println("shutting down (signal)")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("listener error: %v", err)
		}
	}
	socketCancel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = publicSrv.Shutdown(ctx)
	_ = internalSrv.Shutdown(ctx)
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// loadConfig reads the process environment.
func loadConfig() (config, error) { return loadConfigFromEnv(os.Getenv) }

// loadConfigFromEnv builds and validates a config from a getenv function.
// Split out so tests can supply a fake environment.
func loadConfigFromEnv(getenv func(string) string) (config, error) {
	envOr := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	cfg := config{
		publicListen:    envOr("LISTEN_PUBLIC", defaultPublicListen),
		internalListen:  envOr("LISTEN_INTERNAL", defaultInternalListen),
		serviceSocket:   getenv("GC_SERVICE_SOCKET"),
		gcAPIBase:       strings.TrimRight(envOr("GC_API_BASE_URL", defaultGCAPIBase), "/"),
		cityName:        getenv("GC_CITY_NAME"),
		provider:        envOr("ADAPTER_PROVIDER", defaultProvider),
		workspaceID:     getenv("SLACK_WORKSPACE_ID"),
		botToken:        getenv("SLACK_BOT_TOKEN"),
		signingSecret:   getenv("SLACK_SIGNING_SECRET"),
		appToken:        getenv("SLACK_APP_TOKEN"),
		inboundTarget:   envOr("SLACK_MINI_INBOUND_TARGET", defaultInboundTarget),
		slackAPIBase:    strings.TrimRight(envOr("SLACK_API_BASE", defaultSlackAPIBase), "/"),
		registerOnStart: envOr("REGISTER_ON_START", "true") == "true",
	}

	// proxy_process mode: gc reaches the adapter via GC_API_BASE_URL +
	// GC_SERVICE_URL_PREFIX. gc appends the endpoint path itself, so the
	// registered callback base must not include it.
	if cfg.serviceSocket != "" {
		urlPrefix := strings.TrimRight(getenv("GC_SERVICE_URL_PREFIX"), "/")
		if urlPrefix == "" {
			return cfg, errors.New("GC_SERVICE_SOCKET is set but GC_SERVICE_URL_PREFIX is empty — controller-injected env is incomplete")
		}
		cfg.internalCallbackURL = cfg.gcAPIBase + urlPrefix
	} else {
		// Standalone TCP mode: no controller-injected URL prefix, so derive
		// the callback base from the internal listener. Leaving it empty would
		// self-register an empty callback_url and break gc→adapter callbacks.
		cfg.internalCallbackURL = tcpCallbackURL(cfg.internalListen)
	}

	var missing []string
	if cfg.workspaceID == "" {
		missing = append(missing, "SLACK_WORKSPACE_ID")
	}
	if cfg.botToken == "" {
		missing = append(missing, "SLACK_BOT_TOKEN")
	}
	// The signing secret verifies HTTP webhook signatures. Socket Mode has no
	// request signatures — authenticity comes from the app-token-authenticated
	// connection the adapter itself opens — so requiring it there would be
	// demanding a credential the transport never consults.
	if cfg.signingSecret == "" && !cfg.socketMode() {
		missing = append(missing, "SLACK_SIGNING_SECRET")
	}
	if cfg.cityName == "" {
		missing = append(missing, "GC_CITY_NAME")
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	// Fail fast on the easiest Socket Mode mistake: pasting the bot token
	// (xoxb-) into SLACK_APP_TOKEN. Slack would answer invalid_auth on every
	// reconnect forever, which reads as a network fault rather than a typo.
	if cfg.socketMode() && !strings.HasPrefix(cfg.appToken, "xapp-") {
		return cfg, errors.New("SLACK_APP_TOKEN must be an app-level token (xapp-...); the bot token belongs in SLACK_BOT_TOKEN")
	}
	// cityName is interpolated into every /v0/city/{city}/... URL. Reject
	// URL-significant characters so a city name cannot alter routing.
	if strings.ContainsAny(cfg.cityName, "/?#%") {
		return cfg, fmt.Errorf("GC_CITY_NAME must not contain '/', '?', '#', or '%%': %q", cfg.cityName)
	}
	return cfg, nil
}

// tcpCallbackURL derives the gc→adapter callback base from the internal
// listener address for standalone (TCP) mode, where there is no
// proxy_process URL prefix. gc appends the endpoint path itself, so the
// returned URL carries no trailing path. A wildcard or empty bind host is
// rewritten to loopback because gc dials a concrete address.
func tcpCallbackURL(internalListen string) string {
	host, port, err := net.SplitHostPort(internalListen)
	if err != nil {
		return "http://" + internalListen
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// --- inbound: Slack events → gc extmsg ------------------------------------

type slackEventEnvelope struct {
	Type      string          `json:"type"`
	Challenge string          `json:"challenge,omitempty"`
	TeamID    string          `json:"team_id,omitempty"`
	Event     json.RawMessage `json:"event,omitempty"`
}

// slackMessageEvent is the subset of an app_mention event Tier 1 reads.
type slackMessageEvent struct {
	Type        string `json:"type"`
	Subtype     string `json:"subtype,omitempty"`
	User        string `json:"user,omitempty"`
	BotID       string `json:"bot_id,omitempty"`
	Text        string `json:"text,omitempty"`
	Channel     string `json:"channel,omitempty"`
	TS          string `json:"ts,omitempty"`
	ThreadTS    string `json:"thread_ts,omitempty"`
	ChannelType string `json:"channel_type,omitempty"`
}

func handleSlackEvents(cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxInboundBody))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		ts := r.Header.Get("X-Slack-Request-Timestamp")
		sig := r.Header.Get("X-Slack-Signature")
		if !verifySlackSignature(cfg.signingSecret, ts, body, sig) {
			log.Printf("slack signature verify FAILED")
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		var env slackEventEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}

		// URL verification handshake (Slack sends this once when the
		// Events API endpoint is registered).
		if env.Type == "url_verification" && env.Challenge != "" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(env.Challenge))
			return
		}

		// Ack immediately so Slack does not retry, then bridge.
		w.WriteHeader(http.StatusOK)
		go bridgeEvent(cfg, env)
	}
}

// bridgeEvent decodes an app_mention and POSTs it to gc's extmsg inbound.
// Non-app_mention events, bot/system messages, and empty bodies are
// dropped — Tier 1 handles only direct human mentions. It runs in its own
// goroutine; the gc call is bounded by gcCallTimeout so a stalled gc
// cannot leak goroutines under sustained traffic.
func bridgeEvent(cfg config, env slackEventEnvelope) {
	if env.Type != "event_callback" || len(env.Event) == 0 {
		return
	}
	var msg slackMessageEvent
	if err := json.Unmarshal(env.Event, &msg); err != nil {
		log.Printf("decode slack event: %v", err)
		return
	}
	if msg.Type != "app_mention" {
		return
	}
	if msg.BotID != "" || msg.Subtype != "" || msg.User == "" {
		return
	}
	text := stripLeadingMention(msg.Text)
	if text == "" {
		return
	}

	inbound := externalInboundMessage{
		ProviderMessageID: msg.TS,
		Conversation: conversationRef{
			ScopeID:        cfg.cityName,
			Provider:       cfg.provider,
			AccountID:      cfg.workspaceID,
			ConversationID: msg.Channel,
			Kind:           slackKindFromChannelType(msg.ChannelType, msg.Channel),
		},
		Actor: externalActor{
			ID:          msg.User,
			DisplayName: msg.User, // resolving a display name needs users.info — defer to Tier 2
		},
		Text:             text,
		ExplicitTarget:   cfg.inboundTarget,
		ReplyToMessageID: msg.ThreadTS,
		DedupKey:         "slack-" + msg.TS,
		ReceivedAt:       time.Now().UTC(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), gcCallTimeout)
	defer cancel()
	if err := postInbound(ctx, cfg, inbound); err != nil {
		log.Printf("inbound POST failed: %v", err)
		return
	}
	log.Printf("inbound: chan=%s user=%s ts=%s thread=%s target=%s text=%dch",
		msg.Channel, msg.User, msg.TS, msg.ThreadTS, cfg.inboundTarget, len(text))
}

// leadingMentionRE matches one or more leading Slack user-mention tokens
// (`<@U…>`) and surrounding whitespace. Slack delivers app_mention text
// with the bot mention inline (e.g. "<@U0BOT> status?"); stripping it
// yields the human-meant message body.
var leadingMentionRE = regexp.MustCompile(`^\s*(?:<@[A-Z0-9]+>\s*)+`)

func stripLeadingMention(text string) string {
	return strings.TrimSpace(leadingMentionRE.ReplaceAllString(text, ""))
}

// slackKindFromChannelType maps a Slack channel_type onto a gc
// ConversationKind, falling back to the channel-id prefix.
func slackKindFromChannelType(channelType, channelID string) string {
	switch channelType {
	case "channel", "group", "mpim":
		return "room"
	case "im":
		return "dm"
	}
	if len(channelID) > 0 {
		switch channelID[0] {
		case 'C', 'G':
			return "room"
		case 'D':
			return "dm"
		}
	}
	return "dm"
}

// verifySlackSignature validates Slack's v0 HMAC request signature and
// rejects timestamps whose absolute age exceeds the replay window — both
// stale (past) and far-future. Fails closed on any missing field or parse
// error.
func verifySlackSignature(secret, ts string, body []byte, sig string) bool {
	if secret == "" || ts == "" || sig == "" {
		return false
	}
	tsInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	// Reject when the absolute age exceeds the replay window — both stale
	// (past) and far-future timestamps. time.Since yields a negative
	// duration for future timestamps, so a one-sided ">" check would
	// silently accept them.
	age := time.Since(time.Unix(tsInt, 0))
	if age < 0 {
		age = -age
	}
	if age > slackReplayWindow {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + ts + ":"))
	_, _ = mac.Write(body)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(sig))
}

// --- gc extmsg wire types (mirrored, wire-compatible only) ----------------

type conversationRef struct {
	ScopeID        string `json:"scope_id"`
	Provider       string `json:"provider"`
	AccountID      string `json:"account_id"`
	ConversationID string `json:"conversation_id"`
	Kind           string `json:"kind"`
}

type externalActor struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	IsBot       bool   `json:"is_bot"`
}

type externalInboundMessage struct {
	ProviderMessageID string          `json:"provider_message_id"`
	Conversation      conversationRef `json:"conversation"`
	Actor             externalActor   `json:"actor"`
	Text              string          `json:"text"`
	ExplicitTarget    string          `json:"explicit_target,omitempty"`
	ReplyToMessageID  string          `json:"reply_to_message_id,omitempty"`
	DedupKey          string          `json:"dedup_key,omitempty"`
	ReceivedAt        time.Time       `json:"received_at"`
}

type adapterCapabilities struct {
	SupportsChildConversations bool `json:"SupportsChildConversations"`
	SupportsAttachments        bool `json:"SupportsAttachments"`
	MaxMessageLength           int  `json:"MaxMessageLength"`
}

type adapterRegisterRequest struct {
	Provider     string              `json:"provider"`
	AccountID    string              `json:"account_id"`
	Name         string              `json:"name,omitempty"`
	CallbackURL  string              `json:"callback_url,omitempty"`
	Capabilities adapterCapabilities `json:"capabilities,omitempty"`
}

// postInbound bridges a verified Slack mention into gc.
func postInbound(ctx context.Context, cfg config, msg externalInboundMessage) error {
	body, err := json.Marshal(map[string]any{"message": msg})
	if err != nil {
		return err
	}
	target := fmt.Sprintf("%s/v0/city/%s/extmsg/inbound", cfg.gcAPIBase, url.PathEscape(cfg.cityName))
	if err := postJSON(ctx, target, body); err != nil {
		return fmt.Errorf("post inbound: %w", err)
	}
	return nil
}

// registerAdapter self-registers as an extmsg adapter so gc accepts this
// provider's inbound messages.
func registerAdapter(ctx context.Context, cfg config) error {
	body, err := json.Marshal(adapterRegisterRequest{
		Provider:    cfg.provider,
		AccountID:   cfg.workspaceID,
		Name:        "slack-mini-adapter",
		CallbackURL: cfg.internalCallbackURL,
		Capabilities: adapterCapabilities{
			SupportsChildConversations: false,
			SupportsAttachments:        false,
			MaxMessageLength:           40000, // Slack's chat.postMessage limit
		},
	})
	if err != nil {
		return err
	}
	target := fmt.Sprintf("%s/v0/city/%s/extmsg/adapters", cfg.gcAPIBase, url.PathEscape(cfg.cityName))
	return postJSON(ctx, target, body)
}

// postJSON POSTs a JSON body to a gc API endpoint and treats any >=400
// status as an error, surfacing the response body for diagnostics. ctx
// bounds the call so callers can enforce a timeout.
func postJSON(ctx context.Context, target string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", "gc-slack-mini-adapter")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	return nil
}

// --- outbound: post-message → Slack chat.postMessage ----------------------

// slackPostMessageReq is both the JSON body the post-message.sh wrapper
// POSTs to /post-message and the chat.postMessage payload — the wrapper
// deliberately speaks Slack's own field names, so one type serves both.
type slackPostMessageReq struct {
	Channel  string `json:"channel"`
	Text     string `json:"text"`
	ThreadTS string `json:"thread_ts,omitempty"`
}

type slackPostMessageResp struct {
	OK      bool   `json:"ok"`
	TS      string `json:"ts,omitempty"`
	Channel string `json:"channel,omitempty"`
	Error   string `json:"error,omitempty"`
}

func handlePostMessage(cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req slackPostMessageReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("decode: %v", err))
			return
		}
		if strings.TrimSpace(req.Channel) == "" {
			writeJSONError(w, http.StatusBadRequest, "channel is required")
			return
		}
		if strings.TrimSpace(req.Text) == "" {
			writeJSONError(w, http.StatusBadRequest, "text is required")
			return
		}
		resp, err := postToSlack(r.Context(), cfg.slackAPIBase, cfg.botToken, req)
		if err != nil {
			writeJSONError(w, http.StatusBadGateway, err.Error())
			return
		}
		if !resp.OK {
			writeJSONError(w, http.StatusBadGateway, "slack: "+resp.Error)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"ts":      resp.TS,
			"channel": resp.Channel,
		})
	}
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": msg})
}

// postToSlack posts a message via chat.postMessage using the bot token.
func postToSlack(ctx context.Context, apiBase, token string, req slackPostMessageReq) (*slackPostMessageResp, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, slackPostTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+"/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("post chat.postMessage: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read slack response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("slack http %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var sr slackPostMessageResp
	if err := json.Unmarshal(respBody, &sr); err != nil {
		return nil, fmt.Errorf("decode slack response: %w", err)
	}
	return &sr, nil
}

// listenUDS binds a Unix domain socket, removing any stale entry first so
// restarts succeed, and tightens it to owner-only.
func listenUDS(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("chmod uds: %w", err)
	}
	return lis, nil
}
