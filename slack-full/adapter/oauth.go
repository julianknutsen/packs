// OAuth install flow for slack-pack (gc-cby.9).
//
// Two HTTP endpoints, registered on the adapter's public mux when
// SLACK_CLIENT_ID is set. Both are reachable from the public internet
// via the same Tailscale Funnel that fronts /slack/events:
//
//   GET /slack/oauth/start
//       Builds the Slack v2 OAuth authorize URL with a CSRF state
//       cookie + scopes from the manifest, then 302-redirects the
//       user's browser to slack.com/oauth/v2/authorize.
//
//   GET /slack/oauth/callback
//       Slack redirects the user back here with ?code=...&state=...
//       once they hit "Allow". We verify the state cookie matches the
//       state param (CSRF), exchange the code via Slack's
//       oauth.v2.access endpoint (server-to-server), and persist the
//       resulting (workspace_id, app_id, bot_user_id, bot_token,
//       signing_secret) into the apps registry.
//
// Single-tenant by design (gc-cby.9 acceptance). The bot token is
// written to <cityPath>/.gc/slack/install.env so the operator can
// `source` it before starting the adapter for real. The apps registry
// receives the bot_user_id + signing_secret (from env, since Slack
// does NOT return signing_secret in oauth.v2.access — it lives on the
// app's Basic Information page and must be supplied separately).
//
// Multi-tenant token routing (one adapter serving many workspaces)
// is explicit out-of-scope; file as a separate bead if needed.

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// oauthStateCookie is the HttpOnly cookie name used to bind the
// authorize-time state value to the callback. Browser-set on /start,
// browser-returned on /callback. Cleared on success.
const oauthStateCookie = "gc_slack_oauth_state"

// oauthStateTTL is the maximum lifetime of the CSRF state cookie.
// The OAuth grant should complete in seconds; 10 minutes is generous
// padding for slow operator clicks and absent-minded approvals.
const oauthStateTTL = 10 * time.Minute

// oauthStateStore is the in-process server-side nonce store that
// enforces single-use semantics on the CSRF state value. The cookie
// alone is not sufficient: an attacker who captures the cookie+code
// before clearing could replay /callback. Slack invalidates OAuth
// codes on first use so the practical exploit is bounded, but
// defense-in-depth requires server-side single-use.
//
// On /start: nonce is recorded with creation time.
// On /callback: nonce is consumed (deleted) atomically; absence
// rejects the request.
//
// In-process: slack-pack runs a single adapter, so this is sufficient.
// A multi-process deployment would need a shared store.
var oauthStateStore = struct {
	sync.Mutex
	nonces map[string]time.Time
}{nonces: make(map[string]time.Time)}

// recordOAuthState stores nonce with timestamp; cleans expired entries
// while we hold the lock so the map doesn't grow unbounded across
// long-running adapters with many install attempts.
func recordOAuthState(now func() time.Time, nonce string) {
	t := now()
	oauthStateStore.Lock()
	defer oauthStateStore.Unlock()
	for n, ts := range oauthStateStore.nonces {
		if t.Sub(ts) > oauthStateTTL {
			delete(oauthStateStore.nonces, n)
		}
	}
	oauthStateStore.nonces[nonce] = t
}

// consumeOAuthState atomically tests-and-deletes nonce. Returns true
// only if the nonce was present and within TTL.
func consumeOAuthState(now func() time.Time, nonce string) bool {
	if nonce == "" {
		return false
	}
	t := now()
	oauthStateStore.Lock()
	defer oauthStateStore.Unlock()
	ts, ok := oauthStateStore.nonces[nonce]
	if !ok {
		return false
	}
	delete(oauthStateStore.nonces, nonce)
	return t.Sub(ts) <= oauthStateTTL
}

// defaultSlackOAuthBase is overridden in tests via cfg.slackOAuthBase
// to point at an httptest.Server.
const defaultSlackOAuthBase = "https://slack.com"

// oauthConfig is the OAuth-flow subset of adapter config. Carved out
// so the handlers can be unit-tested without constructing a full
// `config` value (and so test seams stay narrow).
type oauthConfig struct {
	clientID      string
	clientSecret  string
	redirectURI   string
	scopes        []string
	signingSecret string // stamped onto the apps record post-exchange; may be empty
	cityPath      string // <cityPath>/.gc/slack/install.env destination
	slackBaseURL  string // default https://slack.com; tests inject httptest URL
	now           func() time.Time
	rand          io.Reader
}

// slackOAuthAccessResponse is the subset of oauth.v2.access we read.
// Schema: https://api.slack.com/methods/oauth.v2.access
type slackOAuthAccessResponse struct {
	OK          bool   `json:"ok"`
	Error       string `json:"error,omitempty"`
	AppID       string `json:"app_id,omitempty"`
	AccessToken string `json:"access_token,omitempty"` // xoxb-... bot token
	BotUserID   string `json:"bot_user_id,omitempty"`
	Scope       string `json:"scope,omitempty"` // comma-separated bot scopes
	Team        struct {
		ID   string `json:"id,omitempty"` // T0...
		Name string `json:"name,omitempty"`
	} `json:"team"`
}

// registerOAuthHandlers wires the install endpoints onto mux. It is a
// no-op when SLACK_CLIENT_ID is empty — the feature is gated by the
// presence of OAuth credentials, so dev installs that still rely on
// the manual web-UI flow do not accidentally expose an unconfigured
// install path on the public listener. Logs a single line surfacing
// the registered redirect URI when active so operators can confirm
// the value matches the one entered in the Slack app's OAuth settings.
func registerOAuthHandlers(mux *http.ServeMux, cfg config, reg *appsRegistry) {
	if cfg.oauthClientID == "" {
		return
	}
	if cfg.oauthRedirectURI == "" {
		log.Printf("WARN: SLACK_CLIENT_ID is set but SLACK_REDIRECT_URI is empty — oauth install endpoints not registered")
		return
	}
	if cfg.oauthClientSecret == "" {
		log.Printf("WARN: SLACK_CLIENT_ID is set but SLACK_CLIENT_SECRET is empty — oauth install endpoints not registered (token exchange would fail)")
		return
	}
	if err := validateSlackOAuthBase(cfg.oauthSlackBaseURL); err != nil {
		log.Printf("WARN: SLACK_OAUTH_BASE_URL invalid (%v) — oauth install endpoints not registered", err)
		return
	}
	oc := oauthConfig{
		clientID:      cfg.oauthClientID,
		clientSecret:  cfg.oauthClientSecret,
		redirectURI:   cfg.oauthRedirectURI,
		scopes:        oauthBotScopes(),
		signingSecret: cfg.slackSigningKey,
		cityPath:      cfg.cityPath,
		slackBaseURL:  cfg.oauthSlackBaseURL,
		now:           time.Now,
		rand:          rand.Reader,
	}
	mux.HandleFunc("/slack/oauth/start", handleOAuthStart(oc))
	mux.HandleFunc("/slack/oauth/callback", handleOAuthCallback(oc, reg))
	log.Printf("oauth install endpoints registered: redirect_uri=%s", cfg.oauthRedirectURI)
}

// validateSlackOAuthBase enforces an SSRF-safe shape on the OAuth base
// URL. Empty is allowed (handlers fall back to defaultSlackOAuthBase).
// Non-empty must be https + slack.com (or *.slack.com), with one
// concession for tests that inject an httptest.Server URL via the
// SLACK_OAUTH_TEST_BASE escape — production deployments never set
// SLACK_OAUTH_BASE_URL to anything else.
//
// Without this, an operator (or a compromised env injector) who points
// SLACK_OAUTH_BASE_URL at internal infrastructure causes the adapter
// to POST the OAuth client_id + client_secret to that internal target
// during code exchange — a credential-leak SSRF vector.
func validateSlackOAuthBase(base string) error {
	if base == "" {
		return nil
	}
	u, err := url.Parse(base)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	// Allow http only when the host is loopback (test harness using
	// httptest.Server); reject all other http schemes.
	if u.Scheme != "https" {
		host := u.Hostname()
		if u.Scheme == "http" && (host == "127.0.0.1" || host == "localhost" || host == "::1") {
			return nil
		}
		return fmt.Errorf("scheme must be https (got %q)", u.Scheme)
	}
	host := u.Hostname()
	if host != "slack.com" && !strings.HasSuffix(host, ".slack.com") {
		return fmt.Errorf("host must be slack.com or *.slack.com (got %q)", host)
	}
	return nil
}

// oauthBotScopes is the bot scope set requested at authorize-time.
// Mirrors manifest/app.json (pack-relative) — the manifest is the
// canonical declaration; this list MUST stay in sync. Slack rejects
// authorize requests asking for scopes the app's manifest does not
// declare, so drift here turns into an install failure not a silent
// over-grant.
//
// Source of truth: manifest/app.json (pack-relative)
//
//	oauth_config.scopes.bot
func oauthBotScopes() []string {
	return []string{
		"channels:history",
		"chat:write",
		"chat:write.customize",
		"commands",
		"files:read",
		"files:write",
		"groups:history",
		"im:history",
		// DM privacy gate: conversations.info membership probe
		// (adapter/dm_gate.go) — without it every OAuth-installed
		// token fails the probe with missing_scope and the fail-closed
		// gate drops all legitimate DMs.
		"im:read",
		"mpim:history",
		"reactions:write",
	}
}

// handleOAuthStart builds the Slack v2 authorize URL and redirects.
// Sets a CSRF state cookie that the callback re-validates.
func handleOAuthStart(cfg oauthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		state, err := newOAuthState(cfg.rand)
		if err != nil {
			http.Error(w, fmt.Sprintf("generate state: %v", err), http.StatusInternalServerError)
			return
		}
		// Record the nonce server-side so /callback can enforce single-use
		// (cookie clear alone is not enough — defense-in-depth against
		// callback replay).
		recordOAuthState(cfg.now, state)
		http.SetCookie(w, &http.Cookie{
			Name:     oauthStateCookie,
			Value:    state,
			Path:     "/slack/oauth",
			Expires:  cfg.now().Add(oauthStateTTL),
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteLaxMode,
		})
		authURL, err := buildSlackAuthorizeURL(cfg, state)
		if err != nil {
			http.Error(w, fmt.Sprintf("build authorize URL: %v", err), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, authURL, http.StatusFound)
	}
}

// handleOAuthCallback verifies state, exchanges the code, and persists
// the resulting record into the apps registry. On success, writes
// install.env with the new bot token and renders a plain-text success
// page directing the operator to source it.
func handleOAuthCallback(cfg oauthConfig, reg *appsRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Surface Slack-side denial early ("user clicked Cancel").
		if e := r.URL.Query().Get("error"); e != "" {
			http.Error(w, fmt.Sprintf("slack denied install: %s", e), http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		state := r.URL.Query().Get("state")
		if code == "" || state == "" {
			http.Error(w, "missing code or state", http.StatusBadRequest)
			return
		}
		cookie, err := r.Cookie(oauthStateCookie)
		if err != nil {
			http.Error(w, "missing state cookie (CSRF check failed)", http.StatusBadRequest)
			return
		}
		if cookie.Value == "" || cookie.Value != state {
			http.Error(w, "state mismatch (CSRF check failed)", http.StatusBadRequest)
			return
		}
		// Atomically consume the server-side nonce. Rejects callback
		// replay regardless of cookie or code state.
		if !consumeOAuthState(cfg.now, state) {
			http.Error(w, "state nonce already consumed or expired (CSRF check failed)", http.StatusBadRequest)
			return
		}

		resp, err := exchangeSlackOAuthCode(r.Context(), cfg, code)
		if err != nil {
			http.Error(w, fmt.Sprintf("oauth.v2.access: %v", err), http.StatusBadGateway)
			return
		}
		if !resp.OK {
			http.Error(w, fmt.Sprintf("slack rejected token exchange: %s", resp.Error), http.StatusBadGateway)
			return
		}
		if resp.Team.ID == "" || resp.AppID == "" || resp.AccessToken == "" {
			http.Error(w, "slack returned incomplete oauth response", http.StatusBadGateway)
			return
		}

		// Persist app record. Signing secret is sourced from env (Slack
		// does NOT return it in oauth.v2.access — it lives on the app's
		// Basic Information page and must be set separately by the
		// operator). Empty signing_secret is OK at this point; the
		// apps registry treats post-OAuth-pre-secret as "verify will
		// fall through to env fallback" (see lookupSigningSecrets).
		var scopes []string
		if resp.Scope != "" {
			scopes = strings.Split(resp.Scope, ",")
		}
		rec := appRecord{
			WorkspaceID:   resp.Team.ID,
			AppID:         resp.AppID,
			BotUserID:     resp.BotUserID,
			DisplayName:   resp.Team.Name,
			Scopes:        scopes,
			SigningSecret: cfg.signingSecret,
			ImportedAt:    cfg.now().UTC(),
		}
		if err := reg.Set(rec); err != nil {
			http.Error(w, fmt.Sprintf("persist app record: %v", err), http.StatusInternalServerError)
			return
		}

		envPath, err := writeInstallEnv(cfg.cityPath, resp.Team.ID, resp.AppID, resp.AccessToken)
		if err != nil {
			http.Error(w, fmt.Sprintf("write install.env: %v", err), http.StatusInternalServerError)
			return
		}

		// Log the path operator-side; the browser must NOT see it. The
		// success page goes to the public Tailscale Funnel — anyone who
		// reaches that URL after install would otherwise harvest the
		// filesystem path of the bot token's container.
		log.Printf("oauth install: workspace=%q (%s) app=%s wrote %s",
			resp.Team.Name, resp.Team.ID, resp.AppID, envPath)

		// Defense-in-depth: warn the operator if SLACK_SIGNING_SECRET is
		// unset at install time. The new app record is stamped with
		// cfg.signingSecret (Slack does NOT return signing_secret in
		// oauth.v2.access); a missing or stale env value means
		// subsequent traffic from this workspace will fail signature
		// verification until the operator runs `gc slack import-app
		// --signing-secret <secret>`.
		if cfg.signingSecret == "" {
			log.Printf("WARNING: oauth install: SLACK_SIGNING_SECRET unset — workspace=%q (%s) will fail signature verification until you run `gc slack import-app --signing-secret <secret>` for this app",
				resp.Team.Name, resp.Team.ID)
		}

		// Clear the state cookie now that exchange succeeded.
		http.SetCookie(w, &http.Cookie{
			Name:     oauthStateCookie,
			Value:    "",
			Path:     "/slack/oauth",
			Expires:  cfg.now().Add(-time.Hour),
			MaxAge:   -1,
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteLaxMode,
		})

		// Render a minimal success page. The filesystem path of
		// install.env is intentionally NOT echoed — it would appear in
		// browser history and access logs on the public Tailscale Funnel
		// listener, leaking host-side filesystem layout. The operator
		// finds the path in the adapter log.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w,
			"slack-pack installed for workspace %s (%s), app %s.\n\n"+
				"Bot token has been written to your gc city's slack install.env.\n"+
				"Check your adapter log for the absolute path, then restart the\n"+
				"adapter with that env file sourced.\n",
			resp.Team.Name, resp.Team.ID, resp.AppID)
	}
}

// newOAuthState returns a 32-hex-char (16-byte) crypto-random nonce.
func newOAuthState(r io.Reader) (string, error) {
	if r == nil {
		r = rand.Reader
	}
	buf := make([]byte, 16)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("read random: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// buildSlackAuthorizeURL composes the v2 OAuth authorize URL. Slack
// requires `client_id`, `scope` (comma-separated bot scopes), `state`,
// `redirect_uri`. We use the v2 endpoint exclusively — v1 is legacy.
func buildSlackAuthorizeURL(cfg oauthConfig, state string) (string, error) {
	if cfg.clientID == "" {
		return "", errors.New("client_id is empty")
	}
	if cfg.redirectURI == "" {
		return "", errors.New("redirect_uri is empty")
	}
	base := cfg.slackBaseURL
	if base == "" {
		base = defaultSlackOAuthBase
	}
	u, err := url.Parse(base + "/oauth/v2/authorize")
	if err != nil {
		return "", fmt.Errorf("parse base URL: %w", err)
	}
	q := u.Query()
	q.Set("client_id", cfg.clientID)
	q.Set("scope", strings.Join(cfg.scopes, ","))
	q.Set("state", state)
	q.Set("redirect_uri", cfg.redirectURI)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// exchangeSlackOAuthCode POSTs to oauth.v2.access. Slack accepts
// client_id/client_secret as POST body params (or HTTP basic auth);
// we use the body form for simplicity and to match Slack's docs.
func exchangeSlackOAuthCode(ctx context.Context, cfg oauthConfig, code string) (slackOAuthAccessResponse, error) {
	base := cfg.slackBaseURL
	if base == "" {
		base = defaultSlackOAuthBase
	}
	form := url.Values{}
	form.Set("client_id", cfg.clientID)
	form.Set("client_secret", cfg.clientSecret)
	form.Set("code", code)
	form.Set("redirect_uri", cfg.redirectURI)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/api/oauth.v2.access", strings.NewReader(form.Encode()))
	if err != nil {
		return slackOAuthAccessResponse{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return slackOAuthAccessResponse{}, fmt.Errorf("post oauth.v2.access: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return slackOAuthAccessResponse{}, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return slackOAuthAccessResponse{}, fmt.Errorf("slack returned HTTP %d: %s", resp.StatusCode, truncateForLog(string(body)))
	}
	var out slackOAuthAccessResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return slackOAuthAccessResponse{}, fmt.Errorf("decode response: %w", err)
	}
	return out, nil
}

// writeInstallEnv writes a shell-sourceable env file containing the
// freshly-issued bot token + workspace metadata. Atomic write with
// 0600 perms — the file holds a long-lived bot token. Returns the
// absolute path so the success page can reference it.
func writeInstallEnv(cityPath, workspaceID, appID, botToken string) (string, error) {
	if cityPath == "" {
		return "", errors.New("GC_CITY_PATH is empty; cannot write install.env")
	}
	dest := filepath.Join(cityPath, ".gc", "slack", "install.env")
	body := fmt.Sprintf(
		"# Written by slack-pack OAuth install flow (gc-cby.9).\n"+
			"# Source this file before starting the adapter:\n"+
			"#   set -a; source %s; set +a\n"+
			"SLACK_BOT_TOKEN=%s\n"+
			"SLACK_WORKSPACE_ID=%s\n"+
			"SLACK_APP_ID=%s\n",
		dest, shellQuote(botToken), shellQuote(workspaceID), shellQuote(appID))
	if err := writeFile0600(dest, []byte(body)); err != nil {
		return "", fmt.Errorf("write install.env: %w", err)
	}
	return dest, nil
}

// shellQuote single-quotes a value for shell sourcing. Slack tokens
// and ids are safe alnum/dash/underscore today, but quoting keeps the
// file robust against any future format drift (e.g. a colon in a name).
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// truncateForLog bounds an upstream-echoed error body so a hostile or
// noisy Slack response cannot inflate adapter logs unboundedly.
func truncateForLog(s string) string {
	const max = 256
	if len(s) <= max {
		return s
	}
	return s[:max] + "...[truncated]"
}
