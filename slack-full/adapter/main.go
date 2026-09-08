// gc-slack-adapter — out-of-process Slack ↔ gc extmsg bridge.
//
// Registers itself with the gc API as an extmsg adapter (provider=slack).
// Two HTTP endpoints:
//
//	POST /publish        — gc forwards outbound publish requests here;
//	                        we translate to Slack chat.postMessage.
//	POST /slack/events   — Slack forwards user events here; we verify the
//	                        signing secret, normalize, and POST to
//	                        gc /v0/city/{city}/extmsg/inbound.
//
// Threading: gc.PublishRequest.ReplyToMessageID is mapped to Slack
// thread_ts. Slack message ts is returned as PublishReceipt.MessageID
// so subsequent replies thread correctly.
//
// All configuration via env vars — keep secrets out of source.
//
// # Environment contract
//
// Must-set (no default; loadConfigFromEnv returns an error if missing):
//
//   - SLACK_WORKSPACE_ID      Slack team id (e.g. T01234567).
//   - SLACK_BOT_TOKEN         xoxb- bot token. Must have chat:write,
//     reactions:write, files:write, and (for
//     identity overrides) chat:write.customize.
//   - GC_CITY_NAME            Name of the gc city the adapter posts to
//     (matches [workspace].name in city.toml). Used
//     to construct /v0/city/{name}/extmsg/inbound and
//     /v0/city/{name}/session/{id}/messages URLs.
//
// Conditionally required (no default; the dependent endpoint is
// disabled when unset):
//
//   - FILE_UPLOAD_ROOT        Absolute filesystem prefix the adapter is
//     allowed to read for /publish-file. Without
//     it, /publish-file returns 503 (defense-in-
//     depth: anyone on the internal mux could
//     otherwise ask the adapter to upload arbitrary
//     host files like /etc/passwd to Slack). Set
//     to the directory tree gc agents write
//     uploadable artifacts under.
//   - SLACK_SIGNING_SECRET    Single-app fallback for HMAC verification on
//     /slack/events and /slack/interactions. The
//     adapter looks up per-app signing secrets in
//     the apps registry first (keyed by team_id);
//     this env var only takes effect when the
//     registry has no record for the inbound
//     team_id. Multi-app deployments should
//     populate the registry via `gc slack
//     import-app`; single-app dev installs can
//     keep using this env var alone. With neither
//     source set, every inbound is rejected 401
//     (correct fail-closed behavior).
//
// Optional override (sane default; set to override):
//
//   - LISTEN_PUBLIC                Default ":8765". Public TCP listener
//     for /slack/events. Bind 0.0.0.0 if
//     fronted by a tunnel (Tailscale Funnel,
//     ngrok, etc.).
//   - LISTEN_INTERNAL              Default "127.0.0.1:8766". Loopback
//     listener for /publish and other gc-side
//     endpoints. Ignored when GC_SERVICE_SOCKET
//     is set (proxy_process mode).
//   - INTERNAL_CALLBACK_URL        Default "http://127.0.0.1:8766". URL
//     advertised to gc during self-registration.
//     In proxy_process mode this is computed
//     from GC_API_BASE_URL + GC_SERVICE_URL_PREFIX
//     and the env var is ignored.
//   - GC_API_BASE_URL              Default "http://127.0.0.1:9443". Base
//     URL for gc's HTTP API.
//   - ADAPTER_PROVIDER             Default "slack". Provider name used in
//     conversation refs and adapter registration.
//   - REGISTER_ON_START            Default "true". Set "false" to skip
//     /extmsg/adapters self-registration (used
//     by tests + diagnostics).
//   - HANDLE_PREFIX                Default "@". Leading address token
//     recognized on inbound messages for
//     keyword routing (e.g. "@name: text").
//     Empty string disables routing.
//   - IDENTITY_STORE_PATH          Default "/tmp/gc-slack-adapter/identities.json".
//     JSON file backing the per-session
//     chat:write.customize identity registry.
//     Persisted so adapter restarts don't strip
//     identity from running sessions.
//   - HANDLE_ALIAS_STORE_PATH      Default "/tmp/gc-slack-adapter/handle-aliases.json".
//     JSON file backing the cross-channel
//     handle → session-id alias registry.
//   - INBOUND_FILE_STORE           Default "/tmp/gc-slack-adapter/inbound".
//     Directory for downloaded inbound Slack
//     file attachments. Files are organized as
//     <store>/<channel>/<ts>-<safe-filename>
//     and exposed to gc as file:// URLs.
//   - INBOUND_FILE_TTL             Default "168h" (7 days). Maximum age
//     (mtime-based) before the in-process
//     janitor deletes a file. "0" disables the
//     janitor.
//   - INBOUND_FILE_SWEEP_INTERVAL  Default "1h". How often the janitor
//     wakes to scan INBOUND_FILE_STORE. "0"
//     disables the janitor.
//   - SLACK_CHANNEL_MAPPING_PATH    Default "<GC_CITY_PATH>/.gc/slack/channel_mappings.json"
//     when GC_CITY_PATH is set, otherwise
//     "/tmp/gc-slack-adapter/channel_mappings.json".
//     JSON file written by `gc slack
//     map-channel`. Read-only on the adapter
//     side; loaded at startup and re-read on
//     SIGHUP (gc-cby.23).
//   - SLACK_RIG_MAPPING_PATH        Default "<GC_CITY_PATH>/.gc/slack/rig_mappings.json"
//     when GC_CITY_PATH is set, otherwise
//     "/tmp/gc-slack-adapter/rig_mappings.json".
//     JSON file written by `gc slack map-rig`.
//     Read-only on the adapter side; same
//     SIGHUP-or-restart reload contract as
//     SLACK_CHANNEL_MAPPING_PATH. Channel
//     mappings override rig mappings when both
//     claim the same channel.
//   - SLACK_APPS_REGISTRY_PATH      Default "<GC_CITY_PATH>/.gc/slack/apps.json"
//     when GC_CITY_PATH is set, otherwise
//     "/tmp/gc-slack-adapter/apps.json". JSON
//     file written by `gc slack import-app`,
//     populated post-OAuth (gc-cby.9). Read-only
//     on the adapter side; same SIGHUP-or-restart
//     reload contract. Used for per-app signing
//     secret lookup keyed by team_id.
//   - GC_CITY_PATH                 Optional; consulted only to derive
//     SLACK_CHANNEL_MAPPING_PATH,
//     SLACK_RIG_MAPPING_PATH, and
//     SLACK_APPS_REGISTRY_PATH defaults.
//
// # File permissions
//
// IDENTITY_STORE_PATH, HANDLE_ALIAS_STORE_PATH, and INBOUND_FILE_STORE
// are written with 0o700 directories and 0o600 files so contents
// (session-id ↔ persona mappings, cross-channel handle aliases, and
// downloaded inbound Slack files — potentially DM content) are
// readable only by the adapter's UID. On startup the adapter
// additionally tightens any pre-existing files/directories that are
// looser. Operators using a custom-mode parent (setgid for
// shared-group access, etc.) should set perms before adapter start;
// the tightener preserves setuid/setgid/sticky bits and never
// loosens. The proxy_process Unix domain socket
// (GC_SERVICE_SOCKET) is also chmod'd to 0o600 after bind as
// defense-in-depth on top of its 0o700 controller-managed parent dir.
//
// Controller-injected (proxy_process mode only — set by gc when the
// adapter runs as a [[service]]):
//
//   - GC_SERVICE_SOCKET            Path to the UDS the adapter binds for
//     /publish and /healthz. Presence of this
//     var switches the adapter into
//     proxy_process mode.
//   - GC_SERVICE_URL_PREFIX        Required when GC_SERVICE_SOCKET is set
//     (e.g. "/svc/slack"). The adapter's
//     self-registered CallbackURL is computed
//     as GC_API_BASE_URL + GC_SERVICE_URL_PREFIX.
//
// Consumer-specific (referenced by deployment scripts and prompts but
// NOT consumed by the adapter binary):
//
//   - any environment used by sibling tooling (deliver-rollup.sh,
//     resolve_rig_channel.py, etc.) lives outside this binary and is
//     documented in the consumer pack's README.
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
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
)

const (
	// Public listener: serves /slack/events only. Bind to 0.0.0.0 so
	// Tailscale Funnel can reach it.
	defaultPublicListen = ":8765"
	// Internal listener: serves /publish only. Bound to 127.0.0.1 so
	// only processes on this machine (i.e. gc) can reach it.
	defaultInternalListen   = "127.0.0.1:8766"
	defaultInternalCallback = "http://127.0.0.1:8766"
)

// slackAPIBase is a var (not const) so tests can replace it with a fake.
var slackAPIBase = "https://slack.com/api"

// dispatchInflightWG counts in-flight dispatch goroutines that own a
// dispatch-slot release(). Every `go func()` that defers release()
// MUST also Add(1) before the spawn and `defer
// dispatchInflightWG.Done()` inside. The current set of spawn sites:
//
//   - interactions.go: slash → session, block_actions → session,
//     view_submission → session
//   - rig_dispatch.go: block_actions → rig, view_submission → rig
//   - main.go: events-path alias → session (handleSlackEvents)
//
// Tests use it as a barrier: signedSlackInteractionRequest registers
// dispatchInflightWG.Wait via t.Cleanup so any goroutine spawned by
// the test fully drains before the test framework moves on. Without
// the barrier, a leftover goroutine writing to log.Default() races
// the next test's log.SetOutput (gc-cby.36).
//
// Complementary to dispatchTestCompletionHook (rig_dispatch.go): the
// hook is a per-test signal that fires once at goroutine exit and is
// used by tests that assert on dispatch completion side effects;
// the WG is a universal drain barrier covering ALL dispatch sites.
// Folding the hook into the WG is left as a future cleanup.
var dispatchInflightWG sync.WaitGroup

// acquireDispatchSlot tries to acquire one slot on cfg.dispatchSem
// without blocking. On success it returns a release func bound to
// the channel observed at acquire time, plus the channel's capacity
// (handy for log messages); on failure it returns nil and the
// observed cap. The caller must defer release() at goroutine entry.
//
// Capturing the channel reference at acquire time keeps the goroutine
// race-clean even if a future call site builds a fresh cfg between
// acquire and release. A nil cfg.dispatchSem makes the channel send
// case block forever, falling through to default and reporting "not
// acquired"; main() initializes the field before any handler is wired
// in, so production reaches this only if the operator wires a handler
// from a test-style cfg without a sem (in which case the dropped-load
// log line is the intended fail-safe behavior). sec-S-04.
func (c config) acquireDispatchSlot() (release func(), capacity int, ok bool) {
	release, capacity, ok = c.tryAcquireDispatchSlot()
	if !ok {
		dispatchDroppedTotal.Add(1)
	}
	return release, capacity, ok
}

// tryAcquireDispatchSlot attempts to take a dispatch slot WITHOUT counting
// a failed acquire as a drop. The legacy inbound path wraps this via
// acquireDispatchSlot (a miss there is a genuine dropped delivery). The
// company path uses this directly: a company receipt that finds no slot
// stays durably pending for the sweep — backpressure, not a drop — so
// counting it would pollute dispatch_dropped_total and mask real legacy
// loss (F10).
func (c config) tryAcquireDispatchSlot() (release func(), capacity int, ok bool) {
	sem := c.dispatchSem
	semCap := cap(sem)
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, semCap, true
	default:
		return nil, semCap, false
	}
}

type config struct {
	publicListen        string
	internalListen      string // unused when serviceSocket is set
	serviceSocket       string // when set, bind a UDS here for /publish instead of internalListen
	internalCallbackURL string
	gcAPIBase           string
	cityName            string
	provider            string
	accountID           string
	slackBotToken       string
	slackSigningKey     string
	// slackAppID is the switchboard app's own Slack api_app_id (SLACK_APP_ID).
	// When set, an event_callback whose api_app_id equals it verifies against
	// the env signing secret ONLY (Phase 4 verification rule 2, the rooms path)
	// — never the legacy trial set and never a registered agent record that
	// happens to share the id. Empty preserves the pre-Phase-4 behavior (the
	// switchboard's events fall through to the legacy lookupSigningSecrets path).
	slackAppID      string
	registerOnStart bool
	// identityStorePath is the JSON file backing the per-session Slack
	// identity registry (chat:write.customize username/avatar overrides).
	// Persisted so adapter restarts don't strip identity from running
	// sessions.
	identityStorePath string
	// handlePrefix is the leading address token recognized on inbound
	// messages (e.g. "@"). When a message starts with
	// `<prefix><handle>:`, the handle is extracted into ExplicitTarget
	// and the prefix is stripped from the forwarded text. Empty disables
	// keyword routing.
	handlePrefix string
	// handleAliasStorePath is the JSON file backing the handle-alias
	// registry. Maps handle -> gc session id; used to dispatch
	// cross-channel address-by-handle messages (e.g. `@ops:` from any
	// channel routes to the session registered under the "ops" handle
	// even when that session has no Slack binding for the channel).
	handleAliasStorePath string
	// threadSessionsStorePath is the JSON file backing the thread →
	// session registry used by Slack launcher mode (cby.5). When a
	// `@@<handle>` post arrives in a thread, the adapter checks this
	// registry to converge subsequent posts in the same thread on a
	// single agent. Sourced from GC_SLACK_THREAD_SESSIONS_FILE,
	// defaulting to <GC_CITY_PATH>/.gc/slack/thread_sessions.json when
	// GC_CITY_PATH is set, else /tmp/gc-slack-adapter/thread_sessions.json.
	threadSessionsStorePath string
	// roomLaunchPath is the JSON file backing the room-launch mapping
	// registry used by Slack launcher mode (cby.5.3). Maps
	// (workspace_id, channel_id) → pool_template; written by
	// `gc slack enable-room-launch`. Sourced from
	// GC_SLACK_ROOM_LAUNCH_FILE, defaulting to
	// <GC_CITY_PATH>/.gc/slack/room_launch_mappings.json when
	// GC_CITY_PATH is set, else
	// /tmp/gc-slack-adapter/room_launch_mappings.json.
	roomLaunchPath string
	// inboundFileStore is the local directory where inbound Slack file
	// attachments are written so bound sessions can read them directly
	// (no bot-token leak). Files are organized as
	// <store>/<channel>/<ts>-<safe-filename>.
	inboundFileStore string
	// inboundFileTTL is the maximum age (mtime-based) of files in
	// inboundFileStore before the in-process janitor deletes them.
	// Empty or zero disables the janitor entirely.
	inboundFileTTL time.Duration
	// inboundFileSweepInterval is how often the janitor wakes up to
	// scan inboundFileStore. Empty or zero disables the janitor.
	inboundFileSweepInterval time.Duration
	// channelMappingPath is the JSON file written by
	// `gc slack map-channel` mapping (workspace_id, channel_id) →
	// (rig|session, target_id). Read-only on this side; the adapter
	// loads it at startup and re-reads on SIGHUP (gc-cby.23). Sourced
	// from SLACK_CHANNEL_MAPPING_PATH, defaulting to
	// <GC_CITY_PATH>/.gc/slack/channel_mappings.json when GC_CITY_PATH
	// is set, else /tmp/gc-slack-adapter/channel_mappings.json.
	channelMappingPath string
	// rigMappingPath is the JSON file written by `gc slack map-rig`
	// mapping (workspace_id, rig_name) → set-of-channel-ids. Read-only
	// on this side; same SIGHUP-or-restart reload contract as
	// channelMappingPath. Per-channel `map-channel` bindings take
	// precedence over rig defaults — see resolveChannelTarget. Sourced
	// from SLACK_RIG_MAPPING_PATH, defaulting to
	// <GC_CITY_PATH>/.gc/slack/rig_mappings.json when GC_CITY_PATH is
	// set, else /tmp/gc-slack-adapter/rig_mappings.json.
	rigMappingPath string
	// subteamAliasStorePath is the JSON file mapping Slack User Group
	// ("subteam") IDs (e.g. "S0123ABCD") to gc handles. Read-only on
	// this side; same SIGHUP-or-restart reload contract as
	// channelMappingPath. The operator edits the file directly or via a
	// future `gc slack subteam-alias` command. The map is the ONLY gate
	// for the UNLABELED subteam mention shape `<!subteam^Sxxx>` Slack
	// emits in event payloads — without an entry the inbound falls
	// through to channel fanout. The LABELED shape
	// `<!subteam^Sxxx|@handle>` remains gated by handleAliasRegistry
	// against the `@handle` label (bead gpk-2zi). Sourced from
	// SLACK_SUBTEAM_ALIAS_FILE, defaulting to
	// <GC_CITY_PATH>/.gc/slack/subteam-aliases.json when GC_CITY_PATH
	// is set, else /tmp/gc-slack-adapter/subteam-aliases.json. Bead
	// gpk-hmr.2.
	subteamAliasStorePath string
	// userAliasStorePath is the JSON file mapping a bare gc handle
	// (e.g. "mayor") to a raw Slack target ID — a user (Uxxxx/Wxxxx) or
	// a User Group (Sxxxx). handlePublish uses it to rewrite outbound
	// `@handle` body tokens into Slack mention syntax so they render as
	// clickable, notifying mentions instead of literal text (gpk-uha7).
	// This is the outbound inverse of subteamAliasStorePath and follows
	// the identical read-only, SIGHUP-or-restart reload contract: the
	// operator edits the file directly (or via a future `gc slack
	// user-alias` command). A handle absent from the map is left
	// literal — fail-safe, no surprise pings. Sourced from
	// SLACK_USER_ALIAS_FILE, defaulting to
	// <GC_CITY_PATH>/.gc/slack/slack-user-aliases.json when GC_CITY_PATH
	// is set, else /tmp/gc-slack-adapter/slack-user-aliases.json.
	userAliasStorePath string
	// fileUploadRoot is the absolute filesystem prefix
	// /publish-file is allowed to read. Empty disables /publish-file
	// entirely (fail-closed). gc and the adapter share a filesystem,
	// so the trust boundary is the gc controller process — but the
	// internal mux is reachable by anything on the loopback (or, in
	// proxy_process mode, by anything that can connect to the UDS),
	// so confinement here is defense-in-depth: a compromised internal
	// caller cannot ask the adapter to upload arbitrary files (e.g.
	// /etc/passwd) on its behalf. Sourced from FILE_UPLOAD_ROOT.
	fileUploadRoot string
	// dispatchConcurrency caps the number of in-flight inbound
	// dispatch goroutines (slash-command → session, slack-event →
	// session, alias-resolved → session). A burst of inbound traffic
	// otherwise spawns one goroutine per request, each holding an
	// http.Client with a 10s timeout — memory and FD pressure scale
	// linearly with traffic. Sourced from SLACK_DISPATCH_CONCURRENCY,
	// default 50. Must be a positive integer; loadConfig rejects 0,
	// negative, and non-numeric values rather than silently disabling
	// dispatch. sec-S-04.
	dispatchConcurrency int
	// dispatchSem caps the number of concurrent inbound dispatch
	// goroutines. main() initializes this to a buffered channel of
	// size dispatchConcurrency before any handler is wired in;
	// acquireDispatchSlot reads it through the cfg value. Tests build a
	// cfg with their own scoped channel rather than sharing a
	// package-level singleton, so saturation tests can run in parallel
	// without interfering with other tests' slot counts. gc-px8.7
	// (was gc-cby.30).
	dispatchSem chan struct{}
	// appsRegistryPath is the JSON file written by `gc slack import-app`
	// mapping (workspace_id, app_id) → app record (incl. signing_secret
	// populated post-OAuth). Read-only on this side; same SIGHUP-or-
	// restart reload contract as channelMappingPath. Sourced from
	// SLACK_APPS_REGISTRY_PATH, defaulting to
	// <GC_CITY_PATH>/.gc/slack/apps.json when GC_CITY_PATH is set, else
	// /tmp/gc-slack-adapter/apps.json. Used to resolve per-app signing
	// secrets for /slack/events and /slack/interactions request
	// verification.
	appsRegistryPath string
	// appsRegistry is the in-memory snapshot of appsRegistryPath, wired
	// at startup. Nil-safe — when nil, lookupSigningSecrets falls
	// through to slackSigningKey for single-app dev installs.
	appsRegistry *appsRegistry
	// oauthClientID, oauthClientSecret, oauthRedirectURI configure the
	// OAuth install flow (gc-cby.9). When oauthClientID is empty the
	// /slack/oauth/{start,callback} handlers are not registered and
	// install relies on the manual web-UI flow documented in
	// adapter/SETUP.md. When set, the adapter registers the two
	// handlers on the public mux; an operator visits /slack/oauth/start
	// to grant the app to a workspace, and the callback persists the
	// resulting bot_token + workspace_id + app_id into the apps
	// registry and writes <cityPath>/.gc/slack/install.env so the
	// operator can re-source and restart the adapter.
	oauthClientID     string
	oauthClientSecret string
	oauthRedirectURI  string
	// oauthSlackBaseURL overrides the Slack base URL used by the OAuth
	// flow (default https://slack.com). Tests inject an httptest.Server
	// URL via this field; production deployments leave it empty.
	oauthSlackBaseURL string
	// cityPath is the on-disk root of the gc city this adapter is bound
	// to. Sourced from GC_CITY_PATH; required for the rig-target
	// dispatch path (cby.18.3) which must shell `gc bd create` inside the
	// rig's workdir (read from <cityPath>/.beads/routes.jsonl) and
	// `gc sling` from the city root. Empty when GC_CITY_PATH is unset;
	// the rig dispatch path surfaces a fix-it ephemeral in that case.
	cityPath string
	// threadContextCache is the process-singleton cache that
	// short-circuits repeated thread-context fetches for a given
	// (channel, thread_ts). Nil-safe: when nil, processSlackEvent
	// skips the preamble path entirely. Initialized in main(); tests
	// construct one directly. gc-px8.5.
	threadContextCache *threadContextCache
	// slackThreadContextLimit caps how many replies the adapter asks
	// for when seeding thread context. Sourced from
	// SLACK_THREAD_CONTEXT_LIMIT, defaulting to
	// defaultThreadContextLimit. gc-px8.5.
	slackThreadContextLimit int
	// companyDirectoryPath / companyBindingsPath / companyIngressDir are
	// the Slack company-rooms (Phase 1) registry locations. The two JSON
	// registries resolve exactly like the six atomic registries
	// (env override > <GC_CITY_PATH>/.gc/slack/<file> >
	// /tmp/gc-slack-adapter/<file>); the ingress dir mirrors the
	// thread_sessions.json resolution with a chat-ingress/ leaf. The
	// Python CLI (scripts/slack_company_directory.py) resolves the same
	// files. Sourced from SLACK_COMPANY_DIRECTORY_PATH,
	// SLACK_COMPANY_BINDINGS_PATH, SLACK_COMPANY_INGRESS_DIR.
	companyDirectoryPath string
	companyBindingsPath  string
	companyIngressDir    string
	// companyDMBindingsPath / companyAgentAppsPath are the Phase 4 per-agent
	// DM registries (dm_bindings.json, agent_apps.json), resolved exactly
	// like the two Phase 1 registries above (env override >
	// <GC_CITY_PATH>/.gc/slack/<file> > /tmp default). Sourced from
	// SLACK_COMPANY_DM_BINDINGS_PATH and SLACK_COMPANY_AGENT_APPS_PATH.
	companyDMBindingsPath string
	companyAgentAppsPath  string
	// companyVerifySessions gates the advisory session-existence guard
	// (Phase 4): when set, delivery checks GET /v0/city/{city}/session/{id}
	// before the first attempt per (city, session). Advisory only — a guard
	// error or a 404/409 never terminalizes; it just leaves the target
	// pending for the sweep. Sourced from SLACK_COMPANY_VERIFY_SESSIONS.
	companyVerifySessions bool
	// Phase 2 shared-state directories (secrets/intents/delegations/turns/
	// locks). Resolved exactly like the Python side: env override >
	// <GC_CITY_PATH>/.gc/slack/<leaf> > /tmp/gc-slack-adapter/<leaf>. The Go
	// ingress path reads intents (correlation + stale count), reads/writes
	// delegation records (result claims), writes current-turn pointers, and
	// takes advisory locks; the secrets dir is Python-only but resolved here
	// for parity / config visibility.
	companySecretsDir     string
	companyIntentsDir     string
	companyDelegationsDir string
	companyTurnsDir       string
	companyLocksDir       string
	// companyCityAPIs maps a city-qualified binding's city name to that
	// city's supervisor API base URL (each city runs its own supervisor on
	// this host). Parsed from SLACK_COMPANY_CITY_APIS as
	// "city=http://127.0.0.1:8377,other=http://127.0.0.1:8374". The
	// adapter's own city never needs an entry.
	companyCityAPIs map[string]string

	// companySelfBotUserID is the switchboard app's own bot user id,
	// excluded from wake routing so the switchboard never wakes itself.
	// Optional (empty OK in Phase 1). Sourced from
	// SLACK_SWITCHBOARD_BOT_USER_ID.
	companySelfBotUserID string
	// companyVisibleAcks gates the config-driven visible-ack reactions
	// (Phase 3b). Off by default: unset/empty/"0" = off, anything else = on.
	// Sourced from SLACK_COMPANY_VISIBLE_ACKS.
	companyVisibleAcks bool
	// companyGateway owns the durable-admission + delivery path for
	// imported company rooms. Nil disables the company path entirely —
	// every inbound then flows through the legacy path byte-for-byte.
	// Wired in main() before any handler closes over the cfg value.
	companyGateway *companyGateway
}

func loadConfig() (config, error) {
	return loadConfigFromEnv(os.Getenv)
}

// companyStateDirDefault resolves a Phase 2 shared-state directory default:
// <GC_CITY_PATH>/.gc/slack/<leaf> when the city path is set, else
// /tmp/gc-slack-adapter/<leaf>. The env override is applied by the caller.
// This mirrors the Python company outbound module's path resolution leaf for
// leaf.
func companyStateDirDefault(cityPath, leaf string) string {
	if cityPath != "" {
		return filepath.Join(cityPath, ".gc", "slack", leaf)
	}
	return filepath.Join("/tmp/gc-slack-adapter", leaf)
}

// loadConfigFromEnv reads adapter configuration from a getenv function. When
// $GC_SERVICE_SOCKET is set, the adapter switches to proxy_process mode: it
// binds a Unix domain socket for /publish (and /healthz) instead of an
// internal TCP listener, and registers the callback URL gc routes through
// its /svc/{name} mount. This keeps a single binary serving both the legacy
// nohup-managed deployment and the proxy_process deployment.
func loadConfigFromEnv(getenv func(string) string) (config, error) {
	envOrFn := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	cfg := config{
		publicListen:         envOrFn("LISTEN_PUBLIC", defaultPublicListen),
		internalListen:       envOrFn("LISTEN_INTERNAL", defaultInternalListen),
		serviceSocket:        getenv("GC_SERVICE_SOCKET"),
		internalCallbackURL:  strings.TrimRight(envOrFn("INTERNAL_CALLBACK_URL", defaultInternalCallback), "/"),
		gcAPIBase:            strings.TrimRight(envOrFn("GC_API_BASE_URL", "http://127.0.0.1:9443"), "/"),
		cityName:             getenv("GC_CITY_NAME"),
		provider:             envOrFn("ADAPTER_PROVIDER", "slack"),
		accountID:            getenv("SLACK_WORKSPACE_ID"),
		slackBotToken:        getenv("SLACK_BOT_TOKEN"),
		slackSigningKey:      getenv("SLACK_SIGNING_SECRET"),
		slackAppID:           getenv("SLACK_APP_ID"),
		registerOnStart:      envOrFn("REGISTER_ON_START", "true") == "true",
		identityStorePath:    envOrFn("IDENTITY_STORE_PATH", "/tmp/gc-slack-adapter/identities.json"),
		handlePrefix:         envOrFn("HANDLE_PREFIX", "@"),
		handleAliasStorePath: envOrFn("HANDLE_ALIAS_STORE_PATH", "/tmp/gc-slack-adapter/handle-aliases.json"),
		inboundFileStore:     envOrFn("INBOUND_FILE_STORE", "/tmp/gc-slack-adapter/inbound"),
		fileUploadRoot:       getenv("FILE_UPLOAD_ROOT"),
	}

	// channelMappingPath default: prefer the city-rooted path when
	// GC_CITY_PATH is set so a single-host slack-pack deployment
	// "just works" without operator config; fall back to the legacy
	// /tmp/gc-slack-adapter/ tree otherwise. Operators can override
	// explicitly with SLACK_CHANNEL_MAPPING_PATH. Apps registry path
	// follows the same convention.
	defaultMappingPath := "/tmp/gc-slack-adapter/channel_mappings.json"
	defaultRigMappingPath := "/tmp/gc-slack-adapter/rig_mappings.json"
	defaultAppsRegistryPath := "/tmp/gc-slack-adapter/apps.json"
	defaultThreadSessionsPath := "/tmp/gc-slack-adapter/thread_sessions.json"
	defaultRoomLaunchPath := "/tmp/gc-slack-adapter/room_launch_mappings.json"
	defaultSubteamAliasPath := "/tmp/gc-slack-adapter/subteam-aliases.json"
	defaultUserAliasPath := "/tmp/gc-slack-adapter/slack-user-aliases.json"
	if cityPath := getenv("GC_CITY_PATH"); cityPath != "" {
		defaultMappingPath = filepath.Join(cityPath, ".gc", "slack", "channel_mappings.json")
		defaultRigMappingPath = filepath.Join(cityPath, ".gc", "slack", "rig_mappings.json")
		defaultAppsRegistryPath = filepath.Join(cityPath, ".gc", "slack", "apps.json")
		defaultThreadSessionsPath = filepath.Join(cityPath, ".gc", "slack", "thread_sessions.json")
		defaultRoomLaunchPath = filepath.Join(cityPath, ".gc", "slack", "room_launch_mappings.json")
		defaultSubteamAliasPath = filepath.Join(cityPath, ".gc", "slack", "subteam-aliases.json")
		defaultUserAliasPath = filepath.Join(cityPath, ".gc", "slack", "slack-user-aliases.json")
		cfg.cityPath = cityPath
	}
	cfg.channelMappingPath = envOrFn("SLACK_CHANNEL_MAPPING_PATH", defaultMappingPath)
	cfg.rigMappingPath = envOrFn("SLACK_RIG_MAPPING_PATH", defaultRigMappingPath)
	cfg.appsRegistryPath = envOrFn("SLACK_APPS_REGISTRY_PATH", defaultAppsRegistryPath)
	cfg.oauthClientID = getenv("SLACK_CLIENT_ID")
	cfg.oauthClientSecret = getenv("SLACK_CLIENT_SECRET")
	cfg.oauthRedirectURI = getenv("SLACK_REDIRECT_URI")
	cfg.oauthSlackBaseURL = getenv("SLACK_OAUTH_BASE_URL")
	cfg.threadSessionsStorePath = envOrFn("GC_SLACK_THREAD_SESSIONS_FILE", defaultThreadSessionsPath)
	cfg.roomLaunchPath = envOrFn("GC_SLACK_ROOM_LAUNCH_FILE", defaultRoomLaunchPath)
	cfg.subteamAliasStorePath = envOrFn("SLACK_SUBTEAM_ALIAS_FILE", defaultSubteamAliasPath)
	cfg.userAliasStorePath = envOrFn("SLACK_USER_ALIAS_FILE", defaultUserAliasPath)

	// Company-rooms (Phase 1) registry + ingress paths. The two JSON
	// registries follow the same city-rooted-then-/tmp default with an
	// env override as the six atomic registries; the ingress dir mirrors
	// thread_sessions.json resolution with a chat-ingress/ leaf. These
	// MUST match scripts/slack_company_directory.py file for file.
	defaultCompanyDirectoryPath := "/tmp/gc-slack-adapter/company_directory.json"
	defaultCompanyBindingsPath := "/tmp/gc-slack-adapter/company_bindings.json"
	defaultCompanyDMBindingsPath := "/tmp/gc-slack-adapter/dm_bindings.json"
	defaultCompanyAgentAppsPath := "/tmp/gc-slack-adapter/agent_apps.json"
	defaultCompanyIngressDir := "/tmp/gc-slack-adapter/chat-ingress"
	if cfg.cityPath != "" {
		defaultCompanyDirectoryPath = filepath.Join(cfg.cityPath, ".gc", "slack", "company_directory.json")
		defaultCompanyBindingsPath = filepath.Join(cfg.cityPath, ".gc", "slack", "company_bindings.json")
		defaultCompanyDMBindingsPath = filepath.Join(cfg.cityPath, ".gc", "slack", "dm_bindings.json")
		defaultCompanyAgentAppsPath = filepath.Join(cfg.cityPath, ".gc", "slack", "agent_apps.json")
		defaultCompanyIngressDir = filepath.Join(cfg.cityPath, ".gc", "slack", "chat-ingress")
	}
	cfg.companyDirectoryPath = envOrFn("SLACK_COMPANY_DIRECTORY_PATH", defaultCompanyDirectoryPath)
	cfg.companyBindingsPath = envOrFn("SLACK_COMPANY_BINDINGS_PATH", defaultCompanyBindingsPath)
	cfg.companyDMBindingsPath = envOrFn("SLACK_COMPANY_DM_BINDINGS_PATH", defaultCompanyDMBindingsPath)
	cfg.companyAgentAppsPath = envOrFn("SLACK_COMPANY_AGENT_APPS_PATH", defaultCompanyAgentAppsPath)
	cfg.companyIngressDir = envOrFn("SLACK_COMPANY_INGRESS_DIR", defaultCompanyIngressDir)
	cfg.companySelfBotUserID = getenv("SLACK_SWITCHBOARD_BOT_USER_ID")
	if raw := getenv("SLACK_COMPANY_CITY_APIS"); raw != "" {
		cfg.companyCityAPIs = make(map[string]string)
		for _, pair := range strings.Split(raw, ",") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			name, base, ok := strings.Cut(pair, "=")
			name, base = strings.TrimSpace(name), strings.TrimRight(strings.TrimSpace(base), "/")
			if !ok || name == "" || base == "" || strings.ContainsAny(name, "/?#% \t") ||
				(!strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://")) {
				return cfg, fmt.Errorf("SLACK_COMPANY_CITY_APIS: invalid entry %q (want city=http(s)://host:port)", pair)
			}
			cfg.companyCityAPIs[name] = base
		}
	}
	// Visible-ack gate: off unless the env var is a non-empty value other
	// than "0" (the same truthiness the rest of the company config uses).
	if v := strings.TrimSpace(getenv("SLACK_COMPANY_VISIBLE_ACKS")); v != "" && v != "0" {
		cfg.companyVisibleAcks = true
	}
	// Advisory session-existence guard (Phase 4): same truthiness convention.
	// Default off — the guard must never reduce availability below flag-off.
	if v := strings.TrimSpace(getenv("SLACK_COMPANY_VERIFY_SESSIONS")); v != "" && v != "0" {
		cfg.companyVerifySessions = true
	}

	// Phase 2 shared-state directories. Same resolution precedence as the
	// registries above, with the Python leaf names (secrets/,
	// company-delegation-intents/, company-delegations/, company-current-turn/,
	// locks/). These MUST match scripts/slack_company_outbound.py file for
	// file.
	cfg.companySecretsDir = envOrFn("SLACK_COMPANY_SECRETS_DIR", companyStateDirDefault(cfg.cityPath, "secrets"))
	cfg.companyIntentsDir = envOrFn("SLACK_COMPANY_INTENTS_DIR", companyStateDirDefault(cfg.cityPath, "company-delegation-intents"))
	cfg.companyDelegationsDir = envOrFn("SLACK_COMPANY_DELEGATIONS_DIR", companyStateDirDefault(cfg.cityPath, "company-delegations"))
	cfg.companyTurnsDir = envOrFn("SLACK_COMPANY_TURNS_DIR", companyStateDirDefault(cfg.cityPath, "company-current-turn"))
	cfg.companyLocksDir = envOrFn("SLACK_COMPANY_LOCKS_DIR", companyStateDirDefault(cfg.cityPath, "locks"))

	// Retention controls. Defaults: keep inbound files for 7 days,
	// sweep every hour. Setting either to "0" disables the janitor.
	// Invalid duration strings also disable (with a fatal-config error
	// would be too aggressive — log and continue without sweeping).
	if d, err := time.ParseDuration(envOrFn("INBOUND_FILE_TTL", "168h")); err == nil {
		cfg.inboundFileTTL = d
	} else {
		log.Printf("INBOUND_FILE_TTL %q invalid: %v (janitor disabled)", getenv("INBOUND_FILE_TTL"), err)
	}
	if d, err := time.ParseDuration(envOrFn("INBOUND_FILE_SWEEP_INTERVAL", "1h")); err == nil {
		cfg.inboundFileSweepInterval = d
	} else {
		log.Printf("INBOUND_FILE_SWEEP_INTERVAL %q invalid: %v (janitor disabled)", getenv("INBOUND_FILE_SWEEP_INTERVAL"), err)
	}

	// dispatchConcurrency: bound goroutine fan-out on inbound dispatch
	// paths. Reject 0/negative/non-numeric at startup — silently
	// disabling dispatch (cap=0 -> always-drop) is almost certainly a
	// misconfiguration, and a non-numeric value usually means the
	// operator typo'd the var name. sec-S-04.
	raw := envOrFn("SLACK_DISPATCH_CONCURRENCY", "50")
	n, err := strconv.Atoi(raw)
	if err != nil {
		return cfg, fmt.Errorf("SLACK_DISPATCH_CONCURRENCY %q is not an integer: %w", raw, err)
	}
	if n <= 0 {
		return cfg, fmt.Errorf("SLACK_DISPATCH_CONCURRENCY must be > 0, got %d", n)
	}
	cfg.dispatchConcurrency = n

	// slackThreadContextLimit: cap on conversations.replies fetch when
	// seeding thread context (gc-px8.5). Reject 0/negative/non-numeric
	// at startup; an operator who typed an invalid limit almost
	// certainly didn't mean "disable thread context entirely" (silent
	// disable is a footgun). Use defaultThreadContextLimit when unset.
	rawLimit := envOrFn("SLACK_THREAD_CONTEXT_LIMIT", strconv.Itoa(defaultThreadContextLimit))
	limit, err := strconv.Atoi(rawLimit)
	if err != nil {
		return cfg, fmt.Errorf("SLACK_THREAD_CONTEXT_LIMIT %q is not an integer: %w", rawLimit, err)
	}
	if limit <= 0 {
		return cfg, fmt.Errorf("SLACK_THREAD_CONTEXT_LIMIT must be > 0, got %d", limit)
	}
	cfg.slackThreadContextLimit = limit

	if cfg.serviceSocket != "" {
		// proxy_process mode: gc reaches us via $GC_API_BASE_URL +
		// $GC_SERVICE_URL_PREFIX (e.g. http://127.0.0.1:8372/svc/slack).
		// gc's extmsg HTTP adapter appends "/publish" itself when calling,
		// so the registered base URL must NOT include /publish.
		urlPrefix := strings.TrimRight(getenv("GC_SERVICE_URL_PREFIX"), "/")
		if urlPrefix == "" {
			return cfg, errors.New("GC_SERVICE_SOCKET is set but GC_SERVICE_URL_PREFIX is empty — controller-injected env is incomplete")
		}
		if cfg.gcAPIBase == "" {
			return cfg, errors.New("GC_SERVICE_SOCKET is set but GC_API_BASE_URL is empty — cannot compute callback URL for self-registration")
		}
		cfg.internalCallbackURL = cfg.gcAPIBase + urlPrefix
	}

	var missing []string
	if cfg.accountID == "" {
		missing = append(missing, "SLACK_WORKSPACE_ID")
	}
	if cfg.slackBotToken == "" {
		missing = append(missing, "SLACK_BOT_TOKEN")
	}
	// SLACK_SIGNING_SECRET is now optional: gc-cby.16 introduced a
	// per-app apps registry (apps.json) that supplies signing secrets
	// per (workspace_id, app_id). The env var remains as a single-app
	// fallback for dev / legacy installs. lookupSigningSecrets returns
	// no candidates when both sources are empty, and the verify path
	// returns 401 — the correct fail-closed behavior.
	if cfg.cityName == "" {
		// GC_CITY_NAME is required: every inbound POST and every
		// dispatch-to-aliased-session call constructs a URL of the
		// form /v0/city/{cityName}/.... A wrong default silently
		// routes traffic to the wrong city, so fail-fast instead.
		missing = append(missing, "GC_CITY_NAME")
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	// cityName is interpolated into every /v0/city/{cityName}/... URL the
	// adapter constructs. URL-significant characters (/, ?, #, %) here
	// would either change the URL's path structure or be ambiguously
	// interpreted by intermediate proxies — silently routing traffic to
	// the wrong city. cby-set-c added url.PathEscape on the session-scoped
	// dispatch paths, but other cityName interpolation sites still build
	// URLs with bare %s formatting (gc-cby.28 closes those, plus any
	// remaining sites). Until per-call escaping is uniform, this startup
	// guard is the primary defense — a legitimate city name should never
	// contain these characters, so reject them and fail fast. gc-cby.29.
	if strings.ContainsAny(cfg.cityName, "/?#%") {
		return cfg, fmt.Errorf("GC_CITY_NAME must not contain '/', '?', '#', or '%%': %q", cfg.cityName)
	}
	return cfg, nil
}

// gc-side types — mirrored from internal/extmsg/types.go to avoid coupling
// to the gc module. Wire-compatible only.

type conversationRef struct {
	ScopeID              string `json:"scope_id"`
	Provider             string `json:"provider"`
	AccountID            string `json:"account_id"`
	ConversationID       string `json:"conversation_id"`
	ParentConversationID string `json:"parent_conversation_id,omitempty"`
	Kind                 string `json:"kind"`
}

type publishRequest struct {
	SessionID        string            `json:"session_id"`
	Conversation     conversationRef   `json:"conversation"`
	Text             string            `json:"text"`
	ReplyToMessageID string            `json:"reply_to_message_id,omitempty"`
	IdempotencyKey   string            `json:"idempotency_key,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

// metadataKeySourceSessionID is the legacy metadata key gc used to
// propagate the originating session id before PublishRequest gained a
// native SessionID field (gc-kvt). Modern gc binaries write SessionID
// directly; this fallback exists only so older gc binaries publishing
// through this adapter still resolve the per-session identity record.
const metadataKeySourceSessionID = "source_session_id"

type publishReceipt struct {
	Conversation conversationRef `json:"conversation"`
	MessageID    string          `json:"message_id,omitempty"`
	Delivered    bool            `json:"delivered"`
	FailureKind  string          `json:"failure_kind,omitempty"`
}

type externalActor struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	IsBot       bool   `json:"is_bot"`
}

// externalAttachment mirrors extmsg.ExternalAttachment on the gc side.
// URL is a `file://` local path when the adapter has downloaded the bytes
// for inbound files (so bound sessions can read it directly without
// leaking the bot token); for outbound transcripts that originated as
// outbound files, URL is the Slack permalink.
type externalAttachment struct {
	ProviderID string `json:"provider_id"`
	URL        string `json:"url"`
	// mime_type is REQUIRED on the gc side (extmsg.ExternalAttachment):
	// omitting the key gets the whole inbound message rejected with 422,
	// so it is deliberately NOT omitempty. Producers derive a non-empty
	// value via attachmentMIMEType; an empty string here is at worst a
	// degraded value, never a missing required key.
	MIMEType string `json:"mime_type"`
}

type externalInboundMessage struct {
	ProviderMessageID string               `json:"provider_message_id"`
	Conversation      conversationRef      `json:"conversation"`
	Actor             externalActor        `json:"actor"`
	Text              string               `json:"text"`
	ExplicitTarget    string               `json:"explicit_target,omitempty"`
	ReplyToMessageID  string               `json:"reply_to_message_id,omitempty"`
	Attachments       []externalAttachment `json:"attachments,omitempty"`
	DedupKey          string               `json:"dedup_key,omitempty"`
	ReceivedAt        time.Time            `json:"received_at"`
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

// Slack API types

type slackPostMessageReq struct {
	Channel   string `json:"channel"`
	Text      string `json:"text"`
	ThreadTS  string `json:"thread_ts,omitempty"`
	Username  string `json:"username,omitempty"`
	IconURL   string `json:"icon_url,omitempty"`
	IconEmoji string `json:"icon_emoji,omitempty"`
}

type slackPostMessageResp struct {
	OK      bool   `json:"ok"`
	TS      string `json:"ts,omitempty"`
	Channel string `json:"channel,omitempty"`
	Error   string `json:"error,omitempty"`
}

// Slack files-upload-v2 API types.
//
// Slack deprecated the legacy /files.upload endpoint; the supported flow is
// the three-step v2 protocol:
//
//	1. POST /files.getUploadURLExternal (form-urlencoded) with {filename, length}
//	   → {ok, upload_url, file_id}
//	2. PUT raw bytes to the returned upload_url (no auth header — the URL is
//	   pre-signed and short-lived).
//	3. POST /files.completeUploadExternal (JSON) with {files: [{id, title}],
//	   channel_id, initial_comment, thread_ts} — channel posting happens here.
//
// The bot token requires the `files:write` scope. Without it, step 1 returns
// {ok: false, error: "missing_scope"} and the failure propagates as
// FailureKind="permanent" with the auth error logged.

type slackGetUploadURLResp struct {
	OK        bool   `json:"ok"`
	UploadURL string `json:"upload_url,omitempty"`
	FileID    string `json:"file_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

type slackCompleteUploadFile struct {
	ID    string `json:"id"`
	Title string `json:"title,omitempty"`
}

type slackCompleteUploadReq struct {
	Files          []slackCompleteUploadFile `json:"files"`
	ChannelID      string                    `json:"channel_id,omitempty"`
	InitialComment string                    `json:"initial_comment,omitempty"`
	ThreadTS       string                    `json:"thread_ts,omitempty"`
}

type slackCompleteUploadResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Files []struct {
		ID string `json:"id"`
	} `json:"files,omitempty"`
}

// publishFileRequest is the body of POST /publish-file. Mirrors
// publishRequest but adds a file payload (path on the local filesystem
// the adapter can read). The session-id resolution precedence is the
// same: explicit SessionID wins over Metadata["source_session_id"].
type publishFileRequest struct {
	SessionID        string            `json:"session_id,omitempty"`
	Conversation     conversationRef   `json:"conversation"`
	FilePath         string            `json:"file_path"`
	Filename         string            `json:"filename,omitempty"`
	InitialComment   string            `json:"initial_comment,omitempty"`
	ReplyToMessageID string            `json:"reply_to_message_id,omitempty"`
	Title            string            `json:"title,omitempty"`
	IdempotencyKey   string            `json:"idempotency_key,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

// publishFileReceipt mirrors publishReceipt but carries the Slack file_id
// instead of a chat ts. When Delivered=true, FileID is the canonical
// reference for the uploaded file (used by tests + downstream tooling).
type publishFileReceipt struct {
	Conversation conversationRef `json:"conversation"`
	FileID       string          `json:"file_id,omitempty"`
	Delivered    bool            `json:"delivered"`
	FailureKind  string          `json:"failure_kind,omitempty"`
	Error        string          `json:"error,omitempty"`
}

type slackReactionsAddReq struct {
	Channel   string `json:"channel"`
	Name      string `json:"name"`
	Timestamp string `json:"timestamp"`
}

type slackReactionsAddResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// reactRequest is the body the slack pack POSTs to /react. The conversation
// id is the Slack channel id; the message id is the Slack ts. Emoji is the
// reaction name without colons (e.g. "eyes", not ":eyes:").
type reactRequest struct {
	Conversation conversationRef `json:"conversation"`
	MessageID    string          `json:"message_id"`
	Emoji        string          `json:"emoji"`
}

type reactReceipt struct {
	Delivered   bool   `json:"delivered"`
	FailureKind string `json:"failure_kind,omitempty"`
}

// identityRecord is the persisted Slack identity override for a single gc
// session id. All fields are optional; an empty record means "use the default
// bot identity for any publish from this session". Slack's chat.postMessage
// requires the `chat:write.customize` scope for these fields to take effect.
type identityRecord struct {
	Username  string `json:"username,omitempty"`
	IconURL   string `json:"icon_url,omitempty"`
	IconEmoji string `json:"icon_emoji,omitempty"`
}

// identityRequest is the body of POST /identity. SessionID is required;
// every other field is optional. Posting an empty record (only session_id)
// effectively resets the session back to the default bot identity.
type identityRequest struct {
	SessionID string `json:"session_id"`
	Username  string `json:"username,omitempty"`
	IconURL   string `json:"icon_url,omitempty"`
	IconEmoji string `json:"icon_emoji,omitempty"`
}

type identityReceipt struct {
	Stored    bool   `json:"stored"`
	SessionID string `json:"session_id,omitempty"`
}

// identityDeleteReceipt is the response body of DELETE /identity. Existed
// is true when the session id was actually registered before; the call
// succeeds either way (idempotent delete).
type identityDeleteReceipt struct {
	Removed   bool   `json:"removed"`
	Existed   bool   `json:"existed"`
	SessionID string `json:"session_id,omitempty"`
}

// handleAliasRequest is the body of POST /handle-alias. Empty session_id
// removes the alias.
type handleAliasRequest struct {
	Handle    string `json:"handle"`
	SessionID string `json:"session_id"`
}

type handleAliasReceipt struct {
	Stored    bool   `json:"stored"`
	Removed   bool   `json:"removed,omitempty"`
	Handle    string `json:"handle,omitempty"`
	SessionID string `json:"session_id,omitempty"`
}

// handleAliasDeleteReceipt mirrors identityDeleteReceipt for the alias
// registry. Existed is true iff the handle was actually registered.
type handleAliasDeleteReceipt struct {
	Removed bool   `json:"removed"`
	Existed bool   `json:"existed"`
	Handle  string `json:"handle,omitempty"`
}

// gcSessionMessageRequest mirrors handler_session_interaction.go's
// sessionMessageRequest. We POST it to gc /v0/session/{id}/messages to
// inject a system reminder into a session that has no binding for the
// originating Slack conversation.
type gcSessionMessageRequest struct {
	Message string `json:"message"`
}

type slackEventEnvelope struct {
	Type      string          `json:"type"`
	Challenge string          `json:"challenge,omitempty"`
	TeamID    string          `json:"team_id,omitempty"`
	APIAppID  string          `json:"api_app_id,omitempty"`
	EventID   string          `json:"event_id,omitempty"`
	Event     json.RawMessage `json:"event,omitempty"`
}

// slackFile is a subset of Slack's file object, just the fields we need
// to download the bytes and pass useful metadata up to gc. Filetype, Size,
// and URLPrivateDownload feed the company file-hydration path (snippet
// content inlined into the frozen reminder); the download URL is preferred
// over url_private for content fetches because Slack marks it with the
// Content-Disposition that yields the raw bytes rather than an HTML wrapper.
type slackFile struct {
	ID                 string `json:"id"`
	Name               string `json:"name,omitempty"`
	Title              string `json:"title,omitempty"`
	URLPrivate         string `json:"url_private,omitempty"`
	URLPrivateDownload string `json:"url_private_download,omitempty"`
	MIMEType           string `json:"mimetype,omitempty"`
	Filetype           string `json:"filetype,omitempty"`
	// Subtype is set for recordings made inside Slack ("slack_audio"
	// voice clips, "slack_video" video clips), which carry NO
	// mimetype/filetype — the only hint attachmentMIMEType has for an
	// extension-less one.
	Subtype string `json:"subtype,omitempty"`
	Size    int    `json:"size,omitempty"`
}

type slackMessageEvent struct {
	Type        string          `json:"type"`
	Subtype     string          `json:"subtype,omitempty"`
	User        string          `json:"user,omitempty"`
	BotID       string          `json:"bot_id,omitempty"`
	Text        string          `json:"text,omitempty"`
	Channel     string          `json:"channel,omitempty"`
	TS          string          `json:"ts,omitempty"`
	ThreadTS    string          `json:"thread_ts,omitempty"`
	EventTS     string          `json:"event_ts,omitempty"`
	ChannelType string          `json:"channel_type,omitempty"`
	Files       []slackFile     `json:"files,omitempty"`
	Blocks      json.RawMessage `json:"blocks,omitempty"`
	// AppID / BotProfile corroborate a bot author's bots.info resolution
	// (Phase 2c); Metadata carries delegation / result correlation
	// breadcrumbs on company posts.
	AppID      string          `json:"app_id,omitempty"`
	BotProfile json.RawMessage `json:"bot_profile,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	// Initialize the shared dispatch semaphore on the cfg value before
	// any handler closes over it. cap is a fixed positive int —
	// loadConfig rejected 0/negative. sec-S-04. gc-px8.7.
	cfg.dispatchSem = make(chan struct{}, cfg.dispatchConcurrency)
	// Wire the process-wide thread-context cache. Nil-safe consumer
	// path; only the production main() initializes it. gc-px8.5.
	cfg.threadContextCache = newThreadContextCache()
	internalDescr := cfg.internalListen
	if cfg.serviceSocket != "" {
		internalDescr = "uds:" + cfg.serviceSocket
	}
	log.Printf("starting gc-slack-adapter public=%s internal=%s gc=%s city=%s dispatch_concurrency=%d",
		cfg.publicListen, internalDescr, cfg.gcAPIBase, cfg.cityName, cfg.dispatchConcurrency)

	// Tighten any pre-existing /tmp/gc-slack-adapter/* state from
	// pre-fix installs to 0o700 dirs / 0o600 files. Must run BEFORE
	// the public listener binds and BEFORE concurrent writers
	// (registries on first save, janitor goroutine) start, so there's
	// no race with other writers in this process. gc-ywe.6.
	tightenStorePermissions(cfg)

	// Best-effort sweep of orphaned atomic-write .tmp files left over
	// from a previous crashed run. Runs before any registry constructor
	// so a follow-up first save cannot collide with a stale tmp name.
	// Errors are logged inside the helper; only directory-listing
	// failures bubble up here (treated as non-fatal — the registry will
	// still load from <diskPath>).
	for _, p := range []string{
		cfg.identityStorePath,
		cfg.handleAliasStorePath,
		cfg.channelMappingPath,
		cfg.rigMappingPath,
		cfg.appsRegistryPath,
		cfg.threadSessionsStorePath,
		cfg.roomLaunchPath,
	} {
		if err := sweepOrphanTmpFiles(p); err != nil {
			log.Printf("orphan-tmp sweep: %v", err)
		}
	}

	identityReg, err := newIdentityRegistry(cfg.identityStorePath)
	if err != nil {
		log.Fatalf("identity registry: %v", err)
	}
	log.Printf("identity registry: store=%s", cfg.identityStorePath)

	aliasReg, err := newHandleAliasRegistry(cfg.handleAliasStorePath)
	if err != nil {
		log.Fatalf("handle alias registry: %v", err)
	}
	log.Printf("handle alias registry: store=%s", cfg.handleAliasStorePath)

	threadReg, err := newThreadSessionRegistry(cfg.threadSessionsStorePath)
	if err != nil {
		log.Fatalf("thread session registry: %v", err)
	}
	log.Printf("thread session registry: store=%s", cfg.threadSessionsStorePath)

	threadHandleSticky := newThreadHandleStickiness()
	log.Printf("thread handle stickiness: in-memory only (no persistence in v1)")

	roomLaunchReg, err := newRoomLaunchMappingRegistry(cfg.roomLaunchPath)
	if err != nil {
		log.Fatalf("room launch mapping registry: %v", err)
	}
	log.Printf("room launch mapping registry: store=%s (read-only; SIGHUP or restart to reload)",
		cfg.roomLaunchPath)

	subteamAliases, err := newSubteamAliasMap(cfg.subteamAliasStorePath)
	if err != nil {
		log.Fatalf("subteam alias map: %v", err)
	}
	log.Printf("subteam alias map: store=%s entries=%d (read-only; SIGHUP or restart to reload)",
		cfg.subteamAliasStorePath, subteamAliases.Len())

	userAliases, err := newUserAliasMap(cfg.userAliasStorePath)
	if err != nil {
		log.Fatalf("user alias map: %v", err)
	}
	log.Printf("user alias map: store=%s entries=%d (read-only; SIGHUP or restart to reload)",
		cfg.userAliasStorePath, userAliases.Len())

	channelMapReg, err := newChannelMappingRegistry(cfg.channelMappingPath)
	if err != nil {
		log.Fatalf("channel mapping registry: %v", err)
	}
	log.Printf("channel mapping registry: store=%s entries=%d (read-only; SIGHUP or restart to reload)",
		cfg.channelMappingPath, channelMapReg.Len())

	rigMapReg, err := newRigMappingRegistry(cfg.rigMappingPath)
	if err != nil {
		log.Fatalf("rig mapping registry: %v", err)
	}
	log.Printf("rig mapping registry: store=%s entries=%d (read-only; SIGHUP or restart to reload)",
		cfg.rigMappingPath, rigMapReg.Len())

	appsReg, err := newAppsRegistry(cfg.appsRegistryPath)
	if err != nil {
		log.Fatalf("apps registry: %v", err)
	}
	cfg.appsRegistry = appsReg
	log.Printf("apps registry: store=%s entries=%d (read-only; SIGHUP or restart to reload)",
		cfg.appsRegistryPath, appsReg.Len())
	if appsReg.Len() == 0 && cfg.slackSigningKey == "" {
		log.Printf("WARN: apps registry is empty and SLACK_SIGNING_SECRET is unset — all inbound Slack requests will be rejected with 401 until an app is imported (gc slack import-app + OAuth) or the env var is set")
	}

	// Cross-store overlap WARN: surface contradictory bindings (cby.3
	// channel mapping vs cby.4 rig mapping pointing at different rigs
	// for the same channel) at startup so operators see them in
	// adapter logs. resolveChannelTarget always lets channel mapping
	// win at runtime — this is purely observability.
	logCrossStoreOverlapWarnings(channelMapReg, rigMapReg)

	// Company-rooms (Slack company-rooms Phase 1) wiring. The two CLI-
	// written registries load with the never-fatal contract (a corrupt or
	// invalid file installs a nil snapshot and disables company routing
	// while legacy traffic keeps flowing). The durable ingress store is a
	// hard prerequisite for admission, but its construction failure is NOT
	// a legacy fallthrough: the gateway is wired even when the store cannot
	// be created, and runs degraded (barrier stays closed, company-room
	// admissible events get 503 without x-slack-no-retry, /healthz reports
	// the store error, startRecovery retries construction). companyGW must
	// be set on cfg BEFORE handleSlackEvents closes over the cfg value below.
	companyDirStore := &companyDirectoryStore{}
	if err := companyDirStore.Load(cfg.companyDirectoryPath); err != nil {
		log.Printf("company directory: initial load surfaced %v (routing disabled until a valid file is imported)", err)
	}
	companyBindStore := &companyBindingsStore{}
	if err := companyBindStore.Load(cfg.companyBindingsPath, companyDirStore.Snapshot()); err != nil {
		log.Printf("company bindings: initial load surfaced %v", err)
	}
	receipts, rerr := NewIngressReceiptStore(cfg.companyIngressDir)
	if rerr != nil {
		log.Printf("WARN: company ingress store %q: %v — gateway starting DEGRADED (company events 503, never legacy; construction retried)",
			cfg.companyIngressDir, rerr)
	}
	companyGW := newCompanyGateway(cfg, companyDirStore, companyBindStore, receipts)
	if rerr != nil {
		companyGW.setStoreError(rerr)
	}
	cfg.companyGateway = companyGW
	companyHealthStatus.Store(companyGW)
	log.Printf("company gateway: directory=%s bindings=%s dm_bindings=%s agent_apps=%s ingress=%s self_bot=%q dir_loaded=%v bindings_loaded=%v dm_bindings_loaded=%v registered_agent_apps=%d verify_sessions=%v store_ready=%v",
		cfg.companyDirectoryPath, cfg.companyBindingsPath, cfg.companyDMBindingsPath, cfg.companyAgentAppsPath, cfg.companyIngressDir, cfg.companySelfBotUserID,
		companyDirStore.Snapshot() != nil, companyBindStore.Snapshot() != nil,
		companyGW.dmBindStore.Snapshot() != nil, companyGW.agentApps.Snapshot().Len(), cfg.companyVerifySessions, receipts != nil)

	// Public mux: only /slack/events + /slack/interactions
	// (HMAC-verified) and /healthz. Bound to 0.0.0.0 by default so
	// Tailscale Funnel can reach it.
	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/slack/events", handleSlackEvents(cfg, aliasReg, threadReg, roomLaunchReg, subteamAliases, threadHandleSticky))
	publicMux.HandleFunc("/slack/interactions", handleSlackInteractions(cfg, channelMapReg, rigMapReg))
	registerOAuthHandlers(publicMux, cfg, appsReg)
	publicMux.HandleFunc("/healthz", handleHealthz)
	publicMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	// Internal mux: /publish (gc-only). Served either on a UDS that gc
	// proxies through /svc/{name}/ (proxy_process mode), or on a
	// 127.0.0.1 TCP listener (legacy nohup mode).
	internalMux := http.NewServeMux()
	internalMux.HandleFunc("/publish", handlePublish(cfg, identityReg, userAliases, newPublishDedupCache(publishDedupTTL)))
	internalMux.HandleFunc("/publish-file", handlePublishFile(cfg, identityReg))
	internalMux.HandleFunc("/react", handleReact(cfg))
	internalMux.HandleFunc("POST /identity", handleIdentity(identityReg))
	internalMux.HandleFunc("DELETE /identity", handleIdentityDelete(identityReg))
	internalMux.HandleFunc("POST /handle-alias", handleHandleAlias(aliasReg))
	internalMux.HandleFunc("DELETE /handle-alias", handleHandleAliasDelete(aliasReg))
	// Company-rooms operator surface: the receipt listing + redrive endpoints
	// (Phase 3b) backing the `gc slack company-status` / `company-redrive` verbs,
	// plus the Phase 5 body-redaction hook (`gc slack company-redact`).
	// Registered only when the company gateway is wired.
	if cfg.companyGateway != nil {
		internalMux.HandleFunc("/internal/company/receipts", cfg.companyGateway.handleCompanyReceipts)
		internalMux.HandleFunc("/internal/company/redrive", cfg.companyGateway.handleCompanyRedrive)
		internalMux.HandleFunc("/internal/company/redact", cfg.companyGateway.handleCompanyRedact)
	}
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
		if err := registerAdapter(cfg); err != nil {
			log.Fatalf("register adapter: %v", err)
		}
		mode := "LOCALHOST ONLY"
		if cfg.serviceSocket != "" {
			mode = "via gc /svc proxy"
		}
		log.Printf("registered with gc as provider=%s account=%s callback=%s/publish (%s)",
			cfg.provider, cfg.accountID, cfg.internalCallbackURL, mode)
	}

	janitorCtx, janitorCancel := context.WithCancel(context.Background())
	defer janitorCancel()
	go runInboundFileJanitor(janitorCtx, cfg)
	go runDispatchDropSummary(janitorCtx, dispatchDropSummaryInterval, cfg.dispatchConcurrency)

	// Thread-binding teardown subscriber (cby.5.4): listens to gc's
	// city-scoped event stream for terminal session lifecycle events
	// (session.stopped, session.crashed) and drops the corresponding
	// thread→session binding plus any handle aliases the launcher
	// bootstrapped on spawn. Best-effort: a missing gcAPIBase or
	// cityName disables the goroutine cleanly.
	go runThreadTeardownSubscriber(janitorCtx, teardownSubscriberConfig{
		gcAPIBase: cfg.gcAPIBase,
		cityName:  cfg.cityName,
	}, threadReg, aliasReg)

	// Company-rooms startup recovery barrier + periodic sweep. Until the
	// barrier opens, company-admissible events receive 503 (retryable);
	// legacy routes, /healthz, interactions, and the internal listener
	// serve immediately (they never consulted the gateway). The recovery
	// pass and sweep are no-ops when the gateway is nil.
	companyGW.startRecovery(janitorCtx)

	errCh := make(chan error, 2)
	go func() {
		log.Printf("public listener serving on %s (Slack events)", cfg.publicListen)
		errCh <- publicSrv.ListenAndServe()
	}()
	go func() {
		if cfg.serviceSocket != "" {
			log.Printf("internal listener serving on UDS %s (gc proxy_process)", cfg.serviceSocket)
			lis, err := listenUDS(cfg.serviceSocket)
			if err != nil {
				errCh <- fmt.Errorf("listen unix %s: %w", cfg.serviceSocket, err)
				return
			}
			errCh <- internalSrv.Serve(lis)
		} else {
			internalSrv.Addr = cfg.internalListen
			log.Printf("internal listener serving on %s (gc publish only)", cfg.internalListen)
			errCh <- internalSrv.ListenAndServe()
		}
	}()

	// SIGHUP-driven reload of the four CLI-written registry files
	// (apps, channel mappings, rig mappings, room launch mappings) —
	// gc-cby.23. Buffer-size-1 + a separate Notify channel from `stop`
	// so SIGHUP cannot trigger shutdown. reloadStop is closed by the
	// trailing defer below alongside janitorCancel, so the goroutine
	// exits cleanly during shutdown.
	reloadStop := make(chan struct{})
	defer close(reloadStop)
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)
	go runReloadLoop(reloadStop, hupCh, func() {
		logReloadOutcome(appsReg, channelMapReg, rigMapReg, roomLaunchReg, subteamAliases, userAliases)
		// Company stores reload on the same SIGHUP but OUTSIDE the atomic
		// six-registry set: a stale/invalid company file retains its own
		// last-known-good snapshot (handled inside StageReload) and never
		// blocks the six above, which have already committed.
		companyGW.reloadOnSIGHUP()
	})

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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = publicSrv.Shutdown(ctx)
	_ = internalSrv.Shutdown(ctx)
}

// listenUDS binds a Unix domain socket at path, removing any stale entry
// first so restarts succeed. The socket file is left in place on shutdown
// — the controller's proxy_process supervisor cleans it up via
// cleanupProxyProcessSocketPath when the service is closed.
func listenUDS(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", path, err)
	}
	// Defense-in-depth: the controller-managed parent at
	// /tmp/gcsvc-<uid>/<hash>/ is already 0o700 so the socket is
	// unreachable to other UIDs via parent-dir traversal-deny, but
	// chmod the socket itself too in case the parent ever loosens.
	// gc-ywe.6.
	if err := os.Chmod(path, 0o600); err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("chmod uds: %w", err)
	}
	return lis, nil
}

func registerAdapter(cfg config) error {
	body, _ := json.Marshal(adapterRegisterRequest{
		Provider:    cfg.provider,
		AccountID:   cfg.accountID,
		Name:        "slack-adapter",
		CallbackURL: cfg.internalCallbackURL,
		Capabilities: adapterCapabilities{
			SupportsChildConversations: false,
			SupportsAttachments:        true,
			MaxMessageLength:           40000, // Slack's chat.postMessage limit
		},
	})
	// PathEscape cityName so URL-significant characters cannot alter
	// routing on the gc API side (sec-S-06). cityName is operator-supplied
	// via GC_CITY_NAME and gc-cby.29 rejects /?#% at startup, but the
	// per-call escape keeps the wire format correct regardless and matches
	// the dispatch paths that cby-set-c hardened. gc-cby.28.
	target := fmt.Sprintf("%s/v0/city/%s/extmsg/adapters", cfg.gcAPIBase, url.PathEscape(cfg.cityName))
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", "gc-slack-adapter")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("register failed: %s — %s", resp.Status, string(respBody))
	}
	return nil
}

// publishDedupTTL bounds how long a delivered receipt is remembered for
// idempotent replay. It only needs to span the retry-after-timeout window:
// the pack's HTTP client times out at 30s and an agent retry follows shortly
// after, so a couple of minutes comfortably covers the reported failure mode
// (gpk-lbhl) while staying short enough that an intentional identical resend
// minutes later is not silently swallowed.
const publishDedupTTL = 2 * time.Minute

// publishDedupCache remembers delivered publish receipts keyed by the
// caller-supplied idempotency key, so a retry after a delivered-but-
// timed-out POST returns the original receipt instead of posting a second
// Slack message (gpk-lbhl). Only delivered receipts are cached: a retry
// after a genuine (non-delivered) failure must still re-attempt delivery,
// so failures are never remembered. An empty idempotency key disables
// dedup for that call.
type publishDedupCache struct {
	mu      sync.Mutex
	entries map[string]publishDedupEntry
	ttl     time.Duration
	now     func() time.Time
}

type publishDedupEntry struct {
	receipt   publishReceipt
	expiresAt time.Time
}

func newPublishDedupCache(ttl time.Duration) *publishDedupCache {
	return &publishDedupCache{
		entries: make(map[string]publishDedupEntry),
		ttl:     ttl,
		now:     time.Now,
	}
}

// Get returns the cached receipt for key when one is present and unexpired.
func (c *publishDedupCache) Get(key string) (publishReceipt, bool) {
	if key == "" {
		return publishReceipt{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return publishReceipt{}, false
	}
	if !c.now().Before(e.expiresAt) {
		delete(c.entries, key)
		return publishReceipt{}, false
	}
	return e.receipt, true
}

// Put records a delivered receipt under key and sweeps expired entries so
// the map stays bounded under churn. Empty keys and non-delivered receipts
// are ignored.
func (c *publishDedupCache) Put(key string, receipt publishReceipt) {
	if key == "" || !receipt.Delivered {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.entries[key] = publishDedupEntry{receipt: receipt, expiresAt: now.Add(c.ttl)}
	for k, e := range c.entries {
		if !now.Before(e.expiresAt) {
			delete(c.entries, k)
		}
	}
}

func handlePublish(cfg config, reg *identityRegistry, userAliases *userAliasMap, dedup *publishDedupCache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req publishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}

		// SessionID precedence: explicit field wins (used by direct-to-adapter
		// callers like smoke tests). Otherwise fall back to the wire-metadata
		// key gc populates when forwarding from /v0/city/.../extmsg/outbound.
		identitySessionID := req.SessionID
		if identitySessionID == "" {
			identitySessionID = req.Metadata[metadataKeySourceSessionID]
		}
		// Fail closed on identity-less publishes (gpk-jqou). With neither the
		// native session_id nor the legacy metadata.source_session_id set, the
		// adapter would otherwise post at channel root under the default bot
		// identity (as=""), which surfaced as the spurious "gc oversight PL
		// replied in channel" anomaly on 2026-05-25. Every legitimate /publish
		// caller carries a session — the gc HandleOutbound path and the
		// publish / publish-to-channel / reply-current CLIs all resolve one (or
		// fail closed) before reaching here, and system bot notifications use
		// `gc slack post-message`, which posts to Slack directly and never
		// traverses this endpoint. So rejecting attribution-less requests has
		// no legitimate caller to break; it only closes the regression door.
		if identitySessionID == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "publish requires session attribution: provide session_id or metadata.source_session_id",
			})
			return
		}

		// Rewrite outbound @handle body mentions to Slack mention syntax
		// for handles the operator has mapped (gpk-uha7). Unmapped handles
		// and a nil/empty map leave the text untouched, so this is a no-op
		// for installs that haven't curated slack-user-aliases.json.
		rewrittenText := userAliases.rewrite(req.Text)
		post := slackPostMessageReq{
			Channel:  req.Conversation.ConversationID,
			Text:     rewrittenText,
			ThreadTS: req.ReplyToMessageID,
		}
		identityApplied := ""
		if reg != nil {
			if rec, ok := reg.Get(identitySessionID); ok {
				post.Username = rec.Username
				post.IconURL = rec.IconURL
				post.IconEmoji = rec.IconEmoji
				identityApplied = rec.Username
			}
		}
		log.Printf("publish: conv=%s text=%dch reply_to=%s idem=%s session=%s as=%q mentions_rewritten=%t",
			req.Conversation.ConversationID, len(req.Text), req.ReplyToMessageID,
			req.IdempotencyKey, identitySessionID, identityApplied, rewrittenText != req.Text)

		// Idempotent replay: if this idempotency key already produced a
		// delivered receipt, return it without re-posting. This is the
		// chokepoint that absorbs a retry after a delivered-but-timed-out
		// POST (gpk-lbhl) — the original Slack message stands, no duplicate.
		if cached, ok := dedup.Get(req.IdempotencyKey); ok {
			log.Printf("publish: dedup hit idem=%s conv=%s -> returning cached receipt (no re-post)",
				req.IdempotencyKey, req.Conversation.ConversationID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(cached)
			return
		}

		slackResp, err := postToSlack(cfg.slackBotToken, post)
		receipt := publishReceipt{Conversation: req.Conversation}
		switch {
		case err != nil:
			log.Printf("slack POST error: %v", err)
			receipt.Delivered = false
			receipt.FailureKind = "transient"
		case !slackResp.OK:
			log.Printf("slack returned error: %s", slackResp.Error)
			receipt.Delivered = false
			switch slackResp.Error {
			case "channel_not_found", "not_in_channel":
				receipt.FailureKind = "not_found"
			case "invalid_auth", "not_authed", "token_revoked":
				receipt.FailureKind = "auth"
			case "rate_limited":
				receipt.FailureKind = "rate_limited"
			default:
				receipt.FailureKind = "permanent"
			}
		default:
			receipt.Delivered = true
			receipt.MessageID = slackResp.TS
		}
		// Remember delivered receipts so a subsequent retry with the same
		// idempotency key replays this receipt instead of re-posting. Put
		// ignores empty keys and non-delivered receipts.
		dedup.Put(req.IdempotencyKey, receipt)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(receipt)
	}
}

// handlePublishFile serves POST /publish-file. It uploads the file at
// req.FilePath to Slack via the files-upload-v2 protocol and posts it to
// req.Conversation.ConversationID, optionally threaded under
// req.ReplyToMessageID. The bot token requires the `files:write` scope —
// without it, Slack returns {ok: false, error: "missing_scope"} and the
// receipt's FailureKind is "permanent".
//
// Slack's files.completeUploadExternal does NOT accept chat:write.customize
// username/icon overrides, so file posts appear under the default bot
// identity even when an identity record is registered for the source
// session. This is a Slack platform limitation, not an adapter bug.
// The identity lookup still happens for log parity with /publish.
func handlePublishFile(cfg config, reg *identityRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req publishFileRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(req.FilePath) == "" {
			http.Error(w, "file_path is required", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(req.Conversation.ConversationID) == "" {
			http.Error(w, "conversation.conversation_id is required", http.StatusBadRequest)
			return
		}
		// Confinement gate: the adapter only reads files under the
		// configured FILE_UPLOAD_ROOT. Without it, /publish-file is a
		// host-wide arbitrary-read primitive for anyone on the
		// internal mux. Fail-closed when unset rather than silently
		// allowing everything.
		if cfg.fileUploadRoot == "" {
			http.Error(w, "file upload disabled: FILE_UPLOAD_ROOT not configured", http.StatusServiceUnavailable)
			return
		}
		resolvedPath, err := confineFileUploadPath(cfg.fileUploadRoot, req.FilePath)
		if err != nil {
			// Use the request path verbatim in the error so operators
			// can correlate logs without leaking the canonicalized
			// (post-symlink) target.
			http.Error(w, fmt.Sprintf("file_path %q outside FILE_UPLOAD_ROOT: %v", req.FilePath, err), http.StatusForbidden)
			return
		}
		fi, err := os.Stat(resolvedPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("file_path: %v", err), http.StatusBadRequest)
			return
		}
		if fi.IsDir() {
			http.Error(w, "file_path is a directory", http.StatusBadRequest)
			return
		}
		// Symlink escape gate: now that os.Stat confirmed the path
		// exists, resolve symlinks and re-check the in-root invariant
		// so an attacker who plants a symlink inside the root cannot
		// pivot to an arbitrary host file.
		realPath, err := filepath.EvalSymlinks(resolvedPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("file_path: %v", err), http.StatusBadRequest)
			return
		}
		if _, err := confineFileUploadPath(cfg.fileUploadRoot, realPath); err != nil {
			http.Error(w, fmt.Sprintf("file_path %q resolves outside FILE_UPLOAD_ROOT: %v", req.FilePath, err), http.StatusForbidden)
			return
		}
		fileBytes, err := readConfinedFile(cfg.fileUploadRoot, realPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("read file_path: %v", err), http.StatusInternalServerError)
			return
		}
		filename := req.Filename
		if filename == "" {
			filename = filepath.Base(req.FilePath)
		}
		title := req.Title
		if title == "" {
			title = filename
		}

		// Identity lookup: same precedence as /publish. Logged for parity
		// even though Slack's file-upload API ignores chat:write.customize
		// overrides.
		identitySessionID := req.SessionID
		if identitySessionID == "" {
			identitySessionID = req.Metadata[metadataKeySourceSessionID]
		}
		identityApplied := ""
		if reg != nil && identitySessionID != "" {
			if rec, ok := reg.Get(identitySessionID); ok {
				identityApplied = rec.Username
			}
		}
		log.Printf("publish-file: conv=%s file=%s size=%d reply_to=%s session=%s as=%q",
			req.Conversation.ConversationID, filename, len(fileBytes),
			req.ReplyToMessageID, identitySessionID, identityApplied)

		receipt := publishFileReceipt{Conversation: req.Conversation}

		// Step 1: get a pre-signed upload URL.
		urlResp, err := slackGetUploadURL(cfg.slackBotToken, filename, len(fileBytes))
		if err != nil {
			log.Printf("slack files.getUploadURLExternal error: %v", err)
			receipt.FailureKind = "transient"
			receipt.Error = err.Error()
			writeJSON(w, receipt)
			return
		}
		if !urlResp.OK {
			log.Printf("slack files.getUploadURLExternal returned error: %s", urlResp.Error)
			receipt.FailureKind = mapSlackError(urlResp.Error)
			receipt.Error = urlResp.Error
			writeJSON(w, receipt)
			return
		}

		// Step 2: POST bytes (multipart) to the pre-signed URL.
		if err := slackPutFileBytes(urlResp.UploadURL, filename, fileBytes); err != nil {
			log.Printf("slack file upload error: %v", err)
			receipt.FailureKind = "transient"
			receipt.Error = err.Error()
			writeJSON(w, receipt)
			return
		}

		// Step 3: complete the upload — channel posting happens here.
		completeReq := slackCompleteUploadReq{
			Files:          []slackCompleteUploadFile{{ID: urlResp.FileID, Title: title}},
			ChannelID:      req.Conversation.ConversationID,
			InitialComment: req.InitialComment,
			ThreadTS:       req.ReplyToMessageID,
		}
		completeResp, err := slackCompleteUpload(cfg.slackBotToken, completeReq)
		if err != nil {
			log.Printf("slack files.completeUploadExternal error: %v", err)
			receipt.FailureKind = "transient"
			receipt.Error = err.Error()
			writeJSON(w, receipt)
			return
		}
		if !completeResp.OK {
			log.Printf("slack files.completeUploadExternal returned error: %s", completeResp.Error)
			receipt.FailureKind = mapSlackError(completeResp.Error)
			receipt.Error = completeResp.Error
			writeJSON(w, receipt)
			return
		}

		receipt.Delivered = true
		receipt.FileID = urlResp.FileID
		writeJSON(w, receipt)
	}
}

// confineFileUploadPath validates that path is inside root and returns
// the cleaned absolute form on success.
//
// Both root and path are canonicalized with filepath.Abs +
// filepath.Clean. Root is additionally run through EvalSymlinks
// (best-effort) so an operator-configured root that is itself a
// symlink (macOS /var → /private/var, etc.) lines up with paths the
// caller later resolves via EvalSymlinks. The path argument is NOT
// EvalSymlinks'd — the caller is responsible for re-invoking this
// helper on the EvalSymlinks-resolved path once os.Stat has confirmed
// existence (handlePublishFile does this to defeat symlink escape).
//
// Returns an error when:
//   - root or path is empty
//   - root is not absolute (a relative root would be silently
//     resolved against the adapter's cwd, which is a footgun for
//     operators who expect FILE_UPLOAD_ROOT to be a fixed prefix)
//   - either path can't be made absolute
//   - cleaned path is equal to root itself (the root is not an
//     uploadable file even when downstream IsDir would later reject it)
//   - cleaned path is not a strict descendant of root
//
// The returned path is the cleaned absolute form, suitable for passing
// to os.Stat / os.ReadFile.
func confineFileUploadPath(root, path string) (string, error) {
	_, pathAbs, _, err := confinedUploadPath(root, path)
	return pathAbs, err
}

// confinedUploadPath is confineFileUploadPath's full-detail form: it
// additionally returns the canonical root and the root-relative path,
// which openBeneath needs for its component walk.
func confinedUploadPath(root, path string) (rootAbs, pathAbs, rel string, err error) {
	if root == "" {
		return "", "", "", errors.New("FILE_UPLOAD_ROOT is empty")
	}
	if !filepath.IsAbs(root) {
		return "", "", "", fmt.Errorf("FILE_UPLOAD_ROOT %q is not absolute", root)
	}
	if strings.TrimSpace(path) == "" {
		return "", "", "", errors.New("path is empty")
	}
	rootAbs, err = filepath.Abs(root)
	if err != nil {
		return "", "", "", fmt.Errorf("resolving root: %w", err)
	}
	rootAbs = filepath.Clean(rootAbs)
	// Best-effort symlink resolution on the root: if the operator
	// configured a symlinked root (e.g. /var on macOS) the canonical
	// form is what later EvalSymlinks calls on a path will return.
	if resolved, err := filepath.EvalSymlinks(rootAbs); err == nil {
		rootAbs = resolved
	}
	pathAbs, err = filepath.Abs(path)
	if err != nil {
		return "", "", "", fmt.Errorf("resolving path: %w", err)
	}
	pathAbs = filepath.Clean(pathAbs)
	rel, err = filepath.Rel(rootAbs, pathAbs)
	if err != nil {
		return "", "", "", fmt.Errorf("computing relative path: %w", err)
	}
	// Reject the root itself: the helper's contract is "file inside
	// root", and any caller that later treats the returned path as a
	// regular file would otherwise be set up for surprise on
	// directory-typed paths. Anything starting with ".." has escaped.
	// An absolute rel (Windows volume crossing) is also out of bounds.
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", "", "", fmt.Errorf("path %q is outside root %q", pathAbs, rootAbs)
	}
	return rootAbs, pathAbs, rel, nil
}

// readConfinedFile reads realPath after re-asserting that it lies under
// root, then opens it with openBeneath's component-wise walk so neither
// a leaf symlink nor a parent directory swapped for a symlink in the
// TOCTOU window between the caller's EvalSymlinks resolution and the
// read can redirect the open outside root (gc-cby.10; the parent-swap
// residual race was gpk-1ta4).
//
// realPath should be the filepath.EvalSymlinks-resolved canonical path
// the caller has already verified with confineFileUploadPath; the
// internal re-check makes the safe path the only path so a future call
// site cannot regress arbitrary-read safety by skipping confinement.
func readConfinedFile(root, realPath string) ([]byte, error) {
	rootAbs, _, rel, err := confinedUploadPath(root, realPath)
	if err != nil {
		return nil, err
	}
	f, err := openBeneath(rootAbs, rel)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// writeJSON writes the receipt as a JSON response. Errors during encoding
// are logged but not surfaced — the receipt body is best-effort and the
// caller has the HTTP status anyway.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON encode error: %v", err)
	}
}

// mapSlackError maps a Slack error code to a publishReceipt failure kind.
// Shared between /publish and /publish-file so the contract is consistent.
func mapSlackError(slackErr string) string {
	switch slackErr {
	case "channel_not_found", "not_in_channel", "file_not_found":
		return "not_found"
	case "invalid_auth", "not_authed", "token_revoked", "missing_scope", "no_permission":
		return "auth"
	case "rate_limited", "ratelimited":
		return "rate_limited"
	case "":
		return ""
	default:
		return "permanent"
	}
}

// slackGetUploadURL calls files.getUploadURLExternal. Slack accepts both
// form-urlencoded body and query string for this endpoint; we use form
// to keep secrets out of access logs. The returned upload_url is a
// pre-signed URL valid for ~10 minutes.
func slackGetUploadURL(token, filename string, length int) (*slackGetUploadURLResp, error) {
	form := url.Values{}
	form.Set("filename", filename)
	form.Set("length", strconv.Itoa(length))
	httpReq, err := http.NewRequest(http.MethodPost,
		slackAPIBase+"/files.getUploadURLExternal",
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}
	var sr slackGetUploadURLResp
	if err := json.Unmarshal(respBody, &sr); err != nil {
		// Do not embed respBody: a truncated/partial response can still
		// carry upload_url — pre-signed token included — and this error
		// reaches both log.Printf and the HTTP receipt body upstream.
		// Status, size, and the unmarshal error (which reports offsets,
		// not content) are enough to diagnose a malformed response.
		// gpk-la1y.
		return nil, fmt.Errorf("decode slack getUploadURLExternal response (status %s, %d bytes): %w",
			httpResp.Status, len(respBody), err)
	}
	return &sr, nil
}

// slackPutFileBytes POSTs the file contents to a pre-signed Slack upload
// URL using multipart/form-data with a single “filename“ field. The URL
// itself encodes auth — no Bearer header needed. Slack returns 200 OK with
// "OK - <bytes>" on success; we treat any non-2xx as a transport failure.
//
// History: an earlier revision used PUT with Content-Type:
// application/octet-stream. Slack accepted the bytes (returns 200 OK) and
// files.completeUploadExternal returned ok:true with a file_id, but the
// resulting file had empty mimetype/filetype and never actually appeared
// in the channel — files.info reported `shares: {}` and conversations.history
// did not contain the post. The pre-signed URL evidently treats the
// multipart-with-filename pattern as the canonical shape; raw PUT silently
// degrades to a "ghost upload" the channel post step can't bind to.
func slackPutFileBytes(uploadURL string, filename string, body []byte) error {
	// uploadURL is a pre-signed, token-bearing URL (see doc above). Every
	// error below reports safeURL — never the raw uploadURL — so its embedded
	// auth never reaches log.Printf or the HTTP receipt at the publishFile
	// handler's sink. This mirrors slackDownloadToFile's redaction; the two
	// sibling upload steps (slackGetUploadURL, slackCompleteUpload) already
	// redact, and this one must too. gpk-la1y.
	safeURL := redactSlackURL(uploadURL)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, err := mw.CreateFormFile("filename", filename)
	if err != nil {
		return fmt.Errorf("create multipart form file: %w", err)
	}
	if _, err := part.Write(body); err != nil {
		return fmt.Errorf("write multipart body: %w", err)
	}
	if err := mw.Close(); err != nil {
		return fmt.Errorf("close multipart writer: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, uploadURL, &buf)
	if err != nil {
		return redactTransportError("build upload request to", safeURL, err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return redactTransportError("upload POST to", safeURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		// respBody is untrusted origin content: sanitize control chars
		// (log-line injection) and scrub any reflected URL/token. gpk-la1y.
		return fmt.Errorf("upload POST %s: %s — %s", safeURL, resp.Status, sanitizeSlackErrorBody(respBody))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// slackCompleteUpload calls files.completeUploadExternal with a JSON body.
// Channel posting (and threading via thread_ts) happens here, not in a
// separate chat.postMessage call.
func slackCompleteUpload(token string, req slackCompleteUploadReq) (*slackCompleteUploadResp, error) {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequest(http.MethodPost,
		slackAPIBase+"/files.completeUploadExternal",
		bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}
	var sr slackCompleteUploadResp
	if err := json.Unmarshal(respBody, &sr); err != nil {
		// Do not embed respBody: completeUploadExternal responses carry
		// full file objects (url_private, url_private_download) whose
		// URLs can bear auth tokens, and this error reaches log.Printf
		// and the HTTP receipt body upstream. gpk-la1y.
		return nil, fmt.Errorf("decode slack completeUploadExternal response (status %s, %d bytes): %w",
			httpResp.Status, len(respBody), err)
	}
	return &sr, nil
}

// handleReact serves POST /react. It maps reactRequest → Slack
// reactions.add. Emoji name is forwarded verbatim minus surrounding
// colons (clients can send "eyes" or ":eyes:").
func handleReact(cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req reactRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}
		emoji := strings.Trim(req.Emoji, ":")
		if emoji == "" || req.Conversation.ConversationID == "" || req.MessageID == "" {
			http.Error(w, "conversation.conversation_id, message_id, and emoji are required", http.StatusBadRequest)
			return
		}
		log.Printf("react: conv=%s ts=%s emoji=%s", req.Conversation.ConversationID, req.MessageID, emoji)

		slackResp, err := postReactionToSlack(cfg.slackBotToken, slackReactionsAddReq{
			Channel:   req.Conversation.ConversationID,
			Name:      emoji,
			Timestamp: req.MessageID,
		})
		receipt := reactReceipt{}
		switch {
		case err != nil:
			log.Printf("slack reactions.add error: %v", err)
			receipt.FailureKind = "transient"
		case !slackResp.OK:
			// "already_reacted" is benign: the emoji is already on the message.
			if slackResp.Error == "already_reacted" {
				receipt.Delivered = true
			} else {
				log.Printf("slack reactions.add returned error: %s", slackResp.Error)
				switch slackResp.Error {
				case "channel_not_found", "not_in_channel", "message_not_found":
					receipt.FailureKind = "not_found"
				case "invalid_auth", "not_authed", "token_revoked":
					receipt.FailureKind = "auth"
				case "rate_limited":
					receipt.FailureKind = "rate_limited"
				default:
					receipt.FailureKind = "permanent"
				}
			}
		default:
			receipt.Delivered = true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(receipt)
	}
}

func postReactionToSlack(token string, req slackReactionsAddReq) (*slackReactionsAddResp, error) {
	return postReactionMethod(http.DefaultClient, token, "reactions.add", req)
}

// postReactionMethod is the single Slack reactions POST path, parameterized by
// method ("reactions.add" | "reactions.remove") and HTTP client. handleReact
// (add, DefaultClient) and the company visible-ack path (add/remove over the
// gateway's timeout-bounded client) both route through here, so there is no
// second reactions POST implementation.
func postReactionMethod(client *http.Client, token, method string, req slackReactionsAddReq) (*slackReactionsAddResp, error) {
	if client == nil {
		client = http.DefaultClient
	}
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequest(http.MethodPost, slackAPIBase+"/"+method, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}
	var sr slackReactionsAddResp
	if err := json.Unmarshal(respBody, &sr); err != nil {
		return nil, fmt.Errorf("decode slack: %w (body=%s)", err, string(respBody))
	}
	return &sr, nil
}

func postToSlack(token string, req slackPostMessageReq) (*slackPostMessageResp, error) {
	return postMessageWithClient(http.DefaultClient, token, req)
}

// postMessageWithClient is the single Slack chat.postMessage path, parameterized
// by HTTP client. postToSlack (DefaultClient) and the company visible-ack failure
// reply (the gateway's timeout-bounded client) both route through here, so there
// is no second chat.postMessage implementation.
func postMessageWithClient(client *http.Client, token string, req slackPostMessageReq) (*slackPostMessageResp, error) {
	if client == nil {
		client = http.DefaultClient
	}
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequest(http.MethodPost, slackAPIBase+"/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}
	var sr slackPostMessageResp
	if err := json.Unmarshal(respBody, &sr); err != nil {
		return nil, fmt.Errorf("decode slack: %w (body=%s)", err, string(respBody))
	}
	return &sr, nil
}

func handleSlackEvents(cfg config, aliasReg *handleAliasRegistry, threadReg *threadSessionRegistry, roomLaunchReg *roomLaunchMappingRegistry, subteamMap *subteamAliasMap, threadHandleSticky *threadHandleStickiness) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		// Resolve the signing secret(s) BEFORE HMAC. Body is unsigned bytes by
		// definition until verified, so we parse only the small type +
		// api_app_id + team_id head to pick the Phase 4 verification path (env
		// secret for the switchboard/legacy, the app's OWN secret for a
		// registered agent app, trial for the url_verification handshake).
		head := parseEventHead(body)
		ts := r.Header.Get("X-Slack-Request-Timestamp")
		sig := r.Header.Get("X-Slack-Signature")
		// Resolve the agent-apps registration snapshot ONCE per request so the
		// HMAC decision and the DM admission gate agree even if a SIGHUP swaps
		// the registry between them (m7): a mid-request register/deregister can
		// no longer route an event verified as an agent-app DM into the legacy
		// dispatcher, nor admit a legacy-trial-verified event as an owner DM.
		agentApps := cfg.companyGateway.agentAppsSnapshot()
		if !verifyInboundEvent(cfg, agentApps, head, body, ts, sig) {
			log.Printf("slack signature verify FAILED type=%q api_app_id=%q team_id=%q",
				clipTeamIDForLog(head.Type), clipTeamIDForLog(head.APIAppID), clipTeamIDForLog(head.TeamID))
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		var env slackEventEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}

		// URL verification challenge.
		if env.Type == "url_verification" && env.Challenge != "" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(env.Challenge))
			return
		}

		// Company-rooms durable admission (Slack company-rooms Phase 1d).
		// When the event targets an imported company room, the gateway
		// owns the HTTP response (200 on admit/duplicate/non-admissible,
		// 503 without x-slack-no-retry on store failure or a closed
		// startup barrier) and delivery proceeds asynchronously. Every
		// other event — no gateway, no directory, non-company channel,
		// non-message type — falls through to the legacy path below
		// byte-for-byte.
		if cfg.companyGateway.tryHandleEvent(w, r, env, agentApps) {
			return
		}

		// Process event_callback. Always 200 quickly to avoid Slack retries.
		w.WriteHeader(http.StatusOK)
		release, capacity, ok := cfg.acquireDispatchSlot()
		if !ok {
			log.Printf("slack adapter: dispatch queue full (cap=%d), dropping slack event type=%q",
				capacity, env.Type)
			return
		}
		// Slot ownership transfers to processSlackEvent, which either
		// releases on its own return path or hands the slot to its
		// alias-dispatch goroutine. This avoids double-counting against
		// cfg.dispatchSem when an inbound triggers an alias dispatch (which
		// would otherwise hold two slots concurrently — see gc-cby.26
		// Phase 4 review fix).
		go processSlackEvent(cfg, aliasReg, threadReg, roomLaunchReg, subteamMap, threadHandleSticky, env, release)
	}
}

// parseTeamIDFromEventsBody extracts the JSON `team_id` field from a
// Slack /slack/events POST body. The body is unsigned at this point in
// the pipeline, so this is intentionally minimal — no error
// propagation, no full envelope decode. Returns "" on any decode
// failure or missing field; the caller treats "" as "fall through to
// env fallback" inside lookupSigningSecrets.
//
// Body size is already capped upstream at 1 MiB by io.LimitReader.
func parseTeamIDFromEventsBody(body []byte) string {
	var head struct {
		TeamID string `json:"team_id"`
	}
	if err := json.Unmarshal(body, &head); err != nil {
		return ""
	}
	return head.TeamID
}

// eventHead is the minimal pre-HMAC view of a /slack/events body: the fields
// that select the Phase 4 verification path. Parsed from unsigned bytes, so it
// carries no trust — it only routes the request to the right secret.
type eventHead struct {
	Type     string `json:"type"`
	APIAppID string `json:"api_app_id"`
	TeamID   string `json:"team_id"`
}

// parseEventHead extracts the type / api_app_id / team_id head. Returns a zero
// value on any decode failure; every downstream branch fails closed on the
// zero value (no api_app_id match, empty candidate list). Body is already
// capped at 1 MiB upstream.
func parseEventHead(body []byte) eventHead {
	var h eventHead
	_ = json.Unmarshal(body, &h)
	return h
}

// verifyInboundEvent implements the Phase 4 verification order (event POSTs).
// Fail-closed at every step; the switchboard/legacy path stays byte-for-byte
// the existing rooms behavior (lookupSigningSecrets). agentApps is the caller's
// once-per-request registration snapshot (m7): the SAME value must be handed to
// tryHandleEvent so verification and admission never disagree across a SIGHUP.
//
//  1. url_verification: no api_app_id in the handshake — trial-HMAC across the
//     env secret (+ any apps.json secret) and ALL registered agent secrets;
//     echo on any match. Side-effect-free, so a trial is acceptable here and
//     ONLY here.
//  2. event_callback, api_app_id == SLACK_APP_ID (the switchboard's own app):
//     verify against the env signing secret ONLY (the rooms path, unchanged).
//     This branch is authoritative and takes precedence over any registered
//     agent record that happens to carry the same api_app_id — the switchboard
//     identity is pinned to the env secret, never a file-registered one. Empty
//     SLACK_APP_ID disables the pin and lets the switchboard fall to rule 4.
//  3. event_callback, api_app_id == a registered agent app: verify against
//     exactly that record's secret. A mismatch — including a signature valid
//     under a DIFFERENT registered app's secret — is a strict-bind reject
//     (401, counter company_dm_sig_reject). The bind check is authoritative;
//     no fallback (rule 12 spoof defense).
//  4. otherwise (an unknown api_app_id): the legacy lookupSigningSecrets path,
//     UNCHANGED — except a legacy trial that matches a secret which is ALSO a
//     registered agent secret is rejected, because registration opts an app
//     into strict binding permanently.
func verifyInboundEvent(cfg config, agentApps *AgentApps, head eventHead, body []byte, ts, sig string) bool {
	if head.Type == "url_verification" {
		candidates := lookupSigningSecrets(cfg.appsRegistry, cfg.slackSigningKey, head.TeamID)
		candidates = append(candidates, agentApps.SigningSecrets()...)
		return verifySlackSignatureMulti(candidates, ts, body, sig)
	}
	// Rule 2: the switchboard's own api_app_id pins to the env secret only. It
	// is checked BEFORE the registered-agent lookup so a registered record
	// sharing SLACK_APP_ID can never shadow the env-secret path (spec §Verify
	// order rule 2/3 precedence). A mismatch here is a plain 401 (the rooms
	// path), not a company_dm_sig_reject.
	if head.Type == "event_callback" && cfg.slackAppID != "" && head.APIAppID == cfg.slackAppID {
		return verifySlackSignature(cfg.slackSigningKey, ts, body, sig)
	}
	if rec, ok := agentApps.Get(head.APIAppID); ok {
		if verifySlackSignature(rec.SigningSecret, ts, body, sig) {
			return true
		}
		// Strict binding: an event claiming a registered app must verify under
		// that app's own secret or be rejected, even if it verifies under some
		// other registered app's secret (the cross-app spoof).
		cfg.companyGateway.recordDMSigReject()
		return false
	}
	// The legacy trial set is the env/apps.json candidates PLUS every registered
	// agent secret, so a match on a registered secret is DETECTED (not silently
	// unmatched) and explicitly rejected: that app opted into strict binding via
	// registration, so it may never be admitted through the unknown-api_app_id
	// carve-out. A match on a non-registered legacy candidate is accepted.
	candidates := lookupSigningSecrets(cfg.appsRegistry, cfg.slackSigningKey, head.TeamID)
	candidates = append(candidates, agentApps.SigningSecrets()...)
	matched, ok := firstMatchingSecret(candidates, ts, body, sig)
	if !ok {
		return false
	}
	if agentApps.isRegisteredSecret(matched) {
		cfg.companyGateway.recordDMSigReject()
		return false
	}
	return true
}

// firstMatchingSecret trials each candidate secret against the HMAC and
// returns the first that verifies (and true). Fail-closed semantics per
// verifySlackSignature. Used by the legacy verification path so the caller can
// inspect WHICH secret matched (the strict-bind rejection above).
func firstMatchingSecret(secrets []string, ts string, body []byte, sig string) (string, bool) {
	for _, s := range secrets {
		if verifySlackSignature(s, ts, body, sig) {
			return s, true
		}
	}
	return "", false
}

// verifySlackSignatureMulti trials each candidate secret against the
// HMAC and returns true on the first match. Each per-secret call
// inherits fail-closed semantics from verifySlackSignature (malformed
// timestamp, stale window, missing headers); sec-S-01 still pins.
//
// Empty candidate list returns false — the natural fail-closed path
// when neither the apps registry nor the env supplies a secret. The
// extra HMAC ops are cheap and bounded by the small number of gc-
// imported apps per workspace; the trial is mechanical (no judgment
// in Go).
func verifySlackSignatureMulti(secrets []string, ts string, body []byte, sig string) bool {
	for _, s := range secrets {
		if verifySlackSignature(s, ts, body, sig) {
			return true
		}
	}
	return false
}

// clipTeamIDForLog bounds attacker-controlled team_id values before they
// hit log lines. Real Slack team IDs are "T" + 8-11 alphanumerics; 32
// is generous. Pre-HMAC body is unsigned, so an unbounded value would
// allow log amplification (~1 MiB per request).
func clipTeamIDForLog(s string) string {
	const max = 32
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

func verifySlackSignature(secret, ts string, body []byte, sig string) bool {
	if secret == "" || ts == "" || sig == "" {
		return false
	}
	// Reject stale requests (>5 min) to mitigate replay. Fail closed on
	// any timestamp parse error: an attacker who controls the
	// timestamp header must not be able to bypass the replay window
	// just by sending an unparseable value (e.g. "abc", "1.5").
	tsInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	if time.Since(time.Unix(tsInt, 0)) > 5*time.Minute {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + ts + ":"))
	_, _ = mac.Write(body)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(sig))
}

// slackKindFromChannelType maps a Slack message event's channel_type
// onto a gc ConversationKind. Slack channel_type values are:
//
//	"im"       -> direct message between two users  -> dm
//	"channel"  -> public channel                    -> room
//	"group"    -> private channel                   -> room
//	"mpim"     -> multi-party DM (group DM)         -> room
//
// When channel_type is missing, fall back to the channel-id prefix
// (D=im, C=channel, G=group). Defaults to "dm" for safety.
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

// processSlackEvent runs the per-inbound-event work (signature parse,
// postInbound to gc, optional alias dispatch). It owns the dispatch
// slot supplied by handleSlackEvents: the slot is released either on
// the function's own return path, or — when an alias dispatch fans
// out to its own goroutine — transferred to that goroutine's defer.
// The slot is released exactly once.
//
// threadReg is the launcher-mode binding store (cby.5). It may be nil
// in tests or in deployments that disable launcher mode entirely; the
// `@@<handle>` branch falls through to the regular `@<handle>` path
// when nil.
func processSlackEvent(cfg config, aliasReg *handleAliasRegistry, threadReg *threadSessionRegistry, roomLaunchReg *roomLaunchMappingRegistry, subteamMap *subteamAliasMap, threadHandleSticky *threadHandleStickiness, env slackEventEnvelope, release func()) {
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	if env.Type != "event_callback" || len(env.Event) == 0 {
		return
	}
	var msg slackMessageEvent
	if err := json.Unmarshal(env.Event, &msg); err != nil {
		log.Printf("decode slack event: %v", err)
		return
	}
	if msg.Type != "message" && msg.Type != "app_mention" {
		return
	}
	// Skip bot/system messages.
	if msg.BotID != "" || msg.Subtype != "" || msg.User == "" {
		return
	}
	if strings.TrimSpace(msg.Text) == "" {
		return
	}

	// Launcher-mode address parser runs FIRST (cby.5.b). A `@@<handle>`
	// head means "spawn a new thread-bound session" (5.3 wires the
	// spawn) or "the handle is already a long-lived alias — instruct
	// the user to drop one `@`." Either branch terminates here without
	// falling through to postInbound or the single-`@` alias dispatch:
	// the launcher flow is operator-driven control plane, not message
	// transport. The single-`@` parser below only runs on miss, so the
	// existing alias dispatch behavior is unchanged.
	if cfg.handlePrefix != "" && threadReg != nil {
		if h, remainder, ok := parseDoubleHandlePrefix(msg.Text, cfg.handlePrefix); ok {
			handleDoubleHandleDispatch(cfg, aliasReg, threadReg, roomLaunchReg, msg, env.TeamID, h, remainder)
			return
		}
	}

	text := msg.Text
	target := ""
	// Slack User Group mentions (beads gpk-2zi + gpk-hmr.2). Slack
	// delivers two shapes for a User Group mention and both must
	// normalize to the same address-by-handle dispatch path:
	//
	//   Labeled:    <!subteam^TEAMID|@handle>   (autocomplete in text)
	//   Unlabeled:  <!subteam^TEAMID>           (event-payload form)
	//
	// Different gating policy per shape, intentional asymmetry:
	//
	//   - LABELED: gated by aliasReg.Get(@handle) — same gate as the
	//     `@handle:` text-prefix path. The label is in the message
	//     itself, so the gate prevents arbitrary in-workspace User
	//     Groups (whose labels happen to look like gc handle names but
	//     have no registered session) from auto-routing.
	//
	//   - UNLABELED: gated by subteamAliasMap.Get(TEAMID) — Slack does
	//     NOT emit a handle label in this shape, so the operator-edited
	//     subteam-aliases.json IS the allowlist. A subteam ID with no
	//     entry in the map falls through to channel fanout. Locked-down
	//     workspaces without the `usergroups:read` scope still work:
	//     the map is populated off-band, no Slack API call is made.
	//
	// Downstream dispatch (the `if target != "" && aliasReg != nil`
	// block below) is unchanged — it still gates the cross-channel
	// session-message POST on aliasReg.Get, so a subteam-ID resolution
	// to a handle with no registered session yields the channel-bound
	// session seeing ExplicitTarget but no alias goroutine firing.
	// That matches the existing `@handle:` text-prefix semantics.
	if h, sid, rest, ok := parseSubteamMentionPrefix(msg.Text); ok {
		if h != "" {
			// Labeled form: preserve gpk-2zi behavior — aliasReg gate.
			if aliasReg != nil {
				if _, aliased := aliasReg.Get(h); aliased {
					target = h
					text = rest
				}
			}
		} else {
			// Unlabeled form: subteamAliasMap is the gate.
			if mappedHandle, mapped := subteamMap.Get(sid); mapped {
				target = mappedHandle
				text = rest
			}
		}
	}
	if target == "" && cfg.handlePrefix != "" {
		if h, rest := parseHandlePrefix(msg.Text, cfg.handlePrefix); h != "" {
			target = h
			text = rest
		}
	}

	// Thread-stickiness: if no explicit target was parsed AND this is a
	// thread reply (msg.ThreadTS is set and points at an earlier message,
	// not at the current message itself), inherit the target from the
	// thread root if a prior alias-dispatch registered one. Lets a human
	// say `@mayor: hi` once at the top of a thread and then keep replying
	// in the thread without re-tagging. An explicit re-tag in the thread
	// reply still wins because this lookup runs only on parser miss.
	if target == "" && threadHandleSticky != nil && msg.ThreadTS != "" && msg.ThreadTS != msg.TS {
		if stickyHandle, ok := threadHandleSticky.Lookup(msg.Channel, msg.ThreadTS); ok {
			target = stickyHandle
			// text is unchanged: the human didn't write a prefix, so we
			// route the full message body — same shape as if they had
			// typed `@<handle>: <body>` from the start.
		}
	}

	// gc-px8.5 + gc-px8.6: prepend thread-context preamble for inbounds
	// that are replies in a thread. The cache stores per-(target,
	// channel, thread) the ts of the most recent preamble already
	// delivered to that target. Each inbound fetches the thread's
	// reply chain (option B) and the formatter applies the cached ts
	// as a lower bound, so:
	//   - First mention of agent X: full priors window (matches gc-px8.5).
	//   - Subsequent mention of X with peer activity since the last
	//     visit: only the delta — what other bound agents (or human
	//     posts) added between visits — gets prepended (gc-px8.6).
	//   - Subsequent mention of X with no new activity: empty preamble.
	// Errors leave the cached ts unchanged so a transient failure
	// retries on the next inbound rather than silently losing context.
	if msg.ThreadTS != "" && msg.ThreadTS != msg.TS && cfg.threadContextCache != nil {
		sinceTS := cfg.threadContextCache.lastDeliveredFor(target, msg.Channel, msg.ThreadTS)
		fetchCtx, cancel := context.WithTimeout(context.Background(), threadContextFetchTimeout)
		replies, err := fetchThreadReplies(fetchCtx, cfg.slackBotToken, msg.Channel, msg.ThreadTS, cfg.slackThreadContextLimit)
		cancel()
		if err != nil {
			log.Printf("thread context fetch failed chan=%s thread=%s target=%q: %v", msg.Channel, msg.ThreadTS, target, err)
		} else {
			if preamble := formatThreadContextPreamble(replies, msg.TS, sinceTS); preamble != "" {
				text = preamble + text
			}
			cfg.threadContextCache.markDelivered(target, msg.Channel, msg.ThreadTS, msg.TS)
		}
	}

	var attachments []externalAttachment
	if len(msg.Files) > 0 {
		attachments = downloadSlackFiles(cfg, msg.Channel, msg.TS, msg.Files)
	}

	inbound := externalInboundMessage{
		ProviderMessageID: msg.TS,
		Conversation: conversationRef{
			ScopeID:        cfg.cityName,
			Provider:       cfg.provider,
			AccountID:      cfg.accountID,
			ConversationID: msg.Channel,
			Kind:           slackKindFromChannelType(msg.ChannelType, msg.Channel),
		},
		Actor: externalActor{
			ID:          msg.User,
			DisplayName: msg.User, // resolving display name needs users.info — defer
			IsBot:       false,
		},
		Text:             text,
		ExplicitTarget:   target,
		ReplyToMessageID: msg.ThreadTS,
		Attachments:      attachments,
		DedupKey:         "slack-" + msg.TS,
		ReceivedAt:       time.Now().UTC(),
	}
	if err := postInbound(cfg, inbound); err != nil {
		log.Printf("inbound POST failed: %v", err)
		return
	}
	log.Printf("inbound: chan=%s user=%s ts=%s thread=%s target=%q files=%d text=%dch",
		msg.Channel, msg.User, msg.TS, msg.ThreadTS, target, len(attachments), len(text))

	// "Eyes" reaction signals to the human that an agent was explicitly
	// addressed (via `@handle:` prefix or a Slack User Group mention
	// resolved via subteamAliasMap) and is processing the message. Only
	// fires when a target was parsed — generic channel chatter that
	// merely lands on the bound session via postInbound does NOT trigger
	// the eye, because most channel messages aren't intentionally
	// directed at an agent. Fires once per inbound (the alias-dispatch
	// fanout below targets the same Slack TS, so a duplicate react would
	// be a Slack no-op). If alias dispatch later fails, reactAliasDispatchFailure
	// posts ⚠️ on the same TS — that is semantically distinct (transport-layer
	// ack vs. delivery failure) and not a duplicate. Best-effort: errors are
	// logged and don't block the dispatch path.
	if target != "" && cfg.slackBotToken != "" {
		go func(channel, ts string) {
			_, err := postReactionToSlack(cfg.slackBotToken, slackReactionsAddReq{
				Channel:   channel,
				Name:      "eyes",
				Timestamp: ts,
			})
			if err != nil {
				log.Printf("react eyes failed: chan=%s ts=%s: %v", channel, ts, err)
			}
		}(msg.Channel, msg.TS)
	}

	// Cross-channel address-by-handle: if the parsed target matches a
	// registered alias, dispatch the inbound directly to the aliased
	// session via gc's session-message API, regardless of channel
	// binding. The originating channel's bound session still sees the
	// inbound (above) and is expected to stay silent (per its prompt)
	// because target != its handle.
	if target != "" && aliasReg != nil {
		if aliasedSessionID, ok := aliasReg.Get(target); ok {
			// Thread-stickiness bind: record (channel, msg.TS) -> target
			// so subsequent thread replies (whose msg.ThreadTS will
			// equal this msg.TS) inherit the same handle without the
			// human re-tagging. Only binds when the dispatch actually
			// fires — a target with no alias entry shouldn't poison the
			// sticky map. Subsequent thread replies look this up before
			// the parsers run.
			if threadHandleSticky != nil {
				threadHandleSticky.Bind(msg.Channel, msg.TS, target)
			}
			// Transfer the slot we already hold to the alias goroutine.
			// No new acquireDispatchSlot — that would double-count
			// against dispatchSem (gc-cby.26 Phase 4 review fix).
			released = true
			dispatchInflightWG.Add(1)
			go func() {
				defer dispatchInflightWG.Done()
				defer release()
				if !dispatchToAliasedSession(cfg, aliasedSessionID, inbound, target) {
					reactAliasDispatchFailure(cfg.slackBotToken,
						inbound.Conversation.ConversationID, inbound.ProviderMessageID)
				}
			}()
		}
	}
}

// downloadSlackFiles fetches each file's bytes from Slack (Bearer-auth
// against url_private), writes them to
// $INBOUND_FILE_STORE/<channel>/<ts>-<safe-filename>, and returns
// externalAttachment records pointing at the local file:// path. Any file
// that fails to download is dropped from the returned slice and a
// warning is logged — the inbound is still posted with whatever files
// succeeded so the message itself isn't lost.
func downloadSlackFiles(cfg config, channel, ts string, files []slackFile) []externalAttachment {
	if cfg.inboundFileStore == "" {
		log.Printf("inbound file download skipped: INBOUND_FILE_STORE empty (%d files dropped)", len(files))
		return nil
	}
	// Sanitize channel + ts as path components before joining: filepath.Join
	// cleans `..` but does not confine to the base, so a hostile channel id
	// like "../etc" would still escape inboundFileStore. safePathComponent is
	// stricter than safeFilename — Slack channel IDs and ts strings are
	// ID-like, so a strict allowlist is appropriate. gc-ywe.7.
	channelDir := filepath.Join(cfg.inboundFileStore, safePathComponent(channel))
	// 0o700: store may contain DM file content; not world-readable. gc-ywe.6.
	if err := os.MkdirAll(channelDir, 0o700); err != nil {
		log.Printf("inbound file download: mkdir %s: %v", channelDir, err)
		return nil
	}
	tsPrefix := safePathComponent(ts)
	out := make([]externalAttachment, 0, len(files))
	for _, f := range files {
		if f.URLPrivate == "" {
			log.Printf("inbound file %s: url_private empty, dropped", f.ID)
			continue
		}
		name := f.Name
		if name == "" {
			name = f.Title
		}
		if name == "" {
			name = f.ID
		}
		dest := filepath.Join(channelDir, tsPrefix+"-"+safeFilename(name))
		if err := slackDownloadToFile(cfg.slackBotToken, f.URLPrivate, dest); err != nil {
			log.Printf("inbound file %s download failed: %v", f.ID, err)
			continue
		}
		out = append(out, externalAttachment{
			ProviderID: f.ID,
			URL:        "file://" + dest,
			// Never f.MIMEType verbatim: Slack-native recordings (voice
			// clips, video clips) carry an empty mimetype, and gc requires
			// the field — see attachmentMIMEType.
			MIMEType: attachmentMIMEType(f),
		})
	}
	return out
}

// safePathComponent sanitizes a Slack-supplied identifier (channel id, ts)
// for use as a filesystem path component. Stricter than safeFilename: only
// [A-Za-z0-9_.-] survive; everything else (path separators, NUL, control
// chars, whitespace, unicode, punctuation) is replaced with '_'. The first
// leading dot is replaced with '_' so the result can never be `.`, `..`,
// or be treated as a hidden dotfile; any further internal `..` segments
// are harmless because filepath.Join normalizes them within the joined
// path (they cannot escape the parent once the leading byte is `_`).
// Empty input returns "_" so the caller always has a usable non-empty
// component. Length capped at 64 chars — Slack channel IDs are ~10 chars
// and ts strings are ~17 chars, so 64 is generous. gc-ywe.7.
func safePathComponent(s string) string {
	const maxLen = 64
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z',
			r >= 'a' && r <= 'z',
			r >= '0' && r <= '9',
			r == '_' || r == '-' || r == '.':
			b.WriteRune(r)
		default:
			// Non-allowlist runes (including all multi-byte runes) are
			// replaced with a single ASCII underscore. This keeps the
			// invariant: cleaned is pure ASCII below.
			b.WriteRune('_')
		}
	}
	cleaned := b.String()
	if strings.HasPrefix(cleaned, ".") {
		cleaned = "_" + cleaned[1:]
	}
	// cleaned is guaranteed ASCII here (loop above maps every non-allowlist
	// rune to '_'), so the byte-indexed truncation cannot split a multi-byte
	// rune. Do not introduce a non-ASCII character into the allowlist
	// without revisiting this assumption.
	if len(cleaned) > maxLen {
		cleaned = cleaned[:maxLen]
	}
	if cleaned == "" {
		return "_"
	}
	return cleaned
}

// safeFilename strips path separators and other dangerous characters from
// a Slack-supplied filename so it can't escape the inbound file store
// directory. More permissive than safePathComponent: keeps spaces and
// non-ASCII characters that humans expect in filenames. Length is capped
// at 200 chars (well under the typical 255 filename limit) to leave room
// for the leading ts prefix.
func safeFilename(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "file"
	}
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r == '/' || r == '\\' || r == 0:
			b.WriteRune('_')
		case r < 0x20:
			b.WriteRune('_')
		default:
			b.WriteRune(r)
		}
	}
	cleaned := b.String()
	for strings.HasPrefix(cleaned, ".") {
		cleaned = "_" + cleaned[1:]
	}
	if len(cleaned) > 200 {
		cleaned = cleaned[:200]
	}
	if cleaned == "" {
		return "file"
	}
	return cleaned
}

// isSlackFileURL reports whether rawURL is safe to fetch with the Slack bot
// token: scheme must be https, host (lowercased, port stripped) must be one
// of slack.com, *.slack.com, slack-files.com, or *.slack-files.com, and the
// port (if present) must be 443. Trailing-dot FQDNs are rejected by the
// suffix check (see comment in the body). Returns a non-nil error only when
// the input fails URL parsing; policy rejections return (false, nil) so
// callers can distinguish.
//
// Defense against forged inbound url_private values post signing-secret
// compromise (gc-0fn). Without this gate, a forged event can point
// url_private at any URL and slackDownloadToFile sends the bot token in
// the Authorization header to that URL — credential exfiltration plus
// internal-service probing (cloud metadata, gc API on loopback, etc.).
//
// Companion defenses live in buildSlackHTTPClient (gc-vrw):
//   - DNS rebinding: a constrained Dialer rejects connections to private,
//     loopback, link-local, or unspecified addresses regardless of the
//     hostname that resolved to them.
//   - HTTP redirects: CheckRedirect re-applies validateSlackFileURL to
//     each 3xx target so a compromised Slack CDN host cannot 302 the
//     bot token to an attacker-controlled host.
func isSlackFileURL(rawURL string) (bool, error) {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		// This error propagates verbatim into slackDownloadToFile's
		// returned error and adapter logs — see unwrapURLError for why
		// it cannot be wrapped raw. gpk-la1y.
		return false, fmt.Errorf("parse url_private %q: %w", redactSlackURL(rawURL), unwrapURLError(err))
	}
	if !u.IsAbs() {
		// ParseRequestURI accepts absolute paths (e.g. "/files-pri/...") and
		// protocol-relative URLs ("//attacker.com/...") without an error;
		// IsAbs() returns false for both, so they are caught here. A forged
		// url_private must be a full absolute URL, never a path.
		return false, fmt.Errorf("url_private not absolute: %q", redactSlackURL(rawURL))
	}
	if u.Scheme != "https" {
		return false, nil
	}
	if p := u.Port(); p != "" && p != "443" {
		return false, nil
	}
	// Note: we do NOT trim a trailing dot from the host. A trailing-dot FQDN
	// (e.g. "files.slack.com.") is rejected by the suffix check below
	// because the literal string ends in ".com." rather than ".com" or
	// ".slack.com". This is the intended strict policy — Slack never
	// returns trailing-dot hosts in url_private.
	host := strings.ToLower(u.Hostname())
	if host == "slack.com" || host == "slack-files.com" ||
		strings.HasSuffix(host, ".slack.com") ||
		strings.HasSuffix(host, ".slack-files.com") {
		return true, nil
	}
	return false, nil
}

// redactSlackURL returns a form of raw safe to embed in error messages
// and logs: the query string AND fragment are dropped and userinfo is
// removed entirely, while host/path are preserved so log scanners can
// still correlate on the CDN link. Slack CDN url_private links and
// pre-signed upload URLs carry auth tokens in the query (t=xoxe-...,
// token=...) and sometimes the fragment, and a forged url_private may
// carry attacker-chosen credentials in user[:password]@host form.
// url.URL.Redacted() alone is NOT sufficient: it masks the userinfo
// password only (leaving a bare username or the query/fragment intact),
// so we clear those fields explicitly.
//
// Two input shapes bypass the structural field clears: input url.Parse
// rejects outright (handled by the '?'/'#' cut + stripURLUserinfo
// fallback), and an "opaque" URI (scheme:opaque with no "//"), where the
// credential-bearing text lives in u.Opaque untouched by the clears.
// url.Parse accepts the opaque form without error, so we detect it and
// route it through the same textual fallback. Finally, a slackTokenRe
// backstop guarantees no Slack token shape survives in ANY form
// (hierarchical, opaque, or backslash-delimited). gpk-la1y.
func redactSlackURL(raw string) string {
	var safe string
	if u, err := url.Parse(raw); err == nil && u.Opaque == "" {
		u.RawQuery = ""
		u.Fragment = ""
		u.User = nil
		safe = u.String()
	} else {
		safe, _, _ = strings.Cut(raw, "?")
		safe, _, _ = strings.Cut(safe, "#")
		safe = stripURLUserinfo(safe)
	}
	return slackTokenRe.ReplaceAllString(safe, "[redacted-token]")
}

// stripURLUserinfo removes a "user[:pass]@" prefix from the authority of
// a URL-ish string that redactSlackURL could not clear structurally, so
// forged credentials (or a token stuffed into userinfo) do not survive
// its fallback. Only the authority between "://" and the next '/' is
// considered, so an '@' in a later path segment is left alone. Inputs
// lacking a "://" (e.g. an opaque scheme:host@... or backslash-delimited
// form) have no authority we can locate textually; the slackTokenRe
// backstop in redactSlackURL covers token-shaped credentials there. gpk-la1y.
func stripURLUserinfo(s string) string {
	i := strings.Index(s, "://")
	if i < 0 {
		return s
	}
	authStart := i + len("://")
	rest := s[authStart:]
	authority := rest
	if end := strings.IndexByte(rest, '/'); end >= 0 {
		authority = rest[:end]
	}
	at := strings.LastIndexByte(authority, '@')
	if at < 0 {
		return s
	}
	return s[:authStart] + authority[at+1:] + s[authStart+len(authority):]
}

// urlInTextRe matches an absolute URL embedded in free-form text,
// bounded by whitespace, quotes, or angle brackets. It scrubs raw URLs
// that appear inside error text we do not author — see scrubSlackSecrets.
var urlInTextRe = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s"'<>]+`)

// slackTokenRe matches Slack auth token shapes (xoxb-/xoxe-/xoxp-/xoxa-/
// xoxr-/xoxs-/xoxd- and xapp- app tokens). It is the backstop for a token
// that reaches error text WITHOUT travelling inside an absolute URL — a
// token in a relative redirect Location (no scheme://, so urlInTextRe
// misses it), or one reflected bare by a compromised-but-allowlisted
// origin in a 4xx body. gpk-la1y.
var slackTokenRe = regexp.MustCompile(`(xox[a-zA-Z]|xapp)-[A-Za-z0-9-]+`)

// scrubSlackSecrets removes secrets that must never reach adapter logs or
// HTTP receipts from free-form text: every embedded absolute URL is passed
// through redactSlackURL (dropping token-bearing query, fragment, and
// userinfo), and any bare Slack token shape that survives is replaced.
// Both passes are required: net/http parses a redirect Location BEFORE
// CheckRedirect runs and, on failure, interpolates the raw header into a
// *url.Error's text — and that header may be relative, so the URL pass
// alone would miss its token. gpk-la1y.
func scrubSlackSecrets(s string) string {
	s = urlInTextRe.ReplaceAllStringFunc(s, redactSlackURL)
	s = slackTokenRe.ReplaceAllString(s, "[redacted-token]")
	return s
}

// sanitizeSlackErrorBody makes an untrusted HTTP error-response body safe
// to embed in an error that reaches log.Printf and an HTTP receipt: it
// strips control characters (defeating log-line injection via embedded
// CR/LF) and scrubs any URL or bare Slack token the origin reflected.
// gpk-la1y.
func sanitizeSlackErrorBody(b []byte) string {
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case '\t', '\n', '\r':
			return ' '
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, string(b))
	return scrubSlackSecrets(cleaned)
}

// unwrapURLError returns the underlying cause of a *url.Error, or err
// unchanged. A *url.Error's Error() text embeds the full request URL —
// token-bearing query included — so wrapping one with %w leaks the URL
// even when the surrounding message is redacted. Callers unwrap here and
// re-wrap the bare cause against a redactSlackURL form. gpk-la1y.
func unwrapURLError(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err
	}
	return err
}

// redactTransportError re-wraps a net/http transport failure so no
// token-bearing URL reaches adapter logs or HTTP receipts, reporting safeURL
// (a redactSlackURL form) in place of the raw target. unwrapURLError strips
// the *url.Error wrapper — whose Error() embeds the full request URL —
// then scrubSlackSecrets removes any token the bare cause still carries.
// The critical case: net/http parses a redirect Location BEFORE
// CheckRedirect runs and, on a malformed header, returns a *url.Error whose
// .Err text interpolates the raw, token-bearing Location verbatim (absolute
// or relative). Safety rests on unwrapURLError reading only .Err (never
// .URL, which net/http also sets to the raw redirect target) and on
// scrubSlackSecrets as the redaction of last resort. The %w wrap is kept
// when scrubbing is a no-op so errors.Is still matches plain transport
// errors (e.g. connection-refused); it degrades to %s only on the
// URL-bearing path. Shared by slackDownloadToFile (GET) and
// slackPutFileBytes (upload POST). gpk-la1y.
func redactTransportError(action, safeURL string, e error) error {
	cause := unwrapURLError(e)
	causeText := cause.Error()
	if scrubbed := scrubSlackSecrets(causeText); scrubbed != causeText {
		return fmt.Errorf("%s %s: %s", action, safeURL, scrubbed)
	}
	return fmt.Errorf("%s %s: %w", action, safeURL, cause)
}

// validateSlackFileURL is the SSRF gate applied to inbound url_private
// values before slackDownloadToFile sends the bot token. Indirected through
// a package var so tests of unrelated download mechanics (atomic write,
// 4xx handling, permissions) can swap it for a permit-all stub via
// testAllowAnyURL — production callers always see isSlackFileURL.
//
// WARNING: not safe for concurrent test access. Tests that swap this var
// must NOT call t.Parallel(), and must not run alongside any test that
// depends on the production validator. testAllowAnyURL uses t.Cleanup to
// restore the previous value after the test exits.
var validateSlackFileURL = isSlackFileURL

// slackDialIPGuard is the per-IP guard invoked from net.Dialer.Control
// inside buildSlackHTTPClient. Indirected through a package var so the
// existing test helper testAllowAnyURL can also relax the dial-time
// check for tests that point url_private at httptest stubs on
// 127.0.0.1. Production callers always see isPrivateOrLoopbackIP.
//
// WARNING: same concurrency contract as validateSlackFileURL. Tests
// swapping this var must NOT call t.Parallel().
var slackDialIPGuard = isPrivateOrLoopbackIP

// slackDownloadToFile GETs urlPrivate with a Bearer token and streams the
// body to dest via an atomic temp+rename. Non-2xx responses produce an
// error with the truncated body for diagnosis. The url_private host is
// validated against the Slack allowlist before any network I/O — see
// isSlackFileURL for the threat model (gc-0fn).
func slackDownloadToFile(token, urlPrivate, dest string) error {
	// Every error below reports safeURL — never the raw urlPrivate — to
	// keep its token-bearing query and any attacker-chosen userinfo out
	// of adapter logs (see redactSlackURL for the threat model). The
	// validateSlackFileURL branch is covered too: isSlackFileURL redacts
	// its own error messages. gpk-la1y.
	safeURL := redactSlackURL(urlPrivate)
	// Transport failures below route through redactTransportError, which
	// unwraps the *url.Error (whose Error() embeds the full token-bearing
	// request URL, including a raw redirect Location) and reports safeURL.
	// gpk-la1y.
	ok, err := validateSlackFileURL(urlPrivate)
	if err != nil {
		return fmt.Errorf("validating url_private: %w", err)
	}
	if !ok {
		// safeURL preserves the host/path for log-scanner matching while
		// masking attacker-chosen credentials and dropping the query.
		return fmt.Errorf("url_private host not in slack allowlist: %q", safeURL)
	}
	req, err := http.NewRequest(http.MethodGet, urlPrivate, nil)
	if err != nil {
		return redactTransportError("build download request to", safeURL, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := slackHTTPClientSingleton().Do(req)
	if err != nil {
		return redactTransportError("GET", safeURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		// respBody is untrusted origin content: sanitize control chars
		// (log-line injection) and scrub any reflected URL/token. gpk-la1y.
		return fmt.Errorf("GET %s: %s — %s", safeURL, resp.Status, sanitizeSlackErrorBody(respBody))
	}
	tmp := dest + ".tmp"
	// 0o600: file content may be DM-private; rename below preserves this mode. gc-ywe.6.
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open tmp %s: %w", tmp, err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("copy body: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, dest, err)
	}
	return nil
}

// cgnat100_64 is RFC 6598 (100.64.0.0/10), the IPv4 carrier-grade NAT
// space. net.IP.IsPrivate does not cover this range, but Tailscale
// assigns 100.64.x.x addresses to peers and this adapter is documented
// as deployable behind Tailscale Funnel. A url_private host that briefly
// resolves to a Tailscale peer is exactly the DNS-rebinding case the
// dial guard exists to defeat. gc-vrw review.
var cgnat100_64 = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

// isPrivateOrLoopbackIP reports whether ip falls into a range that the
// adapter must never dial when fetching url_private. The set covers
// IPv4 RFC1918 (10/8, 172.16/12, 192.168/16) and IPv6 unique-local
// (fc00::/7) via net.IP.IsPrivate; RFC 6598 carrier-grade NAT
// (100.64.0.0/10) including Tailnet peer ranges; loopback (127/8, ::1)
// via IsLoopback; link-local unicast (169.254/16, fe80::/10) and
// link-local/interface-local multicast; and the unspecified address
// (0.0.0.0, ::). Public unicast addresses, including legitimate Slack
// CDN ranges, return false.
//
// The check is by address only — there is no DNS or hostname lookup
// here. It is invoked from net.Dialer.ControlContext after Go's
// resolver has produced a candidate address but before the connect
// syscall, so a hostname that briefly resolves to a private IP (DNS
// rebinding) is caught at the dial step regardless of how the URL was
// validated. gc-vrw.
func isPrivateOrLoopbackIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsUnspecified() {
		return true
	}
	if ip.IsPrivate() {
		return true
	}
	if v4 := ip.To4(); v4 != nil && cgnat100_64.Contains(v4) {
		return true
	}
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if ip.IsInterfaceLocalMulticast() {
		return true
	}
	return false
}

// buildSlackHTTPClient returns an *http.Client wired with two
// defense-in-depth controls beyond the URL allowlist enforced by
// validateSlackFileURL:
//
//  1. A constrained net.Dialer whose Control hook inspects the
//     resolved address (post-DNS, pre-connect) and refuses any IP that
//     isPrivateOrLoopbackIP flags. This blocks DNS-rebinding attacks
//     where an allowlisted hostname briefly resolves to an internal IP.
//
//  2. A CheckRedirect policy that re-applies validateSlackFileURL to
//     each 3xx target. The default http.Client.CheckRedirect would
//     follow a 302 from a legitimate (or compromised) Slack CDN host
//     to attacker.com, sending the bot token in the Authorization
//     header. This policy aborts the redirect with a typed error
//     before the second hop is dialed.
//
// The function returns a fresh *http.Client (and underlying
// *http.Transport) per call. Production code reaches the client via
// slackHTTPClientSingleton, which wraps a single buildSlackHTTPClient
// invocation in sync.Once so idle-connection pooling is shared across
// batched slackDownloadToFile calls (gc-px8.3). Tests retain direct
// access to construct fresh clients for property assertions.
//
// HTTP proxy environment variables (HTTP_PROXY / HTTPS_PROXY) are
// intentionally NOT honored: a private-IP proxy would bypass the
// dial-time IP guard, since net.Dialer.ControlContext sees the proxy
// address rather than the final Slack target. The slack-pack adapter
// reaches Slack CDN hosts directly in every supported deployment, so
// proxy support is unnecessary and removing it eliminates a real
// SSRF bypass. gc-vrw review.
func buildSlackHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		ControlContext: func(_ context.Context, network, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("dial %s %s: split host/port: %w", network, address, err)
			}
			ip := net.ParseIP(host)
			if ip == nil {
				// net.Dialer resolves to literal IPs before invoking
				// ControlContext, so a non-IP host here is a
				// programming error or an unexpected resolver result —
				// fail closed.
				return fmt.Errorf("dial %s %s: refusing to dial non-literal address %q", network, address, host)
			}
			if slackDialIPGuard(ip) {
				return fmt.Errorf("dial %s %s: refusing to dial private, loopback, or link-local address %s", network, address, ip)
			}
			return nil
		},
	}
	transport := &http.Transport{
		// Proxy intentionally nil — see function doc.
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		// Bound the whole round-trip including response-body read.
		// 5 minutes accommodates a slow ~1 GB Slack file at modest
		// throughput; legitimate downloads finish well inside this.
		Timeout:   5 * time.Minute,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Redirect targets carry the same token-bearing query as the
			// original url_private, and this error's text survives inside
			// the *url.Error returned by Client.Do — redactTransport only
			// sanitizes the outer URL, not this message. Redacted() alone
			// keeps the query, so redact fully here. gpk-la1y.
			if len(via) >= 10 {
				return fmt.Errorf("redirect chain exceeded 10 hops at %s", redactSlackURL(req.URL.String()))
			}
			ok, err := validateSlackFileURL(req.URL.String())
			if err != nil {
				return fmt.Errorf("validating redirect target: %w", err)
			}
			if !ok {
				return fmt.Errorf("refusing redirect to non-slack host %q", redactSlackURL(req.URL.String()))
			}
			return nil
		},
	}
}

// slackHTTPClientSingleton returns the process-wide *http.Client used
// by slackDownloadToFile. The first call constructs the client via
// buildSlackHTTPClient inside a sync.Once; subsequent calls return
// the cached instance, so the underlying *http.Transport's idle
// connection pool is reused across batched downloads (gc-px8.3).
//
// Reuse is safe with the existing test seams: slackDialIPGuard is
// read on every dial (not captured at construction time), so tests
// can swap it via the package var even after the singleton has been
// initialized. validateSlackFileURL inside CheckRedirect is similarly
// resolved per redirect.
//
// Tests that need a fresh client for structural property assertions
// (Transport identity, CheckRedirect non-nil, etc.) should call
// buildSlackHTTPClient directly rather than this accessor.
func slackHTTPClientSingleton() *http.Client {
	slackHTTPClientOnce.Do(func() {
		slackHTTPClient = buildSlackHTTPClient()
	})
	return slackHTTPClient
}

var (
	slackHTTPClient     *http.Client
	slackHTTPClientOnce sync.Once
)

// sweepResult summarizes one pass of the inbound file janitor. All counts
// are over a single sweep; aggregate behavior over time is not tracked
// (the bd issue gc-g52 was scoped to retention, not metrics).
type sweepResult struct {
	FilesRemoved int
	DirsRemoved  int
	BytesRemoved int64
	Errors       []error
}

// sweepInboundStore deletes regular files under root whose mtime is
// older than now-ttl, then removes any channel sub-directories that are
// empty after the file pass. Returns counts and any errors encountered;
// a missing root is not an error (the store is created lazily on first
// inbound). A non-positive ttl is a no-op so callers can guard at the
// config layer without re-checking here.
//
// The function is pure (no goroutines, no logging) so callers can test
// it deterministically with table-driven inputs and a fixed `now`.
func sweepInboundStore(root string, ttl time.Duration, now time.Time) sweepResult {
	var res sweepResult
	if root == "" || ttl <= 0 {
		return res
	}
	cutoff := now.Add(-ttl)

	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return res
		}
		res.Errors = append(res.Errors, fmt.Errorf("read root %s: %w", root, err))
		return res
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			// Files at the root are unexpected (the store layout puts
			// everything under <channel>/) — skip them so we don't delete
			// configuration the operator may have left there.
			continue
		}
		channelDir := filepath.Join(root, entry.Name())
		sweepChannelDir(channelDir, cutoff, &res)
	}
	return res
}

// sweepChannelDir applies the file-age filter to a single channel
// directory and removes the directory itself if it ends up empty.
// Errors are appended to res.Errors but never abort the sweep — one
// unreadable file shouldn't block the rest of the housekeeping pass.
func sweepChannelDir(channelDir string, cutoff time.Time, res *sweepResult) {
	files, err := os.ReadDir(channelDir)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Errorf("read %s: %w", channelDir, err))
		return
	}
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}
		path := filepath.Join(channelDir, f.Name())
		info, err := f.Info()
		if err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("stat %s: %w", path, err))
			continue
		}
		if !info.ModTime().Before(cutoff) {
			continue
		}
		size := info.Size()
		if err := os.Remove(path); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("remove %s: %w", path, err))
			continue
		}
		res.FilesRemoved++
		res.BytesRemoved += size
	}
	// Re-read to see if the directory is now empty; only remove if so.
	remaining, err := os.ReadDir(channelDir)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Errorf("re-read %s: %w", channelDir, err))
		return
	}
	if len(remaining) == 0 {
		if err := os.Remove(channelDir); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("rmdir %s: %w", channelDir, err))
			return
		}
		res.DirsRemoved++
	}
}

// runInboundFileJanitor wakes every cfg.inboundFileSweepInterval and
// runs sweepInboundStore against cfg.inboundFileStore using cfg.inboundFileTTL.
// Returns immediately if either duration is non-positive or the store
// path is empty (janitor disabled). Cancellation via ctx is honored
// between ticks; an in-flight sweep runs to completion since each pass
// is bounded by the directory size.
func runInboundFileJanitor(ctx context.Context, cfg config) {
	if cfg.inboundFileStore == "" || cfg.inboundFileTTL <= 0 || cfg.inboundFileSweepInterval <= 0 {
		log.Printf("inbound file janitor disabled (store=%q ttl=%s interval=%s)",
			cfg.inboundFileStore, cfg.inboundFileTTL, cfg.inboundFileSweepInterval)
		return
	}
	log.Printf("inbound file janitor started: store=%s ttl=%s interval=%s",
		cfg.inboundFileStore, cfg.inboundFileTTL, cfg.inboundFileSweepInterval)
	ticker := time.NewTicker(cfg.inboundFileSweepInterval)
	defer ticker.Stop()
	// Run one sweep promptly on startup so a long-uptime adapter doesn't
	// wait a full interval before the first pass.
	logSweepResult(sweepInboundStore(cfg.inboundFileStore, cfg.inboundFileTTL, time.Now()))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logSweepResult(sweepInboundStore(cfg.inboundFileStore, cfg.inboundFileTTL, time.Now()))
		}
	}
}

// logSweepResult emits one log line per sweep pass at most. Silent
// no-op passes (nothing removed, no errors) don't log to keep noise
// down on idle deployments.
func logSweepResult(res sweepResult) {
	if res.FilesRemoved == 0 && res.DirsRemoved == 0 && len(res.Errors) == 0 {
		return
	}
	log.Printf("inbound file janitor: files_removed=%d dirs_removed=%d bytes_removed=%d errors=%d",
		res.FilesRemoved, res.DirsRemoved, res.BytesRemoved, len(res.Errors))
	for _, err := range res.Errors {
		log.Printf("inbound file janitor error: %v", err)
	}
}

// tightenStorePermissions is a one-shot startup migration helper for
// pre-fix installs. The create-time mode constants in saveLocked +
// downloadSlackFiles + slackDownloadToFile produce 0o700/0o600 for
// every new write, but legacy state from prior versions sits at
// 0o755/0o644. This walks the three configured stores and tightens
// only-if-strictly-looser. Setuid/setgid/sticky bits are preserved
// (operators may deliberately set setgid on a shared-group inbound
// dir). Operator-tighter perms (e.g. 0o400 read-only) are left alone.
// Errors are logged and never fatal — the helper is best-effort.
//
// gc-ywe.6.
func tightenStorePermissions(cfg config) {
	for _, p := range []string{cfg.identityStorePath, cfg.handleAliasStorePath, cfg.channelMappingPath, cfg.rigMappingPath, cfg.threadSessionsStorePath, cfg.roomLaunchPath} {
		if p == "" {
			continue
		}
		tightenPerm(filepath.Dir(p), 0o700)
		tightenPerm(p, 0o600)
	}

	if cfg.inboundFileStore == "" {
		return
	}
	tightenPerm(cfg.inboundFileStore, 0o700)
	// One level deep: each <channel>/ subdir + its immediate
	// children. The adapter owns this layout (downloadSlackFiles
	// at L1316). Don't recurse further — anything deeper is
	// operator-customized territory.
	entries, err := os.ReadDir(cfg.inboundFileStore)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("tighten store: readdir %s: %v", cfg.inboundFileStore, err)
		}
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		channelDir := filepath.Join(cfg.inboundFileStore, e.Name())
		tightenPerm(channelDir, 0o700)
		children, err := os.ReadDir(channelDir)
		if err != nil {
			log.Printf("tighten store: readdir %s: %v", channelDir, err)
			continue
		}
		for _, c := range children {
			if c.IsDir() {
				continue
			}
			tightenPerm(filepath.Join(channelDir, c.Name()), 0o600)
		}
	}
}

// tightenPerm chmods path to target if its perm bits are strictly
// looser. Setuid/setgid/sticky bits are preserved. Missing paths and
// symlinks are no-ops (Go's stdlib has no Lchmod, so following the
// link would chmod the target — refuse to do that). Errors are
// logged and never fatal.
func tightenPerm(path string, target os.FileMode) {
	if path == "" {
		return
	}
	info, err := os.Lstat(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("tighten store: stat %s: %v", path, err)
		}
		return
	}
	if info.Mode()&os.ModeSymlink != 0 {
		log.Printf("tighten store: skipping symlink %s", path)
		return
	}
	mode := info.Mode()
	if mode.Perm()&^target == 0 {
		return
	}
	preserved := mode & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
	final := preserved | target
	if err := os.Chmod(path, final); err != nil {
		log.Printf("tighten store: chmod %s: %v", path, err)
		return
	}
	// Use %v to render special bits symbolically (e.g. "g+s" for setgid)
	// so an operator reading the log can verify preservation.
	log.Printf("tighten store: %s %v -> %v (legacy state)", path, mode, final)
}

// parseHandlePrefix recognizes a leading address token of the form
// "<prefix><handle>" at the start of text, where the handle is followed
// by a colon, whitespace, or end-of-string. Leading whitespace before
// the prefix is tolerated. The handle character class is [A-Za-z0-9_-];
// the consumer chooses what handles map to which sessions via the
// /handle-alias registry. When matched, the handle is returned along
// with the remainder of the text (with any leading separator + single
// leading space trimmed); on no match, the original text is returned
// with an empty handle.
//
// Both `@name: foo` and `@name foo` are accepted because human users
// don't reliably type the colon — the colon is optional, but if it
// appears it must be the first character after the handle.
//
// Examples (with prefix "@"):
//
//	"@gascity: status?"       -> ("gascity", "status?")
//	"@ops foo"                -> ("ops",     "foo")
//	"@ops:hello"              -> ("ops",     "hello")
//	"  @lead hi"              -> ("lead",    "hi")
//	"@gascity"                -> ("gascity", "")
//	"@: foo"                  -> ("",        "@: foo")           (empty handle)
//	"hello @gascity: x"       -> ("",        "hello @gascity: x") (not at start)
//	"@bad/handle x"           -> ("",        "@bad/handle x")    (invalid separator after handle chars)
func parseHandlePrefix(text, prefix string) (handle, remainder string) {
	if prefix == "" {
		return "", text
	}
	trimmed := strings.TrimLeft(text, " \t")
	if !strings.HasPrefix(trimmed, prefix) {
		return "", text
	}
	rest := trimmed[len(prefix):]

	// Scan the longest run of valid handle characters at the start.
	handleEnd := 0
	for i := 0; i < len(rest); i++ {
		r := rest[i]
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' {
			handleEnd = i + 1
		} else {
			break
		}
	}
	if handleEnd == 0 {
		return "", text
	}
	candidate := rest[:handleEnd]
	body := rest[handleEnd:]

	// Handle must end at: end-of-string, colon, or whitespace.
	// Anything else (e.g. `@name.foo`) means this isn't an address token.
	if body == "" {
		return candidate, ""
	}
	sep := body[0]
	switch sep {
	case ':':
		body = body[1:]
	case ' ', '\t', '\n':
		// whitespace separator — leave it; the next trim handles it
	default:
		return "", text
	}
	if len(body) > 0 && (body[0] == ' ' || body[0] == '\t' || body[0] == '\n') {
		body = body[1:]
	}
	return candidate, body
}

// identityRegistry maps gc session ids to per-message Slack identity
// overrides (chat:write.customize username/avatar). When a publish arrives
// for a known session id, the adapter injects username/icon into
// chat.postMessage so each role posts under its own visible name + avatar
// without spinning up a separate bot user.
//
// The registry persists to disk (atomic temp + rename) so adapter restarts
// don't strip identity from running sessions. Reads are RLock-only so
// concurrent /publish calls don't serialize.
type identityRegistry struct {
	mu       sync.RWMutex
	byID     map[string]identityRecord
	diskPath string
}

func newIdentityRegistry(diskPath string) (*identityRegistry, error) {
	r := &identityRegistry{
		byID:     make(map[string]identityRecord),
		diskPath: diskPath,
	}
	if err := r.load(); err != nil {
		return nil, fmt.Errorf("load identity registry from %s: %w", diskPath, err)
	}
	return r, nil
}

// Get returns the identity record for sessionID, plus a boolean indicating
// whether one is registered. Callers should treat the no-record case as
// "use default bot identity" rather than an error.
func (r *identityRegistry) Get(sessionID string) (identityRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.byID[sessionID]
	return rec, ok
}

// Set stores rec for sessionID and persists the registry to disk. An empty
// record (zero username + icon fields) is allowed — it effectively unsets
// the override. To fully delete the entry use Delete instead.
func (r *identityRegistry) Set(sessionID string, rec identityRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[sessionID] = rec
	return r.saveLocked()
}

// Delete removes the identity record for sessionID and persists the
// registry. Returns whether an entry actually existed; missing entries
// are not an error so callers can treat Delete as idempotent.
func (r *identityRegistry) Delete(sessionID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, existed := r.byID[sessionID]
	delete(r.byID, sessionID)
	if err := r.saveLocked(); err != nil {
		return existed, err
	}
	return existed, nil
}

func (r *identityRegistry) load() error {
	if r.diskPath == "" {
		return nil
	}
	f, err := openRegistryFile(r.diskPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open identity store %s: %w", r.diskPath, err)
	}
	defer func() { _ = f.Close() }()
	// LimitReader caps the read at maxRegistryBytes+1 so a hostile or
	// corrupt file can't force a multi-gigabyte allocation before the
	// size check fires (gc-cby.32). The +1 lets us detect overflow
	// precisely: reading exactly maxRegistryBytes+1 means the
	// underlying file is at least maxRegistryBytes+1 bytes.
	data, err := io.ReadAll(io.LimitReader(f, maxRegistryBytes+1))
	if err != nil {
		return fmt.Errorf("read identity store %s: %w", r.diskPath, err)
	}
	if int64(len(data)) > maxRegistryBytes {
		return fmt.Errorf("identity store %s exceeds %d bytes", r.diskPath, maxRegistryBytes)
	}
	var stored map[string]identityRecord
	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("decode identity store: %w", err)
	}
	if stored != nil {
		r.byID = stored
	}
	return nil
}

func (r *identityRegistry) saveLocked() error {
	if r.diskPath == "" {
		return nil
	}
	// 0o700/0o600: store maps session-id ↔ persona display name; not world-readable. gc-ywe.6.
	// writeFile0600 (interactions.go) routes through os.CreateTemp so two
	// writers in the same directory don't collide on a fixed `<path>.tmp`
	// name (gc-px8.4 / gc-cby.14).
	data, err := json.MarshalIndent(r.byID, "", "  ")
	if err != nil {
		return fmt.Errorf("encode identity store: %w", err)
	}
	if err := writeFile0600(r.diskPath, data); err != nil {
		return fmt.Errorf("write identity store: %w", err)
	}
	return nil
}

// handleAliasRegistry maps a handle (consumer-defined string, e.g. a role
// or persona name) to a gc session id. Used by the cross-channel
// address-by-handle dispatcher: when a Slack inbound parses a handle
// that matches an alias, the adapter delivers the inbound directly to
// the aliased session via gc's session-message API, even if that session
// has no Slack binding for the originating channel.
//
// Persists to disk so restarts don't lose mappings; same atomic write
// pattern as the identity registry.
type handleAliasRegistry struct {
	mu       sync.RWMutex
	byHandle map[string]string
	diskPath string
}

func newHandleAliasRegistry(diskPath string) (*handleAliasRegistry, error) {
	r := &handleAliasRegistry{
		byHandle: make(map[string]string),
		diskPath: diskPath,
	}
	if err := r.load(); err != nil {
		return nil, fmt.Errorf("load handle alias registry from %s: %w", diskPath, err)
	}
	return r, nil
}

// Get returns the session id mapped to handle, plus a bool indicating
// whether one is registered. Callers should treat the no-record case as
// "not an alias — fall through to normal channel-binding routing".
func (r *handleAliasRegistry) Get(handle string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sid, ok := r.byHandle[handle]
	return sid, ok
}

// Set stores handle -> sessionID. Empty sessionID removes the entry.
func (r *handleAliasRegistry) Set(handle, sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if sessionID == "" {
		delete(r.byHandle, handle)
	} else {
		r.byHandle[handle] = sessionID
	}
	return r.saveLocked()
}

// findHandlesBySessionID returns every handle currently mapped to
// sessionID. Returns an empty slice (not nil) when sessionID is empty
// or no handles match. Used by the cby.5.4 thread-binding teardown
// subscriber to unwind the alias bootstrap installed in
// dispatchRoomLaunch when the underlying session ends. O(n) over the
// alias map; acceptable while the alias registry is bounded by active
// handles (typically tens to low hundreds). If the alias map ever
// grows large, store the handle alongside the thread binding instead.
func (r *handleAliasRegistry) findHandlesBySessionID(sessionID string) []string {
	if sessionID == "" {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var matches []string
	for handle, sid := range r.byHandle {
		if sid == sessionID {
			matches = append(matches, handle)
		}
	}
	return matches
}

// Delete removes the alias for handle and persists the registry. Returns
// whether an entry actually existed; missing entries are not an error so
// callers can treat Delete as idempotent. This is the explicit counterpart
// to Set with empty sessionID; both work, but Delete is the intent-clear
// verb.
func (r *handleAliasRegistry) Delete(handle string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, existed := r.byHandle[handle]
	delete(r.byHandle, handle)
	if err := r.saveLocked(); err != nil {
		return existed, err
	}
	return existed, nil
}

func (r *handleAliasRegistry) load() error {
	if r.diskPath == "" {
		return nil
	}
	f, err := openRegistryFile(r.diskPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open handle alias store %s: %w", r.diskPath, err)
	}
	defer func() { _ = f.Close() }()
	// LimitReader caps the read at maxRegistryBytes+1 so a hostile or
	// corrupt file can't force a multi-gigabyte allocation before the
	// size check fires (gc-cby.32). The +1 lets us detect overflow
	// precisely: reading exactly maxRegistryBytes+1 means the
	// underlying file is at least maxRegistryBytes+1 bytes.
	data, err := io.ReadAll(io.LimitReader(f, maxRegistryBytes+1))
	if err != nil {
		return fmt.Errorf("read handle alias store %s: %w", r.diskPath, err)
	}
	if int64(len(data)) > maxRegistryBytes {
		return fmt.Errorf("handle alias store %s exceeds %d bytes", r.diskPath, maxRegistryBytes)
	}
	var stored map[string]string
	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("decode handle alias store: %w", err)
	}
	if stored != nil {
		r.byHandle = stored
	}
	return nil
}

func (r *handleAliasRegistry) saveLocked() error {
	if r.diskPath == "" {
		return nil
	}
	// 0o700/0o600: store maps cross-channel @handle → session-id; not world-readable. gc-ywe.6.
	// writeFile0600 (interactions.go) routes through os.CreateTemp so two
	// writers in the same directory don't collide on a fixed `<path>.tmp`
	// name (gc-px8.4 / gc-cby.14).
	data, err := json.MarshalIndent(r.byHandle, "", "  ")
	if err != nil {
		return fmt.Errorf("encode handle alias store: %w", err)
	}
	if err := writeFile0600(r.diskPath, data); err != nil {
		return fmt.Errorf("write handle alias store: %w", err)
	}
	return nil
}

// handleHandleAlias serves POST /handle-alias. Empty session_id removes
// the entry; non-empty stores or replaces it.
func handleHandleAlias(reg *handleAliasRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req handleAliasRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}
		handle := strings.TrimSpace(req.Handle)
		if handle == "" {
			http.Error(w, "handle is required", http.StatusBadRequest)
			return
		}
		removed := strings.TrimSpace(req.SessionID) == ""
		if err := reg.Set(handle, strings.TrimSpace(req.SessionID)); err != nil {
			log.Printf("handle alias store error: %v", err)
			http.Error(w, "store failed", http.StatusInternalServerError)
			return
		}
		log.Printf("handle alias: handle=%q session=%q removed=%v",
			handle, req.SessionID, removed)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(handleAliasReceipt{
			Stored:    !removed,
			Removed:   removed,
			Handle:    handle,
			SessionID: req.SessionID,
		})
	}
}

// handleHandleAliasDelete serves DELETE /handle-alias. The handle is
// taken from either ?handle= query param (preferred for explicit verb)
// or from a JSON body { "handle": "..." } for symmetry with POST. Empty
// handle is rejected.
func handleHandleAliasDelete(reg *handleAliasRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		handle := strings.TrimSpace(r.URL.Query().Get("handle"))
		if handle == "" {
			var req handleAliasRequest
			if r.ContentLength > 0 {
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
					return
				}
				handle = strings.TrimSpace(req.Handle)
			}
		}
		if handle == "" {
			http.Error(w, "handle is required (?handle= or JSON body)", http.StatusBadRequest)
			return
		}
		existed, err := reg.Delete(handle)
		if err != nil {
			log.Printf("handle alias delete error: %v", err)
			http.Error(w, "store failed", http.StatusInternalServerError)
			return
		}
		log.Printf("handle alias delete: handle=%q existed=%v", handle, existed)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(handleAliasDeleteReceipt{
			Removed: true,
			Existed: existed,
			Handle:  handle,
		})
	}
}

// dispatchToAliasedSession POSTs a system reminder to the gc session-message
// endpoint for the aliased session. The payload carries everything the
// receiving session needs to compose a reply: originating channel id (for
// routing the reply back), message ts (for threading), and the inbound text.
//
// Returns true on successful delivery, false on any error. The caller is
// responsible for surface-visible failure signaling (e.g. a ⚠️ reaction).
func dispatchToAliasedSession(cfg config, sessionID string, msg externalInboundMessage, handle string) bool {
	// Every interpolated string is run through neutralizeMarkupBoundaries
	// to prevent a Slack workspace member from forging </system-reminder>
	// boundaries inside the dispatched body and injecting arbitrary
	// system instructions into the receiving aliased session (cby.33,
	// extends cby.17 sanitization to the alias dispatch path).
	//
	// Surface any downloaded attachments (file:// local paths) so the aliased
	// session can Read them — vision works on local files. downloadSlackFiles
	// already wrote each file to local disk and set URL="file://"+localpath
	// and MIMEType; before this the dispatch interpolated only msg.Text and
	// dropped the images entirely. Each field is neutralized like the text
	// path (cby.33) so a forged </system-reminder> in a filename cannot break
	// out of the reminder envelope. Empty string when there are no
	// attachments, leaving the body byte-identical to the text-only form.
	attachmentsBlock := ""
	if len(msg.Attachments) > 0 {
		var ab strings.Builder
		fmt.Fprintf(&ab, "\nAttachments (%d) — saved to local disk; Read the file:// path to view:\n", len(msg.Attachments))
		for i, att := range msg.Attachments {
			name := filepath.Base(strings.TrimPrefix(att.URL, "file://"))
			fmt.Fprintf(&ab, "  %d. %s (%s): %s\n",
				i+1,
				neutralizeMarkupBoundaries(name),
				neutralizeMarkupBoundaries(att.MIMEType),
				neutralizeMarkupBoundaries(att.URL))
		}
		attachmentsBlock = ab.String()
	}
	body := fmt.Sprintf(
		"<system-reminder>\n"+
			"Slack address-by-handle: @%s addressed you from channel %s (Slack ts %s) by user %s.\n"+
			"\n"+
			"Message text:\n"+
			"%s\n"+
			"%s"+
			"\n"+
			"React to this message with writing_hand to signal you are actively working on it:\n"+
			"  gc slack react --emoji writing_hand\n"+
			"\n"+
			"To reply in that channel (threaded under their message), write your reply to a tmpfile and run:\n"+
			"  gc slack publish-to-channel \\\n"+
			"    --conversation-id %s \\\n"+
			"    --thread-ts %s \\\n"+
			"    --body-file <tmpfile>\n"+
			"\n"+
			"This bypasses your local channel binding (you have none for that channel) and posts directly through the slack adapter, with your registered identity applied.\n"+
			"</system-reminder>",
		neutralizeMarkupBoundaries(handle),
		neutralizeMarkupBoundaries(msg.Conversation.ConversationID),
		neutralizeMarkupBoundaries(msg.ProviderMessageID),
		neutralizeMarkupBoundaries(msg.Actor.ID),
		neutralizeMarkupBoundaries(msg.Text),
		attachmentsBlock, // already per-field neutralized; pass raw
		neutralizeMarkupBoundaries(msg.Conversation.ConversationID),
		neutralizeMarkupBoundaries(msg.ProviderMessageID),
	)
	payload, _ := json.Marshal(gcSessionMessageRequest{Message: body})
	// PathEscape cityName and sessionID so URL-significant characters
	// (slash, percent, etc.) cannot alter routing on the gc API side
	// (sec-S-06). cityName comes from operator config and sessionID is
	// currently always gc-internal, but the registry is operator-editable
	// and future cby work may let external systems supply session ids.
	target := fmt.Sprintf("%s/v0/city/%s/session/%s/messages",
		cfg.gcAPIBase, url.PathEscape(cfg.cityName), url.PathEscape(sessionID))
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		log.Printf("alias dispatch: build request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", "gc-slack-adapter-alias")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("alias dispatch: POST %s: %v", target, err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("alias dispatch: %s -> %s: %s", target, resp.Status, string(respBody))
		return false
	}
	log.Printf("alias dispatch: handle=%s -> session=%s OK", handle, sessionID)
	return true
}

// reactAliasDispatchFailure fires a best-effort ⚠️ reaction on the original
// Slack message so a failed alias dispatch is visible in-channel rather than
// silently dropped. Runs inside the dispatch goroutine (already async).
func reactAliasDispatchFailure(token, channelID, ts string) {
	if token == "" {
		return
	}
	resp, err := postReactionToSlack(token, slackReactionsAddReq{
		Channel:   channelID,
		Name:      "warning",
		Timestamp: ts,
	})
	if err != nil {
		log.Printf("react warning (alias dispatch failure): chan=%s ts=%s: %v", channelID, ts, err)
		return
	}
	if !resp.OK {
		log.Printf("react warning (alias dispatch failure): chan=%s ts=%s: slack error=%s", channelID, ts, resp.Error)
	}
}

// handleIdentity serves POST /identity. The caller (gc slack identity)
// supplies a session_id and zero or more of {username, icon_url, icon_emoji}.
// The record is stored in the registry and persisted; subsequent publishes
// keyed by the same session_id pick up the override.
func handleIdentity(reg *identityRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req identityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(req.SessionID) == "" {
			http.Error(w, "session_id is required", http.StatusBadRequest)
			return
		}
		rec := identityRecord{
			Username:  req.Username,
			IconURL:   req.IconURL,
			IconEmoji: req.IconEmoji,
		}
		if err := reg.Set(req.SessionID, rec); err != nil {
			log.Printf("identity store error: %v", err)
			http.Error(w, "store failed", http.StatusInternalServerError)
			return
		}
		log.Printf("identity: session=%s username=%q icon_url=%q icon_emoji=%q",
			req.SessionID, rec.Username, rec.IconURL, rec.IconEmoji)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(identityReceipt{Stored: true, SessionID: req.SessionID})
	}
}

// handleIdentityDelete serves DELETE /identity. The session id is taken
// from either ?session_id= query param (preferred) or JSON body
// { "session_id": "..." }. Idempotent: missing entries return Existed=false
// without error.
func handleIdentityDelete(reg *identityRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
		if sessionID == "" {
			var req identityRequest
			if r.ContentLength > 0 {
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
					return
				}
				sessionID = strings.TrimSpace(req.SessionID)
			}
		}
		if sessionID == "" {
			http.Error(w, "session_id is required (?session_id= or JSON body)", http.StatusBadRequest)
			return
		}
		existed, err := reg.Delete(sessionID)
		if err != nil {
			log.Printf("identity delete error: %v", err)
			http.Error(w, "store failed", http.StatusInternalServerError)
			return
		}
		log.Printf("identity delete: session=%s existed=%v", sessionID, existed)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(identityDeleteReceipt{
			Removed:   true,
			Existed:   existed,
			SessionID: sessionID,
		})
	}
}

func postInbound(cfg config, msg externalInboundMessage) error {
	body, _ := json.Marshal(map[string]any{
		"message": msg,
	})
	// PathEscape cityName for the same reason as registerAdapter (gc-cby.28).
	target := fmt.Sprintf("%s/v0/city/%s/extmsg/inbound", cfg.gcAPIBase, url.PathEscape(cfg.cityName))
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", "gc-slack-adapter")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, string(respBody))
	}
	return nil
}
