package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// companyGateway owns the Slack company-rooms durable-admission and
// delivery path (Slack company-rooms Phase 1d). It sits between the
// signature-verified /slack/events handler and the legacy ack-first path:
// an event that targets an imported company room is admitted to the
// durable receipt store before the HTTP acknowledgment, then routed and
// delivered asynchronously; every other event falls through to the legacy
// path byte-for-byte.
//
// A nil *companyGateway is a valid "company path disabled" value — every
// method guards its nil receiver — so handlers can call through cfg
// unconditionally.
type companyGateway struct {
	// cfg is captured by value at construction (after cfg.dispatchSem is
	// initialized). The gateway never reads cfg.companyGateway, so the
	// copy having a nil back-pointer is fine; the dispatch semaphore is a
	// channel and therefore shared with the live cfg.
	cfg config

	dirStore  *companyDirectoryStore
	bindStore *companyBindingsStore
	// dmBindStore / agentApps are the Phase 4 per-agent DM registries. Both
	// share the never-fatal load contract of dirStore/bindStore (nil snapshot
	// = feature dark). agentApps binds a delivering app's api_app_id to its
	// signing secret; dmBindStore maps an agent to its singleton DM session.
	dmBindStore *dmBindingsStore
	agentApps   *agentAppsStore

	// receipts is the durable ingress store. It may be nil at startup
	// (degraded mode) when NewIngressReceiptStore failed; startRecovery
	// keeps retrying construction from ingressDir and promotes to normal
	// operation (opening the barrier) once it succeeds. Held as an atomic
	// pointer because delivery/sweep/healthz goroutines read it lock-free.
	receipts   atomic.Pointer[IngressReceiptStore]
	ingressDir string
	// storeErr holds the last store-construction error message while the
	// store is unavailable, surfaced on /healthz as the degraded paging
	// hook. Nil once the store is live.
	storeErr atomic.Pointer[string]
	// storeMu serializes store-construction attempts (startRecovery).
	storeMu sync.Mutex

	// threadParticipants is a bounded in-memory index derived from the durable
	// receipt ledger. It is hydrated lazily once per process, updated only after
	// participant evidence is durably committed, and pruned on the receipt sweep
	// retention boundary. The ledger remains the restart authority.
	threadParticipantsMu     sync.Mutex
	threadParticipants       map[threadParticipantKey][]threadParticipantEvent
	threadParticipantsLoaded bool

	// deliveryFailures counts failed target delivery attempts (retryable
	// pending, definitive 4xx, and attempts-exhausted), surfaced on
	// /healthz so silent delivery loss is observable.
	deliveryFailures atomic.Uint64

	deliverClient *http.Client
	// eventClient has no overall HTTP timeout because an SSE body may remain
	// open for the full asynchronous session.message window. The per-request
	// context bounds that wait; ResponseHeaderTimeout still bounds connection
	// setup.
	eventClient *http.Client

	// slackToken is the switchboard bot token used for bots.info author
	// resolution and conversations.* hydration. Empty in Phase 1 / tests
	// that do not exercise the bot legs.
	slackToken string
	// slackClient is the HTTP client for Slack API calls (author resolution,
	// hydration). Separate from deliverClient (which targets gc).
	slackClient *http.Client
	// authors resolves bot_id -> registered directory agent (Phase 2c).
	// Swappable in tests; the production value is an HTTP botInfoResolver.
	authors companyAuthorResolver
	// hydrate fetches the frozen context bundle for one message. Swappable in
	// tests; the production value calls Slack conversations.* with the
	// switchboard token (room deliveries only).
	hydrate func(msg CompanyMessage) companyHydration
	// hydrateDM fetches the frozen context bundle for a DM using the OWNER
	// agent's bot token (im:history) — the switchboard token must never touch
	// a DM channel. Swappable in tests; the production value calls Slack
	// conversations.* with the passed owner token. Reused for mpim hydration
	// (mpim:history), where the token is selected by the admission-owner-first
	// fallback (spec §Hydration).
	hydrateDM func(token string, msg CompanyMessage) companyHydration
	// mpimMemberProbe probes an mpim's membership with a WOKEN agent's own token
	// (conversations.info) before that agent's first delivery, so a forged event
	// for a group the agent is not in fails the target rather than waking it
	// (spec §Admission membership probe). Swappable in tests; nil is treated as
	// an advisory proceed (availability floor, like a probe network error).
	mpimMemberProbe func(token, channel string) mpimProbeOutcome

	// Phase 2 shared-state directories the ingress side reads/writes.
	intentsDir     string
	delegationsDir string
	turnsDir       string
	locksDir       string
	// turnRecordsPrunedAt throttles the immutable turn-record retention scan;
	// the delivery sweep itself runs every minute, while this directory only
	// needs receipt-parity cleanup once an hour.
	turnRecordsPrunedAt atomic.Int64
	// stalePostingIntents is the read-only sweep count of intents stuck past
	// their retry_deadline, surfaced on /healthz as the operator signal.
	stalePostingIntents atomic.Int64

	// barrier gates admission: company-admissible events receive 503
	// (retryable) until one synchronous Pending() scan completes at
	// startup and its receipts are enqueued.
	barrier atomic.Bool

	// mu guards inflight, the in-process single-flight claim set keyed by
	// receipt id. Cross-process safety rests on the receipt store's
	// generation counter; this prevents two goroutines in THIS process
	// from delivering the same receipt concurrently.
	mu       sync.Mutex
	inflight map[string]bool

	// deliverWG tracks in-flight delivery goroutines so tests can wait
	// for delivery to settle. It is Add'd synchronously inside
	// triggerDelivery before the goroutine starts.
	deliverWG sync.WaitGroup

	// chains is the per-root in-process ownership registry (S5): one active
	// chain per root triple, so a sweep pass or a live result trigger for an
	// owned root enqueues into the running chain instead of racing it.
	chains *chainRegistry

	// companyCorrelationParked is the sweep-computed count of receipts parked
	// correlation_pending or ambiguous_pending_delegations, surfaced on
	// /healthz (S7/D5 operator signal).
	companyCorrelationParked atomic.Int64

	// visibleAcks gates the config-driven visible-ack reactions
	// (SLACK_COMPANY_VISIBLE_ACKS). Off by default; all ack traffic is
	// best-effort, asynchronous inside the delivery worker, and never changes
	// receipt status or counts as a delivery failure.
	visibleAcks bool

	// reactHook performs one visible-ack reaction (add/remove) over the
	// switchboard token and classifies it into the ack taxonomy; replyHook
	// posts the one threaded failure notice. Both are function fields so tests
	// can observe ack traffic without a live Slack; production wraps slackReact
	// and the chat.postMessage path (the single reactions/message POST paths).
	reactHook func(method, channel, ts, name string) ackOutcome
	replyHook func(channel, threadTS, text string) bool
	// reactHookTok / replyHookTok are the token-parameterized ack hooks used
	// by DM receipts (the ack actor is the owner agent's token, not the
	// switchboard). When set they take precedence over reactHook/replyHook, so
	// production always routes through the token-aware path (rooms pass the
	// switchboard token; DMs pass the owner token). Tests that only exercise
	// rooms keep setting the untyped reactHook/replyHook spies.
	reactHookTok func(token, method, channel, ts, name string) ackOutcome
	replyHookTok func(token, channel, threadTS, text string) bool

	// secretsDir is the company secrets dir (SLACK_COMPANY_SECRETS_DIR),
	// shared with the Python side, from which DM owner bot tokens are loaded.
	secretsDir string
	// verifySessions gates the advisory session-existence guard (Phase 4).
	verifySessions bool
	// sessionCache is the positive-only session-existence cache: a (city,
	// session) that GET-verified 200 is cached for sessionCacheTTL. Negatives
	// are never cached (sessions 404-then-materialize; aliases 409 transiently).
	sessionCacheMu sync.Mutex
	sessionCache   map[string]time.Time

	// Self-heal counters (P0 delivery hardening), surfaced on /healthz.
	// targetReresolved counts frozen targets whose stale session was rewritten
	// to the current binding on a session-not-found retry (company_target_reresolved);
	// materializeRequests counts session-materialization POSTs fired for cold
	// bound sessions (company_materialize_requests).
	targetReresolved    atomic.Uint64
	materializeRequests atomic.Uint64
	// bootRequests counts session-create POSTs fired by the delivered-asleep
	// boot escalation (company_boot_requests): a target whose synchronous response
	// or correlated async event confirmed delivery but whose post-delivery re-check
	// still showed the session asleep, where the wake POST cleared the drain flag
	// without starting a runtime. Distinct from
	// materializeRequests (the 404 self-heal) so the two levers are observable
	// apart even though they share the throttle gate below.
	bootRequests atomic.Uint64
	// deliveredAsleep counts targets whose synchronous response or correlated
	// async result confirmed delivery but whose post-delivery GET still showed the
	// session asleep/drained. The confirmed terminal result, not HTTP acceptance
	// or the sleep state, makes the target delivered; the counter is observability
	// (company_delivered_asleep).
	deliveredAsleep atomic.Uint64
	// materializeAttempts throttles auto-materialization to at most ONE POST per
	// (city, session) per sweep interval, keyed by city+"\x00"+session → the last
	// attempt time. The delivery worker and the sweep both fire through this gate.
	materializeMu       sync.Mutex
	materializeAttempts map[string]time.Time

	// dmSigRejects counts app-bound signature rejections (a DM event whose
	// api_app_id does not match the secret it was signed with) — /healthz
	// company_dm_sig_reject. dmTokenMissing counts DM deliveries that fell back
	// to context_unavailable + degraded acks because the owner token was
	// missing — /healthz company_dm_token_missing.
	dmSigRejects   atomic.Uint64
	dmTokenMissing atomic.Uint64
	// dmStatusCounts is the sweep-computed gauge of dm-family receipts by kind
	// and status, surfaced on /healthz (company_dm_receipts + company_mpim_receipts).
	// Stored as an immutable pointer swapped each sweep pass.
	dmStatusCounts atomic.Pointer[dmFamilyReceiptCounts]
	// bodyIntegrity is the sweep-computed body gauge surfaced on /healthz
	// (company_body_missing + company_bodies_redacted). Swapped each sweep pass
	// from the single SweepAndPending scan (Phase 5 body-store split).
	bodyIntegrity atomic.Pointer[bodyIntegrityCounts]

	// Roster-drift live-membership eligibility overlay. channelMembersProbe
	// fetches the LIVE conversations.members id set for a channel over the
	// switchboard token (swappable in tests; nil or a probe failure fails the
	// overlay closed). channelMembersCache is the positive-only per-channel cache
	// of that set with a companyChannelMembersTTL window, so at most one Slack
	// call fires per channel per TTL even under a burst of stale-roster mentions.
	// rosterDriftWakes counts the targets the overlay admitted
	// (company_roster_drift_wakes) — the operator's signal that the declarative
	// roster has drifted from live channel membership and should be recaptured.
	channelMembersProbe func(channel string) (map[string]bool, bool)
	channelMembersMu    sync.Mutex
	channelMembersCache map[string]channelMemberSet
	rosterDriftWakes    atomic.Uint64

	retention     time.Duration
	sweepInterval time.Duration
	staleWindow   time.Duration
	// asyncResultTimeout bounds one correlated SSE wait. It mirrors gc's
	// session.message async timeout and is a field so tests can exercise the
	// timeout/recovery path without waiting minutes.
	asyncResultTimeout time.Duration
	now                func() time.Time
}

// sessionCacheTTL bounds a positive session-existence cache entry (Phase 4).
const sessionCacheTTL = 10 * time.Minute

// Roster-drift eligibility overlay tunables. The membership set is cached per
// channel for companyChannelMembersTTL (the design's 5-minute TTL); the probe
// follows conversations.members cursor pages to a bounded cap so a pathological
// channel cannot wedge the delivery worker, and honors at most one Retry-After
// no longer than companyChannelMembersRetryCap before failing closed.
const (
	companyChannelMembersTTL      = 5 * time.Minute
	companyChannelMembersMaxPages = 20
	companyChannelMembersRetryCap = 10 * time.Second
)

// channelMemberSet is one cached live-membership snapshot for a channel: the
// set of member user ids and the clock reading it was fetched at (TTL anchor).
type channelMemberSet struct {
	members   map[string]bool
	fetchedAt time.Time
}

// Advisory session-guard target details (Phase 4). Left on a target that the
// guard blocked; preserved through the attempt-cap exhaustion path so a
// company-redrive shows why the target never posted.
const (
	companyDetailSessionMissing   = "session_missing"
	companyDetailSessionAmbiguous = "session_ambiguous"
	// companyDetailMaterializing marks a bound target whose cold session a
	// materialization POST has just been fired for (P0 self-heal). It is a
	// recoverable, guard-held pending state exactly like session_missing — the
	// sweep re-probes on the 60s cadence and the user-visible failure notice is
	// suppressed while the attempts budget remains.
	companyDetailMaterializing = "materializing"
	// companyReasonFailedDMUnbound is the definitive detail on a DM target
	// whose owner agent has no dm_bindings row — recoverable via company-redrive
	// after a binding is imported (the rooms unbound rule, not a park).
	companyReasonFailedDMUnbound = "failed_dm_unbound"
)

// dmReceiptStatusCounts is the per-status receipt gauge for one dm-family kind.
type dmReceiptStatusCounts struct {
	Received   int
	Routing    int
	Delivered  int
	NoDelivery int
	Failed     int
}

// dmFamilyReceiptCounts splits the dm-family /healthz gauge by receipt kind: the
// DM breakdown surfaces as company_dm_receipts_*, the group-DM breakdown as
// company_mpim_receipts_* (spec §Kind-dispatch inventory: dm-family folded into
// the single scan, reported as company_dm_receipts plus a company_mpim_receipts
// breakdown).
type dmFamilyReceiptCounts struct {
	DM   dmReceiptStatusCounts
	Mpim dmReceiptStatusCounts
}

// Company delivery tunables. Retention is Discord parity (7 days); the
// receipt store enforces its own 24h floor. The stale-reclaim window
// keeps the sweep from stealing a claim from a still-running worker.
const (
	companyDeliverTimeout        = 15 * time.Second
	companyReceiptRetention      = 7 * 24 * time.Hour
	companySweepInterval         = 60 * time.Second
	companyTurnRecordPruneEvery  = time.Hour
	companyStaleReclaimWindow    = 5 * time.Minute
	companyRecoveryRetryInterval = 5 * time.Second
	companyReasonParked          = "parked_no_directory_room"
	// companyReasonBodyIntegrity parks a live receipt whose body sidecar is missing
	// or digest-mismatched (C5/m8): routing an empty message from a null body would
	// terminalize it under an unrelated reason (no_delivery / dm_author_not_allowed)
	// and destroy the wake. Parking keeps recovery open — a later orphan-adoption
	// repair or operator action can restore the body and the sweep redelivers.
	// Plain every-sweep cadence (not an S7 recovery reason, never budget-consuming).
	companyReasonBodyIntegrity   = "parked_body_integrity"
	companyDeliverRequestTag     = "gc-slack-adapter-company"
	companyMaxErrorBodyBytesRead = 4096
	companyTargetPending         = "pending"
	companyTargetDelivered       = "delivered"
	companyTargetFailed          = "failed"
	// companyMaxDeliveryAttempts bounds per-target redrive: once a target
	// has been attempted this many times without success it is marked
	// failed with reason "attempts_exhausted", so a permanently-unroutable
	// target cannot keep a receipt non-terminal (and immortal) forever.
	companyMaxDeliveryAttempts     = 12
	companyReasonAttemptsExhausted = "attempts_exhausted"
	// Target map key prefixes. Bound and unbound targets live under
	// disjoint namespaces (first byte '!' vs 's') separated from the value
	// by NUL, so a session literally named "<agent>" — or even
	// "!unbound:<agent>" — can never collide with a failed-unbound record's
	// key. The Idempotency-Key header still derives from the raw session.
	companyBoundTargetKeyPrefix   = "s\x00"
	companyUnboundTargetKeyPrefix = "!unbound\x00"
)

// companyHealthStatus is the process-singleton gateway pointer read by
// handleHealthz to append the company barrier / write-failure detail
// lines. Set once in main() after the gateway is wired; nil in every test
// that does not exercise the production main(), so /healthz keeps its
// two-line shape there.
var companyHealthStatus atomic.Pointer[companyGateway]

// newCompanyGateway builds a gateway over the loaded stores. cfg must
// already carry the initialized dispatch semaphore. receipts may be nil:
// the gateway then starts degraded (barrier stays closed, company events
// get 503, never legacy) and startRecovery retries construction from
// cfg.companyIngressDir until it succeeds.
func newCompanyGateway(cfg config, dir *companyDirectoryStore, bind *companyBindingsStore, receipts *IngressReceiptStore) *companyGateway {
	slackClient := &http.Client{Timeout: companyDeliverTimeout}
	g := &companyGateway{
		cfg:           cfg,
		dirStore:      dir,
		bindStore:     bind,
		ingressDir:    cfg.companyIngressDir,
		deliverClient: &http.Client{Timeout: companyDeliverTimeout},
		eventClient: &http.Client{Transport: &http.Transport{
			ResponseHeaderTimeout: companyDeliverTimeout,
		}},
		slackToken:          cfg.slackBotToken,
		slackClient:         slackClient,
		authors:             newBotInfoResolver(cfg.slackBotToken),
		intentsDir:          cfg.companyIntentsDir,
		delegationsDir:      cfg.companyDelegationsDir,
		turnsDir:            cfg.companyTurnsDir,
		locksDir:            cfg.companyLocksDir,
		inflight:            make(map[string]bool),
		chains:              newChainRegistry(),
		channelMembersCache: make(map[string]channelMemberSet),
		visibleAcks:         cfg.companyVisibleAcks,
		secretsDir:          cfg.companySecretsDir,
		verifySessions:      cfg.companyVerifySessions,
		sessionCache:        make(map[string]time.Time),
		materializeAttempts: make(map[string]time.Time),
		retention:           companyReceiptRetention,
		sweepInterval:       companySweepInterval,
		staleWindow:         companyStaleReclaimWindow,
		asyncResultTimeout:  companyAsyncResultTimeout,
		now:                 time.Now,
	}
	// Phase 4 DM registries: loaded here (never-fatal) against the directory
	// snapshot so a DM binding / agent-app join can be validated at load. An
	// absent file installs a nil snapshot (feature dark), matching the two
	// Phase 1 registries.
	g.dmBindStore = &dmBindingsStore{}
	_ = g.dmBindStore.Load(cfg.companyDMBindingsPath, dir.Snapshot())
	g.agentApps = &agentAppsStore{}
	_ = g.agentApps.Load(cfg.companyAgentAppsPath, dir.Snapshot())
	g.hydrate = func(msg CompanyMessage) companyHydration {
		return fetchCompanyHydration(g.slackToken, g.slackClient, msg)
	}
	g.hydrateDM = func(token string, msg CompanyMessage) companyHydration {
		return fetchCompanyHydration(token, g.slackClient, msg)
	}
	g.mpimMemberProbe = func(token, channel string) mpimProbeOutcome {
		return probeMpimMembership(token, g.slackClient, channel)
	}
	// Roster-drift overlay probe: LIVE channel membership over the switchboard
	// token (channels:read/groups:read). Swappable in tests; a failure fails the
	// overlay closed so membership can only ADD availability, never remove it.
	g.channelMembersProbe = func(channel string) (map[string]bool, bool) {
		return fetchChannelMembers(g.slackToken, g.slackClient, channel)
	}
	// Visible-ack hooks (token-parameterized): the ack actor's token is chosen
	// per receipt — the switchboard token owns reactions:write and the room
	// receipt lifecycle; a DM receipt's actor is the owner agent's token. Both
	// reuse the single existing POST paths (no second reactions/message
	// implementation) over the gateway's timeout-bounded client so a hung Slack
	// call cannot wedge the delivery worker. The untyped reactHook/replyHook
	// fields are left nil in production; only room-only test spies set them.
	g.reactHookTok = func(token, method, channel, ts, name string) ackOutcome {
		return slackReact(g.slackClient, token, method, channel, ts, name)
	}
	g.replyHookTok = func(token, channel, threadTS, text string) bool {
		if _, err := postMessageWithClient(g.slackClient, token, slackPostMessageReq{
			Channel:  channel,
			ThreadTS: threadTS,
			Text:     text,
		}); err != nil {
			log.Printf("company: visible-ack failure reply channel=%s thread=%s: %v", channel, threadTS, err)
			return false
		}
		return true
	}
	if receipts != nil {
		g.receipts.Store(receipts)
		g.ingressDir = receipts.dir
	}
	return g
}

// agentAppsSnapshot returns the current agent-apps snapshot, nil-safe on a nil
// gateway / nil store so the HTTP verification path can call through cfg
// unconditionally (a nil *AgentApps answers every query fail-closed).
func (g *companyGateway) agentAppsSnapshot() *AgentApps {
	if g == nil || g.agentApps == nil {
		return nil
	}
	return g.agentApps.Snapshot()
}

// recordDMSigReject increments the app-bound signature rejection counter
// (/healthz company_dm_sig_reject). Nil-safe.
func (g *companyGateway) recordDMSigReject() {
	if g == nil {
		return
	}
	g.dmSigRejects.Add(1)
}

// peerEnv bundles the Phase 2 shared-state directories plus the gateway clock
// for the correlation/claim layer.
func (g *companyGateway) peerEnv() companyPeerEnv {
	return companyPeerEnv{
		delegationsDir: g.delegationsDir,
		intentsDir:     g.intentsDir,
		locksDir:       g.locksDir,
		retention:      g.retention,
		now:            g.now,
	}
}

// store returns the live receipt store, or nil while the gateway is
// degraded (store construction has not yet succeeded).
func (g *companyGateway) store() *IngressReceiptStore {
	if g == nil {
		return nil
	}
	return g.receipts.Load()
}

// ensureStore returns the live receipt store, constructing it on first
// success when the gateway started degraded. Construction is serialized;
// a failure records the error for /healthz and leaves the gateway degraded.
func (g *companyGateway) ensureStore() (*IngressReceiptStore, error) {
	if s := g.receipts.Load(); s != nil {
		return s, nil
	}
	g.storeMu.Lock()
	defer g.storeMu.Unlock()
	if s := g.receipts.Load(); s != nil {
		return s, nil
	}
	s, err := NewIngressReceiptStore(g.ingressDir)
	if err != nil {
		msg := err.Error()
		g.storeErr.Store(&msg)
		return nil, err
	}
	g.receipts.Store(s)
	g.storeErr.Store(nil)
	return s, nil
}

// setStoreError records an initial store-construction failure so /healthz
// reports the degraded state immediately, before startRecovery's first
// retry. Nil-safe.
func (g *companyGateway) setStoreError(err error) {
	if g == nil || err == nil {
		return
	}
	msg := err.Error()
	g.storeErr.Store(&msg)
}

// tryHandleEvent applies the company-room admission gate. It returns true
// (having written the HTTP response) when it owns the event, and false to
// let the caller fall through to the legacy path byte-for-byte. A nil
// gateway always returns false. agentApps is the caller's once-per-request
// registration snapshot, forwarded to the DM gate so admission uses exactly the
// snapshot the HMAC verification used (m7); a nil snapshot answers fail-closed.
func (g *companyGateway) tryHandleEvent(w http.ResponseWriter, r *http.Request, env slackEventEnvelope, agentApps *AgentApps) bool {
	if g == nil {
		return false
	}
	if env.Type != "event_callback" || len(env.Event) == 0 {
		return false
	}
	var ev slackMessageEvent
	if err := json.Unmarshal(env.Event, &ev); err != nil {
		// Undecodable inner event: let the legacy async path log + drop it
		// exactly as before rather than 5xx an unkeyable body.
		return false
	}
	// MCP-echo drop precedes EVERY company admission branch (codex round
	// 3): user-token MCP posts arrive with an empty bot_id and no
	// subtype, so an imported company room or registered-agent DM would
	// otherwise admit the echo and preserve the agent→Slack→adapter→
	// agent loop the legacy path already blocks (processSlackEvent has
	// the same check for non-company events). Acked 200, no receipt.
	if strings.Contains(ev.Text, mcpEchoSignature) {
		log.Printf("company: dropping inbound bearing MCP echo signature chan=%s ts=%s text=%dch",
			ev.Channel, ev.TS, len(ev.Text))
		w.WriteHeader(http.StatusOK)
		return true
	}
	switch ev.Type {
	case "message":
		// Fall through to company admission below.
	case "app_mention":
		// Company rooms own app_mention: the paired message.channels event
		// is the canonical admitted copy, so the mention twin is acked with
		// 200 and creates no receipt — it must never reach legacy dispatch,
		// which would double-deliver by waking the channel-bound session.
		// Non-company channels keep today's app_mention behavior byte-for-byte.
		if _, ok := g.dirStore.Snapshot().RoomByChannel(env.TeamID, ev.Channel); !ok {
			return false
		}
		w.WriteHeader(http.StatusOK)
		return true
	default:
		// Other non-message types follow today's path byte-for-byte.
		return false
	}
	// DM admission (Phase 4): a message.im from a registered agent app is a
	// per-agent DM owned by the DM gateway. Any other channel_type falls
	// through to the room/legacy paths; an im from an app that is NOT a
	// registered agent app (a DM to the switchboard) also falls through so its
	// existing legacy behavior is byte-for-byte preserved.
	if ev.ChannelType == "im" {
		return g.tryHandleDMEvent(w, r, env, ev, agentApps)
	}
	// Group-DM admission (Phase 4b): a message.mpim owned by the mpim gateway.
	// A registered agent app's copy admits (origin-key dedup absorbs the other
	// member apps' copies); a switchboard-signed mpim — the switchboard also
	// subscribes to message.mpim — is acked 200 with NO receipt and NO legacy
	// dispatch (it has no business in agent group DMs). Unlike a switchboard DM,
	// an unowned mpim must NOT fall through: the room path would N-plicate it.
	if ev.ChannelType == "mpim" {
		return g.tryHandleMpimEvent(w, r, env, ev, agentApps)
	}
	if _, ok := g.dirStore.Snapshot().RoomByChannel(env.TeamID, ev.Channel); !ok {
		// Not an imported company room (including the nil-directory case):
		// the legacy path handles it. Parking applies only to receipts
		// already admitted, never to admission itself.
		return false
	}

	// From here the gateway owns the HTTP response.

	// Admissibility gate: an explicit subtype allowlist. A non-admissible
	// subtype (channel_join, topic change, hidden edit/delete, …) is
	// acked 200 and creates no receipt.
	if !AdmissibleSubtype(ev.Subtype) {
		w.WriteHeader(http.StatusOK)
		return true
	}
	// An event with no stable (team, channel, ts) identity is logged and
	// dropped with 200 — never 5xx, which would burn Slack's retry budget
	// on an unkeyable event.
	if env.TeamID == "" || ev.Channel == "" || ev.TS == "" {
		log.Printf("company: dropping unkeyable event team=%q chan=%q ts=%q", clipTeamIDForLog(env.TeamID), ev.Channel, ev.TS)
		w.WriteHeader(http.StatusOK)
		return true
	}
	// Startup recovery barrier: company-admissible events are retryable
	// (503, no x-slack-no-retry) until the first Pending() scan completes.
	// The barrier also stays closed while the gateway is degraded (store
	// construction has not yet succeeded), so a degraded store yields 503
	// here, never a legacy fallthrough.
	if !g.barrier.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	// Defensive: the barrier only opens after the store is live, but never
	// admit against a nil store — 503 (retryable), never legacy.
	store := g.store()
	if store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}

	origin := ReceiptOrigin{TeamID: env.TeamID, ChannelID: ev.Channel, TS: ev.TS}
	retryNum, retryReason := parseSlackRetryHeaders(r.Header)
	receipt := &IngressReceipt{
		Origin:      origin,
		EventID:     env.EventID,
		APIAppID:    env.APIAppID,
		RetryNum:    retryNum,
		RetryReason: retryReason,
		Status:      ingressStatusReceived,
		// Freeze the human root at admission (thread_ts, else origin ts) so every
		// root-keyed derivation survives a later body redaction/loss (C7).
		ThreadRootTS: deriveHumanRootTS(decodeCompanyMessage(origin, env.Event)),
		Event:        append(json.RawMessage(nil), env.Event...),
	}
	created, _, err := store.Admit(receipt)
	if err != nil {
		// Receipt-store write failure. 503 WITHOUT x-slack-no-retry so
		// Slack redelivers (~immediately, +1m, +5m, and hourly for 24h
		// with Delayed Events). WriteFailures() already incremented
		// inside Admit; it surfaces on /healthz as the paging hook.
		log.Printf("company: admit failed origin=%+v: %v", origin, err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	if !created {
		// Duplicate origin — an x-slack-retry redelivery of an already
		// admitted event terminates here: ack, no second delivery.
		w.WriteHeader(http.StatusOK)
		return true
	}
	// Admitted. Ack the transport, then trigger asynchronous delivery. If
	// no dispatch slot is free the receipt stays pending and the sweep
	// recovers it — backpressure, never a silent drop.
	w.WriteHeader(http.StatusOK)
	g.triggerDelivery(origin)
	return true
}

// parseSlackRetryHeaders extracts Slack's redelivery hints. A missing or
// non-numeric X-Slack-Retry-Num is treated as 0 (first delivery).
func parseSlackRetryHeaders(h http.Header) (int, string) {
	num := 0
	if v := strings.TrimSpace(h.Get("X-Slack-Retry-Num")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			num = n
		}
	}
	return num, h.Get("X-Slack-Retry-Reason")
}

// triggerDelivery acquires a dispatch slot and fans the delivery of one
// receipt out to a goroutine. On saturation it logs and returns without
// delivering; the receipt stays pending for the sweep. The dispatch
// semaphore, shared with the legacy path, is the process-wide backpressure
// bound. deliverWG.Add happens synchronously so tests can Wait reliably.
func (g *companyGateway) triggerDelivery(origin ReceiptOrigin) {
	if g == nil {
		return
	}
	// S5/S6: a live result-bearing trigger for a root with an active replay
	// chain is routed into that chain instead of racing it (the chain owner
	// will deliver it in order). Cheap when no chain is active.
	if g.enqueueForRoot(origin) {
		return
	}
	// Use the non-counting acquire: a company receipt that finds no slot
	// stays durably pending for the sweep (backpressure), which is NOT a
	// dropped delivery. Counting it would pollute dispatch_dropped_total and
	// the legacy drop-summary heartbeat, masking real legacy loss (F10).
	release, _, ok := g.cfg.tryAcquireDispatchSlot()
	if !ok {
		log.Printf("company: dispatch slot unavailable; receipt %s left pending for sweep", receiptID(origin))
		return
	}
	g.deliverWG.Add(1)
	go func() {
		defer g.deliverWG.Done()
		defer release()
		g.deliverReceipt(origin)
	}()
}

// deliverReceipt is the async delivery worker for one receipt. It holds an
// in-process single-flight claim for the receipt id, claims the receipt by
// a generation-checked Update to "routing", computes the wake set from the
// current directory snapshot, resolves each wake through the bindings
// snapshot, delivers to each bound session, and records the terminal
// status once every target settles. A receipt whose channel matches no
// room in the current snapshot is parked (never terminally resolved,
// never legacy-delivered).
func (g *companyGateway) deliverReceipt(origin ReceiptOrigin) deliverOutcome {
	if g == nil {
		return deliverError
	}
	id := receiptID(origin)
	if !g.acquireSingleFlight(id) {
		return deliverBusy
	}
	defer g.releaseSingleFlight(id)

	store := g.store()
	if store == nil {
		return deliverError // degraded: no store to deliver against yet
	}
	r, err := store.Get(origin)
	if err != nil {
		log.Printf("company: delivery read receipt %s: %v", id, err)
		return deliverError
	}
	if r == nil || isTerminalStatus(r.Status) {
		return deliverTerminal // already settled — safe to advance the chain
	}

	// Body-integrity gate (C5/m8): resolve the body through loadBody (which
	// classifies) rather than the degrade-to-null accessor. A live receipt whose
	// sidecar is missing or digest-mismatched is a recoverable INTEGRITY error, not
	// a routing outcome — routing an empty message from null would terminalize it
	// under an unrelated reason and destroy the wake. Park it (non-terminal,
	// sweep-retried, counted); a later orphan-adoption repair or operator restore
	// reopens delivery. A redacted body is deliberate and keeps the degrade path
	// (redaction is guarded to terminal receipts, so a non-terminal receipt never
	// reaches delivery redacted); an embedded/ok body proceeds.
	body, bodyStat := store.loadBody(r)
	if bodyStat == bodyMissing || bodyStat == bodyMismatch {
		g.parkWithReason(r, companyReasonBodyIntegrity)
		return deliverParkedPreclaim
	}
	msg := decodeCompanyMessage(origin, body)

	// DM-family branch (Phase 4 / 4b): a per-agent DM or group DM has its own
	// routing (owner-app join, self-echo/bot-author, allowed-human policy, and —
	// for mpim — the mention set + membership probe) and its own owner-token
	// custody. Neither takes the result-serialization lock (a direct message is
	// never result-bearing). An mpim receipt routed down the room path would park
	// forever on RoomByChannel (spec §Kind-dispatch inventory), so the family
	// gate is checked BEFORE the room resolution below.
	if isDMFamilyKind(r.Kind) {
		if r.Kind == receiptKindMpim {
			return g.deliverMpimReceipt(r, origin, msg)
		}
		return g.deliverDMReceipt(r, origin, msg)
	}

	// S6 live ordering: hold dgser (root serialization lock) across the whole
	// result-bearing path — from before correlation through finalize — so the
	// snapshot order at the requester equals delivery order. Gated on the
	// message classification (bot-authored AND gc_delegation_result metadata,
	// the only messages that can claim); dgser is the highest-rank lock, so it
	// is always acquired before dgroup/dtuple (deadlock freedom).
	if isResultBearing(msg) {
		lock, lerr := acquireCompanyLock(g.locksDir, rootSerialLockName(origin.TeamID, origin.ChannelID, receiptRootTS(r, msg)))
		if lerr != nil {
			log.Printf("company: dgser acquire receipt=%s: %v", id, lerr)
			return deliverError
		}
		defer lock.release()
	}

	// Visible-ack admission hook (config-gated, best-effort): the first
	// delivery attempt puts 👀 on the origin message. Never blocks or fails
	// delivery, never changes receipt status.
	g.applyAdmissionAck(r)

	dir := g.dirStore.Snapshot()
	room, ok := dir.RoomByChannel(origin.TeamID, origin.ChannelID)
	if !ok {
		// Channel matches no room in the CURRENT snapshot (directory
		// removed, shrunk, or failed to load) — park it.
		g.parkReceipt(r)
		return deliverParkedPreclaim
	}

	// Frozen route (design step 9 / plan 1d): the wake set is computed ONCE,
	// at first delivery. When the receipt already carries recorded targets a
	// redrive drives THOSE targets to terminal states and never recomputes —
	// so a directory that shrinks between redrives can never silently drop a
	// recorded pending target. Terminal no_delivery is legal only when there
	// are no recorded targets AND the freshly computed wake set is empty.
	if len(r.Targets) == 0 {
		// Author resolution (Phase 2c): a bot author is resolved through
		// bots.info before routing so ComputeWakeSet sees the authoritative
		// bot user id. A transient failure parks the receipt for the sweep;
		// an unknown bot falls through the classifier to unknown_bot.
		var authorAgent *CompanyAgent
		if isBotAuthored(msg) {
			res := resolveCompanyAuthor(g.authors, dir, msg)
			switch res.Outcome {
			case botResolveTransient:
				g.parkWithReason(r, peerParkResolutionPending)
				return deliverParkedPreclaim
			case botResolveOK:
				msg.ResolvedBotUserID = res.Agent.BotUserID
				authorAgent = res.Agent
			}
			// botResolveUnknown: leave ResolvedBotUserID empty.
		}

		decision := ComputeWakeSet(dir, msg, g.cfg.companySelfBotUserID)
		if decision.Room == nil {
			g.parkReceipt(r)
			return deliverParkedPreclaim
		}
		// Persist authenticated in-thread company-agent authorship before any
		// terminalization or gc delivery. Later untagged human replies derive their
		// ambient readers from this receipt-backed evidence, including after a
		// restart. A failed write leaves this receipt retryable; participation must
		// never exist only in memory.
		if participant := verifiedThreadParticipant(dir, room, msg, decision, authorAgent); participant != "" && r.ThreadParticipantAgent != participant {
			if err := g.commitReceipt(r, func(cur *IngressReceipt) {
				cur.ThreadParticipantAgent = participant
			}); err != nil {
				log.Printf("company: persist thread participant receipt=%s agent=%s: %v", id, participant, err)
				return deliverError
			}
			g.rememberThreadParticipant(r)
		}
		// Native mentions remain exclusive inside addThreadAmbientWakes. Untagged
		// human thread replies retain configured ambient wakes and add prior
		// authenticated participants under the distinct thread_ambient kind.
		decision = g.addThreadAmbientWakes(dir, room, r, msg, decision)
		if len(decision.Wakes) == 0 {
			// Live-membership eligibility overlay (roster drift): a mention that
			// would terminalize mentioned_no_eligible because company_directory.json
			// is stale can still be mention-eligible when the mentioned agent's bot
			// is a GENUINE live channel member (a workspace admin invited the app but
			// no operator has recaptured the roster yet). The overlay only ADDS
			// availability — a probe failure or a non-member leaves the fail-closed
			// terminalize below intact — never persists to the directory, never
			// broadens ambient, and never admits a non-directory identity.
			if overlay := g.admitRosterDriftWakes(dir, room, decision, msg); len(overlay) > 0 {
				decision.Wakes = overlay
				decision.Reason = ""
			} else {
				// Admitted but nobody woken (bot author, empty ambient set,
				// mentioned-but-ineligible with no live-member drift, …): terminal
				// no_delivery carrying the machine-readable routing reason.
				if err := g.commitReceipt(r, func(cur *IngressReceipt) {
					cur.Status = ingressStatusNoDelivery
					cur.Reason = decision.Reason
				}); err != nil {
					log.Printf("company: finalize no_delivery %s: %v", id, err)
					return deliverError
				}
				g.applyTerminalAck(r)
				return deliverTerminal
			}
		}

		// Freeze the wake set into (agent, kind, delegation_key) triples. For
		// a company-bot author the peer layer refines each wake into a
		// delegation vs a metadata-gated result (claiming the record), and may
		// park the receipt fail-closed on an ambiguous or in-flight
		// correlation.
		frozen, park := g.freezeWakes(dir, room, msg, decision, authorAgent)
		if park != "" {
			g.parkWithReason(r, park)
			return g.parkAdvanceOutcome(r)
		}

		// Freeze the synthesis snapshot for the peer_result leg (at most one per
		// receipt) into the SAME routing commit that freezes targets, so a
		// redrive re-renders byte-identical synthesis fields even if the record
		// is later pruned — the same frozen-bytes discipline as Hydration.
		var synthesis json.RawMessage
		for _, fw := range frozen {
			if fw.Kind != wakeKindPeerResult || fw.Snapshot == nil {
				continue
			}
			data, merr := json.Marshal(fw.Snapshot)
			if merr != nil {
				log.Printf("company: marshal synthesis snapshot %s: %v", id, merr)
				return deliverError
			}
			synthesis = data
			break
		}

		// Claim: mark routing and record every target — with its idempotency
		// key — BEFORE any gc submission, so a crash mid-delivery replays
		// with the same key. A missing binding resolves to a failed target
		// with no legacy fallback. This freezes the route.
		bindings := g.bindStore.Snapshot()
		now := g.now().UTC()
		if err := g.commitReceipt(r, func(cur *IngressReceipt) {
			cur.Status = ingressStatusRouting
			cur.Reason = ""
			if len(synthesis) > 0 {
				cur.Synthesis = synthesis
			}
			// A redrive that finds the record materialized (Python's lazy
			// reconciliation ran) claims and delivers normally: clear the S7
			// recovery backoff fields so a prior correlation park leaves no trace.
			cur.RecoveryAttempts = 0
			cur.RecoveryNextAt = time.Time{}
			cur.RecoveryReason = ""
			g.ensureTargets(cur, room, frozen, bindings, now)
		}); err != nil {
			log.Printf("company: claim routing %s: %v", id, err)
			return deliverError
		}
	}

	// Frozen hydration (Phase 2c): fetch the verified root + bounded excerpt
	// exactly once, at first delivery, and persist it so redrives re-render
	// byte-identical reminders. Only fetched when at least one bound target is
	// still pending (nobody to hydrate otherwise).
	//
	// A persist failure must NOT deliver with unpersisted hydration: commitReceipt
	// sets r.Hydration in memory before the Update, so proceeding would POST bytes
	// that are not on disk and a later redrive would refetch a possibly-different
	// bundle and re-render a divergent body under the same Idempotency-Key. Leave
	// the target pending (frozen-byte identity preserved) so the sweep refetches
	// and re-persists before any POST.
	if len(r.Hydration) == 0 && hasPendingBoundTarget(r) {
		hy := g.hydrate(msg)
		data, merr := json.Marshal(hy)
		if merr != nil {
			log.Printf("company: marshal hydration %s: %v", id, merr)
			return deliverError
		}
		if err := g.commitReceipt(r, func(cur *IngressReceipt) {
			cur.Hydration = data
		}); err != nil {
			log.Printf("company: freeze hydration %s (leaving pending, no delivery): %v", id, err)
			return deliverError
		}
	}
	var hydration companyHydration
	if len(r.Hydration) > 0 {
		_ = json.Unmarshal(r.Hydration, &hydration)
	}
	threadRootTS := receiptRootTS(r, msg)

	// Deliver each still-pending recorded target (frozen route). Results are
	// collected in memory and applied in a single finalize commit. removeKeys
	// carries the stale keys of any targets re-resolved to a new session so the
	// finalize commit drops them from the target map.
	now := g.now().UTC()
	results := make(map[string]TargetDelivery, len(r.Targets))
	removeKeys := make(map[string]bool)
	awaiting := make([]string, 0, len(r.Targets))
	awaitingSleeping := make(map[string]bool)
	for key, td := range r.Targets {
		if td.Status != companyTargetPending || td.Session == "" {
			continue
		}
		// A prior pass already crossed gc's asynchronous acceptance boundary.
		// Queue only the correlated event wait: never rerun guards, pointers, or
		// the POST, because session.message does not expose replay idempotency. The
		// waits start after this loop so one slow target cannot delay submission to
		// every remaining target.
		if td.RequestID != "" {
			results[key] = td
			awaiting = append(awaiting, key)
			continue
		}
		if td.Attempts >= companyMaxDeliveryAttempts {
			// Bounded-attempts cap: a target that has failed this many times
			// is a terminal, visible failure rather than an immortal redrive.
			td.Status = companyTargetFailed
			td.Detail = fmt.Sprintf("%s after %d attempts: %s", companyReasonAttemptsExhausted, td.Attempts, td.Detail)
			td.UpdatedAt = now
			results[key] = td
			g.deliveryFailures.Add(1)
			log.Printf("company: delivery exhausted receipt=%s session=%s attempts=%d", id, td.Session, td.Attempts)
			continue
		}
		// Advisory session-existence guard (Phase 4, gated; applies to room and
		// DM deliveries equally): a 404/409 leaves the target pending (do NOT
		// post) with the guard detail, consuming one attempt so the cap still
		// bounds the loop; the sweep re-checks. A guard error or flag-off never
		// blocks.
		guard := g.sessionGuardBlock(td.City, td.Session)
		if guard.blocked {
			td.Attempts++
			td.UpdatedAt = now
			td.Status = companyTargetPending
			td.Detail = guard.detail
			if guard.detail == companyDetailSessionMissing {
				// Self-heal a session-not-found hold: re-resolve a stale binding or
				// materialize the cold session. Never fails the target — the notice
				// stays suppressed while the attempts budget remains.
				heal := g.healSessionMissing(r, room, key, td)
				recordHeal(results, removeKeys, key, heal)
				log.Printf("company: session guard held+heal receipt=%s session=%s attempts=%d detail=%s", id, heal.td.Session, heal.td.Attempts, heal.td.Detail)
				continue
			}
			results[key] = td
			log.Printf("company: session guard held receipt=%s session=%s attempts=%d detail=%s", id, td.Session, td.Attempts, guard.detail)
			continue
		}
		if guard.sleeping {
			// The guard GET showed the target asleep/drained: wake it before the
			// message POST so the delivered message is actually processed rather
			// than silently queued. Advisory — a wake failure still proceeds.
			g.wakeSession(r, td)
		}
		// The immutable turn record and compatibility pointer are durable BEFORE
		// the gc POST on every wake, so Python verbs can select this exact route
		// the instant the turn can act. A state-write failure is retryable: skip
		// the POST and leave the target pending for the sweep.
		ptr := companyPointerFromTarget(r, room, td, threadRootTS, now)
		ptr, perr := persistCurrentTurn(g.turnsDir, ptr)
		if perr != nil {
			td.Attempts++
			td.UpdatedAt = now
			td.Status = companyTargetPending
			td.Detail = "current-turn pointer write: " + perr.Error()
			results[key] = td
			g.deliveryFailures.Add(1)
			log.Printf("company: pointer write receipt=%s session=%s: %v", id, td.Session, perr)
			continue
		}
		body := renderCompanyReminder(room, companyReminderAuthorClass(td.Kind), td.Kind, msg.Text, origin.TS, threadRootTS, hydration, r.Synthesis, &ptr)
		result := g.postCompanyMessage(td, body)
		td.Attempts++
		td.UpdatedAt = now
		if result.disposition == postAccepted {
			if err := g.persistCompanyMessageAcceptance(r, key, &td, result); err != nil {
				log.Printf("company: persist async acceptance receipt=%s session=%s request=%q: %v", id, td.Session, result.requestID, err)
				return deliverError
			}
			results[key] = td
			awaiting = append(awaiting, key)
			awaitingSleeping[key] = guard.sleeping
			continue
		}
		switch result.disposition {
		case postDelivered:
			td.Status = companyTargetDelivered
			td.Detail = ""
			if guard.sleeping && g.sessionAsleepNow(td.City, td.Session) {
				// The synchronous response or correlated event confirmed delivery, but
				// the wake did not take and the session is still asleep. Keep the
				// confirmed terminal result and surface the sleep state separately.
				g.deliveredAsleep.Add(1)
				log.Printf("company: delivered to still-asleep session receipt=%s session=%s", id, td.Session)
				// Boot escalation: the wake cleared the drain flag without starting a
				// runtime, so materialize one (throttled, advisory) to actually process
				// the queued message.
				g.tryBoot(r, td)
			}
			results[key] = td
		case postRetryable:
			// Timeout / connection error / 5xx / 408 / 429: leave pending for
			// the sweep to retry with the same key.
			td.Status = companyTargetPending
			td.Detail = result.detail
			g.deliveryFailures.Add(1)
			log.Printf("company: delivery pending receipt=%s session=%s attempts=%d: %s", id, td.Session, td.Attempts, result.detail)
			results[key] = td
		case postSessionMissing:
			// 404: the bound session vanished between the guard check and the POST
			// (or the guard is off). Keep it pending and self-heal — re-resolve a
			// stale binding or materialize the cold session — never a failure.
			td.Status = companyTargetPending
			td.Detail = companyDetailSessionMissing
			heal := g.healSessionMissing(r, room, key, td)
			recordHeal(results, removeKeys, key, heal)
			log.Printf("company: delivery session-missing+heal receipt=%s session=%s attempts=%d detail=%s", id, heal.td.Session, heal.td.Attempts, heal.td.Detail)
		default:
			// Definitive 4xx (not 404/408/429): gc rejected the submission on its
			// merits — mark the target failed rather than retry forever.
			td.Status = companyTargetFailed
			td.Detail = result.detail
			g.deliveryFailures.Add(1)
			log.Printf("company: delivery failed receipt=%s session=%s attempts=%d: %s", id, td.Session, td.Attempts, result.detail)
			results[key] = td
		}
	}

	// Every uncorrelated target has now been submitted (or classified without a
	// POST). Only now open the per-request SSE waits. This preserves prompt
	// fan-out even when the first accepted request takes minutes to settle.
	for _, key := range awaiting {
		td := results[key]
		result := g.settleCompanyAsyncTarget(&td)
		switch result.disposition {
		case postDelivered:
			if awaitingSleeping[key] && g.sessionAsleepNow(td.City, td.Session) {
				g.deliveredAsleep.Add(1)
				log.Printf("company: delivered to still-asleep session receipt=%s session=%s", id, td.Session)
				g.tryBoot(r, td)
			}
		case postRetryable:
			log.Printf("company: async result pending receipt=%s session=%s request=%q detail=%q", id, td.Session, td.RequestID, result.detail)
		default:
			log.Printf("company: async delivery failed receipt=%s session=%s request=%q detail=%q", id, td.Session, td.RequestID, result.detail)
		}
		results[key] = td
	}

	if err := g.commitReceipt(r, func(cur *IngressReceipt) {
		if cur.Targets == nil {
			cur.Targets = make(map[string]TargetDelivery, len(results))
		}
		for k := range removeKeys {
			delete(cur.Targets, k)
		}
		for k, v := range results {
			mergeCompanyTargetResult(cur.Targets, k, v)
		}
		status, reason := computeReceiptStatus(cur.Targets)
		cur.Status = status
		cur.Reason = reason
	}); err != nil {
		log.Printf("company: finalize %s: %v", id, err)
		return deliverError
	}
	if isTerminalStatus(r.Status) {
		// delivered / failed / no_delivery: run the terminal visible-ack hook
		// (👀→✅ / 👀→⚠️+threaded reply / removes-only), then advance the chain.
		g.applyTerminalAck(r)
		return deliverTerminal
	}
	// Still routing (a pending/retryable target remains): abort the chain
	// remainder — the next sweep pass rebuilds and resumes.
	return deliverPending
}

// parkAdvanceOutcome classifies a freezeWakes-park exit: a correlation park
// that terminalized under the S7 budget (correlation_recovery_exhausted) is a
// committed terminal state; any other park is pre-claim. Both advance the chain,
// but the distinction keeps the outcome honest for the sequencer and tests.
func (g *companyGateway) parkAdvanceOutcome(r *IngressReceipt) deliverOutcome {
	if isTerminalStatus(r.Status) {
		return deliverTerminal
	}
	return deliverParkedPreclaim
}

// frozenWake is a routing decision resolved to (agent, kind, delegation_key),
// ready to freeze into a TargetDelivery. Human legs (ambient/targeted) and the
// uncorrelated peer_input leg carry an empty DelegationKey; the correlated
// peer_delegation / peer_result legs carry the delegation-record filename.
type frozenWake struct {
	Agent         CompanyAgent
	Kind          string
	DelegationKey string
	// Snapshot is the frozen synthesis snapshot for the peer_result leg
	// (non-nil iff Kind is peer_result); deliverReceipt marshals it into the
	// receipt's Synthesis field in the routing commit that freezes targets.
	Snapshot *companySynthesisSnapshot
	// RosterDriftOverlay marks a wake admitted by the live-membership eligibility
	// overlay. Such an agent has no room binding by construction, so ensureTargets
	// resolves its session via dm_bindings (failed_dm_unbound when absent) rather
	// than the room-binding-only path.
	RosterDriftOverlay bool
}

// freezeWakes converts a RouteDecision into frozen wakes. Human decisions map
// one-to-one. A company-bot decision runs the peer correlation layer, which
// refines each wake to peer_delegation, peer_result (claiming the record on a
// metadata-gated result), or keyless peer_input, and can request a fail-closed
// park (returned as a non-empty reason).
//
// The five-condition trust checklist is applied as an assertion, not a filter:
// ComputeWakeSet's company-bot leg already enforces conditions 1-4 and
// member/eligibility, and the binding condition is handled downstream by
// ensureTargets (an unbound receiver becomes a recorded failed target, never a
// silent drop — mirroring the human legs). A checklist failure here would mean
// the router and the checklist disagree, so it is logged as a defense signal.
func (g *companyGateway) freezeWakes(dir *CompanyDirectory, room *CompanyRoom, msg CompanyMessage, decision RouteDecision, authorAgent *CompanyAgent) (frozen []frozenWake, parkReason string) {
	if decision.Author != AuthorCompanyBot || authorAgent == nil {
		for _, wt := range decision.Wakes {
			frozen = append(frozen, frozenWake{Agent: wt.Agent, Kind: wt.Kind, RosterDriftOverlay: wt.Overlay})
		}
		return frozen, ""
	}
	bindings := g.bindStore.Snapshot()
	mentionSet := make(map[string]bool)
	for _, id := range ExtractMentionIDs(msg.Blocks, msg.Text) {
		mentionSet[id] = true
	}
	for _, wt := range decision.Wakes {
		agent := wt.Agent
		if reason := evaluatePeerTrust(dir, bindings, room, authorAgent, &agent, mentionSet); reason != peerTrustOK && reason != peerTrustReceiverUnbound {
			// Only the unbound case is a legitimate downstream failure; any
			// other disagreement is a routing invariant violation worth a log.
			log.Printf("company: peer trust checklist flagged author=%s receiver=%s: %s", authorAgent.Name, wt.Agent.Name, reason)
		}
	}
	peerWakes, park, err := g.peerEnv().resolvePeerWakes(msg, authorAgent, decision.Wakes)
	if err != nil {
		// S7: a correlation-layer I/O or scan error (ReadDir, lock open/flock,
		// intent scan) must NOT consume the attempt budget. Park under the
		// distinct, non-counting correlation_error reason (plain every-sweep
		// cadence) rather than the budget-consuming correlation_pending, so a
		// transient degraded-infra window can never terminalize a trusted,
		// claimable result. This mirrors the S1 relock-exhaustion path
		// (resolveResultWake), which already returns peerParkCorrelationError.
		log.Printf("company: peer correlation error: %v", err)
		return nil, peerParkCorrelationError
	}
	if park != "" {
		return nil, park
	}
	for _, pw := range peerWakes {
		frozen = append(frozen, frozenWake{Agent: pw.Agent, Kind: pw.Kind, DelegationKey: pw.DelegationKey, Snapshot: pw.Snapshot})
	}
	return frozen, ""
}

// ensureTargets adds a TargetDelivery for every frozen wake not already
// recorded, preserving the state of any target from a prior attempt (a
// delivered target stays delivered). Bound and unbound targets live under
// disjoint key namespaces (companyBoundTargetKeyPrefix /
// companyUnboundTargetKeyPrefix) so no session name — however pathological —
// can collide with a failed-unbound record. The idempotency key still derives
// from the raw session.
func (g *companyGateway) ensureTargets(r *IngressReceipt, room *CompanyRoom, wakes []frozenWake, bindings *CompanyBindings, now time.Time) {
	if r.Targets == nil {
		r.Targets = make(map[string]TargetDelivery, len(wakes))
	}
	for _, wt := range wakes {
		var session, targetCity string
		binding, bound := bindings.BindingFor(room.Name, wt.Agent.Name)
		if bound {
			// Existing room bindings win when present, even for an overlay agent.
			session, targetCity = binding.Session, binding.City
		} else if wt.RosterDriftOverlay {
			// An overlay-admitted agent has no room binding for this room by
			// construction (it is not a declared roster member). Resolve its
			// canonical per-agent session via dm_bindings — the singleton session
			// the agent already runs — carrying that binding's city.
			if db, ok := g.dmBindStore.Snapshot().BindingFor(wt.Agent.Name); ok {
				session, targetCity, bound = db.Session, db.City, true
			}
		}
		if !bound {
			key := companyUnboundTargetKeyPrefix + wt.Agent.Name
			if _, exists := r.Targets[key]; exists {
				continue
			}
			detail := fmt.Sprintf("no company binding for (room=%s, agent=%s)", room.Name, wt.Agent.Name)
			if wt.RosterDriftOverlay {
				// Overlay agent with neither a room binding nor a dm binding: the
				// normal unbound handling, surfaced under failed_dm_unbound
				// (redrive-recoverable once a binding is imported).
				detail = companyReasonFailedDMUnbound
			}
			r.Targets[key] = TargetDelivery{
				Kind:          wt.Kind,
				Status:        companyTargetFailed,
				Detail:        detail,
				UpdatedAt:     now,
				Agent:         wt.Agent.Name,
				DelegationKey: wt.DelegationKey,
			}
			continue
		}
		key := companyBoundTargetKeyPrefix + session
		if _, exists := r.Targets[key]; exists {
			continue // preserve prior attempt state
		}
		r.Targets[key] = TargetDelivery{
			Session:        session,
			City:           targetCity,
			Kind:           wt.Kind,
			Status:         companyTargetPending,
			IdempotencyKey: companyIdempotencyKey(r.ID, session),
			UpdatedAt:      now,
			Agent:          wt.Agent.Name,
			DelegationKey:  wt.DelegationKey,
		}
	}
}

// admitRosterDriftWakes runs the live-membership eligibility overlay for a
// terminal mentioned_no_eligible human decision. For each mentioned directory
// agent excluded only by the (possibly stale) roster, it admits the agent as a
// targeted wake iff the agent's bot_user_id is a LIVE member of the channel. The
// member set is fetched once per channel and cached (companyChannelMembersTTL);
// any probe failure admits nobody (fail closed) so the check can only ADD
// availability, never reduce it. Each admitted target increments
// company_roster_drift_wakes and logs the drift so operators can recapture the
// roster declaratively. Returns nil when the overlay admits nobody.
func (g *companyGateway) admitRosterDriftWakes(dir *CompanyDirectory, room *CompanyRoom, decision RouteDecision, msg CompanyMessage) []WakeTarget {
	if g == nil || len(decision.OverlayCandidates) == 0 {
		return nil
	}
	members, ok := g.liveChannelMembers(msg.ChannelID)
	if !ok {
		return nil // probe failed — keep today's fail-closed mentioned_no_eligible
	}
	var out []WakeTarget
	for i := range decision.OverlayCandidates {
		a := decision.OverlayCandidates[i]
		if !members[a.BotUserID] {
			continue
		}
		out = append(out, WakeTarget{Agent: a, Kind: wakeKindTargeted, Overlay: true})
		g.rosterDriftWakes.Add(1)
		log.Printf("company: roster-drift eligibility overlay admitted agent=%s bot_user_id=%s room=%s channel=%s (live channel member, directory roster stale — recapture declaratively)",
			a.Name, a.BotUserID, room.Name, msg.ChannelID)
	}
	return out
}

// liveChannelMembers returns the LIVE member id set for a channel, served from a
// positive per-channel cache with a companyChannelMembersTTL window so a burst of
// stale-roster mentions fires at most one Slack call per channel per TTL. A probe
// failure is never cached (each mention re-probes until it succeeds) and returns
// ok=false so the caller fails closed.
func (g *companyGateway) liveChannelMembers(channel string) (map[string]bool, bool) {
	if channel == "" {
		return nil, false
	}
	now := g.now()
	g.channelMembersMu.Lock()
	if ent, ok := g.channelMembersCache[channel]; ok && now.Sub(ent.fetchedAt) < companyChannelMembersTTL {
		members := ent.members
		g.channelMembersMu.Unlock()
		return members, true
	}
	g.channelMembersMu.Unlock()

	probe := g.channelMembersProbe
	if probe == nil {
		return nil, false
	}
	members, ok := probe(channel)
	if !ok {
		return nil, false
	}
	g.channelMembersMu.Lock()
	if g.channelMembersCache == nil {
		g.channelMembersCache = make(map[string]channelMemberSet)
	}
	g.channelMembersCache[channel] = channelMemberSet{members: members, fetchedAt: now}
	g.channelMembersMu.Unlock()
	return members, true
}

// slackConversationsMembersResp is the subset of conversations.members the
// roster-drift overlay consumes.
type slackConversationsMembersResp struct {
	OK               bool     `json:"ok"`
	Error            string   `json:"error,omitempty"`
	Members          []string `json:"members"`
	ResponseMetadata struct {
		NextCursor string `json:"next_cursor"`
	} `json:"response_metadata"`
}

// fetchChannelMembers returns the LIVE member id set for a channel via
// conversations.members over the switchboard token. Cursor pages are followed to
// a bounded cap; a single Retry-After (no longer than companyChannelMembersRetryCap)
// is honored on a 429 before giving up. ANY failure — network, missing scope,
// repeated/large 429, non-ok body, or an unexhausted page cap — returns ok=false
// so the overlay fails closed (membership can only add availability).
func fetchChannelMembers(token string, client *http.Client, channel string) (map[string]bool, bool) {
	if token == "" || channel == "" || client == nil {
		return nil, false
	}
	members := make(map[string]bool)
	cursor := ""
	retried := false
	for page := 0; page < companyChannelMembersMaxPages; page++ {
		q := url.Values{}
		q.Set("channel", channel)
		q.Set("limit", "1000")
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		req, err := http.NewRequest(http.MethodGet, slackAPIBase+"/conversations.members?"+q.Encode(), nil)
		if err != nil {
			return nil, false
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			return nil, false
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			// Honor exactly one Retry-After within the cap, then fail closed. A
			// missing/too-large backpressure signal is a plain fail-closed rather
			// than an unbounded worker stall.
			delay := time.Duration(retryAfterSeconds(resp.Header)) * time.Second
			resp.Body.Close()
			if retried || delay <= 0 || delay > companyChannelMembersRetryCap {
				return nil, false
			}
			retried = true
			time.Sleep(delay)
			continue // retry the SAME cursor page
		}
		raw, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if rerr != nil || resp.StatusCode >= 300 {
			return nil, false
		}
		var body slackConversationsMembersResp
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, false
		}
		if !body.OK {
			return nil, false // missing_scope, channel_not_found, … → fail closed
		}
		for _, m := range body.Members {
			if m != "" {
				members[m] = true
			}
		}
		cursor = body.ResponseMetadata.NextCursor
		if cursor == "" {
			return members, true // cursor exhausted — complete membership view
		}
	}
	// Hit the page cap without exhausting the cursor: an incomplete view. Fail
	// closed rather than admit on a partial member set.
	return nil, false
}

// isBotAuthored reports whether a message is bot-authored (bot_message
// subtype or a non-empty bot id).
func isBotAuthored(msg CompanyMessage) bool {
	return msg.Subtype == "bot_message" || msg.BotID != ""
}

// hasPendingBoundTarget reports whether any recorded target is a pending bound
// session (i.e. there is someone to hydrate + deliver to).
func hasPendingBoundTarget(r *IngressReceipt) bool {
	for _, td := range r.Targets {
		if td.Status == companyTargetPending && td.Session != "" {
			return true
		}
	}
	return false
}

// companyReminderAuthorClass derives the reminder's author label from the
// frozen wake kind, so redrives never depend on re-resolving the author.
func companyReminderAuthorClass(kind string) string {
	if isPeerKind(kind) {
		return "company_bot"
	}
	return "human"
}

// companyIdempotencyKey builds the per-target key carried as the
// Idempotency-Key header on the session submission.
func companyIdempotencyKey(receiptID, session string) string {
	return "ingress:" + receiptID + ":target:" + session
}

// computeReceiptStatus derives the receipt-level status from its targets:
// any pending target keeps the receipt non-terminal (routing); otherwise
// any failed target makes it failed; all delivered makes it delivered.
func computeReceiptStatus(targets map[string]TargetDelivery) (status, reason string) {
	anyPending, failed := false, 0
	for _, t := range targets {
		switch t.Status {
		case companyTargetDelivered:
		case companyTargetFailed:
			failed++
		default:
			anyPending = true
		}
	}
	if anyPending {
		return ingressStatusRouting, ""
	}
	if failed > 0 {
		return ingressStatusFailed, fmt.Sprintf("%d target(s) failed delivery", failed)
	}
	return ingressStatusDelivered, ""
}

// postDisposition classifies the outcome of a session-message POST so the
// delivery loop can route a session-not-found 404 into the self-heal pipeline
// instead of terminalizing it as a definitive failure.
type postDisposition int

const (
	postDelivered      postDisposition = iota // synchronous gc 2xx, or matching async success event
	postAccepted                              // gc accepted async work; persist correlation before waiting
	postRetryable                             // transient POST or event-stream failure; retry/reconnect according to correlation state
	postSessionMissing                        // 404 — the bound session does not exist (self-heal path)
	postDefinitive                            // any other definitive 4xx / unrecoverable construction error
)

// postCompanyMessage POSTs an already-rendered reminder body to the bound
// session's gc messages endpoint with the target's Idempotency-Key. It returns:
//   - postAccepted for a valid asynchronous 202 response. The caller must
//     durably persist its request_id/event_cursor before opening the event stream.
//   - postDelivered only for a synchronous non-202 2xx response.
//   - postRetryable for outcomes whose success is unknown or transient —
//     timeout, connection error, 5xx, 408, 429 — which stay pending for the
//     sweep to retry with the same key.
//   - postSessionMissing for a 404: the bound session does not exist. The
//     caller keeps the target pending and runs the self-heal pipeline
//     (re-resolution / materialization) rather than failing it.
//   - postDefinitive for any other definitive rejection: another 4xx, or an
//     unrecoverable request-construction error. The caller marks the target
//     failed rather than retrying forever.
func (g *companyGateway) postCompanyMessage(td TargetDelivery, body string) companyPostResult {
	payload, err := json.Marshal(gcSessionMessageRequest{Message: body})
	if err != nil {
		// Deterministic construction failure: retrying cannot help.
		return companyPostResult{disposition: postDefinitive, detail: "marshal session-message body: " + err.Error()}
	}
	// City-qualified bindings deliver to sessions in other gc cities; each
	// city runs its own supervisor, so the target city selects both the URL
	// path segment and the API base (SLACK_COMPANY_CITY_APIS). An empty
	// City means the adapter's own city and base. A city with no configured
	// base is a definitive configuration failure — retrying cannot help
	// until the operator fixes the map and redrives.
	targetCity, apiBase, err := g.companyTargetAPI(td)
	if err != nil {
		return companyPostResult{disposition: postDefinitive, detail: err.Error()}
	}
	target := fmt.Sprintf("%s/v0/city/%s/session/%s/messages",
		apiBase, url.PathEscape(targetCity), url.PathEscape(td.Session))
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return companyPostResult{disposition: postDefinitive, detail: "build request: " + err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", companyDeliverRequestTag)
	req.Header.Set("Idempotency-Key", td.IdempotencyKey)

	resp, err := g.deliverClient.Do(req)
	if err != nil {
		// Timeout / connection error: outcome unknown, retry.
		return companyPostResult{disposition: postRetryable, detail: "POST: " + err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, companyMaxAsyncResponseBodyBytes+1))
		if readErr != nil {
			return companyPostResult{disposition: postDefinitive, detail: "read async acceptance: " + readErr.Error()}
		}
		if len(body) > companyMaxAsyncResponseBodyBytes {
			return companyPostResult{disposition: postDefinitive, detail: "async acceptance body too large"}
		}
		var accepted companyAsyncAccepted
		if err := json.Unmarshal(body, &accepted); err != nil {
			return companyPostResult{disposition: postDefinitive, detail: "decode async acceptance: " + err.Error()}
		}
		requestID, eventCursor, err := normalizeCompanyAsyncAcceptance(accepted)
		if err != nil {
			return companyPostResult{disposition: postDefinitive, detail: err.Error()}
		}
		return companyPostResult{
			disposition: postAccepted,
			requestID:   requestID,
			eventCursor: eventCursor,
		}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return companyPostResult{disposition: postDelivered}
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, companyMaxErrorBodyBytesRead))
	detail := fmt.Sprintf("%s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	switch {
	case resp.StatusCode == http.StatusNotFound:
		// 404: the bound session does not exist. Recoverable — the caller keeps
		// the target pending and runs the self-heal pipeline (re-resolve the
		// stale binding, or materialize the cold session) rather than failing it.
		return companyPostResult{disposition: postSessionMissing, detail: detail}
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests:
		// 408 / 429: transient, retry.
		return companyPostResult{disposition: postRetryable, detail: detail}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// Definitive 4xx: rejected on its merits, do not retry.
		return companyPostResult{disposition: postDefinitive, detail: detail}
	default:
		// 5xx and anything else non-2xx: retry.
		return companyPostResult{disposition: postRetryable, detail: detail}
	}
}

// companySessionCreateRequest is the POST /v0/city/{city}/sessions body used to
// materialize a cold named/pool session. name is the template/pool name; kind is
// always "agent" (the only kind the adapter materializes).
type companySessionCreateRequest struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// companySessionRecord is the subset of the supervisor's GET session record the
// self-heal path reads: the template a non-template-named instance was minted
// from, used to derive the materialization name when the bound session itself
// does not look like a template/pool name.
type companySessionRecord struct {
	Template string `json:"template,omitempty"`
	// State is the supervisor's lifecycle state for the session. The delivery
	// path reads it to detect an asleep/drained session that would silently queue
	// a delivered message without processing it (the pre-delivery wake trigger).
	State string `json:"state,omitempty"`
}

// isSleepingSessionState reports whether a supervisor session state means the
// session would accept a delivered message (202) but never process it until
// woken. Only the two states that exhibit the silent-queue defect count; any
// other (or unknown/empty) state is treated as awake so the wake path never
// fires speculatively.
func isSleepingSessionState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "asleep", "drained":
		return true
	default:
		return false
	}
}

// looksLikeTemplateName reports whether a session name looks like a
// template/pool name that gc can materialize directly: a config-form dotted
// name (e.g. "teams.pm") or its dunder runtime alias (e.g. "teams__pm"). An
// adhoc instance id (no dot, no dunder) is not materializable by name and must
// fall back to the supervisor record's template field.
func looksLikeTemplateName(session string) bool {
	return strings.Contains(session, ".") || strings.Contains(session, "__")
}

// currentBindingSession resolves the CURRENT bound session (and its city
// qualifier) for a target's (agent, room-or-dm) from the loaded registries. A
// dm-family receipt resolves through dm_bindings (keyed by agent); a room
// receipt through the room bindings (keyed by room+agent). bound=false means the
// binding row is gone — the caller must NOT re-resolve or materialize (unbound
// semantics stay intact). This mirrors applyRedrive's resolver so the retry path
// and the operator redrive re-resolve identically.
func (g *companyGateway) currentBindingSession(r *IngressReceipt, room *CompanyRoom, agent string) (session, city string, bound bool) {
	if agent == "" {
		return "", "", false
	}
	if isDMFamilyKind(r.Kind) {
		if bd, ok := g.dmBindStore.Snapshot().BindingFor(agent); ok {
			return bd.Session, bd.City, true
		}
		return "", "", false
	}
	if room != nil {
		if bd, ok := g.bindStore.Snapshot().BindingFor(room.Name, agent); ok {
			return bd.Session, bd.City, true
		}
	}
	return "", "", false
}

// sessionMissingHealResult carries the outcome of one self-heal pass. td is the
// (possibly re-resolved) target to record; when rekeyed is set the target moved
// to a new bound-key namespace (newKey) and the stale oldKey must be dropped from
// the receipt's target map in the finalize commit.
type sessionMissingHealResult struct {
	td      TargetDelivery
	newKey  string
	oldKey  string
	rekeyed bool
}

// healSessionMissing runs the self-heal pipeline for a BOUND target whose current
// delivery attempt hit a session-not-found condition — the advisory guard's 404
// hold or a delivery POST 404. It never fails the target: the caller keeps it
// pending so no user-visible failure notice fires while the attempts budget
// remains (P0 §failure-notice suppression).
//
//   - Auto-re-resolution (P0 item 1): if the current binding for this target's
//     (agent, room/dm) names a DIFFERENT session than the frozen one, the target
//     adopts the current binding (session, city, freshly-derived idempotency key)
//     and is re-keyed. The next sweep probes the live session. Attempts are NOT
//     reset — the budget still bounds a binding that keeps churning.
//   - Auto-materialization (P0 item 2): if the binding still names this exact
//     (cold) session, fire at most ONE materialize POST per (city, session) per
//     sweep interval.
//   - Binding row gone: neither re-resolve nor materialize (unbound semantics).
//
// The caller has already set td.Status=pending, bumped Attempts, and stamped
// UpdatedAt; healSessionMissing owns only the Session/City/IdempotencyKey rewrite
// and the recoverable Detail.
func (g *companyGateway) healSessionMissing(r *IngressReceipt, room *CompanyRoom, key string, td TargetDelivery) sessionMissingHealResult {
	session, city, bound := g.currentBindingSession(r, room, td.Agent)
	switch {
	case bound && session != "" && session != td.Session:
		old := td.Session
		td.Session = session
		td.City = city
		td.IdempotencyKey = companyIdempotencyKey(r.ID, session)
		td.Detail = companyDetailSessionMissing // stays guard-held → swept on the 60s cadence
		g.targetReresolved.Add(1)
		log.Printf("company: target re-resolved receipt=%s agent=%s session %s->%s (stale frozen binding)",
			r.ID, td.Agent, old, session)
		return sessionMissingHealResult{td: td, newKey: companyBoundTargetKeyPrefix + session, oldKey: key, rekeyed: true}
	case bound && session == td.Session:
		if g.tryMaterialize(r, td) {
			td.Detail = companyDetailMaterializing
		} else {
			td.Detail = companyDetailSessionMissing
		}
		return sessionMissingHealResult{td: td}
	default:
		// Binding row gone: leave the target pending under session_missing. It is
		// no longer re-resolvable and ages out under the attempts cap; we never
		// fabricate a session for a vanished binding (unbound semantics).
		td.Detail = companyDetailSessionMissing
		return sessionMissingHealResult{td: td}
	}
}

// recordHeal merges one self-heal pass into the delivery loop's in-memory
// results, handling re-keying: a re-resolved target lands under its new bound
// key and the stale key is queued for deletion in the finalize commit.
func recordHeal(results map[string]TargetDelivery, removeKeys map[string]bool, key string, heal sessionMissingHealResult) {
	if heal.rekeyed {
		removeKeys[heal.oldKey] = true
		results[heal.newKey] = heal.td
		return
	}
	results[key] = heal.td
}

// tryMaterialize fires at most ONE session-materialization POST per (city,
// session) per sweep interval for a cold bound target stuck in session_missing.
// Template/pool-shaped bindings POST /v0/city/{city}/sessions. Exact bindings
// POST /v0/city/{city}/session/{name}/wake instead: the supervisor only
// materializes that request when {name} is a configured named session, while a
// stale concrete session name returns 404 without minting an orphan from its
// former template. Both use materialize:<receipt>:<session> as their
// Idempotency-Key. Best-effort: any transport/status outcome leaves the target
// pending for the next sweep to re-probe — the attempts budget bounds everything.
func (g *companyGateway) tryMaterialize(r *IngressReceipt, td TargetDelivery) bool {
	targetCity, apiBase, ok := g.companyCityBase(td.City)
	if !ok {
		// No configured base for this city — the delivery POST already surfaces
		// the misconfiguration definitively; nothing to materialize against.
		return false
	}
	// Throttle gate: record the attempt under the lock BEFORE any work so two
	// receipts racing the same cold session can never double-fire within a sweep
	// interval (the strongest at-most-once guarantee).
	throttleKey := targetCity + "\x00" + td.Session
	g.materializeMu.Lock()
	if last, seen := g.materializeAttempts[throttleKey]; seen && g.now().Sub(last) < g.sweepInterval {
		g.materializeMu.Unlock()
		return false
	}
	g.materializeAttempts[throttleKey] = g.now()
	g.materializeMu.Unlock()

	if !looksLikeTemplateName(td.Session) {
		return g.postSessionWake(r, td, apiBase, targetCity, "materialize", &g.materializeRequests)
	}

	name := g.materializeName(apiBase, targetCity, td.Session)
	if name == "" {
		log.Printf("company: materialize skip receipt=%s session=%s: no derivable template/pool name",
			r.ID, td.Session)
		return false
	}
	return g.postSessionCreate(r, td, apiBase, targetCity, name, "materialize", &g.materializeRequests)
}

// tryBoot fires the delivered-asleep boot escalation for a target whose
// synchronous response or correlated async event confirmed delivery but whose
// post-delivery re-check still showed the session asleep/drained — the wake POST
// cleared the drain flag without starting a runtime, so the message sits queued
// and unprocessed. It materializes a runtime
// with POST /v0/city/{city}/sessions {"name": <n>, "kind": "agent"},
// Idempotency-Key boot:<receipt>:<session>, counted as company_boot_requests and
// throttled through the SAME per-(city,session) gate as tryMaterialize so a
// session is never double-POSTed within one sweep interval.
//
// <n> reuses tryMaterialize's name discipline for template/pool aliases. Exact
// named-session targets use the safe /session/{name}/wake endpoint, which wakes
// or materializes only the configured canonical identity and cannot create an
// adhoc worker from a stale concrete name. Best-effort: any transport/status
// outcome leaves the target delivered (advisory).
func (g *companyGateway) tryBoot(r *IngressReceipt, td TargetDelivery) bool {
	targetCity, apiBase, ok := g.companyCityBase(td.City)
	if !ok {
		// No configured base for this city — nothing to boot against.
		return false
	}
	// Throttle gate shared with tryMaterialize: record the attempt under the lock
	// BEFORE any POST so racing receipts (and a same-sweep 404 heal) can never
	// double-fire a session-create within one interval.
	throttleKey := targetCity + "\x00" + td.Session
	g.materializeMu.Lock()
	if last, seen := g.materializeAttempts[throttleKey]; seen && g.now().Sub(last) < g.sweepInterval {
		g.materializeMu.Unlock()
		return false
	}
	g.materializeAttempts[throttleKey] = g.now()
	g.materializeMu.Unlock()

	if !looksLikeTemplateName(td.Session) {
		return g.postSessionWake(r, td, apiBase, targetCity, "boot", &g.bootRequests)
	}
	return g.postSessionCreate(r, td, apiBase, targetCity, td.Session, "boot", &g.bootRequests)
}

// postSessionCreate performs the session-create POST shared by the 404 self-heal
// (tryMaterialize) and the delivered-asleep boot escalation (tryBoot). Callers
// own throttling and name derivation; this owns the request, the counter, and
// the best-effort outcome. verb is BOTH the Idempotency-Key prefix and the log
// label ("materialize" or "boot"). Returns true iff the POST was attempted — a
// transport error still counts as attempted so the caller does not re-derive
// within the throttle interval.
func (g *companyGateway) postSessionCreate(r *IngressReceipt, td TargetDelivery, apiBase, targetCity, name, verb string, counter *atomic.Uint64) bool {
	payload, err := json.Marshal(companySessionCreateRequest{Name: name, Kind: "agent"})
	if err != nil {
		return false
	}
	target := fmt.Sprintf("%s/v0/city/%s/sessions", apiBase, url.PathEscape(targetCity))
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", companyDeliverRequestTag)
	req.Header.Set("Idempotency-Key", verb+":"+r.ID+":"+td.Session)

	counter.Add(1)
	resp, err := g.deliverClient.Do(req)
	if err != nil {
		log.Printf("company: %s POST receipt=%s city=%s session=%s name=%s: %v",
			verb, r.ID, targetCity, td.Session, name, err)
		return true
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, companyMaxErrorBodyBytesRead))
	log.Printf("company: %s requested receipt=%s city=%s session=%s name=%s status=%d",
		verb, r.ID, targetCity, td.Session, name, resp.StatusCode)
	return true
}

// materializeName derives the session-create "name" for a materialization POST.
// Only template/pool-shaped bindings are materializable; an exact session name
// must be repaired by rebinding to a live stable session, never by minting an
// unrelated instance from its former template.
func (g *companyGateway) materializeName(apiBase, city, session string) string {
	if looksLikeTemplateName(session) {
		return session
	}
	return ""
}

// companyCityBase resolves the (targetCity, apiBase) a target delivers against.
// An empty City means the adapter's own city and gcAPIBase. A city that matches
// the adapter's own city also uses gcAPIBase. A different city uses its
// SLACK_COMPANY_CITY_APIS base; ok=false when that city has no configured base,
// so best-effort probes/mutations skip rather than post to the wrong host.
func (g *companyGateway) companyCityBase(city string) (targetCity, apiBase string, ok bool) {
	if city == "" || city == g.cfg.cityName {
		tc := city
		if tc == "" {
			tc = g.cfg.cityName
		}
		return tc, g.cfg.gcAPIBase, true
	}
	mapped, found := g.cfg.companyCityAPIs[city]
	if !found {
		return city, "", false
	}
	return city, mapped, true
}

// wakeSession fires a best-effort wake POST for a target whose pre-delivery guard
// GET showed it asleep/drained. It is advisory: any transport error or non-2xx
// status is logged and swallowed so the caller proceeds to the message POST
// regardless — the message still queues, and a post-delivery re-check surfaces a
// session that never woke via company_delivered_asleep. The Idempotency-Key
// (wake:<receipt>:<session>) makes a redrive's repeat wake a supervisor no-op.
func (g *companyGateway) wakeSession(r *IngressReceipt, td TargetDelivery) {
	targetCity, apiBase, ok := g.companyCityBase(td.City)
	if !ok {
		return
	}
	g.postSessionWake(r, td, apiBase, targetCity, "wake", nil)
}

// postSessionWake performs the safe named-session wake/materialization POST.
// The supervisor resolves the path target against configured named sessions;
// an absent stale concrete session therefore returns 404 without creating a
// replacement from an unrelated template. Returns true iff the HTTP request
// was attempted. counter may be nil for the ordinary pre-delivery wake.
func (g *companyGateway) postSessionWake(r *IngressReceipt, td TargetDelivery, apiBase, targetCity, verb string, counter *atomic.Uint64) bool {
	target := fmt.Sprintf("%s/v0/city/%s/session/%s/wake",
		apiBase, url.PathEscape(targetCity), url.PathEscape(td.Session))
	req, err := http.NewRequest(http.MethodPost, target, nil)
	if err != nil {
		return false
	}
	req.Header.Set("X-GC-Request", companyDeliverRequestTag)
	req.Header.Set("Idempotency-Key", verb+":"+r.ID+":"+td.Session)
	if counter != nil {
		counter.Add(1)
	}
	resp, err := g.deliverClient.Do(req)
	if err != nil {
		log.Printf("company: session wake POST verb=%s receipt=%s city=%s session=%s: %v", verb, r.ID, targetCity, td.Session, err)
		return true
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, companyMaxErrorBodyBytesRead))
	log.Printf("company: session wake requested verb=%s receipt=%s city=%s session=%s status=%d",
		verb, r.ID, targetCity, td.Session, resp.StatusCode)
	return true
}

// sessionAsleepNow does a single best-effort GET — independent of the positive
// existence cache — and reports whether the session's state is still
// asleep/drained. Used only after a delivered POST to a session that was asleep
// before the wake: a true here is the silent-queue case. Any error, non-2xx, or
// unparseable/absent state reports false (assume awake) — an advisory probe never
// converts a delivered target into a failure.
func (g *companyGateway) sessionAsleepNow(city, session string) bool {
	if session == "" {
		return false
	}
	targetCity, apiBase, ok := g.companyCityBase(city)
	if !ok {
		return false
	}
	target := fmt.Sprintf("%s/v0/city/%s/session/%s",
		apiBase, url.PathEscape(targetCity), url.PathEscape(session))
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return false
	}
	req.Header.Set("X-GC-Request", companyDeliverRequestTag)
	resp, err := g.deliverClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, companyMaxErrorBodyBytesRead))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	var rec companySessionRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return false
	}
	return isSleepingSessionState(rec.State)
}

// decodeCompanyMessage reconstructs the router's CompanyMessage view from
// the stored inner event. The origin supplies the canonical keys; the
// decoded event supplies the routing fields (subtype, author, text,
// blocks, thread, app/metadata for the bot legs).
func decodeCompanyMessage(origin ReceiptOrigin, event json.RawMessage) CompanyMessage {
	var ev slackMessageEvent
	_ = json.Unmarshal(event, &ev)
	return CompanyMessage{
		TeamID:          origin.TeamID,
		ChannelID:       origin.ChannelID,
		TS:              origin.TS,
		ThreadTS:        ev.ThreadTS,
		AppID:           ev.AppID,
		BotProfileAppID: parseBotProfileAppID(ev.BotProfile),
		Metadata:        ev.Metadata,
		UserID:          ev.User,
		BotID:           ev.BotID,
		Subtype:         ev.Subtype,
		Text:            ev.Text,
		Blocks:          ev.Blocks,
		Files:           ev.Files,
	}
}

// parkReceipt records the directory-park state: non-terminal, Status
// "received", Reason "parked_no_directory_room".
func (g *companyGateway) parkReceipt(r *IngressReceipt) {
	g.parkWithReason(r, companyReasonParked)
}

// parkWithReason records a non-terminal parked state carrying a
// machine-readable reason (directory park, author-resolution-pending, or a
// peer correlation park). For the two backoff reasons (correlation_pending and
// ambiguous_pending_delegations) it drives the S7 attempt schedule via
// parkWithRecovery; for every other reason it stays idempotent — an already-
// parked receipt with the same reason is left untouched to avoid generation
// churn on every sweep (correlation_error, author_resolution_pending, and the
// directory park all keep their plain every-sweep cadence).
func (g *companyGateway) parkWithReason(r *IngressReceipt, reason string) {
	if isRecoveryReason(reason) {
		g.parkWithRecovery(r, reason)
		return
	}
	if r.Status == ingressStatusReceived && r.Reason == reason {
		return
	}
	log.Printf("company: parking receipt %s reason=%s", r.ID, reason)
	if err := g.commitReceipt(r, func(cur *IngressReceipt) {
		cur.Status = ingressStatusReceived
		cur.Reason = reason
	}); err != nil {
		log.Printf("company: park receipt %s: %v", r.ID, err)
	}
}

// isRecoveryReason reports whether a park reason is subject to the S7 bounded
// backoff. correlation_error and the Phase 1/2 parks explicitly are not: they
// keep the plain every-sweep cadence and never consume the attempt budget.
func isRecoveryReason(reason string) bool {
	return reason == peerParkCorrelationPending || reason == peerParkAmbiguousPending
}

// isCorrelationParked reports whether a receipt is currently parked under a
// correlation reason counted on /healthz as company_correlation_parked
// (correlation_pending or ambiguous_pending_delegations).
func isCorrelationParked(r *IngressReceipt) bool {
	return r.Status == ingressStatusReceived &&
		(r.Reason == peerParkCorrelationPending || r.Reason == peerParkAmbiguousPending)
}

// parkWithRecovery drives the S7 correlation-park backoff. The first park under
// a recovery reason is immediately eligible on the next pass (no initial delay,
// attempts unchanged). A re-park under the SAME reason is exactly the case
// "a redrive re-ran resolveResultWake and found the posting intent still in
// flight" (the idempotent-early-return case): it counts one attempt and either
// schedules the next backed-off pass or terminalizes. correlation_pending goes
// terminal (failed / correlation_recovery_exhausted, counted in
// deliveryFailures) at attempt 6; the Slack-born ambiguous park (D5) uses the
// same schedule but NEVER terminalizes — it keeps retrying at the 15-minute cap.
func (g *companyGateway) parkWithRecovery(r *IngressReceipt, reason string) {
	alreadyParked := r.Status == ingressStatusReceived && r.Reason == reason
	if !alreadyParked {
		// First park under this recovery reason: immediately eligible next pass.
		// The attempt budget is scoped PER reason — a transition to a different
		// recovery reason (or from a non-recovery park, e.g. a correlation_error
		// interlude) starts a fresh streak, so an unrelated prior streak (a
		// long-lived ambiguous park, or a burst of correlation_error re-parks)
		// can never near-instantly terminalize this genuine correlation_pending.
		log.Printf("company: parking receipt %s reason=%s (recovery)", r.ID, reason)
		if err := g.commitReceipt(r, func(cur *IngressReceipt) {
			cur.Status = ingressStatusReceived
			cur.Reason = reason
			cur.RecoveryReason = reason
			cur.RecoveryAttempts = 0
			cur.RecoveryNextAt = time.Time{}
		}); err != nil {
			log.Printf("company: park receipt %s: %v", r.ID, err)
		}
		return
	}
	attempts := r.RecoveryAttempts + 1
	if reason == peerParkCorrelationPending && attempts >= companyPeerRecoveryMaxAttempts {
		log.Printf("company: correlation park exhausted receipt=%s attempts=%d -> terminal", r.ID, attempts)
		if err := g.commitReceipt(r, func(cur *IngressReceipt) {
			cur.Status = ingressStatusFailed
			cur.Reason = companyReasonRecoveryExhausted
			cur.RecoveryReason = reason
			cur.RecoveryAttempts = attempts
			cur.RecoveryNextAt = time.Time{}
		}); err != nil {
			log.Printf("company: terminalize correlation park %s: %v", r.ID, err)
			return
		}
		g.deliveryFailures.Add(1)
		g.applyTerminalAck(r)
		return
	}
	next := g.now().UTC().Add(nextRecoveryDelay(attempts))
	log.Printf("company: correlation park backoff receipt=%s reason=%s attempts=%d next=%s", r.ID, reason, attempts, next.Format(time.RFC3339))
	if err := g.commitReceipt(r, func(cur *IngressReceipt) {
		cur.Status = ingressStatusReceived
		cur.Reason = reason
		cur.RecoveryReason = reason
		cur.RecoveryAttempts = attempts
		cur.RecoveryNextAt = next
	}); err != nil {
		log.Printf("company: schedule correlation park %s: %v", r.ID, err)
	}
}

// commitReceipt applies apply(r) and persists via a generation-checked
// Update. On ErrStale it re-reads the on-disk receipt, re-applies the
// intent onto that fresh base (a merge, not a blind overwrite), and
// retries exactly once.
func (g *companyGateway) commitReceipt(r *IngressReceipt, apply func(cur *IngressReceipt)) error {
	store := g.store()
	if store == nil {
		return errors.New("company: receipt store unavailable")
	}
	apply(r)
	err := store.Update(r)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrStale) {
		return err
	}
	fresh, gerr := store.Get(r.Origin)
	if gerr != nil {
		return gerr
	}
	if fresh == nil {
		return fmt.Errorf("company: receipt %s vanished during update", r.ID)
	}
	*r = *fresh
	apply(r)
	return store.Update(r)
}

func (g *companyGateway) acquireSingleFlight(id string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inflight[id] {
		return false
	}
	g.inflight[id] = true
	return true
}

func (g *companyGateway) releaseSingleFlight(id string) {
	g.mu.Lock()
	delete(g.inflight, id)
	g.mu.Unlock()
}

// startRecovery runs the company-scoped startup barrier: it (re)constructs
// the receipt store if the gateway started degraded, then retries the
// synchronous Pending() scan until it succeeds, opens the admission
// barrier, and starts the periodic sweep (which waits one interval before
// its first pass). While the store cannot be constructed the barrier stays
// closed, so company-admissible events get 503 (retryable) — never legacy
// fallthrough — and /healthz reports the store error. A nil gateway is a
// no-op.
func (g *companyGateway) startRecovery(ctx context.Context) {
	if g == nil {
		return
	}
	go func() {
		for {
			if _, err := g.ensureStore(); err != nil {
				log.Printf("company: receipt store unavailable (degraded): %v; retrying in %s", err, companyRecoveryRetryInterval)
				select {
				case <-ctx.Done():
					return
				case <-time.After(companyRecoveryRetryInterval):
					continue
				}
			}
			if err := g.recoverPending(); err != nil {
				log.Printf("company: startup recovery scan failed: %v; retrying in %s", err, companyRecoveryRetryInterval)
				select {
				case <-ctx.Done():
					return
				case <-time.After(companyRecoveryRetryInterval):
					continue
				}
			}
			g.barrier.Store(true)
			log.Printf("company: startup recovery complete; admission barrier open")
			go g.runSweep(ctx)
			return
		}
	}()
}

// recoverPending runs one synchronous Pending() scan and enqueues every
// eligible non-terminal receipt for delivery. Per-root replay chains
// (result-bearing receipts and correlation parks) are ordered per S5 and driven
// sequentially by one worker chain; every other receipt keeps the store's
// (ReceivedAt, Origin.TS) order and is triggered concurrently as before. It
// applies the same recoveryDue eligibility as the sweep so a restart neither
// bypasses the S7 backoff nor bumps attempts. It does NOT open the barrier — the
// caller does that on success — so tests can drive it directly.
func (g *companyGateway) recoverPending() error {
	store := g.store()
	if store == nil {
		return errors.New("company: receipt store unavailable")
	}
	pending, err := store.Pending()
	if err != nil {
		return err
	}
	now := g.now()
	var eligible []*IngressReceipt
	for _, rec := range pending {
		if !recoveryDue(rec, now) {
			continue
		}
		eligible = append(eligible, rec)
	}
	chains, rest := g.orderPendingForReplay(eligible)
	for _, c := range chains {
		g.triggerChain(c)
	}
	for _, rec := range rest {
		g.triggerDelivery(rec.Origin)
	}
	return nil
}

// triggerChain acquires one dispatch slot for a whole replay chain and drives
// it sequentially and synchronously in a goroutine (deliverChain). The single
// slot bounds a chain to the same process-wide backpressure as an individual
// delivery; on saturation the chain is left durably pending for the next sweep.
func (g *companyGateway) triggerChain(c replayChain) {
	if g == nil || len(c.Origins) == 0 {
		return
	}
	release, _, ok := g.cfg.tryAcquireDispatchSlot()
	if !ok {
		log.Printf("company: dispatch slot unavailable; replay chain of %d left pending for sweep", len(c.Origins))
		return
	}
	g.deliverWG.Add(1)
	go func() {
		defer g.deliverWG.Done()
		defer release()
		g.deliverChain(c)
	}()
}

// runSweep is the periodic redrive loop. It waits one interval before its
// first pass (NewTicker semantics) and exits on ctx cancel.
func (g *companyGateway) runSweep(ctx context.Context) {
	ticker := time.NewTicker(g.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.sweepOnce()
		}
	}
}

// sweepOnce GC's terminal receipts past retention and redrives eligible
// non-terminal receipts (parked, pending, or a stale routing claim) in a
// single directory scan (SweepAndPending).
func (g *companyGateway) sweepOnce() {
	store := g.store()
	if store == nil {
		return
	}
	pending, healNeeded, _, dmCounts, bodyCounts, err := store.SweepAndPending(g.retention)
	if err != nil {
		log.Printf("company: sweep: %v", err)
		return
	}
	now := g.now()
	g.pruneImmutableTurnRecords(time.Now())
	g.pruneThreadParticipants(now)
	parked := 0
	var eligible []*IngressReceipt
	for _, rec := range pending {
		if isCorrelationParked(rec) {
			// Count every correlation/ambiguity park (eligible or backed-off) for
			// the /healthz operator signal — computed in this existing scan (S7).
			parked++
		}
		if !sweepEligible(rec, now, g.staleWindow) {
			continue
		}
		eligible = append(eligible, rec)
	}
	g.companyCorrelationParked.Store(int64(parked))
	// Per-root replay chains (S5) are driven sequentially; every other eligible
	// receipt keeps store order and is triggered concurrently as before.
	chains, rest := g.orderPendingForReplay(eligible)
	for _, c := range chains {
		g.triggerChain(c)
	}
	for _, rec := range rest {
		g.triggerDelivery(rec.Origin)
	}
	// Terminal-ack sweep-healing (Phase 3b): a terminal receipt stranded on
	// AckState=="eyes"/"warned" (its terminal ack was rate-limited or the process
	// crashed between finalize and the ack commit) is healed here, within
	// retention. The heal set rides the same SweepAndPending scan (no second
	// full-store pass); it is applied only when acks are enabled — the default
	// (off) pays nothing.
	if g.visibleAcks {
		for _, r := range healNeeded {
			g.applyTerminalAck(r)
		}
	}
	// Read-only scan of the intents dir for the /healthz operator signal:
	// intents stuck in "posting" past their retry_deadline (a wedged Python
	// outbound flow). The Go side never reposts — this is a count only.
	g.stalePostingIntents.Store(int64(g.peerEnv().countStalePostingIntents()))
	// Phase 4 DM receipt gauge (by status) for /healthz, computed inside the
	// single SweepAndPending scan above (no second full-store pass under the
	// store mutex — m8).
	g.dmStatusCounts.Store(&dmCounts)
	// Phase 5 body-integrity gauge (missing / redacted) for /healthz, computed
	// inside the same SweepAndPending scan — no second directory pass (m8).
	g.bodyIntegrity.Store(&bodyCounts)
}

func (g *companyGateway) pruneImmutableTurnRecords(now time.Time) {
	unix := now.Unix()
	for {
		last := g.turnRecordsPrunedAt.Load()
		if last != 0 && unix-last < int64(companyTurnRecordPruneEvery/time.Second) {
			return
		}
		if g.turnRecordsPrunedAt.CompareAndSwap(last, unix) {
			break
		}
	}
	removed, err := sweepCurrentTurnRecords(g.turnsDir, now.Add(-g.retention))
	if err != nil {
		log.Printf("company: immutable turn-record sweep: %v", err)
		return
	}
	if removed > 0 {
		log.Printf("company: immutable turn-record sweep removed=%d", removed)
	}
}

// sweepEligible reports whether the sweep should redrive a non-terminal
// receipt. Unclaimed/pending receipts (Status != routing: received or
// parked) are always eligible; a claimed (routing) receipt is reclaimed
// only once its claim goes stale, so the sweep never steals work from a
// live in-flight worker.
//
// The receipt-level UpdatedAt is the claim timestamp: it is refreshed on
// every Update, so a routing receipt whose most recent claim/finalize is
// fresher than the window is a live claim and skipped; a stale one (a
// crashed worker) is reclaimed. A routing receipt with no recorded
// UpdatedAt is reclaimed immediately.
func sweepEligible(r *IngressReceipt, now time.Time, window time.Duration) bool {
	if isTerminalStatus(r.Status) {
		return false
	}
	// S7 backoff gate: a correlation park scheduled for a future RecoveryNextAt
	// is not yet due (an adapter restart neither bypasses the backoff nor bumps
	// the attempt count — recoverPending consults recoveryDue too).
	if !recoveryDue(r, now) {
		return false
	}
	if r.Status != ingressStatusRouting {
		return true
	}
	// A session-existence guard hold commits the receipt back to routing with a
	// fresh UpdatedAt, but it is NOT a live in-flight claim — the worker
	// deliberately deferred and released. The spec promises the 60s sweep
	// re-checks such holds, so exempt them from the 5-minute stale-reclaim gate
	// (applies to room and DM deliveries equally). The in-process single-flight
	// claim + generation-checked Update still bound concurrent redrives.
	if guardHeldPending(r) {
		return true
	}
	// A correlated async target has already completed the only mutating step
	// (the POST). Reclaiming it merely reconnects the read-only event watcher,
	// so it is safe and desirable on the next sweep after a disconnect.
	if asyncResultPending(r) {
		return true
	}
	if r.UpdatedAt.IsZero() {
		return true
	}
	return now.Sub(r.UpdatedAt) >= window
}

func asyncResultPending(r *IngressReceipt) bool {
	for _, td := range r.Targets {
		if td.Status == companyTargetPending && td.RequestID != "" {
			return true
		}
	}
	return false
}

// guardHeldPending reports whether any of a receipt's targets is pending behind
// a session-existence guard hold (session_missing / session_ambiguous). Such a
// receipt sits in status routing but is not being actively worked — the last
// worker held it for the sweep to re-check on the 60s cadence.
func guardHeldPending(r *IngressReceipt) bool {
	for _, td := range r.Targets {
		if td.Status != companyTargetPending {
			continue
		}
		if td.Detail == companyDetailSessionMissing || td.Detail == companyDetailSessionAmbiguous ||
			td.Detail == companyDetailMaterializing {
			return true
		}
	}
	return false
}

// reloadOnSIGHUP stages/commits both company stores independently of the
// six-registry atomic set. A stale or invalid file retains its own
// last-known-good snapshot (handled inside StageReload) and never blocks
// the six. Directory is reloaded first because bindings validity depends
// on it. A nil gateway is a no-op.
func (g *companyGateway) reloadOnSIGHUP() {
	if g == nil {
		return
	}
	_ = g.dirStore.StageReload(g.cfg.companyDirectoryPath)
	_ = g.bindStore.StageReload(g.cfg.companyBindingsPath, g.dirStore.Snapshot())
	// Phase 4 registries reload on the same SIGHUP, after the directory (both
	// validate against its snapshot). Each keeps its own last-known-good on a
	// bad file (StageReload contract).
	if g.dmBindStore != nil {
		_ = g.dmBindStore.StageReload(g.cfg.companyDMBindingsPath, g.dirStore.Snapshot())
	}
	if g.agentApps != nil {
		_ = g.agentApps.StageReload(g.cfg.companyAgentAppsPath, g.dirStore.Snapshot())
	}
	log.Printf("company registries reloaded: directory_loaded=%v bindings_loaded=%v dm_bindings_loaded=%v agent_apps=%d",
		g.dirStore.Snapshot() != nil, g.bindStore.Snapshot() != nil,
		g.dmBindStore.Snapshot() != nil, g.agentApps.Snapshot().Len())
}

// healthzDetail returns the company status lines appended to /healthz: the
// barrier state, receipt-store readiness plus its last construction error
// (the degraded-mode paging hook), the write-failure counter, the
// delivery-failure counter, and directory/bindings snapshot state. This is
// the sole status surface for the company path — there is no separate
// gateway status payload endpoint in this pack.
func (g *companyGateway) healthzDetail() string {
	if g == nil {
		return ""
	}
	store := g.store()
	storeReady := store != nil
	var writeFailures uint64
	var bodyDigestMismatch uint64
	if store != nil {
		writeFailures = store.WriteFailures()
		bodyDigestMismatch = store.BodyDigestMismatches()
	}
	storeErr := ""
	if p := g.storeErr.Load(); p != nil {
		storeErr = *p
	}
	var dm dmFamilyReceiptCounts
	if p := g.dmStatusCounts.Load(); p != nil {
		dm = *p
	}
	var body bodyIntegrityCounts
	if p := g.bodyIntegrity.Load(); p != nil {
		body = *p
	}
	// registered_agent_apps count + directory-join warnings (Phase 4). Warnings
	// are recomputed against the live directory snapshot each call so an
	// operator sees them clear once the directory is fixed (no restart needed).
	apps := g.agentApps.Snapshot()
	joinWarnings := apps.JoinWarnings(g.dirStore.Snapshot())
	return fmt.Sprintf(
		"company_barrier_ready=%v\ncompany_store_ready=%v\ncompany_store_error=%q\ncompany_write_failures=%d\ncompany_delivery_failures=%d\ncompany_target_reresolved=%d\ncompany_materialize_requests=%d\ncompany_boot_requests=%d\ncompany_delivered_asleep=%d\ncompany_roster_drift_wakes=%d\ncompany_stale_posting_intents=%d\ncompany_correlation_parked=%d\ncompany_directory_loaded=%v\ncompany_bindings_loaded=%v\ncompany_dm_bindings_loaded=%v\nregistered_agent_apps=%d\nregistered_agent_apps_join_warnings=%d\ncompany_dm_sig_reject=%d\ncompany_dm_token_missing=%d\ncompany_dm_receipts_received=%d\ncompany_dm_receipts_routing=%d\ncompany_dm_receipts_delivered=%d\ncompany_dm_receipts_no_delivery=%d\ncompany_dm_receipts_failed=%d\ncompany_mpim_receipts_received=%d\ncompany_mpim_receipts_routing=%d\ncompany_mpim_receipts_delivered=%d\ncompany_mpim_receipts_no_delivery=%d\ncompany_mpim_receipts_failed=%d\ncompany_body_missing=%d\ncompany_bodies_redacted=%d\ncompany_body_digest_mismatch=%d\n",
		g.barrier.Load(),
		storeReady,
		storeErr,
		writeFailures,
		g.deliveryFailures.Load(),
		g.targetReresolved.Load(),
		g.materializeRequests.Load(),
		g.bootRequests.Load(),
		g.deliveredAsleep.Load(),
		g.rosterDriftWakes.Load(),
		g.stalePostingIntents.Load(),
		g.companyCorrelationParked.Load(),
		g.dirStore.Snapshot() != nil,
		g.bindStore.Snapshot() != nil,
		g.dmBindStore.Snapshot() != nil,
		apps.Len(),
		len(joinWarnings),
		g.dmSigRejects.Load(),
		g.dmTokenMissing.Load(),
		dm.DM.Received,
		dm.DM.Routing,
		dm.DM.Delivered,
		dm.DM.NoDelivery,
		dm.DM.Failed,
		dm.Mpim.Received,
		dm.Mpim.Routing,
		dm.Mpim.Delivered,
		dm.Mpim.NoDelivery,
		dm.Mpim.Failed,
		body.Missing,
		body.Redacted,
		bodyDigestMismatch,
	)
}
