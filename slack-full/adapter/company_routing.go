package main

import (
	"encoding/json"
	"regexp"
)

// AuthorClass is the fail-closed classification of a company-room message's
// author. Only human authors can wake sessions in Phase 1; every bot class
// is retained so the reason a message delivered nothing is machine
// readable and so the Phase 2 company-bot mention leg can turn on without
// changing the classifier.
type AuthorClass int

const (
	AuthorHuman      AuthorClass = iota // has a user id, no bot id, allowlisted subtype
	AuthorCompanyBot                    // bot resolving to a registered agent (single-agent resolution lands in Phase 2)
	AuthorBot                           // any other bot / webhook / integration author
	AuthorSelf                          // the switchboard's own bot user
)

func (c AuthorClass) String() string {
	switch c {
	case AuthorHuman:
		return "human"
	case AuthorCompanyBot:
		return "company_bot"
	case AuthorBot:
		return "bot"
	case AuthorSelf:
		return "self"
	default:
		return "unknown"
	}
}

// Wake kinds recorded on each WakeTarget and mirrored into the receipt's
// per-target delivery record.
const (
	wakeKindAmbient        = "ambient"
	wakeKindThreadAmbient  = "thread_ambient"
	wakeKindTargeted       = "targeted"
	wakeKindPeerDelegation = "peer_delegation"
	wakeKindPeerResult     = "peer_result"
	// wakeKindPeerInput is an uncorrelated company-bot wake: peer chatter
	// without gc metadata, an unmatched identifiable reply, or a
	// gate-failed / clarifying / out-of-window post. It carries NO
	// delegation_key. peer_delegation and peer_result ALWAYS carry a
	// non-empty delegation_key; peer_input never does.
	wakeKindPeerInput = "peer_input"
	// wakeKindDM is the single wake on an admitted per-agent DM (Phase 4):
	// an allowed human's direct message to an agent app, delivered to that
	// agent's singleton DM-bound session. No mention/ambient/delegation
	// semantics — one owner, one target. Shares the "dm" literal with the
	// receipt kind and the current-turn pointer kind.
	wakeKindDM = receiptKindDM
)

// Machine-readable no-delivery reasons. An empty Reason on a RouteDecision
// with a non-empty Wakes set means "delivered normally".
const (
	wakeReasonNoDirectory          = "no_directory"           // no directory loaded; caller falls through to legacy
	wakeReasonNotCompanyRoom       = "not_company_room"       // channel is not an imported company room
	wakeReasonSubtypeNotAdmissible = "subtype_not_admissible" // defense-in-depth subtype gate
	wakeReasonCompanySelf          = "company_self"           // switchboard's own post
	wakeReasonCompanyBotNoMention  = "company_bot_no_mention" // registered company bot with no native company mention
	wakeReasonCompanyBotNotMember  = "company_bot_not_member" // resolved company bot is not a member of this room
	wakeReasonUnknownBot           = "unknown_bot"            // unknown bot / webhook / integration
	wakeReasonNoAmbientMembers     = "no_ambient_members"     // unmentioned human, room has no ambient wake set
	wakeReasonMentionedNoEligible  = "mentioned_no_eligible"  // mentioned agents are not member+eligible
	// DM no-delivery reasons (Phase 4). A DM that wakes nobody carries one
	// of these on the terminal receipt so the denial is machine-readable
	// (policy denials must never be silent drops).
	wakeReasonDMSelfEcho         = "dm_self_echo"          // the owner app's own outbound post, echoed back
	wakeReasonDMAuthorNotAllowed = "dm_author_not_allowed" // human author denied by the DM allowlist / team check
	wakeReasonDMOwnerUnknown     = "dm_owner_unknown"      // owner app no longer joins a directory agent
	wakeReasonDMNotMember        = "dm_membership_denied"  // owner app cannot access the DM conversation (foreign human↔human DM)
	wakeReasonDMMemberUnknown    = "dm_membership_unknown" // membership probe could not answer (missing token / transport error)
	// wakeReasonDMAppUnregistered parks (non-terminal, sweep-recoverable) a DM
	// receipt whose agent-apps registry is unavailable at routing (nil snapshot,
	// or the owner record missing) — a transient infra/reload failure, not a
	// policy answer. Distinct from dm_author_not_allowed, which is the terminal
	// denial reserved for a LIVE registry that answered the policy question.
	wakeReasonDMAppUnregistered = "dm_app_unregistered"
)

// AdmissibleSubtype reports whether a Slack message subtype is admissible:
// "" (absent), "file_share", "thread_broadcast", or "bot_message".
// Everything else (channel_join, channel_topic, hidden edit/delete records,
// etc.) is not admitted.
func AdmissibleSubtype(subtype string) bool {
	switch subtype {
	case "", "file_share", "thread_broadcast", "bot_message":
		return true
	default:
		return false
	}
}

// CompanyMessage is the minimal decoded inner Slack event the router needs.
// Blocks is the raw `blocks` array so the mention extractor can walk
// rich_text without the router committing to Slack's full block schema.
type CompanyMessage struct {
	TeamID, ChannelID, TS, ThreadTS string
	UserID, BotID                   string
	Subtype                         string
	Text                            string
	Blocks                          json.RawMessage
	// AppID / BotProfileAppID are the event's self-declared app identifiers,
	// used only to corroborate a bots.info resolution (never as the trust
	// anchor). Populated from the raw event by the delivery worker.
	AppID           string
	BotProfileAppID string
	// ResolvedBotUserID is the authoritative bots.info -> user_id resolution
	// for a bot author, populated by the delivery worker so ComputeWakeSet
	// stays pure. Empty means "not (yet) resolved to a company agent".
	ResolvedBotUserID string
	// Metadata is the raw Slack message metadata object (event_type +
	// event_payload) carried on delegation / result posts; the correlation
	// layer (company_peer.go) reads it for claim admission.
	Metadata json.RawMessage
	// Files is the message's attached file objects (subtype file_share). The
	// hydrator reads them to render a files section and inline snippet content
	// into the frozen reminder; empty for the common text-only message so its
	// reminder bytes are unchanged.
	Files []slackFile
}

// WakeTarget names one agent to wake and why (ambient vs targeted).
type WakeTarget struct {
	Agent CompanyAgent
	Kind  string // wakeKindAmbient | wakeKindThreadAmbient | wakeKindTargeted | peer kinds
	// Overlay marks a target admitted by the delivery worker's live-membership
	// eligibility overlay (a stale-roster mention of a genuine live member). The
	// pure router NEVER sets it — it is stamped only in company_delivery.go — and
	// it drives the dm_bindings session fallback in ensureTargets (an overlay
	// agent has no room binding by construction).
	Overlay bool
}

// RouteDecision is the pure routing result. Room == nil means the channel
// is not a company room and the caller must fall through to the legacy
// path. A non-nil Room with an empty Wakes set means the company path
// admitted the message but woke nobody; Reason says why.
type RouteDecision struct {
	Room   *CompanyRoom
	Author AuthorClass
	Wakes  []WakeTarget
	Reason string
	// OverlayCandidates lists the mentioned directory agents that were excluded
	// from the wake set ONLY by roster membership/mention_wake, populated solely
	// on the AuthorHuman mentioned_no_eligible path (empty on every other
	// decision). It is the exact candidate set the delivery worker's
	// live-membership eligibility overlay may admit as targeted wakes when a
	// candidate's bot is a genuine live channel member — never persisted, never
	// broadening ambient, never a non-directory identity.
	OverlayCandidates []CompanyAgent
}

// companyMentionTokenRE matches a canonical Slack user mention token in
// top-level text: <@U012ABC> or <@U012ABC|display label>. Subteam
// (<!subteam^…>), channel (<#C…>), and special (<!here>) tokens are
// intentionally not matched — only user mentions can name a company agent.
var companyMentionTokenRE = regexp.MustCompile(`<@([A-Z0-9]+)(?:\|[^>]*)?>`)

// richTextElement is one node in a Slack rich_text block. Mentions appear
// as {"type":"user","user_id":"U…"} leaves; container nodes
// (rich_text_section, rich_text_list, …) carry nested Elements.
type richTextElement struct {
	Type     string            `json:"type"`
	UserID   string            `json:"user_id"`
	Elements []richTextElement `json:"elements"`
}

// richTextBlock is a top-level message block; only "rich_text" blocks carry
// user mention leaves.
type richTextBlock struct {
	Type     string            `json:"type"`
	Elements []richTextElement `json:"elements"`
}

// ExtractMentionIDs returns the union of user IDs found as rich_text "user"
// elements in blocks and as canonical <@U…> / <@U…|label> tokens in text,
// deduplicated with block-order-then-text-order preserved. Neither source
// alone is sufficient: Slack guarantees rich_text blocks only for end-user
// client messages, while bot-composed messages may carry mentions only as
// text tokens. Parsing is defensive — malformed blocks contribute nothing
// rather than erroring, and plain text that merely resembles "@name"
// carries no canonical token and matches nobody.
func ExtractMentionIDs(blocks json.RawMessage, text string) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, id := range mentionIDsFromBlocks(blocks) {
		add(id)
	}
	for _, m := range companyMentionTokenRE.FindAllStringSubmatch(text, -1) {
		add(m[1])
	}
	return out
}

func mentionIDsFromBlocks(blocks json.RawMessage) []string {
	if len(blocks) == 0 {
		return nil
	}
	var parsed []richTextBlock
	if err := json.Unmarshal(blocks, &parsed); err != nil {
		return nil // defensive: unparseable blocks contribute no mentions
	}
	var out []string
	var walk func(els []richTextElement)
	walk = func(els []richTextElement) {
		for i := range els {
			e := els[i]
			if e.Type == "user" && e.UserID != "" {
				out = append(out, e.UserID)
			}
			if len(e.Elements) > 0 {
				walk(e.Elements)
			}
		}
	}
	for i := range parsed {
		if parsed[i].Type == "rich_text" {
			walk(parsed[i].Elements)
		}
	}
	return out
}

// ComputeWakeSet is the pure routing core (no I/O). It implements the
// design doc's routing table fail-closed:
//
//   - non-company channel                -> Room == nil (fall through to legacy)
//   - non-allowlisted subtype            -> no wakes (defense in depth)
//   - switchboard self / any bot author  -> no wakes in Phase 1, reason recorded
//   - human, no native company mention   -> the room's ambient_wake members
//   - human, one+ native company mention -> only the mentioned member+eligible
//     agents (ambient suppressed; mentions are exclusive)
//
// Native mentions are matched exactly against directory bot_user_ids;
// a matched id still passes membership and mention-eligibility before it
// wakes anyone, and each named receiver wakes at most once.
func ComputeWakeSet(dir *CompanyDirectory, msg CompanyMessage, selfBotUserID string) RouteDecision {
	if dir == nil {
		return RouteDecision{Reason: wakeReasonNoDirectory}
	}
	room, ok := dir.RoomByChannel(msg.TeamID, msg.ChannelID)
	if !ok {
		return RouteDecision{Reason: wakeReasonNotCompanyRoom}
	}
	dec := RouteDecision{Room: room}

	// Defense in depth: the HTTP admission gate already dropped
	// non-allowlisted subtypes, but re-check so this function can never
	// wake anyone on one.
	if !AdmissibleSubtype(msg.Subtype) {
		dec.Reason = wakeReasonSubtypeNotAdmissible
		return dec
	}

	dec.Author = classifyAuthor(dir, msg, selfBotUserID)
	switch dec.Author {
	case AuthorSelf:
		dec.Reason = wakeReasonCompanySelf
		return dec
	case AuthorCompanyBot:
		// Company-bot mention leg (Phase 2c): a resolved company bot that is
		// a member of this room wakes each eligible mentioned member (itself
		// excluded). The refinement of the wake kind (peer_delegation vs
		// peer_result) and the delegation-record claim happen in the delivery
		// worker; here every bot-authored wake is a peer_delegation.
		author, _ := dir.AgentByBotUserID(msg.ResolvedBotUserID)
		if author == nil || !dir.IsMember(room, author.Name) {
			dec.Reason = wakeReasonCompanyBotNotMember
			return dec
		}
		companyMentions := companyMentionAgents(dir, msg)
		if len(companyMentions) == 0 {
			dec.Reason = wakeReasonCompanyBotNoMention
			return dec
		}
		seen := make(map[string]bool)
		for _, a := range companyMentions {
			if a.BotUserID == author.BotUserID {
				continue // self-exclusion: a bot never wakes itself
			}
			if !dir.IsMember(room, a.Name) || !dir.IsMentionEligible(room, a.Name) {
				continue
			}
			if seen[a.BotUserID] {
				continue
			}
			seen[a.BotUserID] = true
			dec.Wakes = append(dec.Wakes, WakeTarget{Agent: *a, Kind: wakeKindPeerDelegation})
		}
		if len(dec.Wakes) == 0 {
			dec.Reason = wakeReasonMentionedNoEligible
		}
		return dec
	case AuthorBot:
		dec.Reason = wakeReasonUnknownBot
		return dec
	}

	// AuthorHuman.
	companyMentions := companyMentionAgents(dir, msg)

	if len(companyMentions) > 0 {
		// Exclusive mention routing: ambient is suppressed entirely, even
		// when no mentioned agent turns out member+eligible.
		seen := make(map[string]bool)
		var driftCandidates []CompanyAgent
		candidateSeen := make(map[string]bool)
		for _, a := range companyMentions {
			if !dir.IsMember(room, a.Name) || !dir.IsMentionEligible(room, a.Name) {
				// Excluded ONLY by roster membership/mention_wake — and because
				// companyMentions is already directory-resolved, this is exactly
				// the candidate set the delivery worker's live-membership overlay
				// may admit when the roster has drifted from live channel state.
				if !candidateSeen[a.BotUserID] {
					candidateSeen[a.BotUserID] = true
					driftCandidates = append(driftCandidates, *a)
				}
				continue
			}
			if seen[a.BotUserID] {
				continue
			}
			seen[a.BotUserID] = true
			dec.Wakes = append(dec.Wakes, WakeTarget{Agent: *a, Kind: wakeKindTargeted})
		}
		if len(dec.Wakes) == 0 {
			// Terminal mentioned_no_eligible — surface the drift candidates so the
			// delivery worker can verify live membership before terminalizing.
			dec.Reason = wakeReasonMentionedNoEligible
			dec.OverlayCandidates = driftCandidates
		}
		return dec
	}

	// Ambient routing: an unmentioned human message wakes the room's
	// ambient set (validated ⊆ members at parse time).
	seen := make(map[string]bool)
	for _, name := range room.AmbientWake {
		a, ok := dir.AgentByName(name)
		if !ok {
			continue
		}
		if seen[a.BotUserID] {
			continue
		}
		seen[a.BotUserID] = true
		dec.Wakes = append(dec.Wakes, WakeTarget{Agent: *a, Kind: wakeKindAmbient})
	}
	if len(dec.Wakes) == 0 {
		dec.Reason = wakeReasonNoAmbientMembers
	}
	return dec
}

// companyMentionAgents returns the directory agents named by the message's
// native mention set (union of rich_text user leaves and canonical text
// tokens), in mention order.
func companyMentionAgents(dir *CompanyDirectory, msg CompanyMessage) []*CompanyAgent {
	var out []*CompanyAgent
	for _, id := range ExtractMentionIDs(msg.Blocks, msg.Text) {
		if a, ok := dir.AgentByBotUserID(id); ok {
			out = append(out, a)
		}
	}
	return out
}

// classifyAuthor applies the fail-closed author classification. Self is
// checked first (the switchboard's own bot user id, matched against either
// the raw or resolved author id); then any bot author (bot_message subtype or
// a non-empty bot id) is a registered company bot ONLY when its authoritative
// bots.info resolution (ResolvedBotUserID) maps to a directory agent —
// otherwise an unknown bot. A message with neither a user nor a bot identity is
// malformed and treated as a bot (no delivery).
//
// A bot author is NEVER classified company-bot from the raw event `user`
// fallback: during delivery resolveCompanyAuthor runs first, so a definitive
// unknown / corroboration mismatch leaves ResolvedBotUserID empty, and this
// function must fail closed to unknown rather than re-open the bot on a raw id
// that merely happens to equal a registered agent's bot_user_id.
func classifyAuthor(dir *CompanyDirectory, msg CompanyMessage, selfBotUserID string) AuthorClass {
	if selfBotUserID != "" && (msg.UserID == selfBotUserID || msg.ResolvedBotUserID == selfBotUserID) {
		return AuthorSelf
	}
	if msg.Subtype == "bot_message" || msg.BotID != "" {
		if msg.ResolvedBotUserID != "" {
			if _, ok := dir.AgentByBotUserID(msg.ResolvedBotUserID); ok {
				return AuthorCompanyBot
			}
		}
		return AuthorBot
	}
	if msg.UserID == "" {
		return AuthorBot
	}
	return AuthorHuman
}
