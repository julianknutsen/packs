package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

// --- request bodies (one per outbound verb wrapper) -----------------------

type publishRequest struct {
	SessionID      string `json:"session_id"`
	Body           string `json:"body"`
	ReplyTo        string `json:"reply_to,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type publishToChannelRequest struct {
	SessionID      string `json:"session_id,omitempty"`
	ChannelID      string `json:"channel_id"`
	ThreadTS       string `json:"thread_ts,omitempty"`
	Body           string `json:"body"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type replyCurrentRequest struct {
	SessionID string `json:"session_id"`
	Body      string `json:"body"`
	ReplyTo   string `json:"reply_to,omitempty"`
	// ConversationID optionally asserts which conversation the caller
	// believes it is replying to. reply-current always targets the session's
	// latest inbound, so this is redundant by construction — but slack-full
	// and discord both accept the flag, and gc's injected reply instruction
	// passes it, so rejecting it stranded Tier 2 agents with a command that
	// looked right and failed. Validated rather than ignored: a mismatch
	// means the agent is replying to a conversation it no longer owns.
	ConversationID string `json:"conversation_id,omitempty"`
	ThreadCurrent  bool   `json:"thread_current,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type reactRequest struct {
	SessionID      string `json:"session_id,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	MessageID      string `json:"message_id,omitempty"`
	Emoji          string `json:"emoji"`
}

// applyIdentity injects a session's registered username/avatar override
// into a chat.postMessage request. It returns the applied username (for the
// response/log), or "" when the session has no identity override.
func (s *server) applyIdentity(req *slackPostMessageReq, sessionID string) string {
	if sessionID == "" {
		return ""
	}
	id, ok := s.identityFor(sessionID)
	if !ok {
		return ""
	}
	req.Username = id.Username
	req.IconURL = id.IconURL
	req.IconEmoji = id.IconEmoji
	return id.Username
}

// post sends a chat.postMessage and writes a uniform receipt. channel and
// text are required by the caller; identity is applied from sessionID.
//
// idempotencyKey makes the post idempotent: if the same key already produced
// a delivered receipt within the dedup window, the original receipt is
// replayed without a second Slack POST — the chokepoint that absorbs a retry
// after a delivered-but-timed-out POST (gpk-bm3f). An empty key disables
// dedup for the call.
func (s *server) post(w http.ResponseWriter, r *http.Request, sessionID, channel, text, threadTS, idempotencyKey string) {
	if cached, ok := s.dedup.Get(idempotencyKey); ok {
		log.Printf("post: dedup hit idem=%s channel=%s -> returning cached receipt (no re-post)", idempotencyKey, channel)
		writeJSON(w, http.StatusOK, cached)
		return
	}
	req := slackPostMessageReq{Channel: channel, Text: text, ThreadTS: threadTS}
	identityApplied := s.applyIdentity(&req, sessionID)
	resp, err := s.postToSlack(r.Context(), req)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}
	if !resp.OK {
		writeJSONError(w, http.StatusBadGateway, "slack: "+resp.Error)
		return
	}
	receipt := postReceipt{OK: true, TS: resp.TS, Channel: resp.Channel, IdentityApplied: identityApplied}
	// Remember delivered receipts so a retry with the same key replays this
	// receipt instead of re-posting. Put ignores empty keys and failures.
	s.dedup.Put(idempotencyKey, receipt)
	writeJSON(w, http.StatusOK, receipt)
}

// handlePublish posts into the single channel a session is bound to. A
// session bound to zero channels (run bind-dm/bind-room first) or to more
// than one (ambiguous — use publish-to-channel) is a fail-fast error.
func (s *server) handlePublish() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req publishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("decode: %v", err))
			return
		}
		if strings.TrimSpace(req.SessionID) == "" {
			writeJSONError(w, http.StatusBadRequest, "session_id is required")
			return
		}
		if strings.TrimSpace(req.Body) == "" {
			writeJSONError(w, http.StatusBadRequest, "body is required")
			return
		}
		channels := s.channelsForSession(req.SessionID)
		switch {
		case len(channels) == 0:
			writeJSONError(w, http.StatusBadRequest,
				fmt.Sprintf("session %q has no channel binding; run bind-dm or bind-room first", req.SessionID))
			return
		case len(channels) > 1:
			writeJSONError(w, http.StatusBadRequest,
				fmt.Sprintf("session %q is bound to multiple channels (%s); use publish-to-channel --channel to disambiguate",
					req.SessionID, strings.Join(channels, ", ")))
			return
		}
		log.Printf("publish: session=%s channel=%s reply_to=%s idem=%s", req.SessionID, channels[0], req.ReplyTo, req.IdempotencyKey)
		s.post(w, r, req.SessionID, channels[0], req.Body, req.ReplyTo, req.IdempotencyKey)
	}
}

// handlePublishToChannel posts directly to a channel by id, bypassing the
// binding lookup. The session id (if supplied) still drives the identity
// override.
func (s *server) handlePublishToChannel() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req publishToChannelRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("decode: %v", err))
			return
		}
		if strings.TrimSpace(req.ChannelID) == "" {
			writeJSONError(w, http.StatusBadRequest, "channel_id is required")
			return
		}
		if strings.TrimSpace(req.Body) == "" {
			writeJSONError(w, http.StatusBadRequest, "body is required")
			return
		}
		log.Printf("publish-to-channel: session=%s channel=%s thread_ts=%s idem=%s", req.SessionID, req.ChannelID, req.ThreadTS, req.IdempotencyKey)
		s.post(w, r, req.SessionID, req.ChannelID, req.Body, req.ThreadTS, req.IdempotencyKey)
	}
}

// handleReplyCurrent replies into the conversation of the latest inbound
// message delivered to the session. With --thread-current it threads under
// that message; with an explicit reply-to it threads under that ts; with
// neither it posts unthreaded into the same channel. Falls back to the
// session's single binding when no inbound has been seen this process.
func (s *server) handleReplyCurrent() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req replyCurrentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("decode: %v", err))
			return
		}
		if strings.TrimSpace(req.SessionID) == "" {
			writeJSONError(w, http.StatusBadRequest, "session_id is required")
			return
		}
		if strings.TrimSpace(req.Body) == "" {
			writeJSONError(w, http.StatusBadRequest, "body is required")
			return
		}
		if req.ReplyTo != "" && req.ThreadCurrent {
			writeJSONError(w, http.StatusBadRequest, "reply_to and thread_current are mutually exclusive")
			return
		}

		var channel, threadTS string
		if ref, ok := s.latestInbound(req.SessionID); ok {
			channel = ref.channelID
			switch {
			case req.ReplyTo != "":
				threadTS = req.ReplyTo
			case req.ThreadCurrent:
				threadTS = ref.threadTS
			}
		} else {
			// No inbound seen this process — fall back to the session's
			// single channel binding so a fresh adapter can still reply.
			channels := s.channelsForSession(req.SessionID)
			if len(channels) != 1 {
				writeJSONError(w, http.StatusBadRequest,
					"no recent inbound for this session and no single channel binding; use publish-to-channel")
				return
			}
			channel = channels[0]
			threadTS = req.ReplyTo // thread_current has no message to thread under here
		}
		// An asserted conversation that does not match the resolved target
		// means the session's latest inbound moved on (or the caller copied
		// an id from elsewhere). Failing here is the point: the alternative
		// is a reply delivered into the wrong channel.
		if req.ConversationID != "" && req.ConversationID != channel {
			writeJSONError(w, http.StatusConflict, fmt.Sprintf(
				"conversation_id %q does not match this session's current reply target %q",
				req.ConversationID, channel))
			return
		}
		// Derive a deterministic key when the caller supplied none, so a
		// retry of the same reply dedupes instead of double-posting after a
		// delivered-but-timed-out POST (gpk-bm3f). The channel/thread are
		// resolved above, so the fingerprint is computed here rather than
		// client-side (unlike slack-full).
		idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
		if idempotencyKey == "" {
			idempotencyKey = deriveReplyIdempotencyKey(req.SessionID, channel, threadTS, req.Body)
		}
		log.Printf("reply-current: session=%s channel=%s thread_ts=%s idem=%s", req.SessionID, channel, threadTS, idempotencyKey)
		s.post(w, r, req.SessionID, channel, req.Body, threadTS, idempotencyKey)
	}
}

// handleReact adds an emoji reaction. Explicit mode names channel+ts
// directly; otherwise it reacts on the latest inbound message delivered to
// the session. already_reacted is treated as success (the emoji is on the
// message either way).
func (s *server) handleReact() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req reactRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("decode: %v", err))
			return
		}
		emoji := strings.Trim(req.Emoji, ":")
		if emoji == "" {
			writeJSONError(w, http.StatusBadRequest, "emoji is required")
			return
		}

		explicit := req.ConversationID != "" || req.MessageID != ""
		var channel, ts string
		if explicit {
			if req.ConversationID == "" || req.MessageID == "" {
				writeJSONError(w, http.StatusBadRequest, "conversation_id and message_id must be passed together")
				return
			}
			channel, ts = req.ConversationID, req.MessageID
		} else {
			if strings.TrimSpace(req.SessionID) == "" {
				writeJSONError(w, http.StatusBadRequest, "session_id is required when conversation_id/message_id are not given")
				return
			}
			ref, ok := s.latestInbound(req.SessionID)
			if !ok {
				writeJSONError(w, http.StatusBadRequest,
					"no recent inbound for this session; pass conversation_id and message_id explicitly")
				return
			}
			channel, ts = ref.channelID, ref.messageTS
		}

		log.Printf("react: channel=%s ts=%s emoji=%s", channel, ts, emoji)
		resp, err := s.addReaction(r.Context(), channel, ts, emoji)
		if err != nil {
			writeJSONError(w, http.StatusBadGateway, err.Error())
			return
		}
		if !resp.OK && resp.Error != "already_reacted" {
			writeJSONError(w, http.StatusBadGateway, "slack: "+resp.Error)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"channel": channel,
			"ts":      ts,
			"emoji":   emoji,
		})
	}
}
