package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// slackCollector stands in for the Slack web API, recording chat.postMessage
// and reactions.add calls and returning a configurable response.
type slackCollector struct {
	srv        *httptest.Server
	mu         sync.Mutex
	posts      []slackPostMessageReq
	reactions  []slackReactionsAddReq
	postError  string // when set, returned as {ok:false,error:...}
	reactError string
}

func newSlackCollector(t *testing.T, s *server) *slackCollector {
	t.Helper()
	c := &slackCollector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/chat.postMessage"):
			var req slackPostMessageReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			c.posts = append(c.posts, req)
			resp := slackPostMessageResp{OK: c.postError == "", TS: "99.9", Channel: req.Channel, Error: c.postError}
			_ = json.NewEncoder(w).Encode(resp)
		case strings.HasSuffix(r.URL.Path, "/reactions.add"):
			var req slackReactionsAddReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			c.reactions = append(c.reactions, req)
			_ = json.NewEncoder(w).Encode(slackReactionsAddResp{OK: c.reactError == "", Error: c.reactError})
		default:
			t.Errorf("unexpected slack path %s", r.URL.Path)
		}
	}))
	t.Cleanup(c.srv.Close)
	s.cfg.slackAPIBase = c.srv.URL
	return c
}

func doJSON(h http.HandlerFunc, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func TestHandlePublish(t *testing.T) {
	srv := newTestServer(t)
	slack := newSlackCollector(t, srv)
	mustBind(t, srv, "C1", "room", "s1")
	if _, err := srv.upsertIdentity("s1", "Gas City PL", "", "robot_face"); err != nil {
		t.Fatal(err)
	}

	rec := doJSON(srv.handlePublish(), `{"session_id":"s1","body":"build green","reply_to":"50.0"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(slack.posts) != 1 {
		t.Fatalf("want 1 slack post, got %d", len(slack.posts))
	}
	post := slack.posts[0]
	if post.Channel != "C1" || post.Text != "build green" || post.ThreadTS != "50.0" {
		t.Errorf("post = %+v", post)
	}
	if post.Username != "Gas City PL" || post.IconEmoji != "robot_face" {
		t.Errorf("identity not injected: %+v", post)
	}
}

func TestHandlePublishNoBinding(t *testing.T) {
	srv := newTestServer(t)
	newSlackCollector(t, srv)
	rec := doJSON(srv.handlePublish(), `{"session_id":"s1","body":"hi"}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "no channel binding") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandlePublishAmbiguous(t *testing.T) {
	srv := newTestServer(t)
	newSlackCollector(t, srv)
	mustBind(t, srv, "C1", "room", "s1")
	mustBind(t, srv, "C2", "room", "s1")
	rec := doJSON(srv.handlePublish(), `{"session_id":"s1","body":"hi"}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "multiple channels") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandlePublishValidation(t *testing.T) {
	srv := newTestServer(t)
	newSlackCollector(t, srv)
	for name, body := range map[string]string{
		"no session": `{"body":"hi"}`,
		"no body":    `{"session_id":"s1"}`,
		"bad json":   `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if rec := doJSON(srv.handlePublish(), body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d", rec.Code)
			}
		})
	}
}

func TestHandlePublishToChannel(t *testing.T) {
	srv := newTestServer(t)
	slack := newSlackCollector(t, srv)
	rec := doJSON(srv.handlePublishToChannel(), `{"channel_id":"C9","body":"ping","thread_ts":"1.2"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if slack.posts[0].Channel != "C9" || slack.posts[0].ThreadTS != "1.2" {
		t.Errorf("post = %+v", slack.posts[0])
	}
}

func TestHandlePublishToChannelValidation(t *testing.T) {
	srv := newTestServer(t)
	newSlackCollector(t, srv)
	if rec := doJSON(srv.handlePublishToChannel(), `{"body":"hi"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing channel: status=%d", rec.Code)
	}
}

func TestHandleReplyCurrent(t *testing.T) {
	srv := newTestServer(t)
	slack := newSlackCollector(t, srv)
	srv.recordInbound("s1", inboundRef{channelID: "C1", messageTS: "70.1", threadTS: "70.0"})

	t.Run("thread-current threads under root", func(t *testing.T) {
		rec := doJSON(srv.handleReplyCurrent(), `{"session_id":"s1","body":"on it","thread_current":true}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		last := slack.posts[len(slack.posts)-1]
		if last.Channel != "C1" || last.ThreadTS != "70.0" {
			t.Errorf("post = %+v", last)
		}
	})

	t.Run("unthreaded by default", func(t *testing.T) {
		rec := doJSON(srv.handleReplyCurrent(), `{"session_id":"s1","body":"fyi"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d", rec.Code)
		}
		if last := slack.posts[len(slack.posts)-1]; last.ThreadTS != "" {
			t.Errorf("expected unthreaded, got thread_ts=%q", last.ThreadTS)
		}
	})

	t.Run("reply_to and thread_current conflict", func(t *testing.T) {
		rec := doJSON(srv.handleReplyCurrent(), `{"session_id":"s1","body":"x","reply_to":"1","thread_current":true}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status=%d", rec.Code)
		}
	})

	// gc's injected reply instruction passes --conversation-id, and both
	// slack-full and discord accept it. Tier 2 rejecting it left agents with
	// a command that looked correct and failed.
	t.Run("matching conversation_id is accepted", func(t *testing.T) {
		rec := doJSON(srv.handleReplyCurrent(), `{"session_id":"s1","body":"ack","conversation_id":"C1"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if last := slack.posts[len(slack.posts)-1]; last.Channel != "C1" {
			t.Errorf("post channel = %q, want C1", last.Channel)
		}
	})

	t.Run("mismatched conversation_id is refused", func(t *testing.T) {
		before := len(slack.posts)
		rec := doJSON(srv.handleReplyCurrent(), `{"session_id":"s1","body":"oops","conversation_id":"C_OTHER"}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d, want 409 body=%s", rec.Code, rec.Body.String())
		}
		// The whole point: a stale assertion must not post anywhere.
		if len(slack.posts) != before {
			t.Errorf("posted %d message(s) despite a conversation mismatch", len(slack.posts)-before)
		}
	})
}

func TestHandleReplyCurrentFallbackToBinding(t *testing.T) {
	srv := newTestServer(t)
	slack := newSlackCollector(t, srv)
	mustBind(t, srv, "C7", "dm", "s1") // no recorded inbound

	rec := doJSON(srv.handleReplyCurrent(), `{"session_id":"s1","body":"hello"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if slack.posts[0].Channel != "C7" {
		t.Errorf("fallback channel = %q, want C7", slack.posts[0].Channel)
	}
}

func TestHandleReplyCurrentNoTarget(t *testing.T) {
	srv := newTestServer(t)
	newSlackCollector(t, srv)
	rec := doJSON(srv.handleReplyCurrent(), `{"session_id":"ghost","body":"hello"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleReact(t *testing.T) {
	srv := newTestServer(t)
	slack := newSlackCollector(t, srv)
	srv.recordInbound("s1", inboundRef{channelID: "C1", messageTS: "70.1", threadTS: "70.0"})

	t.Run("current", func(t *testing.T) {
		rec := doJSON(srv.handleReact(), `{"session_id":"s1","emoji":":eyes:"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		r := slack.reactions[len(slack.reactions)-1]
		if r.Channel != "C1" || r.Timestamp != "70.1" || r.Name != "eyes" {
			t.Errorf("reaction = %+v", r)
		}
	})

	t.Run("explicit", func(t *testing.T) {
		rec := doJSON(srv.handleReact(), `{"conversation_id":"C2","message_id":"5.5","emoji":"tada"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d", rec.Code)
		}
		r := slack.reactions[len(slack.reactions)-1]
		if r.Channel != "C2" || r.Timestamp != "5.5" || r.Name != "tada" {
			t.Errorf("reaction = %+v", r)
		}
	})
}

func TestHandleReactValidation(t *testing.T) {
	srv := newTestServer(t)
	newSlackCollector(t, srv)
	cases := map[string]string{
		"no emoji":             `{"session_id":"s1"}`,
		"explicit half":        `{"conversation_id":"C1","emoji":"eyes"}`,
		"no session no target": `{"emoji":"eyes"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if rec := doJSON(srv.handleReact(), body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandleReactAlreadyReacted(t *testing.T) {
	srv := newTestServer(t)
	slack := newSlackCollector(t, srv)
	slack.reactError = "already_reacted"
	rec := doJSON(srv.handleReact(), `{"conversation_id":"C2","message_id":"5.5","emoji":"eyes"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("already_reacted should be success, got status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestOutboundHandlersRejectBadJSON(t *testing.T) {
	srv := newTestServer(t)
	newSlackCollector(t, srv)
	handlers := map[string]http.HandlerFunc{
		"publish":            srv.handlePublish(),
		"publish-to-channel": srv.handlePublishToChannel(),
		"reply-current":      srv.handleReplyCurrent(),
		"react":              srv.handleReact(),
	}
	for name, h := range handlers {
		t.Run(name, func(t *testing.T) {
			if rec := doJSON(h, `{bad`); rec.Code != http.StatusBadRequest {
				t.Fatalf("%s: status=%d, want 400", name, rec.Code)
			}
		})
	}
}

func TestOutboundSlackUnreachable(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.slackAPIBase = "http://127.0.0.1:1" // connection refused
	mustBind(t, srv, "C1", "room", "s1")
	srv.recordInbound("s1", inboundRef{channelID: "C1", messageTS: "70.1", threadTS: "70.0"})

	if rec := doJSON(srv.handlePublish(), `{"session_id":"s1","body":"hi"}`); rec.Code != http.StatusBadGateway {
		t.Errorf("publish unreachable status = %d, want 502", rec.Code)
	}
	if rec := doJSON(srv.handleReact(), `{"session_id":"s1","emoji":"eyes"}`); rec.Code != http.StatusBadGateway {
		t.Errorf("react unreachable status = %d, want 502", rec.Code)
	}
}

func TestHandlePublishSlackError(t *testing.T) {
	srv := newTestServer(t)
	slack := newSlackCollector(t, srv)
	slack.postError = "channel_not_found"
	mustBind(t, srv, "C1", "room", "s1")
	rec := doJSON(srv.handlePublish(), `{"session_id":"s1","body":"hi"}`)
	if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), "channel_not_found") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
