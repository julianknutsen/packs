package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestReadBackPublishedMessageFromChannelHistory(t *testing.T) {
	newSlackReadbackServer(t, "/conversations.history", func(query url.Values) {
		assertSlackReadbackQuery(t, query, "C123", "1700.001", "1")
	}, `{"ok":true,"messages":[{"ts":"1700.001","text":"exact body"}]}`)

	if err := readBackPublishedMessage(
		nil, "xoxb-test", "C123", "1700.001", "", "exact body",
	); err != nil {
		t.Fatalf("readBackPublishedMessage: %v", err)
	}
}

func TestReadBackPublishedThreadReplyUsesConversationReplies(t *testing.T) {
	server := newSlackReadbackServer(t, "/conversations.replies", func(query url.Values) {
		assertSlackReadbackQuery(t, query, "C123", "1700.002", "2")
		if got := query.Get("ts"); got != "1699.001" {
			t.Errorf("thread ts = %q, want 1699.001", got)
		}
	}, `{"ok":true,"messages":[{"ts":"1699.001","text":"parent"},{"ts":"1700.002","thread_ts":"1699.001","text":"reply"}]}`)

	if err := readBackPublishedMessage(
		server.Client(), "xoxb-test", "C123", "1700.002", "1699.001", "reply",
	); err != nil {
		t.Fatalf("readBackPublishedMessage: %v", err)
	}
}

func TestReadBackPublishedMessageFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		response string
		want     string
	}{
		{name: "message absent", status: http.StatusOK, response: `{"ok":true,"messages":[]}`, want: "not present"},
		{name: "content mismatch", status: http.StatusOK, response: `{"ok":true,"messages":[{"ts":"1700.001","text":"mutated"}]}`, want: "content mismatch"},
		{name: "Slack rejection", status: http.StatusOK, response: `{"ok":false,"error":"missing_scope"}`, want: "missing_scope"},
		{name: "transport status", status: http.StatusTooManyRequests, response: `rate limited`, want: "HTTP 429"},
		{name: "malformed response", status: http.StatusOK, response: `{`, want: "decode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.response))
			}))
			t.Cleanup(server.Close)

			originalBase := slackAPIBase
			slackAPIBase = server.URL
			t.Cleanup(func() { slackAPIBase = originalBase })

			err := readBackPublishedMessage(
				server.Client(), "xoxb-test", "C123", "1700.001", "", "exact body",
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestReadBackPublishedMessageBoundsAndClassifiesInstrumentFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", slackReadbackResponseLimit+1)))
	}))
	t.Cleanup(server.Close)
	originalBase := slackAPIBase
	slackAPIBase = server.URL
	t.Cleanup(func() { slackAPIBase = originalBase })

	err := readBackPublishedMessage(
		server.Client(), "xoxb-test", "C123", "1700.001", "", "exact body",
	)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversize error = %v, want bounded-response failure", err)
	}
	if got := slackReadbackFailureKind(err); got != slackReadbackUnavailable {
		t.Fatalf("failure kind = %q, want %q", got, slackReadbackUnavailable)
	}

	transportErr := errors.New("transport unavailable")
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	})}
	err = readBackPublishedMessage(
		client, "xoxb-test", "C123", "1700.001", "", "exact body",
	)
	if !errors.Is(err, transportErr) {
		t.Fatalf("transport error = %v, want wrapped sentinel", err)
	}
	if got := slackReadbackFailureKind(errors.New("unknown")); got != slackReadbackUnavailable {
		t.Fatalf("fallback failure kind = %q, want %q", got, slackReadbackUnavailable)
	}
}

func TestNewSlackReadbackRequestRejectsMissingIdentity(t *testing.T) {
	for _, args := range [][4]string{
		{"", "C123", "1700.001", ""},
		{"xoxb-test", "", "1700.001", ""},
		{"xoxb-test", "C123", "", ""},
	} {
		if _, err := newSlackReadbackRequest(args[0], args[1], args[2], args[3]); err == nil {
			t.Fatalf("newSlackReadbackRequest%q succeeded, want error", args)
		}
	}
}

func TestHandlePublishRequiresExactSlackReadback(t *testing.T) {
	tests := []struct {
		name          string
		history       string
		wantDelivered bool
		wantFailure   string
	}{
		{
			name:          "exact message is delivered",
			history:       `{"ok":true,"messages":[{"ts":"1700.001","text":"hello"}]}`,
			wantDelivered: true,
		},
		{
			name:        "missing message fails loudly",
			history:     `{"ok":true,"messages":[]}`,
			wantFailure: "readback_unconfirmed",
		},
		{
			name:        "broken read path is distinct from absent acknowledgement",
			history:     `{"ok":false,"error":"missing_scope"}`,
			wantFailure: "readback_unavailable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/chat.postMessage":
					_, _ = w.Write([]byte(`{"ok":true,"channel":"C123","ts":"1700.001"}`))
				case "/conversations.history":
					_, _ = w.Write([]byte(test.history))
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			originalBase := slackAPIBase
			slackAPIBase = server.URL
			t.Cleanup(func() { slackAPIBase = originalBase })

			req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(
				`{"session_id":"gc-1","conversation":{"conversation_id":"C123","kind":"room"},"text":"hello"}`,
			))
			recorder := httptest.NewRecorder()
			handlePublish(config{slackBotToken: "xoxb-test"}, nil, nil, newPublishDedupCache(publishDedupTTL))(recorder, req)

			var receipt publishReceipt
			if err := json.Unmarshal(recorder.Body.Bytes(), &receipt); err != nil {
				t.Fatalf("decode receipt %q: %v", recorder.Body.String(), err)
			}
			if receipt.Delivered != test.wantDelivered || receipt.FailureKind != test.wantFailure {
				t.Fatalf("receipt = %+v, want delivered=%v failure=%q", receipt, test.wantDelivered, test.wantFailure)
			}
		})
	}
}

func TestHandlePublishReadsBackTheRewrittenSlackContent(t *testing.T) {
	var posted slackPostMessageReq
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/chat.postMessage":
			if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
				t.Errorf("decode post: %v", err)
			}
			_, _ = w.Write([]byte(`{"ok":true,"channel":"C123","ts":"1700.001"}`))
		case "/conversations.history":
			response := map[string]any{
				"ok":       true,
				"messages": []map[string]string{{"ts": "1700.001", "text": posted.Text}},
			}
			_ = json.NewEncoder(w).Encode(response)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	originalBase := slackAPIBase
	slackAPIBase = server.URL
	t.Cleanup(func() { slackAPIBase = originalBase })
	aliases := newUserAliasMapForTest(t, map[string]string{"mayor": "U123ABC"})
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(
		`{"session_id":"gc-1","conversation":{"conversation_id":"C123","kind":"room"},"text":"hello @mayor"}`,
	))
	recorder := httptest.NewRecorder()
	handlePublish(config{slackBotToken: "xoxb-test"}, nil, aliases, newPublishDedupCache(publishDedupTTL))(recorder, req)

	var receipt publishReceipt
	if err := json.Unmarshal(recorder.Body.Bytes(), &receipt); err != nil {
		t.Fatalf("decode receipt %q: %v", recorder.Body.String(), err)
	}
	if !receipt.Delivered {
		t.Fatalf("receipt = %+v, want delivered", receipt)
	}
	if posted.Text != "hello <@U123ABC>" {
		t.Fatalf("posted text = %q, want rewritten mention", posted.Text)
	}
}

func newSlackReadbackServer(
	t *testing.T,
	wantPath string,
	checkQuery func(url.Values),
	response string,
) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wantPath {
			t.Errorf("path = %q, want %q", r.URL.Path, wantPath)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer xoxb-test" {
			t.Errorf("authorization = %q, want bearer token", got)
		}
		checkQuery(r.URL.Query())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(response))
	}))
	t.Cleanup(server.Close)

	originalBase := slackAPIBase
	slackAPIBase = server.URL
	t.Cleanup(func() { slackAPIBase = originalBase })
	return server
}

func assertSlackReadbackQuery(t *testing.T, query url.Values, channel, messageTS, limit string) {
	t.Helper()
	for key, want := range map[string]string{
		"channel":   channel,
		"oldest":    messageTS,
		"latest":    messageTS,
		"inclusive": "true",
		"limit":     limit,
	} {
		if got := query.Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}
