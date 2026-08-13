package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const slackReadbackResponseLimit = 1 << 20

const (
	slackReadbackUnconfirmed = "readback_unconfirmed"
	slackReadbackUnavailable = "readback_unavailable"
)

var slackReadbackHTTPClient = &http.Client{Timeout: 15 * time.Second}

type slackReadbackMessage struct {
	TS   string `json:"ts"`
	Text string `json:"text"`
}

type slackReadbackResponse struct {
	OK       bool                   `json:"ok"`
	Messages []slackReadbackMessage `json:"messages"`
	Error    string                 `json:"error"`
}

type slackReadbackError struct {
	kind string
	err  error
}

func (e *slackReadbackError) Error() string { return e.err.Error() }

func (e *slackReadbackError) Unwrap() error { return e.err }

func unavailableReadback(format string, args ...any) error {
	return &slackReadbackError{kind: slackReadbackUnavailable, err: fmt.Errorf(format, args...)}
}

func unconfirmedReadback(format string, args ...any) error {
	return &slackReadbackError{kind: slackReadbackUnconfirmed, err: fmt.Errorf(format, args...)}
}

func slackReadbackFailureKind(err error) string {
	if typed, ok := err.(*slackReadbackError); ok {
		return typed.kind
	}
	return slackReadbackUnavailable
}

// readBackPublishedMessage verifies a post through Slack's read API rather
// than trusting chat.postMessage's write response as delivery evidence.
func readBackPublishedMessage(
	client *http.Client,
	token string,
	channel string,
	messageTS string,
	threadTS string,
	expectedText string,
) error {
	if client == nil {
		client = http.DefaultClient
	}
	request, err := newSlackReadbackRequest(token, channel, messageTS, threadTS)
	if err != nil {
		return unavailableReadback("build Slack readback: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return unavailableReadback("Slack readback request: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, slackReadbackResponseLimit+1))
	if err != nil {
		return unavailableReadback("read Slack readback response: %w", err)
	}
	if len(body) > slackReadbackResponseLimit {
		return unavailableReadback("Slack readback response exceeds %d bytes", slackReadbackResponseLimit)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return unavailableReadback("Slack readback HTTP %d: %s", response.StatusCode, clipBodyForLog(body))
	}

	var result slackReadbackResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return unavailableReadback("decode Slack readback response: %w", err)
	}
	if !result.OK {
		return unavailableReadback("Slack readback rejected: %s", result.Error)
	}
	for _, message := range result.Messages {
		if message.TS != messageTS {
			continue
		}
		if message.Text != expectedText {
			return unconfirmedReadback("Slack readback content mismatch for message %s", messageTS)
		}
		return nil
	}
	return unconfirmedReadback("Slack message %s not present in channel %s readback", messageTS, channel)
}

func newSlackReadbackRequest(token, channel, messageTS, threadTS string) (*http.Request, error) {
	if token == "" || channel == "" || messageTS == "" {
		return nil, fmt.Errorf("Slack readback requires token, channel, and message ts")
	}
	query := url.Values{
		"channel":   {channel},
		"oldest":    {messageTS},
		"latest":    {messageTS},
		"inclusive": {"true"},
		"limit":     {"1"},
	}
	method := "conversations.history"
	if threadTS != "" {
		method = "conversations.replies"
		query.Set("ts", threadTS)
		// Slack includes the thread parent in conversations.replies even when
		// the time range selects a reply. Leave room for that parent and the
		// exact posted reply; the timestamp/content match below remains exact.
		query.Set("limit", "2")
	}
	request, err := http.NewRequest(http.MethodGet, slackAPIBase+"/"+method+"?"+query.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("build Slack readback request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	return request, nil
}
