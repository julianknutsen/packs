package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// Slack voice clips (file subtype "slack_audio") arrive with an EMPTY
// mimetype AND filetype — files.info reports mimetype "" filetype ""
// subtype "slack_audio" name "audio_message.m4a" for them. gc's
// extmsg.ExternalAttachment declares mime_type as a required property,
// so the adapter must always derive one rather than pass Slack's value
// through verbatim.
func TestAttachmentMIMETypeDerivation(t *testing.T) {
	cases := []struct {
		name string
		file slackFile
		want string
	}{
		{"slack mimetype wins over extension", slackFile{Name: "x.bin", MIMEType: "image/png"}, "image/png"},
		{"slack mimetype is trimmed", slackFile{Name: "x.m4a", MIMEType: " audio/mp4 "}, "audio/mp4"},
		{"slack mimetype passes through in its own case", slackFile{Name: "x.m4a", MIMEType: "IMAGE/PNG"}, "IMAGE/PNG"},
		{"voice clip by .m4a extension", slackFile{Name: "audio_message.m4a", Subtype: "slack_audio"}, "audio/mp4"},
		{"extension is case-insensitive", slackFile{Name: "CLIP.M4A"}, "audio/mp4"},
		{"trailing whitespace in name still resolves", slackFile{Name: "clip.M4A "}, "audio/mp4"},
		{"title used when name empty", slackFile{Title: "memo.mp3"}, "audio/mpeg"},
		{"title used when name extension unknown", slackFile{Name: "blob.zzzunknown", Title: "memo.mp3"}, "audio/mpeg"},
		{"slack filetype code used when no extension", slackFile{Name: "noext", Filetype: "m4a"}, "audio/mp4"},
		{"already-dotted filetype code still resolves", slackFile{Name: "noext", Filetype: ".m4a"}, "audio/mp4"},
		{"subtype used when filetype unknown", slackFile{Name: "noext", Filetype: "zzzunknown", Subtype: "slack_audio"}, "audio/mp4"},
		{"slack_audio subtype when nothing else", slackFile{Name: "noext", Subtype: "slack_audio"}, "audio/mp4"},
		{"slack_video subtype when nothing else", slackFile{Name: "noext", Subtype: "slack_video"}, "video/mp4"},
		{"stdlib table, charset parameter stripped", slackFile{Name: "style.css"}, "text/css"},
		{"unknown extension falls back to octet-stream", slackFile{Name: "blob.zzzunknown"}, "application/octet-stream"},
		{"no hints at all falls back to octet-stream", slackFile{ID: "F1"}, "application/octet-stream"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := attachmentMIMEType(tc.file); got != tc.want {
				t.Fatalf("attachmentMIMEType(%+v) = %q, want %q", tc.file, got, tc.want)
			}
		})
	}
}

// The downloaded attachment record carries the derived type, not
// Slack's empty string — this is the exact shape that made gc 422 the
// whole inbound message.
func TestDownloadSlackFilesDerivesMIMETypeForVoiceClip(t *testing.T) {
	testAllowAnyURL(t)
	slackStub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("AAC-BYTES"))
	}))
	t.Cleanup(slackStub.Close)

	cfg := config{
		slackBotToken:    "xoxb-test",
		inboundFileStore: filepath.Join(t.TempDir(), "inbound"),
	}
	files := []slackFile{{
		ID:         "F1VOICE",
		Name:       "audio_message.m4a",
		Title:      "audio_message.m4a",
		URLPrivate: slackStub.URL + "/files/F1VOICE",
		MIMEType:   "",
		Filetype:   "",
		Subtype:    "slack_audio",
	}}
	got := downloadSlackFiles(cfg, "C123", "1234.5678", files)
	if len(got) != 1 {
		t.Fatalf("got %d attachments, want 1", len(got))
	}
	if got[0].MIMEType != "audio/mp4" {
		t.Fatalf("attachment mime_type = %q, want audio/mp4", got[0].MIMEType)
	}
}

// mime_type mirrors a REQUIRED property on the gc side: it must be
// serialized even when empty so a payload can never be rejected for a
// missing key (an empty string is at worst a degraded value, never a
// 422).
func TestExternalAttachmentAlwaysSerializesMIMEType(t *testing.T) {
	raw, err := json.Marshal(externalAttachment{ProviderID: "F1", URL: "file:///tmp/x"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"mime_type":""`) {
		t.Fatalf("mime_type must always be present, got %s", raw)
	}
}
