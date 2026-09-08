package main

import (
	"mime"
	"path"
	"strings"
)

// --- inbound attachment media types ------------------------------------
//
// gc's extmsg.ExternalAttachment declares mime_type a REQUIRED property,
// and Slack does not always populate one: files recorded inside Slack
// itself — voice clips (file subtype "slack_audio", name
// "audio_message.m4a") and video clips ("slack_video") — arrive with
// mimetype "" AND filetype "", even from files.info. Passing Slack's
// value through verbatim produced a payload without the key, a
// deterministic 422 from gc, and the whole inbound message — the file
// AND its text caption — silently never reached the bound session.
// attachmentMIMEType therefore always derives a value; the adapter
// never emits an attachment without one.

// fallbackAttachmentMIMEType is the last-resort attachment type when
// neither Slack nor the file name says anything usable.
const fallbackAttachmentMIMEType = "application/octet-stream"

// slackExtensionMIMETypes pins the media types Slack clients actually
// produce, independent of the host's mime.types database (which differs
// between macOS and Linux and lacks .m4a on some images). Consulted
// before mime.TypeByExtension.
var slackExtensionMIMETypes = map[string]string{
	".m4a": "audio/mp4", ".aac": "audio/aac", ".mp3": "audio/mpeg", ".wav": "audio/wav",
	".ogg": "audio/ogg", ".oga": "audio/ogg", ".opus": "audio/opus", ".flac": "audio/flac",
	".mp4": "video/mp4", ".m4v": "video/mp4", ".mov": "video/quicktime", ".webm": "video/webm",
	".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif",
	".webp": "image/webp", ".heic": "image/heic", ".svg": "image/svg+xml",
	".pdf": "application/pdf", ".txt": "text/plain", ".md": "text/markdown", ".csv": "text/csv",
	".json": "application/json", ".zip": "application/zip",
}

// slackSubtypeMIMETypes maps Slack-native recording subtypes — which
// carry no mimetype/filetype at all — to the container Slack encodes
// them in.
var slackSubtypeMIMETypes = map[string]string{
	"slack_audio": "audio/mp4", // voice clips: AAC in an MP4 container (audio_message.m4a)
	"slack_video": "video/mp4", // video clips
}

// attachmentMIMEType derives the mime_type reported to gc for an inbound
// Slack file. Precedence: Slack's own mimetype → the file name's
// extension (name, then title) → Slack's filetype code treated as an
// extension → the Slack-native subtype → application/octet-stream.
// Never returns "".
func attachmentMIMEType(f slackFile) string {
	if mt := strings.TrimSpace(f.MIMEType); mt != "" {
		return mt
	}
	for _, name := range []string{f.Name, f.Title} {
		if mt := mimeTypeByExtension(path.Ext(name)); mt != "" {
			return mt
		}
	}
	if ft := strings.TrimSpace(f.Filetype); ft != "" {
		if mt := mimeTypeByExtension("." + ft); mt != "" {
			return mt
		}
	}
	if mt := slackSubtypeMIMETypes[strings.ToLower(strings.TrimSpace(f.Subtype))]; mt != "" {
		return mt
	}
	return fallbackAttachmentMIMEType
}

// mimeTypeByExtension resolves a dotted extension via the pinned Slack
// table, then the stdlib/host database with any parameters (e.g.
// "; charset=utf-8") stripped so the value is a bare media type. ""
// when unknown or ext is empty. Input is canonicalized to exactly one
// leading dot, lowercased and whitespace-trimmed, so a file name with
// trailing whitespace ("clip.M4A ") or an already-dotted filetype code
// (".m4a" → "..m4a" at the call site) still resolves.
func mimeTypeByExtension(ext string) string {
	ext = strings.TrimLeft(strings.ToLower(strings.TrimSpace(ext)), ".")
	if ext == "" {
		return ""
	}
	ext = "." + ext
	if mt, ok := slackExtensionMIMETypes[ext]; ok {
		return mt
	}
	mt := mime.TypeByExtension(ext)
	if mt == "" {
		return ""
	}
	if mediaType, _, err := mime.ParseMediaType(mt); err == nil {
		return mediaType
	}
	return mt
}
