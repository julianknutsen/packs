package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultPublicListen   = "0.0.0.0:8775"
	defaultInternalListen = "127.0.0.1:8776"
	defaultGCAPIBase      = "http://127.0.0.1:9443"
	defaultProvider       = "slack"
	defaultInboundTarget  = "mayor"
	defaultSlackAPIBase   = "https://slack.com/api"

	// maxInboundBody caps the /slack/events body read. The body is
	// unsigned until HMAC-verified, so bounding it pre-verify limits a
	// memory-amplification vector. Slack event payloads are small.
	maxInboundBody = 1 << 20 // 1 MiB

	// slackReplayWindow rejects requests whose signed timestamp is older
	// than this, mitigating replay of a captured signature.
	slackReplayWindow = 5 * time.Minute

	slackPostTimeout = 15 * time.Second

	// gcCallTimeout bounds outbound calls to the gc API so a stalled gc
	// cannot pin an inbound-bridge goroutine (or block startup
	// registration) forever.
	gcCallTimeout = 15 * time.Second

	// Startup registration retry: on a supervisor/city restart the adapter
	// can come up before the city has finished adopting sessions, so the
	// first registration gets a 404 "city not found or not running". The
	// adapter retries with exponential backoff (initial→max) until
	// registerDeadline rather than exiting on the first 404 and killing all
	// Slack comms until a manual restart; past the deadline it fails loudly.
	registerInitialBackoff = 1 * time.Second
	registerMaxBackoff     = 30 * time.Second
	registerDeadline       = 5 * time.Minute
)

// config holds the adapter's resolved runtime configuration. It extends
// slack-mini's Tier-1 config with the on-disk registry directory that
// Tier 2 needs for channel bindings, per-session identities, and handle
// aliases.
type config struct {
	publicListen        string
	internalListen      string
	serviceSocket       string
	gcAPIBase           string
	internalCallbackURL string
	cityName            string
	cityPath            string
	registryDir         string
	provider            string
	workspaceID         string
	botToken            string
	signingSecret       string
	appToken            string
	inboundTarget       string
	slackAPIBase        string
	registerOnStart     bool
}

// socketMode reports whether the adapter takes inbound over a Socket Mode
// WebSocket instead of the public HTTP listener. The app-level token is the
// selector: it is required for Socket Mode and useless without it, so its
// presence is the whole contract — there is no second toggle to keep in sync.
func (c config) socketMode() bool { return c.appToken != "" }

func (c config) channelMappingsPath() string {
	return filepath.Join(c.registryDir, "channel_mappings.json")
}

func (c config) identitiesPath() string {
	return filepath.Join(c.registryDir, "identities.json")
}

func (c config) handleAliasesPath() string {
	return filepath.Join(c.registryDir, "handle_aliases.json")
}

// loadConfig reads the process environment.
func loadConfig() (config, error) { return loadConfigFromEnv(os.Getenv) }

// loadConfigFromEnv builds and validates a config from a getenv function.
// Split out so tests can supply a fake environment.
func loadConfigFromEnv(getenv func(string) string) (config, error) {
	envOr := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	cfg := config{
		publicListen:    envOr("LISTEN_PUBLIC", defaultPublicListen),
		internalListen:  envOr("LISTEN_INTERNAL", defaultInternalListen),
		serviceSocket:   getenv("GC_SERVICE_SOCKET"),
		gcAPIBase:       strings.TrimRight(envOr("GC_API_BASE_URL", defaultGCAPIBase), "/"),
		cityName:        getenv("GC_CITY_NAME"),
		cityPath:        getenv("GC_CITY_PATH"),
		provider:        envOr("ADAPTER_PROVIDER", defaultProvider),
		workspaceID:     getenv("SLACK_WORKSPACE_ID"),
		botToken:        getenv("SLACK_BOT_TOKEN"),
		signingSecret:   getenv("SLACK_SIGNING_SECRET"),
		appToken:        getenv("SLACK_APP_TOKEN"),
		inboundTarget:   envOr("SLACK_CHANNEL_INBOUND_TARGET", defaultInboundTarget),
		slackAPIBase:    strings.TrimRight(envOr("SLACK_API_BASE", defaultSlackAPIBase), "/"),
		registerOnStart: envOr("REGISTER_ON_START", "true") == "true",
	}

	// The registry directory holds the three on-disk registries. It
	// defaults to <GC_CITY_PATH>/.gc/slack-channel when GC_CITY_PATH is
	// set; SLACK_CHANNEL_REGISTRY_DIR overrides it outright (tests, or a
	// deployment that stores state elsewhere).
	cfg.registryDir = getenv("SLACK_CHANNEL_REGISTRY_DIR")
	if cfg.registryDir == "" && cfg.cityPath != "" {
		cfg.registryDir = filepath.Join(cfg.cityPath, ".gc", "slack-channel")
	}

	// proxy_process mode: gc reaches the adapter via GC_API_BASE_URL +
	// GC_SERVICE_URL_PREFIX. gc appends the endpoint path itself, so the
	// registered callback base must not include it.
	if cfg.serviceSocket != "" {
		urlPrefix := strings.TrimRight(getenv("GC_SERVICE_URL_PREFIX"), "/")
		if urlPrefix == "" {
			return cfg, errors.New("GC_SERVICE_SOCKET is set but GC_SERVICE_URL_PREFIX is empty — controller-injected env is incomplete")
		}
		cfg.internalCallbackURL = cfg.gcAPIBase + urlPrefix
	} else {
		// Standalone TCP mode: no controller-injected URL prefix, so derive
		// the callback base from the internal listener. Leaving it empty would
		// self-register an empty callback_url and break gc→adapter callbacks.
		cfg.internalCallbackURL = tcpCallbackURL(cfg.internalListen)
	}

	var missing []string
	if cfg.workspaceID == "" {
		missing = append(missing, "SLACK_WORKSPACE_ID")
	}
	if cfg.botToken == "" {
		missing = append(missing, "SLACK_BOT_TOKEN")
	}
	// The signing secret verifies HTTP webhook signatures. Socket Mode has no
	// request signatures — authenticity comes from the app-token-authenticated
	// connection the adapter itself opens — so requiring it there would be
	// demanding a credential the transport never consults.
	if cfg.signingSecret == "" && !cfg.socketMode() {
		missing = append(missing, "SLACK_SIGNING_SECRET")
	}
	if cfg.cityName == "" {
		missing = append(missing, "GC_CITY_NAME")
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	// Fail fast on the easiest Socket Mode mistake: pasting the bot token
	// (xoxb-) into SLACK_APP_TOKEN. Slack would answer invalid_auth on every
	// reconnect forever, which reads as a network fault rather than a typo.
	if cfg.socketMode() && !strings.HasPrefix(cfg.appToken, "xapp-") {
		return cfg, errors.New("SLACK_APP_TOKEN must be an app-level token (xapp-...); the bot token belongs in SLACK_BOT_TOKEN")
	}
	// Tier 2 keeps on-disk registries, so it needs a place to put them.
	// Require either GC_CITY_PATH (the normal gc-supervised path) or an
	// explicit override rather than silently writing to the CWD.
	if cfg.registryDir == "" {
		return cfg, errors.New("registry directory is unset: set GC_CITY_PATH (preferred) or SLACK_CHANNEL_REGISTRY_DIR")
	}
	// cityName is interpolated into every /v0/city/{city}/... URL. Reject
	// URL-significant characters so a city name cannot alter routing.
	if strings.ContainsAny(cfg.cityName, "/?#%") {
		return cfg, fmt.Errorf("GC_CITY_NAME must not contain '/', '?', '#', or '%%': %q", cfg.cityName)
	}
	return cfg, nil
}

// tcpCallbackURL derives the gc→adapter callback base from the internal
// listener address for standalone (TCP) mode, where there is no
// proxy_process URL prefix. gc appends the endpoint path itself, so the
// returned URL carries no trailing path. A wildcard or empty bind host is
// rewritten to loopback because gc dials a concrete address.
func tcpCallbackURL(internalListen string) string {
	host, port, err := net.SplitHostPort(internalListen)
	if err != nil {
		return "http://" + internalListen
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}
