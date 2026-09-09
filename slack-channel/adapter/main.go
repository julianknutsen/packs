// gc-slack-channel-adapter — the Tier-2 ("slack-channel") Slack ↔ gc bridge.
//
// slack-channel is the "team channel ↔ session graph" middle tier. It is
// built on slack-mini's single-file kernel (HMAC-verified Events API
// receiver + chat.postMessage outbound) and adds the state slack-mini
// deliberately omits:
//
//   - Channel binding registry (channel_mappings.json): a Slack channel or
//     DM bound to one or more gc sessions. A non-mention message in a bound
//     channel is delivered to every bound session — Tier 1 only handled
//     app_mention.
//   - Identity registry (identities.json): a per-session username/avatar
//     override injected into chat.postMessage (chat:write.customize).
//   - Handle aliases (handle_aliases.json): "@handle[:]" address-by-handle
//     routing to a session from any channel.
//
// The adapter is the single owner of all three registries: verb wrappers
// POST to it over gc's /svc/slack-channel reverse proxy (no operator CLI
// binary, no direct file writes from bash), and the inbound path reads them
// under a lock.
//
// Tier 2 explicitly EXCLUDES (those are slack-full / Tier 3): multi-rig
// routing, channel-name pattern resolvers, room-launch, the apps registry +
// slash-command intake, peer fanout, file upload, and double-handle
// dispatch.
//
// Required env:
//
//	SLACK_BOT_TOKEN        Bot token (xoxb-...) for chat.postMessage +
//	                       reactions.add.
//	SLACK_SIGNING_SECRET   HMAC secret for verifying Slack request
//	                       signatures on /slack/events and
//	                       /slack/interactions.
//	SLACK_WORKSPACE_ID     Slack workspace (team) id; the extmsg account id
//	                       and the registries' workspace component.
//	GC_CITY_NAME           gc city the adapter bridges into.
//	GC_CITY_PATH           On-disk root of the gc city; the registry
//	                       directory defaults to <GC_CITY_PATH>/.gc/
//	                       slack-channel (override with
//	                       SLACK_CHANNEL_REGISTRY_DIR).
//
// Controller-injected env (proxy_process mode):
//
//	GC_SERVICE_SOCKET      UDS path the internal listener binds.
//	GC_SERVICE_URL_PREFIX  Reverse-proxy prefix gc routes to this service.
//	GC_API_BASE_URL        gc API base (default http://127.0.0.1:9443).
//
// Optional env:
//
//	LISTEN_PUBLIC                 Public bind for /slack/events +
//	                              /slack/interactions (default 0.0.0.0:8775).
//	LISTEN_INTERNAL               TCP bind for the internal mux when
//	                              GC_SERVICE_SOCKET is unset (default
//	                              127.0.0.1:8776).
//	REGISTER_ON_START             "true" (default) self-registers as an
//	                              extmsg adapter.
//	ADAPTER_PROVIDER              extmsg provider name (default "slack").
//	SLACK_CHANNEL_INBOUND_TARGET  Fallback session for an unbound,
//	                              unaliased app_mention (default "mayor").
//	SLACK_CHANNEL_REGISTRY_DIR    Override for the registry directory.
//	SLACK_API_BASE                Slack web API base (default
//	                              https://slack.com/api).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	srv, err := newServer(cfg)
	if err != nil {
		log.Fatalf("init: %v", err)
	}

	internalDescr := cfg.internalListen
	if cfg.serviceSocket != "" {
		internalDescr = "uds:" + cfg.serviceSocket
	}
	inboundDescr := "http:" + cfg.publicListen
	if cfg.socketMode() {
		inboundDescr = "socket-mode (no public listener)"
	}
	log.Printf("starting gc-slack-channel-adapter inbound=%s internal=%s gc=%s city=%s registry=%s target=%s",
		inboundDescr, internalDescr, cfg.gcAPIBase, cfg.cityName, cfg.registryDir, cfg.inboundTarget)

	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/slack/events", srv.handleSlackEvents())
	publicMux.HandleFunc("/slack/interactions", srv.handleInteractions())
	publicMux.HandleFunc("/healthz", handleHealthz)
	publicMux.HandleFunc("/", http.NotFound)

	internalMux := http.NewServeMux()
	internalMux.HandleFunc("POST /publish", srv.handlePublish())
	internalMux.HandleFunc("POST /publish-to-channel", srv.handlePublishToChannel())
	internalMux.HandleFunc("POST /reply-current", srv.handleReplyCurrent())
	internalMux.HandleFunc("POST /react", srv.handleReact())
	internalMux.HandleFunc("POST /bindings", srv.handleBind())
	internalMux.HandleFunc("POST /identity", srv.handleIdentitySet())
	internalMux.HandleFunc("DELETE /identity", srv.handleIdentityRemove())
	internalMux.HandleFunc("POST /handle-alias", srv.handleAliasSet())
	internalMux.HandleFunc("DELETE /handle-alias", srv.handleAliasRemove())
	internalMux.HandleFunc("/healthz", handleHealthz)

	// ReadTimeout/WriteTimeout bound slow clients on the public,
	// attacker-reachable listener (the body is unsigned until verified);
	// ReadHeaderTimeout alone leaves a slow-body drip able to pin a
	// connection up to the maxInboundBody size.
	publicSrv := &http.Server{
		Addr:              cfg.publicListen,
		Handler:           publicMux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	internalSrv := &http.Server{
		Handler:           internalMux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	// Set the internal TCP address before launching the serve goroutine so
	// the field is not written concurrently with a shutdown read. Unused in
	// UDS mode (Serve(lis) ignores Addr).
	if cfg.serviceSocket == "" {
		internalSrv.Addr = cfg.internalListen
	}

	if cfg.registerOnStart {
		regCtx, cancel := context.WithTimeout(context.Background(), registerDeadline)
		err := srv.registerAdapterWithRetry(regCtx, retryPolicy{
			initialBackoff: registerInitialBackoff,
			maxBackoff:     registerMaxBackoff,
		})
		cancel()
		if err != nil {
			log.Fatalf("register adapter: %v", err)
		}
		log.Printf("registered with gc as provider=%s account=%s callback=%s",
			cfg.provider, cfg.workspaceID, cfg.internalCallbackURL)
	}

	// Socket Mode owns the inbound path when enabled; the public listener is
	// not merely unused but never bound, which is the point on a network that
	// cannot accept inbound connections.
	socketCtx, socketCancel := context.WithCancel(context.Background())
	defer socketCancel()

	errCh := make(chan error, 2)
	if cfg.socketMode() {
		go func() {
			// runSocketMode reconnects internally and returns only when its
			// context is cancelled, so a return here means shutdown.
			if err := srv.runSocketMode(socketCtx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	} else {
		go func() {
			log.Printf("public listener serving on %s (Slack events + interactions)", cfg.publicListen)
			errCh <- publicSrv.ListenAndServe()
		}()
	}
	go func() {
		if cfg.serviceSocket != "" {
			log.Printf("internal listener serving on UDS %s (gc proxy_process)", cfg.serviceSocket)
			lis, err := listenUDS(cfg.serviceSocket)
			if err != nil {
				errCh <- err
				return
			}
			errCh <- internalSrv.Serve(lis)
			return
		}
		log.Printf("internal listener serving on %s (verb endpoints)", cfg.internalListen)
		errCh <- internalSrv.ListenAndServe()
	}()

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
	socketCancel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = publicSrv.Shutdown(ctx)
	_ = internalSrv.Shutdown(ctx)
}

// listenUDS binds a Unix domain socket, removing any stale entry first so
// restarts succeed, and tightens it to owner-only.
func listenUDS(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("chmod uds: %w", err)
	}
	return lis, nil
}
