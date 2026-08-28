// authgate is a credential-checking reverse proxy the rig puts in front of the
// broker, and it exists for one reason: the rig's broker runs with JWT_ENABLED
// unset, so it answers 200 to any bearer and every SASL password would be
// accepted. A facade pointed at it can be shown to FORWARD a credential, never
// to have one refused.
//
// So the M5 listener talks to this instead. It is the auth layer and nothing
// else: one exact-match check on `Authorization: Bearer <token>`, 401 when it
// fails, a verbatim forward when it passes — the same 401 the broker's
// server/src/auth.rs and the proxy's err_401 return, which is the answer
// handlers::sasl_authenticate maps to SASL_AUTHENTICATION_FAILED.
//
//	AUTHGATE_ADDR=127.0.0.1:6698 AUTHGATE_UPSTREAM=http://127.0.0.1:6699 \
//	AUTHGATE_TOKEN=… go run ./authgate
//
// Nothing here logs a credential, at any level: a refusal is logged as the
// method and path that was refused, and the token itself never reaches a log
// line, an error body or the process's own output.
package main

import (
	"crypto/subtle"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

func main() {
	addr := env("AUTHGATE_ADDR", "127.0.0.1:6698")
	upstream := env("AUTHGATE_UPSTREAM", "http://127.0.0.1:6699")
	token := os.Getenv("AUTHGATE_TOKEN")
	if token == "" {
		log.Fatal("AUTHGATE_TOKEN is empty: a gate that accepts everything is the thing this replaces")
	}

	target, err := url.Parse(upstream)
	if err != nil {
		log.Fatalf("AUTHGATE_UPSTREAM=%s is not a URL: %v", upstream, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("upstream %s %s: %v", r.Method, r.URL.Path, err)
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, `{"error":"upstream unreachable"}`)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// /health is what the rig polls to know the gate is up, and it is on the
		// broker's own JWT_SKIP_PATHS for the same reason: a liveness probe that
		// needs a credential is a liveness probe of the credential.
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"ok"}`)
			return
		}
		if !authorized(r.Header.Get("Authorization"), token) {
			log.Printf("refused %s %s", r.Method, r.URL.Path)
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":"unauthorized"}`)
			return
		}
		proxy.ServeHTTP(w, r)
	})

	log.Printf("authgate on %s in front of %s", addr, upstream)
	server := &http.Server{Addr: addr, Handler: mux} //nolint:gosec // a rig-local gate
	log.Fatal(server.ListenAndServe())
}

// authorized compares in constant time — not because a rig needs it, but
// because the shape of a credential check is worth copying correctly.
func authorized(header, token string) bool {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(token)) == 1
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
