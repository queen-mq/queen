# Queen Proxy

Secure authentication proxy for Queen message queue with role-based access control.
Queen server has not authentication or authorization, so this proxy is necessary to secure the access to the Queen server if you want to expose it to the internet.
Mainly intended for exposing the Webapp.

## Features

- JWT-based authentication
- Optional Sign in with Google (OAuth 2.0 / OIDC)
- Optional external SSO passthrough (verify any IDP via JWKS)
- Role-based access control (admin, read-write, read-only)
- Auto-initializes database schema on startup
- Strips large headers to prevent upstream errors
- WebSocket support

## Prerequisites

- Node.js 22+
- PostgreSQL database
- Queen server running

## Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Set Environment Variables

```bash
# PostgreSQL (uses the same PG_* names as the broker)
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=postgres
export PG_USER=postgres
export PG_PASSWORD=postgres
# Optional SSL
# export PG_USE_SSL=true
# export PG_SSL_REJECT_UNAUTHORIZED=true

# Upstream broker
export QUEEN_SERVER_URL=http://localhost:6632

# Internal JWT
export JWT_SECRET=your-secret-key-change-in-production
export JWT_EXPIRES_IN=24h

# HTTP listen
export PORT=3000
```

### 3. Start the Proxy

```bash
npm start
```

The proxy will automatically create the database schema on first run.

### 4. Create Users

```bash
npm run create-user
```

Follow the prompts to create users with different roles:
- **admin**: Full access to all operations
- **read-write**: Can perform GET, POST, PUT, DELETE
- **read-only**: Can only perform GET operations

## Usage

1. Access Queen through the proxy at `http://localhost:3000`
2. Login with your credentials
3. All requests are authenticated and authorized based on your role
4. Logout button appears in the sidebar when behind proxy

## Docker

### Build

```bash
docker build -t queen-proxy .
```

### Run

```bash
docker run -p 3000:3000 \
  -e PG_HOST=postgres \
  -e PG_DB=postgres \
  -e PG_USER=postgres \
  -e PG_PASSWORD=postgres \
  -e QUEEN_SERVER_URL=http://queen-server:6632 \
  -e JWT_SECRET=your-secret-key \
  queen-proxy
```

### Create User in Docker

```bash
docker exec -it <container-id> node src/create-user.js
```

## Kubernetes

Deploy using Helm charts in `helm/` directory:

```bash
# Stage environment
helm upgrade --install queen-proxy ./helm -f helm/stage.yaml

# Production environment
helm upgrade --install queen-proxy ./helm -f helm/prod.yaml
```

## Environment Variables

### Database (PostgreSQL)

The proxy uses the **same `PG_*` variable names as the broker** so a single set of secrets can configure both.

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DB` | `postgres` | PostgreSQL database name |
| `PG_USER` | `postgres` | PostgreSQL username |
| `PG_PASSWORD` | `postgres` | PostgreSQL password |
| `PG_USE_SSL` | _(unset)_ | If set to any truthy value, enable SSL |
| `PG_SSL_REJECT_UNAUTHORIZED` | `false` | If `"true"`, require valid TLS certificates (otherwise allow self-signed) |

### HTTP / Upstream

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | Proxy HTTP listen port |
| `QUEEN_SERVER_URL` | `http://localhost:8080` | Upstream Queen broker URL — set this to `http://localhost:6632` for the local broker default |
| `NODE_ENV` | `development` | Set to `production` to enable secure cookies (HTTPS-only) |

### Internal JWT (issued by the proxy after password login)

| Variable | Default | Description |
|----------|---------|-------------|
| `JWT_SECRET` | `change-me-in-production` | ⚠️ Override in any non-dev deployment. Shared with the broker's `JWT_SECRET` when using HS256. |
| `JWT_EXPIRES_IN` | `24h` | Token expiration ( `'never'` disables expiry) |

### External SSO passthrough (verify tokens minted by an external IdP)

When `EXTERNAL_JWKS_URL` is set, the proxy accepts JWTs minted by an external identity provider (Okta, Auth0, BetterAuth, Keycloak, …) and validates them via JWKS. The `JWT_*` aliases on the right of each fallback below let you reuse the broker's own JWT env names.

| Variable | Default | Description |
|----------|---------|-------------|
| `EXTERNAL_JWKS_URL` (or `JWT_JWKS_URL`) | _(unset)_ | JWKS endpoint URL. Setting this enables external SSO. |
| `EXTERNAL_ISSUER` (or `JWT_ISSUER`) | _(unset)_ | Expected `iss` claim |
| `EXTERNAL_AUDIENCE` (or `JWT_AUDIENCE`) | _(unset)_ | Expected `aud` claim |

### Google Sign-In

| Variable | Default | Description |
|----------|---------|-------------|
| `GOOGLE_CLIENT_ID` | _(unset)_ | Google OAuth 2.0 client id (enables "Sign in with Google" when set together with the secret + redirect URI) |
| `GOOGLE_CLIENT_SECRET` | _(unset)_ | Google OAuth 2.0 client secret |
| `GOOGLE_REDIRECT_URI` | _(unset)_ | Must match the Authorized redirect URI in Google Cloud Console, e.g. `https://queen.example.com/api/auth/google/callback` |
| `GOOGLE_ALLOWED_DOMAINS` | _(empty)_ | Comma-separated domain allowlist matched against the `hd` claim or the email domain. Empty = allow any verified email. |
| `GOOGLE_AUTO_PROVISION` | `false` | If `true`, create a local user on first Google login. If `false`, the user must already exist in `queen_proxy.users` (matched by email). |
| `GOOGLE_DEFAULT_ROLE` | `read-only` | Role assigned to auto-provisioned Google users (`admin`, `read-write`, or `read-only`). |

## Sign in with Google

When `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` and `GOOGLE_REDIRECT_URI` are
all set, the login page exposes a "Sign in with Google" button and the proxy
runs the OAuth 2.0 Authorization Code flow:

1. Browser hits `GET /api/auth/google` → 302 to `accounts.google.com`.
2. Google redirects back to `GET /api/auth/google/callback?code=…&state=…`.
3. The proxy exchanges the code, verifies the `id_token` against Google's JWKS,
   then resolves the local user:
   - by `google_sub` if previously linked, else
   - by verified email (links the Google identity to the existing local user),
     else
   - auto-provisions a new user when `GOOGLE_AUTO_PROVISION=true`, else
   - denies with `?error=not_provisioned`.
4. A standard internal JWT cookie is set, identical to the password flow, so
   the rest of the system (RBAC + Queen forwarding) is unchanged.

### Google Cloud Console setup

1. Create an OAuth 2.0 Client ID of type **Web application**.
2. Add the redirect URI you'll set in `GOOGLE_REDIRECT_URI`. The proxy serves
   the callback under both of these paths — pick whichever you prefer:
   - `/api/auth/google/callback` (legacy / matches the rest of `/api/auth/*`)
   - `/api/oauth/callback/google` (Tinyauth / Better-Auth style — easier to
     extend with `/api/oauth/callback/<other-provider>` later)
3. Request scopes `openid email profile` (the proxy does this automatically).

## Traefik ForwardAuth

When `FORWARD_AUTH_ENABLED=true`, the proxy exposes
`GET /api/auth/forward-auth` — a stateless endpoint compatible with Traefik's
[ForwardAuth](https://doc.traefik.io/traefik/reference/routing-configuration/http/middlewares/forwardauth/)
middleware. Any service routed through Traefik can be SSO‑protected by adding
the middleware to its router (Grafana, Prometheus, the Traefik dashboard,
QueenMQ itself, …).

### Behavior

- `Authorization: Bearer <ext-jwt>` present → verified against `EXTERNAL_JWKS_URL`.
  By default the bearer is passed through unchanged (IdP is source of truth).
  Set `FORWARD_AUTH_ALWAYS_MINT=true` to instead mint a fresh short‑lived
  internal JWT carrying the same claims — useful when you want the broker to
  trust a single issuer (the proxy) regardless of where the original token
  came from.
- No bearer, but a session cookie (`token`) on the configured `COOKIE_DOMAIN`
  → cookie is verified, a fresh short‑lived `internal JWT` is minted, and that
  is forwarded as `Authorization: Bearer …` to the upstream. The long‑lived
  session cookie value never leaves the proxy.
- No credentials → bounce. Browser navigations get a `302` to
  `https://${AUTH_HOST}/login?redirect_uri=<original>`; API/XHR callers
  (`Accept: application/json` or `X-Requested-With`) get a `401` plus an
  `X-Forward-Auth-Location` header pointing at the same login URL.
- After identity is verified, an optional `FORWARD_AUTH_ALLOWED_EMAILS`
  allowlist applies. Rejected requests get the same bounce as unauthenticated
  ones, and the login page surfaces a `?error=not_allowed` message.

The handler reads `X-Forwarded-Method`, `X-Forwarded-Proto`, `X-Forwarded-Host`
and `X-Forwarded-Uri` to reconstruct the original client‑facing URL — never its
own `req.url`. That's how Traefik tells it what the client actually asked for.

### Topology

```
                     ┌──────────────────────────────────┐
                     │        auth.example.com          │
                     │  (queen-proxy: ForwardAuth +     │
                     │   /login + /api/auth/google)     │
                     └──────────────▲───────────────────┘
                                    │
   user ──► Traefik ───── 302 ──────┤   (no cookie / bad token)
              │                     │
              │ ForwardAuth probe   │
              │  GET /api/auth/     │
              │      forward-auth   │
              ▼                     │
   grafana.example.com ◄── 200 ─────┘   (Authorization + X-Auth-* headers)
   prom.example.com
   queen.example.com
```

`auth.example.com` writes the session cookie with `Domain=.example.com` so
every protected subdomain can read it.

### Compose snippet

Tinyauth‑style label set on the proxy itself defines the middleware; protected
services then opt into it on their router:

```yaml
services:
  queen-proxy:
    image: queen-proxy:latest
    environment:
      FORWARD_AUTH_ENABLED: "true"
      AUTH_HOST: auth.example.com
      COOKIE_DOMAIN: .example.com
      FORWARD_AUTH_ALLOWED_REDIRECT_HOSTS: grafana.example.com,prom.example.com,queen.example.com,traefik.example.com
      FORWARD_AUTH_EMIT_HEADERS: Authorization,X-Auth-User,X-Auth-Email,X-Auth-Role,X-Auth-Sub
      # plus the existing JWT_SECRET, GOOGLE_*, PG_* envs
    labels:
      traefik.enable: "true"
      traefik.http.routers.auth.rule: Host(`auth.example.com`)
      traefik.http.middlewares.queen-fa.forwardauth.address: http://queen-proxy:3000/api/auth/forward-auth
      traefik.http.middlewares.queen-fa.forwardauth.authResponseHeaders: Authorization,X-Auth-User,X-Auth-Email,X-Auth-Role,X-Auth-Sub
      traefik.http.middlewares.queen-fa.forwardauth.trustForwardHeader: "true"
      # Optional: opt-in method-RBAC middleware (read-only role → GET only).
      # Apply only to routers whose upstream tolerates it (NOT Grafana — its
      # /login is a POST).
      traefik.http.middlewares.queen-fa-rbac.headers.customRequestHeaders.X-Forward-Auth-RBAC: method

  grafana:
    image: grafana/grafana
    labels:
      traefik.enable: "true"
      traefik.http.routers.grafana.rule: Host(`grafana.example.com`)
      traefik.http.routers.grafana.middlewares: queen-fa@docker

  queen-broker:
    image: queen-mq:latest
    labels:
      traefik.enable: "true"
      traefik.http.routers.queen.rule: Host(`queen.example.com`)
      # Identity check + method RBAC, since the broker speaks REST.
      traefik.http.routers.queen.middlewares: queen-fa@docker,queen-fa-rbac@docker
```

### ForwardAuth env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `FORWARD_AUTH_ENABLED` | `false` | Master switch — when `false`, the route is not registered. |
| `AUTH_HOST` | _(unset)_ | Public hostname of the proxy. Used to build absolute redirect URLs. Required for the 302 path; without it, the handler always returns 401. |
| `COOKIE_DOMAIN` | _(unset)_ | When set (e.g. `.example.com`), the session cookie scope is widened to all subdomains and OAuth state is encoded as a signed JWT in the `state` query param (cookies wouldn't survive the cross‑host bounce). Also implicitly allows any host under that suffix in `?redirect_uri=` so operators don't have to enumerate every upstream. Empty = legacy host‑only behavior. |
| `FORWARD_AUTH_ALLOWED_REDIRECT_HOSTS` | _(empty)_ | Comma list of hosts allowed in `?redirect_uri=`. Combined with the `COOKIE_DOMAIN` implicit allowance: a host passes if it is in this list **or** shares the cookie‑domain suffix. With both empty, all cross‑host redirects are denied (fail‑closed). |
| `FORWARD_AUTH_ALLOWED_EMAILS` | _(empty)_ | Optional second‑stage allowlist applied after identity is verified (Google `email_verified` or external JWT `email`). Empty = no email‑level restriction. Useful when `GOOGLE_ALLOWED_DOMAINS` is too broad and only a small set of admins should reach Grafana/Prometheus. |
| `FORWARD_AUTH_ALWAYS_MINT` | `false` | When `true`, even bearer requests get a freshly minted internal JWT instead of the original token being passed through. Lets the broker trust a single issuer (the proxy) regardless of where the original token came from. |
| `FORWARD_AUTH_EMIT_HEADERS` | `Authorization` | Comma list from `Authorization`, `X-Auth-User`, `X-Auth-Email`, `X-Auth-Sub`, `X-Auth-Role`, `X-Auth-Groups`. Per‑route subset is still controlled by Traefik's `forwardauth.authResponseHeaders`. |

### Internal JWT

The proxy mints an internal JWT for any upstream request whose origin needs
cookie‑auth (always) or whose bearer was reissued because
`FORWARD_AUTH_ALWAYS_MINT=true`.

Two signing modes:

- **Symmetric (default, no extra config)** — HS256 with `INTERNAL_JWT_SIGNING_KEY`
  (falls back to `JWT_SECRET`). Broker verifies via the same shared secret;
  perfect for single‑deployment setups. The JWKS endpoint returns an empty
  key set in this mode.
- **Asymmetric (`INTERNAL_JWT_PRIVATE_KEY_PEM` set)** — RS256 / EdDSA / ES256
  picked automatically from the PEM key type. The corresponding public key is
  served at the JWKS endpoints below, so the broker can verify with
  `JWT_JWKS_URL=https://${AUTH_HOST}/.well-known/jwks.json` instead of
  sharing a secret. Tokens carry a `kid` so rotation works.

| Variable | Default | Description |
|----------|---------|-------------|
| `INTERNAL_JWT_SIGNING_KEY` | falls back to `JWT_SECRET` | HMAC secret used in symmetric mode. Ignored in asymmetric mode. |
| `INTERNAL_JWT_PRIVATE_KEY_PEM` | _(unset)_ | Asymmetric private key (PKCS#8 PEM). Setting this switches signing to RS256/EdDSA/ES256 and enables the JWKS endpoint. |
| `INTERNAL_JWT_KEY_ID` | _(auto)_ | Override the `kid` advertised on the JWKS entry. Default is a stable SHA‑256 thumbprint of the public key. |
| `INTERNAL_JWT_ALG` | `HS256` | Symmetric only — set to `HS384`/`HS512` if needed. Asymmetric mode ignores this and picks the alg from the key. |
| `INTERNAL_JWT_ISSUER` | `queen-proxy` | `iss` claim. |
| `INTERNAL_JWT_AUDIENCE` | _(empty)_ | `aud` claim, omitted when empty. |
| `INTERNAL_JWT_EXPIRES_IN` | `15m` | Short‑lived; only crosses the trust boundary to upstreams. |
| `INTERNAL_JWT_CLAIM_MAPPING` | `sub:sub,role:role,email:email,preferred_username:username` | `<dst>:<src>` pairs projecting the source claims into the new token. |

#### JWKS endpoint

| Path | Notes |
|------|-------|
| `GET /.well-known/jwks.json` | RFC 8615 canonical location |
| `GET /api/auth/jwks` | Same content under the `/api/auth/*` surface |

Both are public (no auth required) and return the same JWKS document. The
broker can poll either one. With `INTERNAL_JWT_PRIVATE_KEY_PEM` unset the
endpoint returns `{ "keys": [] }` — harmless to poll, just nothing to verify.

Generating a key (Ed25519, recommended):

```bash
openssl genpkey -algorithm Ed25519 -out internal-jwt.pem
# then export INTERNAL_JWT_PRIVATE_KEY_PEM="$(cat internal-jwt.pem)"
```

Or RSA if your broker doesn't support EdDSA:

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out internal-jwt.pem
```

### Per‑route opt‑ins (request headers)

The proxy reads these headers off the ForwardAuth probe to override
per‑middleware behavior. Set them via Traefik label
(`headers.customRequestHeaders.<Name>`):

- `X-Forward-Auth-RBAC: method` — enforce the `checkMethodAccess` rules
  (`read-only` → GET only). Default is identity‑only.
- `X-Forward-Auth-Headers: X-Auth-User,X-Auth-Email` — narrow the global
  `FORWARD_AUTH_EMIT_HEADERS` allowlist for this route. Names outside the
  global allowlist are ignored.

### Security notes

- The `Authorization` header is only emitted when the route's
  `forwardauth.authResponseHeaders` opts into it — Traefik discards anything
  else from the response. This prevents an unrelated route from silently
  pulling a bearer token.
- `?redirect_uri=` is always validated against
  `FORWARD_AUTH_ALLOWED_REDIRECT_HOSTS`. Same‑origin relative paths
  (`/foo`) are always allowed; protocol‑relative URLs (`//evil.com`) are
  always rejected.
- In cross‑host (`COOKIE_DOMAIN`) mode, OAuth `state` is a `5m` HS256 JWT
  signed with `JWT_SECRET`; the legacy `g_state`/`g_nonce`/`g_next` cookies
  are not written. This is required because the auth host can't pre‑seed a
  cookie on a different upstream's domain before the OAuth bounce.
- Logout clears the `.example.com` cookie locally only — no IdP RP‑Initiated
  Logout call (issue #30 design choice).

## Security

- Passwords are hashed using bcrypt with 10 salt rounds
- JWT tokens stored in HTTP-only cookies
- Database schema: `queen_proxy`
- Headers stripped before forwarding to Queen (prevents header size errors)

## Roles

### Admin
- Full access to all HTTP methods
- Can manage system settings

### Read-Write
- GET, POST, PUT, DELETE operations
- Standard user access

### Read-Only
- GET operations only
- Monitoring/viewing access

