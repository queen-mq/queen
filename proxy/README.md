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
2. Add the redirect URI you'll set in `GOOGLE_REDIRECT_URI` (must include the
   `/api/auth/google/callback` path).
3. Request scopes `openid email profile` (the proxy does this automatically).

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

