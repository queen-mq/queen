# Publishing the MCP server to the MCP Registry

`server.json` lists Queen's MCP server in the official MCP Registry
(`registry.modelcontextprotocol.io`) as `com.queenmq/queen`: a remote Streamable HTTP server at
`https://queenmq.com/mcp`. The `com.queenmq` namespace is proven by a DNS TXT record on
`queenmq.com`. Publish after the server is live, because the registry requires a remote server to be
reachable at its URL.

1. Install the publisher: `brew install mcp-publisher` (Homebrew had 1.8.1 on 2026-10-03).

2. Make an Ed25519 key with OpenSSL 3. The macOS `/usr/bin/openssl` is LibreSSL and fails with
   `Algorithm Ed25519 not found`, so use Homebrew's. Keep the key out of the repository:

   ```bash
   OPENSSL=/opt/homebrew/opt/openssl@3/bin/openssl
   KEY=~/.config/mcp-publisher/queenmq.com.pem
   mkdir -p ~/.config/mcp-publisher
   $OPENSSL genpkey -algorithm Ed25519 -out "$KEY" && chmod 600 "$KEY"
   ```

3. Print the TXT record:

   ```bash
   PUBLIC_KEY="$($OPENSSL pkey -in "$KEY" -pubout -outform DER | tail -c 32 | base64)"
   echo "queenmq.com. IN TXT \"v=MCPv1; k=ed25519; p=${PUBLIC_KEY}\""
   ```

4. Add it in Cloudflare (zone `queenmq.com`, DNS, Records): type `TXT`, name `@`, content
   `v=MCPv1; k=ed25519; p=<PUBLIC_KEY>` without the quotes. It must sit on the apex, never under a
   selector such as `_mcp-auth`; other TXT records on the apex can stay. Wait until
   `dig +short TXT queenmq.com` shows it.

5. Log in. The token is saved in `~/.config/mcp-publisher/token.json`:

   ```bash
   PRIVATE_KEY="$($OPENSSL pkey -in "$KEY" -noout -text | grep -A3 "priv:" | tail -n +2 | tr -d ' :\n')"
   mcp-publisher login dns --domain queenmq.com --private-key "${PRIVATE_KEY}"
   ```

6. Validate and publish, from the repository root:

   ```bash
   cd mcp
   mcp-publisher validate
   mcp-publisher publish
   ```

7. Check the entry:
   `curl -s "https://registry.modelcontextprotocol.io/v0.1/servers?search=com.queenmq/queen"`.

8. Later changes. A published version is immutable, so any change to `server.json` (description,
   icons, URL) needs a higher `version` and a new publish. Log in again first if the publisher
   reports `Invalid or expired Registry JWT token`. Keep the TXT record while you publish; if you
   rotate the key, delete the old record, because a stale one is tried first and fails the login.
   The registry is still a preview and may reset its data: if the entry disappears, publish again.

9. GitHub and VS Code. GitHub's MCP Registry (`github.com/mcp`) is what the `@mcp` search in VS
   Code's Extensions view shows, and it takes its entries from the official registry. Adding a new
   server there is still a manual curation step (a GitHub maintainer said so in May 2026), so once
   `com.queenmq/queen` is in the official registry, ask GitHub to include it: GitHub's blog post on
   publishing says to email `partnerships@github.com`. After that, each new version syncs from the
   official registry by itself.
