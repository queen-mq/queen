package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"

	"github.com/smartpricing/queen/clients/client-cli/internal/auth"
	"github.com/smartpricing/queen/clients/client-cli/internal/config"
	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var (
	loginMethod   string
	loginUsername string
	loginPassword string
	loginToken    string
	loginContext  string
	loginNoOpen   bool
)

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate against the broker / proxy and store a bearer token",
	Long: `Authenticate and persist a bearer credential for the active context.

Methods:
  --method token            paste a JWT or a qk_ cluster API key
  --method password         POST /auth/login on the proxy (default if -u set)
  --method google           open the browser to the proxy's Google OAuth flow
  --method github           open the browser to the proxy's GitHub OAuth flow

The token is stored in the OS keychain by default and the config file holds
only a 'keychain://<context>' reference. Pass --context to bind to a
specific context, otherwise the active context is used.

The password flow stores the proxy's session JWT, which expires with
QUEEN_PROXY_JWT_TTL_S (24h by default). The browser flows can only hand back
the short bearer from /auth/session-token (15 minutes), because the session
cookie is httpOnly and the proxy accepts no loopback redirect target. For
unattended use create a cluster API key and store it with
'--method token' instead - it does not expire.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		f, err := config.Load(gf.configPath)
		if err != nil {
			return clierr.Userf("load config: %v", err)
		}
		ctxName := loginContext
		if ctxName == "" {
			ctxName = gf.contextName
		}
		if ctxName == "" {
			ctxName = f.CurrentContext
		}
		if ctxName == "" {
			return clierr.Userf("no context selected; run 'queenctl config set-context' first")
		}
		c := f.FindContext(ctxName)
		if c == nil {
			return clierr.Userf("context %q does not exist", ctxName)
		}
		if c.Server == "" && gf.server == "" {
			return clierr.Userf("context %q has no server URL", ctxName)
		}
		serverURL := c.Server
		if gf.server != "" {
			serverURL = gf.server
		}

		method := loginMethod
		if method == "" {
			switch {
			case loginToken != "":
				method = "token"
			case loginUsername != "":
				method = "password"
			default:
				method = "token"
			}
		}

		var jwt string
		switch method {
		case "token":
			jwt, err = readToken(cmd.InOrStdin())
			if err != nil {
				return clierr.User(err)
			}
		case "password":
			jwt, err = passwordFlow(serverURL)
			if err != nil {
				if errors.Is(err, auth.ErrInvalidCredentials) {
					return clierr.Auth(err)
				}
				return clierr.Server(err)
			}
		case auth.ProviderGoogle, auth.ProviderGitHub:
			jwt, err = oauthFlow(serverURL, method)
			if err != nil {
				return clierr.Server(err)
			}
		default:
			return clierr.Userf("unknown --method %q", method)
		}

		if jwt == "" {
			return clierr.Userf("no token captured")
		}
		ref, err := config.StoreToken(ctxName, jwt)
		if err != nil {
			// Fall back to literal so the user can still log in on systems
			// without a keychain (CI containers, headless boxes).
			fmt.Fprintf(os.Stderr, "queenctl: keychain unavailable, falling back to literal storage: %v\n", err)
			ref = "literal:" + jwt
		}
		c.TokenRef = ref
		f.SetContext(*c)
		if err := config.Save(gf.configPath, f); err != nil {
			return clierr.Userf("save config: %v", err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "logged in as context=%s server=%s (token stored in %s)\n",
				ctxName, serverURL, ref)
		}
		return nil
	},
}

var logoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Forget the stored JWT for the active context",
	RunE: func(cmd *cobra.Command, args []string) error {
		f, err := config.Load(gf.configPath)
		if err != nil {
			return clierr.Userf("load config: %v", err)
		}
		ctxName := loginContext
		if ctxName == "" {
			ctxName = gf.contextName
		}
		if ctxName == "" {
			ctxName = f.CurrentContext
		}
		if ctxName == "" {
			return clierr.Userf("no context selected")
		}
		c := f.FindContext(ctxName)
		if c == nil {
			return clierr.Userf("no such context %q", ctxName)
		}
		_ = config.DeleteToken(ctxName)
		c.TokenRef = ""
		f.SetContext(*c)
		if err := config.Save(gf.configPath, f); err != nil {
			return clierr.Userf("save config: %v", err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "logged out from %s\n", ctxName)
		}
		return nil
	},
}

func readToken(in any) (string, error) {
	if loginToken != "" {
		return strings.TrimSpace(loginToken), nil
	}
	fmt.Fprint(os.Stderr, "Paste JWT or API key and press Enter: ")
	if term.IsTerminal(int(syscall.Stdin)) {
		bb, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(bb)), nil
	}
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func passwordFlow(serverURL string) (string, error) {
	user := loginUsername
	if user == "" {
		fmt.Fprint(os.Stderr, "Email: ")
		r := bufio.NewReader(os.Stdin)
		line, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		user = strings.TrimSpace(line)
	}
	pw := loginPassword
	if pw == "" {
		fmt.Fprint(os.Stderr, "Password: ")
		bb, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		pw = string(bb)
	}
	return auth.PasswordLogin(serverURL, user, pw, gf.insecure)
}

// oauthFlow drives a browser sign-in for provider. The proxy sets the session
// as an httpOnly cookie on the browser, so the CLI cannot capture it: the
// authorize URL carries next=/auth/session-token, which leaves the browser on
// the JSON document holding a bearer minted from that session.
func oauthFlow(serverURL, provider string) (string, error) {
	ok, err := auth.IsProviderEnabled(serverURL, provider, gf.insecure)
	if err != nil {
		return "", fmt.Errorf("check %s config: %w", provider, err)
	}
	if !ok {
		return "", fmt.Errorf("%s login is not enabled on this proxy (set %s_CLIENT_ID and %s_CLIENT_SECRET on it)",
			provider, strings.ToUpper(provider), strings.ToUpper(provider))
	}
	authURL, err := auth.AuthorizeURL(serverURL, provider)
	if err != nil {
		return "", err
	}
	tokenURL, err := auth.SessionTokenURL(serverURL)
	if err != nil {
		return "", err
	}
	if !loginNoOpen {
		_ = openBrowser(authURL)
	}
	fmt.Fprintf(os.Stderr, `
queenctl: %s login

  1. Finish the sign-in in your browser. The proxy then lands you on
     %s, which prints {"token": "...", "expires_in": ...}.
  2. Copy the value of "token" and paste it below.

The pasted bearer is short-lived (15 minutes). For unattended use store a
cluster API key with 'queenctl login --method token' instead.

Authorize URL (open if your browser didn't): %s

Paste token: `, provider, tokenURL, authURL)
	if term.IsTerminal(int(syscall.Stdin)) {
		bb, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(bb)), nil
	}
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func openBrowser(u string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	case "linux":
		cmd = exec.Command("xdg-open", u)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", u)
	default:
		return fmt.Errorf("unsupported platform")
	}
	return cmd.Start()
}

func init() {
	loginCmd.Flags().StringVar(&loginMethod, "method", "", "auth method: token | password | google | github")
	loginCmd.Flags().StringVarP(&loginUsername, "user", "u", "", "email (proxy password flow)")
	loginCmd.Flags().StringVar(&loginPassword, "password", "", "password (avoid: prefer interactive prompt)")
	loginCmd.Flags().StringVar(&loginToken, "token", "", "JWT or qk_ API key to store (skips prompt)")
	loginCmd.Flags().StringVar(&loginContext, "context", "", "target context (default: active)")
	loginCmd.Flags().BoolVar(&loginNoOpen, "no-browser", false, "do not auto-open the browser for the OAuth flows")
	logoutCmd.Flags().StringVar(&loginContext, "context", "", "target context (default: active)")
	rootCmd.AddCommand(loginCmd, logoutCmd)
}
