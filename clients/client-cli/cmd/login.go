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

	"github.com/smartpricing/queen/client-cli/internal/auth"
	"github.com/smartpricing/queen/client-cli/internal/config"
	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
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
	Short: "Authenticate against the broker / proxy and store a JWT",
	Long: `Authenticate and persist a JWT for the active context.

Methods:
  --method token            paste a JWT (works with any IdP / JWKS setup)
  --method password         POST /api/login on the proxy (default if -u set)
  --method google           open the browser to the proxy's Google OAuth flow

The token is stored in the OS keychain by default and the config file holds
only a 'keychain://<context>' reference. Pass --context to bind to a
specific context, otherwise the active context is used.`,
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
		case "google":
			jwt, err = googleFlow(serverURL)
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
	fmt.Fprint(os.Stderr, "Paste JWT and press Enter: ")
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
		fmt.Fprint(os.Stderr, "Username: ")
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

func googleFlow(serverURL string) (string, error) {
	ok, err := auth.IsGoogleEnabled(serverURL)
	if err != nil {
		return "", fmt.Errorf("check google config: %w", err)
	}
	if !ok {
		return "", errors.New("Google auth is not enabled on this proxy (set GOOGLE_CLIENT_ID/SECRET/REDIRECT_URI on the proxy)")
	}
	authURL, err := auth.GoogleAuthorizeURL(serverURL)
	if err != nil {
		return "", err
	}
	if !loginNoOpen {
		_ = openBrowser(authURL)
	}
	fmt.Fprintf(os.Stderr, `
queenctl: Google login

  1. The proxy will set a 'token' cookie in your browser after the OAuth
     callback. The dashboard's "About" panel exposes that JWT on a button
     labelled "Copy CLI token".
  2. If you don't see the panel yet, open your browser DevTools while on
     the dashboard and copy the value of the 'token' cookie.
  3. Paste it below.

Authorize URL (open if your browser didn't): %s

Paste JWT: `, authURL)
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
	loginCmd.Flags().StringVar(&loginMethod, "method", "", "auth method: token | password | google")
	loginCmd.Flags().StringVarP(&loginUsername, "user", "u", "", "username (proxy password flow)")
	loginCmd.Flags().StringVar(&loginPassword, "password", "", "password (avoid: prefer interactive prompt)")
	loginCmd.Flags().StringVar(&loginToken, "token", "", "JWT to store (skips prompt)")
	loginCmd.Flags().StringVar(&loginContext, "context", "", "target context (default: active)")
	loginCmd.Flags().BoolVar(&loginNoOpen, "no-browser", false, "do not auto-open the browser for --method google")
	logoutCmd.Flags().StringVar(&loginContext, "context", "", "target context (default: active)")
	rootCmd.AddCommand(loginCmd, logoutCmd)
}
