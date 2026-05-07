package cmd

import (
	"fmt"

	"github.com/smartpricing/queen/client-cli/internal/config"
	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	"github.com/smartpricing/queen/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage queenctl contexts (~/.queen/config.yaml)",
	Long: `Inspect and edit the queenctl configuration.

Multiple contexts can be configured side-by-side and switched per-invocation
with --context, or globally with 'queenctl config use-context'. Tokens are
stored in the OS keychain whenever possible; the config file itself contains
only a reference, not the secret.`,
}

var configViewCmd = &cobra.Command{
	Use:   "view",
	Short: "Print the resolved configuration as YAML",
	RunE: func(cmd *cobra.Command, args []string) error {
		f, err := config.Load(gf.configPath)
		if err != nil {
			return clierr.Userf("load: %v", err)
		}
		r, err := rendererFor(output.View{}, stdout())
		if err != nil {
			return err
		}
		if r.Format == output.FormatTable {
			r.Format = output.FormatYAML
		}
		return r.Render(f)
	},
}

var configCurrentCmd = &cobra.Command{
	Use:     "current-context",
	Short:   "Print the active context name",
	Aliases: []string{"current"},
	RunE: func(cmd *cobra.Command, args []string) error {
		f, err := config.Load(gf.configPath)
		if err != nil {
			return clierr.Userf("load: %v", err)
		}
		if f.CurrentContext == "" {
			return clierr.Empty("no current-context set")
		}
		fmt.Fprintln(stdout(), f.CurrentContext)
		return nil
	},
}

var configGetContextsCmd = &cobra.Command{
	Use:     "get-contexts",
	Short:   "List configured contexts",
	Aliases: []string{"list", "ls"},
	RunE: func(cmd *cobra.Command, args []string) error {
		f, err := config.Load(gf.configPath)
		if err != nil {
			return clierr.Userf("load: %v", err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "CURRENT", Format: func(v any) string {
					if b, ok := v.(bool); ok && b {
						return "*"
					}
					return ""
				}},
				{Header: "NAME"},
				{Header: "SERVER"},
				{Header: "AUTH", Path: "auth"},
				{Header: "INSECURE", Wide: true},
			},
			RowsFrom: func(_ any) []any {
				rows := make([]any, 0, len(f.Contexts))
				for _, c := range f.Contexts {
					row := map[string]any{
						"current":  c.Name == f.CurrentContext,
						"name":     c.Name,
						"server":   c.Server,
						"auth":     authLabel(c.TokenRef),
						"insecure": c.Insecure,
					}
					rows = append(rows, row)
				}
				return rows
			},
		}
		r, err := rendererFor(view, stdout())
		if err != nil {
			return err
		}
		return r.Render(nil)
	},
}

func authLabel(ref string) string {
	switch {
	case ref == "":
		return "none"
	case ref == "env":
		return "env"
	case len(ref) >= 11 && ref[:11] == "keychain://":
		return "keychain"
	case len(ref) >= 7 && ref[:7] == "file://":
		return "file"
	case len(ref) >= 8 && ref[:8] == "literal:":
		return "literal"
	default:
		return "literal"
	}
}

var (
	setCtxServer      string
	setCtxToken       string
	setCtxTokenRef    string
	setCtxInsecure    bool
	setCtxNoKeychain  bool
)

var configSetContextCmd = &cobra.Command{
	Use:   "set-context <name>",
	Short: "Create or update a context",
	Long: `Upsert a named context. If --token is provided it is stored in the
OS keychain by default and the config file holds only a 'keychain://<name>'
reference. Pass --no-keychain to fall back to a 'literal:' reference (less
secure - useful for CI bootstrap), or use --token-ref to set a custom
reference (env, file:///abs/path, keychain://other).`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		f, err := config.Load(gf.configPath)
		if err != nil {
			return clierr.Userf("load: %v", err)
		}
		c := f.FindContext(name)
		if c == nil {
			c = &config.Context{Name: name}
		}
		if cmd.Flags().Changed("server") {
			c.Server = setCtxServer
		}
		if cmd.Flags().Changed("insecure") {
			c.Insecure = setCtxInsecure
		}
		if cmd.Flags().Changed("token-ref") {
			c.TokenRef = setCtxTokenRef
		}
		if cmd.Flags().Changed("token") {
			if setCtxNoKeychain {
				c.TokenRef = "literal:" + setCtxToken
			} else {
				ref, err := config.StoreToken(name, setCtxToken)
				if err != nil {
					return clierr.Auth(err)
				}
				c.TokenRef = ref
			}
		}
		f.SetContext(*c)
		if f.CurrentContext == "" {
			f.CurrentContext = name
		}
		if err := config.Save(gf.configPath, f); err != nil {
			return clierr.Userf("save: %v", err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "context %q updated\n", name)
		}
		return nil
	},
}

var configUseContextCmd = &cobra.Command{
	Use:   "use-context <name>",
	Short: "Set the default context",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		f, err := config.Load(gf.configPath)
		if err != nil {
			return clierr.Userf("load: %v", err)
		}
		if f.FindContext(name) == nil {
			return clierr.Userf("no such context %q", name)
		}
		f.CurrentContext = name
		if err := config.Save(gf.configPath, f); err != nil {
			return clierr.Userf("save: %v", err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "switched to context %q\n", name)
		}
		return nil
	},
}

var configDeleteContextCmd = &cobra.Command{
	Use:     "delete-context <name>",
	Short:   "Remove a context",
	Aliases: []string{"rm"},
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		f, err := config.Load(gf.configPath)
		if err != nil {
			return clierr.Userf("load: %v", err)
		}
		c := f.FindContext(name)
		if c == nil {
			return clierr.Userf("no such context %q", name)
		}
		// Best-effort keychain cleanup if the ref points to one.
		if c.TokenRef == "keychain://"+name {
			_ = config.DeleteToken(name)
		}
		f.DeleteContext(name)
		if err := config.Save(gf.configPath, f); err != nil {
			return clierr.Userf("save: %v", err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "context %q removed\n", name)
		}
		return nil
	},
}

func init() {
	configSetContextCmd.Flags().StringVar(&setCtxServer, "server", "", "broker URL")
	configSetContextCmd.Flags().StringVar(&setCtxToken, "token", "", "bearer token (stored in keychain by default)")
	configSetContextCmd.Flags().StringVar(&setCtxTokenRef, "token-ref", "", "token reference: env | keychain://id | file:///abs | literal:value")
	configSetContextCmd.Flags().BoolVar(&setCtxInsecure, "insecure", false, "skip TLS verification")
	configSetContextCmd.Flags().BoolVar(&setCtxNoKeychain, "no-keychain", false, "store --token literally in the config file (less secure)")

	configCmd.AddCommand(configViewCmd, configCurrentCmd, configGetContextsCmd,
		configSetContextCmd, configUseContextCmd, configDeleteContextCmd)
	rootCmd.AddCommand(configCmd)
}
