package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// Cobra ships an auto-generated completion command. We wrap it to give it a
// nicer help string and keep all command definitions in this package for
// discoverability.
var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate shell completion scripts",
	Long: `Generate the autocompletion script for queenctl.

Install in your current shell with one of:

  bash:        source <(queenctl completion bash)
  zsh:         source <(queenctl completion zsh)
  fish:        queenctl completion fish | source
  powershell:  queenctl completion powershell | Out-String | Invoke-Expression

To persist completion across sessions, write to your shell's completion
directory (varies per OS); 'make completion' in the source tree will generate
all four scripts under ./completions/.`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "bash":
			return rootCmd.GenBashCompletionV2(os.Stdout, true)
		case "zsh":
			return rootCmd.GenZshCompletion(os.Stdout)
		case "fish":
			return rootCmd.GenFishCompletion(os.Stdout, true)
		case "powershell":
			return rootCmd.GenPowerShellCompletionWithDesc(os.Stdout)
		}
		return nil
	},
}

func init() {
	// Replace the stock completion command (cobra adds it automatically) with
	// our annotated wrapper. Cobra exposes the field directly.
	rootCmd.CompletionOptions.HiddenDefaultCmd = true
	rootCmd.AddCommand(completionCmd)
}
