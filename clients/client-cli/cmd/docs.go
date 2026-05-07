package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var docsCmd = &cobra.Command{
	Use:   "docs",
	Short: "Print Queen MQ documentation links / open the website",
	Long: `Prints a curated list of pointers to https://queenmq.com plus the
in-repo developer guide. Pass --open to launch the website in your default
browser.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		links := []struct{ Title, URL string }{
			{"Quickstart", "https://queenmq.com/quickstart.html"},
			{"Concepts", "https://queenmq.com/concepts.html"},
			{"Architecture", "https://queenmq.com/architecture.html"},
			{"HTTP API", "https://queenmq.com/http-api.html"},
			{"Clients", "https://queenmq.com/clients.html"},
			{"Server / Operations", "https://queenmq.com/server.html"},
			{"Dashboard", "https://queenmq.com/dashboard.html"},
			{"Benchmarks", "https://queenmq.com/benchmarks.html"},
			{"Sizing", "https://queenmq.com/sizing.html"},
		}
		open, _ := cmd.Flags().GetBool("open")
		topic, _ := cmd.Flags().GetString("topic")
		target := "https://queenmq.com/"
		if topic != "" {
			target = "https://queenmq.com/" + topic + ".html"
		}
		if open {
			return openBrowser(target)
		}
		fmt.Fprintln(stdout(), "Queen MQ documentation")
		fmt.Fprintln(stdout(), "----------------------")
		for _, l := range links {
			fmt.Fprintf(stdout(), "%-22s %s\n", l.Title, l.URL)
		}
		fmt.Fprintln(stdout())
		fmt.Fprintln(stdout(), "Pass --open to launch in your browser, or --topic <name> to deep-link.")
		return nil
	},
}

func init() {
	docsCmd.Flags().Bool("open", false, "open queenmq.com in the default browser")
	docsCmd.Flags().String("topic", "", "deep-link topic (e.g. quickstart, http-api)")
	rootCmd.AddCommand(docsCmd)
}
