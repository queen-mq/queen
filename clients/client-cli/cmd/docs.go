package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// The website is organised in sections (start/, use/, deploy/, reference/,
// internals/, benchmarks/). It used to be flat .html files at the root, and
// those old URLs still resolve, but only through the 301 table in
// webdoc/dist/_redirects, which is a compatibility shim kept for URLs already
// indexed by search engines rather than a contract this CLI should link
// against. Every URL below is the canonical target that table points at. The
// full page list with descriptions is published at https://queenmq.com/llms.txt.
const docsSite = "https://queenmq.com"

// Pointers written for an AI agent rather than a browser. The brief is
// self-contained and about 30 KB, which fits the input ceiling of a fetch
// pipeline; llms-full.txt exists too, but it is roughly fifty times larger and
// most pipelines truncate it without reporting that they did, so it is
// deliberately not advertised here.
const (
	docsBriefURL = docsSite + "/llms-brief.txt"
	docsIndexURL = docsSite + "/llms.txt"
)

// The curated front page: one entry per thing somebody who has just typed
// queenctl is likely to want next.
var docsLinks = []struct{ Title, URL string }{
	{"Quickstart", docsSite + "/start/quickstart/"},
	{"Data model", docsSite + "/use/model/"},
	{"Clients", docsSite + "/use/"},
	{"HTTP API", docsSite + "/reference/http/"},
	{"queenctl", docsSite + "/reference/queenctl/"},
	{"Self-hosting", docsSite + "/deploy/"},
	{"Operations", docsSite + "/deploy/operations/"},
	{"Dashboard", docsSite + "/deploy/dashboard/"},
	{"PostgreSQL / sizing", docsSite + "/deploy/postgres/"},
	{"Internals", docsSite + "/internals/"},
	{"Benchmarks", docsSite + "/benchmarks/"},
}

// Short names accepted by --topic. The keys the flat site used are all kept, so
// `--topic concepts` still works and now lands on the page that content moved
// to. Anything not listed here is treated as a site path, which keeps the
// hundred-odd pages this table does not enumerate reachable.
var docsTopics = map[string]string{
	"architecture": "/internals/",
	"benchmarks":   "/benchmarks/",
	"cli":          "/reference/queenctl/",
	"clients":      "/use/",
	"compare":      "/start/compare/",
	"concepts":     "/use/model/",
	"dashboard":    "/deploy/dashboard/",
	"deploy":       "/deploy/",
	"http-api":     "/reference/http/",
	"internals":    "/internals/",
	"kv":           "/use/kv/",
	"limits":       "/reference/limits/",
	"model":        "/use/model/",
	"operations":   "/deploy/operations/",
	"queenctl":     "/reference/queenctl/",
	"quickstart":   "/start/quickstart/",
	"reference":    "/reference/",
	"server":       "/deploy/",
	"sizing":       "/deploy/postgres/",
	"streams":      "/use/streams/",
	"timers":       "/use/timers/",
}

// docsTarget turns a --topic value into a URL. Section-style, with the trailing
// slash: the site emits directory-style pages and its asset server answers the
// slashless form with a 307 to the slashed one, so a link written without it
// costs every caller a second request.
func docsTarget(topic string) string {
	t := strings.Trim(strings.TrimSpace(topic), "/")
	t = strings.TrimSuffix(t, ".html") // muscle memory from the flat site
	t = strings.Trim(t, "/")
	if t == "" {
		return docsSite + "/"
	}
	if path, ok := docsTopics[strings.ToLower(t)]; ok {
		return docsSite + path
	}
	return docsSite + "/" + t + "/"
}

var docsCmd = &cobra.Command{
	Use:   "docs",
	Short: "Print Queen MQ documentation links / open the website",
	Long: `Prints a curated list of pointers to https://queenmq.com.

Pass --open to launch the website in your default browser, --topic <name> to
deep-link a page or section (a short name such as quickstart or http-api, or
any site path such as reference/http/pop), and --llms to print the summary
published for AI agents instead of the human-facing list.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		open, _ := cmd.Flags().GetBool("open")
		topic, _ := cmd.Flags().GetString("topic")
		llms, _ := cmd.Flags().GetBool("llms")

		switch {
		case llms:
			if open {
				return openBrowser(docsBriefURL)
			}
			if quiet() {
				fmt.Fprintln(stdout(), docsBriefURL)
				fmt.Fprintln(stdout(), docsIndexURL)
				return nil
			}
			fmt.Fprintln(stdout(), "Queen MQ documentation for AI agents")
			fmt.Fprintln(stdout(), "------------------------------------")
			fmt.Fprintf(stdout(), "%-22s %s\n", "Summary (~30 KB)", docsBriefURL)
			fmt.Fprintf(stdout(), "%-22s %s\n", "Page index", docsIndexURL)
			return nil
		case topic != "":
			target := docsTarget(topic)
			if open {
				return openBrowser(target)
			}
			fmt.Fprintln(stdout(), target)
			return nil
		case open:
			return openBrowser(docsSite + "/")
		}

		if !quiet() {
			fmt.Fprintln(stdout(), "Queen MQ documentation")
			fmt.Fprintln(stdout(), "----------------------")
		}
		for _, l := range docsLinks {
			fmt.Fprintf(stdout(), "%-22s %s\n", l.Title, l.URL)
		}
		if !quiet() {
			fmt.Fprintln(stdout())
			fmt.Fprintln(stdout(), "Pass --open to launch in your browser, --topic <name> to deep-link")
			fmt.Fprintln(stdout(), "(quickstart, http-api, internals, or any site path such as")
			fmt.Fprintln(stdout(), "reference/http/pop), or --llms for the AI-agent summary.")
		}
		return nil
	},
}

func init() {
	docsCmd.Flags().Bool("open", false, "open the target in the default browser")
	docsCmd.Flags().String("topic", "", "deep-link topic or site path (e.g. quickstart, http-api, reference/http/pop)")
	docsCmd.Flags().Bool("llms", false, "print the documentation URLs published for AI agents")
	docsCmd.MarkFlagsMutuallyExclusive("topic", "llms")
	rootCmd.AddCommand(docsCmd)
}
