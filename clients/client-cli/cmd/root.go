// Package cmd defines the queenctl Cobra command tree. Each command lives
// in its own file in this package and registers itself in init() via
// rootCmd.AddCommand.
package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/smartpricing/queen/client-cli/internal/config"
	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	"github.com/smartpricing/queen/client-cli/internal/output"
	"github.com/smartpricing/queen/client-cli/internal/sdk"
	"github.com/spf13/cobra"
)

// Build-time variables, populated via -ldflags by the Makefile / goreleaser.
var (
	BuildVersion = "dev"
	BuildCommit  = "none"
	BuildDate    = "unknown"
)

// Global flags - bound to rootCmd.PersistentFlags(). Read via the helpers
// below so commands stay decoupled from the cobra plumbing.
type globalFlags struct {
	contextName string
	server      string
	token       string
	output      string
	noColor     bool
	color       bool
	insecure    bool
	quiet       bool
	noHeaders   bool
	configPath  string
}

var gf globalFlags

var rootCmd = &cobra.Command{
	Use:   "queenctl",
	Short: "Operator CLI for Queen MQ",
	Long: `queenctl is the operator-grade command-line client for Queen MQ
(https://queenmq.com).

It exposes the broker's data plane (push, pop, ack, transaction, lease) and
admin plane (queues, partitions, consumer groups, DLQ, traces, maintenance,
metrics, analytics) through a single static binary built on top of the
official Go SDK.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	Version:       BuildVersion,
}

// Execute is the entry point invoked from main. It runs rootCmd and maps
// errors to queenctl's exit-code convention.
func Execute() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	rootCmd.SetContext(ctx)
	err := rootCmd.ExecuteContext(ctx)
	clierr.Exit(err)
}

func init() {
	rootCmd.SetVersionTemplate(fmt.Sprintf("queenctl %s (commit %s, built %s)\n",
		BuildVersion, BuildCommit, BuildDate))

	pf := rootCmd.PersistentFlags()
	pf.StringVar(&gf.contextName, "context", "", "named context from ~/.queen/config.yaml (overrides current-context)")
	pf.StringVar(&gf.server, "server", "", "broker URL (overrides context; same as $QUEEN_SERVER)")
	pf.StringVar(&gf.token, "token", "", "bearer token (overrides context; same as $QUEEN_TOKEN)")
	pf.StringVarP(&gf.output, "output", "o", "", "output format: table|json|ndjson|yaml|wide|jsonpath=<expr>")
	pf.BoolVar(&gf.noColor, "no-color", false, "disable ANSI color")
	pf.BoolVar(&gf.color, "color", false, "force ANSI color")
	pf.BoolVar(&gf.insecure, "insecure", false, "skip TLS verification")
	pf.BoolVarP(&gf.quiet, "quiet", "q", false, "suppress non-essential output")
	pf.BoolVar(&gf.noHeaders, "no-headers", false, "omit table headers")
	pf.StringVar(&gf.configPath, "config", "", "path to config file (default ~/.queen/config.yaml)")
}

// resolved loads the config, applies env + flag overrides, and returns the
// effective context. Returns clierr-typed errors.
func resolved() (*config.Resolved, error) {
	f, err := config.Load(gf.configPath)
	if err != nil {
		return nil, clierr.Userf("load config: %v", err)
	}
	var insec *bool
	if rootCmd.PersistentFlags().Changed("insecure") {
		insec = &gf.insecure
	}
	r, err := config.Resolve(f, config.Overrides{
		Context:  gf.contextName,
		Server:   gf.server,
		Token:    gf.token,
		Insecure: insec,
	})
	if err != nil {
		return nil, clierr.User(err)
	}
	return r, nil
}

// newClient is the standard preamble for any command that talks to a broker.
// It returns a connected sdk.Client and a deferable cleanup.
func newClient() (*sdk.Client, func(), error) {
	r, err := resolved()
	if err != nil {
		return nil, func() {}, err
	}
	c, err := sdk.New(r)
	if err != nil {
		return nil, func() {}, clierr.Server(err)
	}
	cleanup := func() {
		_ = c.Close(context.Background())
	}
	return c, cleanup, nil
}

// rendererFor builds an output.Renderer from the current global flags and
// the per-command view declaration. The default format is table for TTYs
// and json for pipes (so tail | push works without -o ndjson plumbing).
func rendererFor(view output.View, w io.Writer) (*output.Renderer, error) {
	format, jsonpath, err := output.ParseFormat(gf.output)
	if err != nil {
		return nil, clierr.User(err)
	}
	if format == "" {
		if output.IsTTY(w) {
			format = output.FormatTable
		} else {
			format = output.FormatJSON
		}
	}
	var colorOverride *bool
	if gf.noColor {
		f := false
		colorOverride = &f
	} else if gf.color {
		t := true
		colorOverride = &t
	}
	return &output.Renderer{
		Format:    format,
		JSONPath:  jsonpath,
		Color:     output.ColorEnabled(w, colorOverride),
		NoHeaders: gf.noHeaders,
		View:      view,
		Out:       w,
	}, nil
}

// stdout is the canonical writer; commands take it indirectly so tests can
// swap it.
func stdout() io.Writer { return os.Stdout }

// quiet returns true when -q was passed.
func quiet() bool { return gf.quiet }
