// Package main is a thin entry point that exposes the cobra command tree as a
// `queenctl`-named binary when installed with:
//
//	go install github.com/smartpricing/queen/clients/client-cli/cmd/queenctl@latest
//
// The default `go install ...clients/client-cli@latest` would otherwise
// produce a binary named `client-cli` (Go derives the binary name from the
// last path segment of the main package). This subpackage is the canonical
// `go install` target so users always get the documented `queenctl` name
// without having to rename anything.
package main

import "github.com/smartpricing/queen/clients/client-cli/cmd"

func main() {
	cmd.Execute()
}
