package output

import (
	"io"
	"os"

	"github.com/mattn/go-isatty"
)

// IsTTY reports whether w is a terminal that supports interactive output.
func IsTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fd := f.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

// ColorEnabled reports whether ANSI color codes should be emitted, taking
// into account NO_COLOR (https://no-color.org/), CLICOLOR=0 and the explicit
// override flag.
func ColorEnabled(w io.Writer, override *bool) bool {
	if override != nil {
		return *override
	}
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if os.Getenv("CLICOLOR") == "0" {
		return false
	}
	if os.Getenv("CLICOLOR_FORCE") != "" {
		return true
	}
	return IsTTY(w)
}
