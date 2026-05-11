package cmd

import (
	"strings"
	"time"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/internal/timefmt"
	queen "github.com/smartpricing/queen/clients/client-go"
)

// epochZero is used to express "seek to beginning" since the server only
// supports {toEnd: bool} or {timestamp: RFC3339}. Seeking to the unix
// epoch effectively rewinds the cursor before any real message.
const epochZero = "1970-01-01T00:00:00Z"

// parseSeekTo converts the user-facing --to argument into the wire-shape
// expected by /api/v1/consumer-groups/:cg/queues/:q/seek. The argument is
// case-insensitive.
//
//	"now" / "end" / "latest"           -> {ToEnd: true}
//	"beginning" / "earliest" / "start" -> {Timestamp: epochZero}
//	"5m ago" / "2h ago" / "30s" / ...  -> relative timestamp parsed by timefmt
//	RFC3339 / "YYYY-MM-DD" / unix sec  -> absolute timestamp
//
// Returns a clierr.User error if the argument is empty or unparseable.
func parseSeekTo(arg string) (queen.SeekConsumerGroupOptions, error) {
	a := strings.TrimSpace(arg)
	if a == "" {
		return queen.SeekConsumerGroupOptions{}, clierr.Userf("--to is required")
	}
	switch strings.ToLower(a) {
	case "now", "end", "latest":
		return queen.SeekConsumerGroupOptions{ToEnd: true}, nil
	case "beginning", "earliest", "start":
		return queen.SeekConsumerGroupOptions{Timestamp: epochZero}, nil
	}
	t, err := timefmt.Parse(a)
	if err != nil {
		return queen.SeekConsumerGroupOptions{}, clierr.User(err)
	}
	if t.IsZero() {
		return queen.SeekConsumerGroupOptions{Timestamp: epochZero}, nil
	}
	return queen.SeekConsumerGroupOptions{Timestamp: t.UTC().Format(time.RFC3339)}, nil
}
