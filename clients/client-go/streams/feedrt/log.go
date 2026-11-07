package feedrt

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// The runner's logs, as the JS SDK's: off unless QUEEN_CLIENT_LOG is set,
// and always on stderr.
var logOn = func() bool {
	switch strings.ToLower(os.Getenv("QUEEN_CLIENT_LOG")) {
	case "true", "1", "debug", "info", "warn":
		return true
	}
	return false
}()

func logAt(level, op string, fields map[string]interface{}) {
	if !logOn {
		return
	}
	b, _ := jsv.Marshal(fields)
	fmt.Fprintf(os.Stderr, "[%s] [%s] [%s] %s\n", time.Now().UTC().Format(time.RFC3339Nano), level, op, b)
}

func logInfo(op string, fields map[string]interface{})  { logAt("INFO", op, fields) }
func logWarn(op string, fields map[string]interface{})  { logAt("WARN", op, fields) }
func logError(op string, fields map[string]interface{}) { logAt("ERROR", op, fields) }
