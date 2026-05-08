// Package streams_integration_test contains live-broker integration tests
// for the Go streaming SDK. Mirror of the JS test-v2/stream/* and Python
// tests/streams_integration/. Tests skip automatically when QUEEN_URL is
// unset to "skip" or the broker is unreachable.
package streams_integration

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

var (
	testClient *queen.Queen
	queenURL   string
)

// TestMain spins up a single Queen client used by every test in this package.
// Skips the whole suite when the broker is unreachable.
func TestMain(m *testing.M) {
	queenURL = os.Getenv("QUEEN_URL")
	if queenURL == "" {
		queenURL = "http://localhost:6632"
	}
	if queenURL == "skip" {
		fmt.Println("[streams-integration] QUEEN_URL=skip, skipping suite")
		os.Exit(0)
	}
	if !reachable(queenURL) {
		fmt.Printf("[streams-integration] Queen at %s not reachable, skipping suite\n", queenURL)
		os.Exit(0)
	}
	var err error
	testClient, err = queen.New(queenURL)
	if err != nil {
		fmt.Printf("Failed to create Queen client: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = testClient.Close(context.Background())
	os.Exit(code)
}

func reachable(url string) bool {
	c := &http.Client{Timeout: 2 * time.Second}
	r, err := c.Get(url + "/health")
	if err != nil {
		return false
	}
	defer r.Body.Close()
	return r.StatusCode >= 200 && r.StatusCode < 300
}

var nameCounter int64

// mkName builds a unique test queue / query name. Mirror of JS mkName.
func mkName(testName, suffix string) string {
	atomic.AddInt64(&nameCounter, 1)
	stamp := fmt.Sprintf("%x", time.Now().UnixMilli())
	slug := strings.ToLower(strings.ReplaceAll(testName, "_", "-"))
	tail := ""
	if suffix != "" {
		tail = "-" + suffix
	}
	return fmt.Sprintf("test-stream-%s-%s-%d%s", slug, stamp, nameCounter, tail)
}
