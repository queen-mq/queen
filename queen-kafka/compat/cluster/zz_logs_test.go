package cluster

import "testing"

// The last assertion of the run, and the one nothing else can make.
//
// Four of the design's failure modes are correct-looking on the wire and loud
// only in the facade's own log: a commit that was FENCED off (the zombie
// writer), a node that LOST its id to another process, a registry that could
// not be REACHED, and a facade that is ALONE in its cluster because the other
// nodes are writing into a different prefix or a different Queen tenant. A
// suite that only reads responses would pass through all four.
//
// So every WARN and ERROR a facade emitted while this suite ran is a failure
// unless it is on the closed allow-list in helpers_test.go. Boot noise from
// before the first test is excluded by the byte offsets TestMain recorded: it
// is not this suite's to judge, and a facade that ticks once before its peers
// have registered legitimately warns.
//
// The file is named to sort last: Go runs a package's tests in the order their
// files sort, and this one has to see what the others caused.
func TestZZFacadeLogsHaveNoUnexpectedWarnings(t *testing.T) {
	scanFacadeLogs(t)
}
