package queen

import (
	"errors"
	"fmt"
	"sync"
)

// Conflation, client side (PLAN_CONFLATION.md §4).
//
// Conflation is last-value delivery: a pop of a partition returns only the
// NEWEST visible message and commits everything below it. It is a property of
// the consumer GROUP, declared here and persisted by the broker on first
// registration, after which the stored value wins for every consumer of that
// group (§1.1, §3.3). This client therefore never assumes its own flag took
// effect — it reads the answer off the response.
//
// Two response keys carry that answer, and telling them apart is the whole job
// of this file:
//
//	"conflation":true         the effective policy for this group IS conflating.
//	                          Emitted on every conflating pop including EMPTY
//	                          ones, which is what makes the check below fire on
//	                          the first round trip instead of after a backlog.
//	"conflationConflict":true this request disagreed with the stored value. The
//	                          stored value won; nothing flipped (§3.3).

// ErrConflationUnsupported is returned by Pop and by the consume loop when this
// client asked for conflation and the broker's response did not acknowledge it.
//
// WHY THIS IS AN ERROR AND NOT A WARNING. No SDK in this repo does version or
// capability negotiation, so against a broker older than 1.1.0 the unknown
// `conflation` query param is simply ignored: the pop succeeds, returns the
// whole backlog, and a consumer that asked for "only the newest state" quietly
// grinds through every stale message. The status code, the message shape and
// the ack path all look perfectly healthy. A log line would not be read. So the
// consume loop stops instead (§4, the degrade-loudly blockquote).
//
// Callers can match it with errors.Is.
var ErrConflationUnsupported = errors.New("conflation was requested but this broker did not apply it — requires broker >= 1.1.0")

// conflationConflictSeen gates the conflict warning to one per (queue, group)
// per process (§3.3 item 3). Per process, not per pop: a fleet in the middle of
// a rolling deploy has half its consumers disagreeing with the stored value on
// EVERY pop, and a per-response warning would bury the log it is meant to
// surface — the same reason the broker side rate-limits its own line rather
// than logging per request.
var conflationConflictSeen sync.Map // map[string]struct{}, key = queue + NUL + group

// conflationConflictWarn is the sink for that warning. It is a package variable
// so the test suite can count emissions; production always uses the logger.
var conflationConflictWarn = func(queue, group string) {
	logWarn("conflation.conflict", map[string]interface{}{
		"queue": queue,
		"group": group,
		"message": "this consumer declared a conflation setting that disagrees with the one stored " +
			"for the group; the STORED setting wins and this consumer keeps running. Align the " +
			"declaration, or delete and recreate the consumer group to change its policy.",
	})
}

// noteConflationConflict emits the warning the first time this process sees a
// conflict for this (queue, group), and never again.
func noteConflationConflict(queue, group string) {
	if group == "" {
		group = QueueModeConsumerGroup
	}
	if _, loaded := conflationConflictSeen.LoadOrStore(queue+"\x00"+group, struct{}{}); loaded {
		return
	}
	conflationConflictWarn(queue, group)
}

// checkConflationEcho inspects one pop response. It returns
// ErrConflationUnsupported only for the old-broker case: conflation was
// requested and the response carries NEITHER key.
//
// A conflict is deliberately not an error. It is a 1.1.0 broker answering "the
// group is already registered the other way, my value wins" — during a rolling
// deploy half the fleet sends the flag and half does not, and failing the half
// that is already correct is how a warning-shaped event takes down a service
// (§3.3 Q3; §7.3 E2E-4 requires both consumers to keep working).
func checkConflationEcho(result map[string]interface{}, requested bool, queue, group string) error {
	if conflict, _ := result["conflationConflict"].(bool); conflict {
		noteConflationConflict(queue, group)
		return nil
	}
	if !requested {
		return nil
	}
	if applied, _ := result["conflation"].(bool); applied {
		return nil
	}
	return fmt.Errorf("%w (queue %q, group %q)", ErrConflationUnsupported, queue, group)
}

// conflationTarget labels a pop for the warning registry and the error message.
// A namespace/task pop has no single queue name, so it is keyed on the pair it
// actually addressed.
func conflationTarget(queue, namespace, task string) string {
	if queue != "" {
		return queue
	}
	if namespace != "" || task != "" {
		ns := namespace
		if ns == "" {
			ns = "*"
		}
		tk := task
		if tk == "" {
			tk = "*"
		}
		return ns + "/" + tk
	}
	return "*"
}
