package queen

import (
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Pop autopilot, client side.
//
// The broker owns a controller that sizes a pop from state this client cannot
// see: how many partitions of the (queue, group) are ready, how old their
// oldest ready message is, at what rate messages are arriving. Two knobs are
// under its control — `partitions` (the sweep width) and `batch` (the message
// budget for the sweep).
//
// THE RULE, and it is the only one: an explicit user value is sacred. Autopilot
// applies ONLY to the knobs the user left unset, and it applies to them one by
// one. A consumer that pins `Partitions(1)` and says nothing about batch keeps
// its single-partition claim forever and lets the broker size the batch; the
// pinned dimension is never "adjusted", not even towards a value the controller
// would consider better.
//
// The wire shape follows the conflation precedent (see conflation.go): a client
// that is not engaging autopilot sends the byte-identical request it sent
// before this feature existed.
//
//	autopilot=true      emitted ONLY when at least one of the two knobs is
//	                    being left to the broker. Never as autopilot=false.
//	partitions / batch  OMITTED for the dimensions the broker is choosing,
//	                    sent exactly as before for the ones the user set.
//
// WHAT AN OLD BROKER DOES, and why there is no capability check here. A broker
// older than 1.2 ignores unknown query params: the request succeeds, and the
// two omitted knobs fall back to the SERVER-side defaults (batch 200,
// partitions 1) instead of the old client-side ones. That is a sizing
// difference, not a correctness one — nothing is lost, misordered or delivered
// twice — so unlike conflation (which silently hands a last-value consumer a
// whole backlog, hence ErrConflationUnsupported) this degrades quietly and on
// purpose. Callers who need the old numbers against an old broker set them
// explicitly, or turn autopilot off.

// EnvPopAutopilot is the environment variable that disables pop autopilot for a
// whole process: QUEEN_SDK_POP_AUTOPILOT=off restores the client-side defaults
// this SDK applied before autopilot existed, byte for byte. It is read once, at
// client construction (New), so a single deployment can be rolled back without
// touching code.
//
// "off", "false", "0", "no" and "disabled" all disable it (case-insensitive,
// surrounding space ignored). Every other value, including the empty one,
// leaves autopilot on.
const EnvPopAutopilot = "QUEEN_SDK_POP_AUTOPILOT"

// popAutopilotDisabledByEnv reports whether EnvPopAutopilot asks for the
// pre-autopilot behavior.
func popAutopilotDisabledByEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvPopAutopilot))) {
	case "off", "false", "0", "no", "disabled":
		return true
	}
	return false
}

// popSizing is the batch/partitions half of a pop's query string, for both of
// this SDK's param builders.
//
// IT EXISTS SO THERE IS EXACTLY ONE COPY OF THE EMISSION RULE. pop and consume
// build their query strings separately (QueueBuilder.buildPopParams vs
// ConsumerManager.buildParams) and the two have drifted before — the comment in
// consumer_manager.go is there because of it. A rule with three branches and a
// per-dimension carve-out is precisely the kind that gets copied wrong, so the
// two builders fill this struct and call apply instead of restating it.
type popSizing struct {
	// Batch and MaxPartitions are the USER's values. Zero means unset — the
	// dimension the broker gets to choose — which is why neither builder may
	// substitute a default before filling this in.
	Batch         int
	MaxPartitions int

	// FallbackBatch is the client-side default applied to an unset Batch when
	// autopilot is NOT engaged. The two builders disagree here on purpose: the
	// pop path substitutes PopDefaults.Batch (it always did), while the consume
	// path passes 0 because ConsumerManager.buildParams has always emitted
	// opts.Batch verbatim, defaults having been applied upstream. Byte-identity
	// with the pre-autopilot SDK is the point.
	FallbackBatch int

	// Autopilot is the resolved decision for this call: the builder's own
	// .Autopilot()/Autopilot field if there was one, otherwise the client-wide
	// default (on, unless EnvPopAutopilot turned it off).
	Autopilot bool
}

// apply writes batch, partitions and autopilot into params.
//
// Note the case that looks like an omission and is not: when the user set BOTH
// knobs there is nothing left for the controller to decide, so autopilot=true
// is NOT emitted and the request is byte-identical to the one this SDK sent
// before autopilot existed. Sending the flag anyway would be harmless on the
// broker and dishonest in a packet capture.
func (s popSizing) apply(params url.Values) {
	batchSet := s.Batch > 0
	partitionsSet := s.MaxPartitions > 0

	if s.Autopilot && !(batchSet && partitionsSet) {
		params.Set("autopilot", "true")
		if batchSet {
			params.Set("batch", strconv.Itoa(s.Batch))
		}
		if partitionsSet {
			params.Set("partitions", strconv.Itoa(s.MaxPartitions))
		}
		return
	}

	batch := s.Batch
	if !batchSet {
		batch = s.FallbackBatch
	}
	params.Set("batch", strconv.Itoa(batch))
	// The legacy gate: partitions travels only above 1, because 1 IS the
	// server-side default and a v4-era client never sent it.
	if s.MaxPartitions > 1 {
		params.Set("partitions", strconv.Itoa(s.MaxPartitions))
	}
}

// AutopilotDecision is what the broker chose for one pop, echoed back in the
// response under "autopilot" when the request engaged autopilot. It is
// additive: a broker that does not send it, or a pop that never asked, leaves
// the field nil everywhere it is exposed.
//
// Reading it is optional — the messages are already sized by it — but it is the
// only way to see the controller working from the client side, and the only
// input to the empty-poll pacing below.
type AutopilotDecision struct {
	// Partitions is the sweep width the broker used for this pop.
	Partitions int `json:"partitions,omitempty"`
	// Batch is the message budget the broker used for this pop.
	Batch int `json:"batch,omitempty"`
	// WaitMillis is the broker's advice on how long to wait before polling
	// again (wire name: waitMs). It is present only when the broker has an
	// opinion, and it is advice, not a lease: the consume loop honors it for
	// the sleep it was already taking between empty non-waiting pops, nothing
	// more. Zero means "no advice".
	WaitMillis int `json:"waitMs,omitempty"`
}

// parseAutopilotDecision pulls the additive "autopilot" object out of a decoded
// pop response, returning nil when it is absent or not an object.
//
// Unknown keys inside it are ignored, and an unknown-shaped value is treated as
// absent rather than as an error: this field is the broker telling the client
// what it did, and a client that refuses to run because a newer broker grew a
// fourth number would be a self-inflicted outage.
func parseAutopilotDecision(result map[string]interface{}) *AutopilotDecision {
	if result == nil {
		return nil
	}
	raw, ok := result["autopilot"].(map[string]interface{})
	if !ok {
		return nil
	}
	d := &AutopilotDecision{}
	// float64 and not json.Number: the HTTP client decodes with plain
	// json.Unmarshal on purpose (see the UseNumber warning in http_client.go).
	if v, ok := raw["partitions"].(float64); ok {
		d.Partitions = int(v)
	}
	if v, ok := raw["batch"].(float64); ok {
		d.Batch = int(v)
	}
	if v, ok := raw["waitMs"].(float64); ok {
		d.WaitMillis = int(v)
	}
	return d
}

// emptyPollBackoff is the sleep the consume loop has always taken between two
// empty pops that are NOT long-polling. A waiting pop already blocks on the
// broker, so it never reaches here.
const emptyPollBackoff = 100 * time.Millisecond

// emptyPollDelay is how long to wait after an empty pop: the broker's advice
// when it gave one, the historical constant otherwise.
//
// The advice is honored as given, without a ceiling of this client's invention.
// The sleep it feeds is inside a select on the caller's context, so even an
// absurd value cannot outlive a cancellation — which is the only property that
// has to hold locally.
func emptyPollDelay(d *AutopilotDecision) time.Duration {
	if d != nil && d.WaitMillis > 0 {
		return time.Duration(d.WaitMillis) * time.Millisecond
	}
	return emptyPollBackoff
}
