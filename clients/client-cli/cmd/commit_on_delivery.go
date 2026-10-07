package cmd

import queen "github.com/smartpricing/queen/clients/client-go/v2"

// withCommitOnDelivery asks a pop to commit at delivery: the broker moves the
// group's cursor past the messages as it hands them out (autoAck=true on the
// wire), with no lease and nothing to ack.
//
// Two SDKs can sit behind this binary. go.mod pins client-go v2.0.0, which
// names that option AutoAck; `go install` builds against it. The go.work build
// (make build, the Docker image, these tests) links the client-go in this tree,
// which names it CommitOnDelivery and no longer sends AutoAck on a pop. Calling
// AutoAck alone would make that build pop leased messages, so the interface
// check below calls CommitOnDelivery when the linked SDK has it and AutoAck
// otherwise: both builds send autoAck=true.
//
// After the client-go release that adds CommitOnDelivery, bump go.mod to it,
// call qb.CommitOnDelivery(enabled) at the two call sites (pop, bench) and
// delete this function.
func withCommitOnDelivery(qb *queen.QueueBuilder, enabled bool) *queen.QueueBuilder {
	if c, ok := any(qb).(interface {
		CommitOnDelivery(bool) *queen.QueueBuilder
	}); ok {
		return c.CommitOnDelivery(enabled)
	}
	return qb.AutoAck(enabled)
}
