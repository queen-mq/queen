package main

// M7 F4's ACL scenario, and the strictest acceptance in this whole runner:
// DescribeAcls (29), CreateAcls (30) and DeleteAcls (31) must produce ZERO
// divergence against the oracle. Not one deliberate key, not one accepted one.
//
// That bar is reachable because the facade is not approximating Kafka here, it
// is reproducing a specific Kafka CONFIGURATION: the oracle
// (`apache/kafka:3.9.1`, rig-diff.sh) runs with no `authorizer.class.name`, and
// `AclApis` answers all three APIs from `Errors.SECURITY_DISABLED` when the
// authorizer is `None` — with TWO different literals, which this runner is what
// measured: "No Authorizer is configured on the broker" from the hand-built
// describe response, and "No Authorizer is configured." from the
// `SecurityDisabledException` the create and delete paths raise. Queen has no
// ACL model, so that is the whole and only answer the facade has too
// (`src/handlers/acls.rs`, which carries both).
//
// What that means for a failure here: every key below is a SEMANTICS bug rather
// than a deviation. A differing message is a drift in the copied sentence; a
// differing result COUNT is the per-element shape being got wrong, which is the
// one mistake in this family a Java client silently reads as "the call
// succeeded and did nothing".
//
// Three things are deliberately not recorded, per this runner's own rules:
// throttle values, correlation ids, and anything the broker generates freely.
// Nothing here writes to either broker, so this scenario needs no topic, no
// group and no ordering against any other one.

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios, scenario{
		name: "acls",
		desc: "describe, create and delete ACLs against a broker with no authorizer",
		run:  scenACLs,
	})
}

// The advertised window, which is the schema's whole window for all three:
// every field of all three request and response schemas is marked 1-3, so the
// only thing a version changes is the encoding (flexible from v2). The walk is
// therefore an ENCODING walk, and it is worth making because v1 and v2 put the
// same bytes on the wire in two different shapes.
var aclVersions = []int16{1, 2, 3}

// The ANY filter: what `kafka-acls.sh --list` and `AclBindingFilter.ANY` send.
// Every field is spelled ANY (1) rather than left at zero — zero is UNKNOWN,
// and Apache Kafka REJECTS a filter containing UNKNOWN elements while it is
// still parsing the request, which would make this scenario measure the
// oracle's argument validation instead of its authorizer branch.
func anyDescribeFilter() kmsg.DescribeACLsRequest {
	req := kmsg.NewDescribeACLsRequest()
	req.ResourceType = 1        // ANY
	req.ResourcePatternType = 1 // ANY
	req.Operation = 1           // ANY
	req.PermissionType = 1      // ANY
	return req
}

// A filter naming one topic, one principal and one operation: what
// `kafka-acls.sh --list --topic orders --principal User:alice` sends. Recorded
// beside the ANY filter so the scenario proves the answer does not depend on
// the filter — on EITHER broker.
func narrowDescribeFilter(topic string) kmsg.DescribeACLsRequest {
	principal := "User:alice"
	host := "*"
	req := kmsg.NewDescribeACLsRequest()
	req.ResourceType = 2 // TOPIC
	req.ResourceName = &topic
	req.ResourcePatternType = 3 // LITERAL
	req.Principal = &principal
	req.Host = &host
	req.Operation = 3      // READ
	req.PermissionType = 3 // ALLOW
	return req
}

// A creation the way `kafka-acls.sh --add --allow-principal User:alice
// --operation Read --topic <t>` composes one. Fully specified for the same
// reason the filter above is: Kafka refuses a creation carrying UNKNOWN
// elements before it ever looks at the authorizer.
func aclCreation(topic string, operation int8) kmsg.CreateACLsRequestCreation {
	c := kmsg.NewCreateACLsRequestCreation()
	c.ResourceType = 2 // TOPIC
	c.ResourceName = topic
	c.ResourcePatternType = 3 // LITERAL
	c.Principal = "User:alice"
	c.Host = "*"
	c.Operation = kmsg.ACLOperation(operation)
	c.PermissionType = 3 // ALLOW
	return c
}

func describeACLs(c *runctx, prefix string, k *conn, req kmsg.DescribeACLsRequest, version int16) {
	resp, _, err := k.doT(&req, version, 20*time.Second)
	if err != nil {
		c.rec.bad(prefix, err)
		return
	}
	dr := resp.(*kmsg.DescribeACLsResponse)
	c.rec.add(prefix+".error", "%s", errName(dr.ErrorCode))
	// THE observation. The message is what `kafka-acls.sh` prints to an
	// operator, so a drift in it is a drift in the only explanation they get.
	c.rec.add(prefix+".message", "%s", showStrPtr(dr.ErrorMessage))
	c.rec.add(prefix+".resources", "%d", len(dr.Resources))
}

func createACLs(c *runctx, prefix string, k *conn, creations []kmsg.CreateACLsRequestCreation, version int16) {
	req := kmsg.NewCreateACLsRequest()
	req.Creations = creations
	resp, _, err := k.doT(&req, version, 20*time.Second)
	if err != nil {
		c.rec.bad(prefix, err)
		return
	}
	cr := resp.(*kmsg.CreateACLsResponse)
	// One result per creation, and the count is diffed first because it is the
	// half a client branches on: Kafka builds this response by MAPPING the
	// error over the request's creations, so an empty request is an empty
	// result list and no error anywhere.
	c.rec.add(prefix+".n_results", "%d", len(cr.Results))
	for i, r := range cr.Results {
		c.rec.add(fmt.Sprintf("%s.result%d.error", prefix, i), "%s", errName(r.ErrorCode))
		c.rec.add(fmt.Sprintf("%s.result%d.message", prefix, i), "%s", showStrPtr(r.ErrorMessage))
	}
}

func deleteACLs(c *runctx, prefix string, k *conn, filters []kmsg.DeleteACLsRequestFilter, version int16) {
	req := kmsg.NewDeleteACLsRequest()
	req.Filters = filters
	resp, _, err := k.doT(&req, version, 20*time.Second)
	if err != nil {
		c.rec.bad(prefix, err)
		return
	}
	dr := resp.(*kmsg.DeleteACLsResponse)
	c.rec.add(prefix+".n_results", "%d", len(dr.Results))
	for i, r := range dr.Results {
		c.rec.add(fmt.Sprintf("%s.filter%d.error", prefix, i), "%s", errName(r.ErrorCode))
		c.rec.add(fmt.Sprintf("%s.filter%d.message", prefix, i), "%s", showStrPtr(r.ErrorMessage))
		// Nothing was matched because neither broker holds an ACL to match.
		c.rec.add(fmt.Sprintf("%s.filter%d.matching", prefix, i), "%d", len(r.MatchingACLs))
	}
}

func scenACLs(c *runctx) {
	topic := c.topic("acls")

	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	// No topic is created and none is needed: the resource an ACL names does
	// not have to exist on either broker, which is itself part of why this
	// scenario is independent of every other one.

	for _, version := range aclVersions {
		v := fmt.Sprintf("v%d", version)

		// ----------------------------------------------------------- describe
		describeACLs(c, "describe."+v+".any", k, anyDescribeFilter(), version)
		describeACLs(c, "describe."+v+".narrow", k, narrowDescribeFilter(topic), version)

		// ------------------------------------------------------------- create
		//
		// Zero, one and two creations. The zero case is the shape a
		// top-level-only error would get wrong in the direction a client cannot
		// see: no results and no error reads as success.
		createACLs(c, "create."+v+".empty", k, nil, version)
		createACLs(c, "create."+v+".one", k,
			[]kmsg.CreateACLsRequestCreation{aclCreation(topic, 3)}, version)
		createACLs(c, "create."+v+".two", k, []kmsg.CreateACLsRequestCreation{
			aclCreation(topic, 3), // READ
			aclCreation(topic, 4), // WRITE
		}, version)

		// ------------------------------------------------------------- delete
		//
		// One specific filter and one ANY filter in the same request: the
		// per-filter mapping has to hold for both, and `deleteAcls(ANY)` is
		// what an AdminClient sends when a UI offers "delete all".
		specific := kmsg.NewDeleteACLsRequestFilter()
		specific.ResourceType = 2 // TOPIC
		specific.ResourceName = &topic
		specific.ResourcePatternType = 3 // LITERAL
		specific.Operation = 3           // READ
		specific.PermissionType = 3      // ALLOW

		anyFilter := kmsg.NewDeleteACLsRequestFilter()
		anyFilter.ResourceType = 1        // ANY
		anyFilter.ResourcePatternType = 1 // ANY
		anyFilter.Operation = 1           // ANY
		anyFilter.PermissionType = 1      // ANY

		deleteACLs(c, "delete."+v+".empty", k, nil, version)
		deleteACLs(c, "delete."+v+".two", k,
			[]kmsg.DeleteACLsRequestFilter{specific, anyFilter}, version)
	}

	// The describe answer after every create and delete above is the same one
	// it was before them, on both brokers, which is what "nothing was written"
	// looks like from the outside.
	describeACLs(c, "after.describe.any", k, anyDescribeFilter(), aclVersions[len(aclVersions)-1])
}
