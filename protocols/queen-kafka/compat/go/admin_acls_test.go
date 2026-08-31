// M7 F4: the ACL family against a running facade — DescribeAcls (29),
// CreateAcls (30) and DeleteAcls (31), driven by franz-go's `kmsg`.
//
// Every one of them is answered SECURITY_DISABLED (54) with the message an
// Apache Kafka broker with no `authorizer.class.name` answers — "No Authorizer
// is configured on the broker" for describe, "No Authorizer is configured." for
// the two writes, which really are two different sentences in `AclApis` and are
// pinned separately below. Queen has no ACL model — authorization here
// is Queen's own, over the connection's bearer — so there is nothing to
// describe, create or delete, and the facade says so in the protocol instead of
// closing the connection on an un-advertised key.
//
// What these tests assert is what a CLIENT does with that answer, which is the
// half the crate's own unit tests cannot see:
//
//   - the error decodes into franz-go's typed `kerr.SecurityDisabled`, which is
//     the object a Java `AdminClient` turns into `SecurityDisabledException` and
//     `kafka-acls.sh` prints;
//   - the per-element shape is right — one result per creation, one per filter —
//     because a top-level-only error would decode as "the call succeeded and
//     returned nothing", which is the opposite of the answer;
//   - and an EMPTY creations/filters list answers an empty result list with no
//     error at all, because Kafka maps the error over the request.
//
// The version dimension is deliberately absent here and it is a property of the
// client rather than a gap: franz-go rewrites the version of everything it
// sends to the highest the broker advertises, so a SetVersion(1) below goes out
// at 3 anyway. The per-version walk lives where frames are written by hand — the
// crate's dispatch walk (`src/conn.rs`) and the differential runner
// (`compat/differential/admin_acls.go`), which is also where the message is
// diffed against `apache/kafka:3.9.1` itself.
package compat

import (
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Apache Kafka's own sentences for an authorizer-less broker, byte for byte, and
// there are TWO of them: `AclApis.handleDescribeAcls` builds its response by
// hand with `.setErrorMessage("No Authorizer is configured on the broker")`,
// while the create and delete paths raise `SecurityDisabledException("No
// Authorizer is configured.")` and let `getErrorResponse` carry its message.
// Measured off `apache/kafka:3.9.1`, not read off a doc — the four extra words
// and the missing full stop are exactly the kind of drift this pin exists for.
// `src/handlers/acls.rs` carries both literals for the same reason.
const noAuthorizer = "No Authorizer is configured."
const noAuthorizerDescribe = "No Authorizer is configured on the broker"

const errSecurityDisabled int16 = 54

// The ACL enum values, spelled rather than left at zero. Zero is UNKNOWN in
// every one of these enums and Apache Kafka rejects a filter or a creation
// carrying an UNKNOWN element while it is still parsing the request — so a test
// that left them at zero would be measuring argument validation, not the
// authorizer branch. Kept here so the facade is asked exactly what the oracle
// is asked.
const (
	aclAny        int8 = 1
	aclTopic      int8 = 2
	aclLiteral    int8 = 3
	aclRead       int8 = 3
	aclWrite      int8 = 4
	aclAllow      int8 = 3
	aclAnyPattern int8 = 1
)

func describeACLs(t *testing.T, cl *kgo.Client, req *kmsg.DescribeACLsRequest) *kmsg.DescribeACLsResponse {
	t.Helper()
	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("DescribeAcls: %v", err)
	}
	return resp
}

// anyACLFilter is `AclBindingFilter.ANY`: what `kafka-acls.sh --list` sends and
// what every admin UI probes an unknown broker with at connect.
func anyACLFilter() *kmsg.DescribeACLsRequest {
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceType(aclAny)
	req.ResourcePatternType = kmsg.ACLResourcePatternType(aclAnyPattern)
	req.Operation = kmsg.ACLOperation(aclAny)
	req.PermissionType = kmsg.ACLPermissionType(aclAny)
	return req
}

func aclCreation(topic string, operation int8) kmsg.CreateACLsRequestCreation {
	c := kmsg.NewCreateACLsRequestCreation()
	c.ResourceType = kmsg.ACLResourceType(aclTopic)
	c.ResourceName = topic
	c.ResourcePatternType = kmsg.ACLResourcePatternType(aclLiteral)
	c.Principal = "User:alice"
	c.Host = "*"
	c.Operation = kmsg.ACLOperation(operation)
	c.PermissionType = kmsg.ACLPermissionType(aclAllow)
	return c
}

func aclDeleteFilter(topic string) kmsg.DeleteACLsRequestFilter {
	f := kmsg.NewDeleteACLsRequestFilter()
	f.ResourceType = kmsg.ACLResourceType(aclTopic)
	f.ResourceName = &topic
	f.ResourcePatternType = kmsg.ACLResourcePatternType(aclLiteral)
	f.Operation = kmsg.ACLOperation(aclRead)
	f.PermissionType = kmsg.ACLPermissionType(aclAllow)
	return f
}

// CHECK A1. The advertised window, read the way every client reads it: three
// keys, `1..=3` each, which is the schema's whole window because nothing varies
// inside it. The floor is KIP-896's — v0 was dropped from all three — so a
// client that only speaks v0 is one that predates the schema, not one this
// facade turned away.
func TestAclApisAdvertiseTheDesignedWindows(t *testing.T) {
	cl := newClient(t)
	req := kmsg.NewPtrApiVersionsRequest()
	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("ApiVersions: %v", err)
	}
	want := map[int16][2]int16{
		29: {1, 3}, // DescribeAcls
		30: {1, 3}, // CreateAcls
		31: {1, 3}, // DeleteAcls
	}
	seen := map[int16]bool{}
	for _, a := range resp.ApiKeys {
		w, ok := want[a.ApiKey]
		if !ok {
			continue
		}
		seen[a.ApiKey] = true
		if a.MinVersion != w[0] || a.MaxVersion != w[1] {
			t.Errorf("api key %d advertises v%d-v%d, want v%d-v%d",
				a.ApiKey, a.MinVersion, a.MaxVersion, w[0], w[1])
		}
	}
	for key := range want {
		if !seen[key] {
			t.Errorf("api key %d is not advertised at all", key)
		}
	}
}

// CHECK A2. DescribeAcls: the error is TOP LEVEL, the resource list is empty,
// and the message is the oracle's. The typed error is asserted as well as the
// code, because the typed error is what a real client branches on — an admin UI
// hides its ACL tab on `SecurityDisabledException` and renders an error page on
// anything it does not recognise.
func TestDescribeAclsIsSecurityDisabled(t *testing.T) {
	cl := newClient(t)
	resp := describeACLs(t, cl, anyACLFilter())

	if resp.ErrorCode != errSecurityDisabled {
		t.Fatalf("DescribeAcls answered %d, want SECURITY_DISABLED (%d)",
			resp.ErrorCode, errSecurityDisabled)
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); !errors.Is(err, kerr.SecurityDisabled) {
		t.Errorf("code %d decodes as %v, want kerr.SecurityDisabled", resp.ErrorCode, err)
	}
	if resp.ErrorMessage == nil || *resp.ErrorMessage != noAuthorizerDescribe {
		t.Errorf("DescribeAcls message = %v, want %q",
			showPtr(resp.ErrorMessage), noAuthorizerDescribe)
	}
	if len(resp.Resources) != 0 {
		t.Errorf("DescribeAcls returned %d resources on a broker with no ACL model",
			len(resp.Resources))
	}
}

// CHECK A3. No filter changes the answer, which is what makes the refusal
// stateless rather than merely empty: a client cannot learn anything about this
// broker by varying what it asks for.
func TestDescribeAclsIgnoresTheFilter(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	principal := "User:alice"
	host := "10.0.0.1"

	narrow := kmsg.NewPtrDescribeACLsRequest()
	narrow.ResourceType = kmsg.ACLResourceType(aclTopic)
	narrow.ResourceName = &topic
	narrow.ResourcePatternType = kmsg.ACLResourcePatternType(aclLiteral)
	narrow.Principal = &principal
	narrow.Host = &host
	narrow.Operation = kmsg.ACLOperation(aclRead)
	narrow.PermissionType = kmsg.ACLPermissionType(aclAllow)

	wide := describeACLs(t, cl, anyACLFilter())
	one := describeACLs(t, cl, narrow)

	if wide.ErrorCode != one.ErrorCode {
		t.Errorf("two filters, two codes: %d and %d", wide.ErrorCode, one.ErrorCode)
	}
	if showPtr(wide.ErrorMessage) != showPtr(one.ErrorMessage) {
		t.Errorf("two filters, two messages: %q and %q",
			showPtr(wide.ErrorMessage), showPtr(one.ErrorMessage))
	}
	if len(one.Resources) != 0 {
		t.Errorf("a narrow filter matched %d resources", len(one.Resources))
	}
}

// CHECK A4. CreateAcls answers ONE RESULT PER CREATION, and this is the check
// the whole file is worth writing for. Kafka builds this response by mapping
// the error over the request's creations, so:
//
//   - three creations are three refusals, each carrying the message;
//   - and NO creations are no results and no error, which a client reads as a
//     successful no-op — correctly, because it asked for nothing.
//
// Getting this wrong in the other direction (a single top-level error) is
// invisible to a test that only checks "it failed" and fatal to a client, which
// would read an empty result list as "all my ACLs were created".
func TestCreateAclsAnswersOneResultPerCreation(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	for _, n := range []int{0, 1, 3} {
		req := kmsg.NewPtrCreateACLsRequest()
		for i := 0; i < n; i++ {
			op := aclRead
			if i%2 == 1 {
				op = aclWrite
			}
			req.Creations = append(req.Creations, aclCreation(topic, op))
		}
		resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
		if err != nil {
			t.Fatalf("CreateAcls with %d creations: %v", n, err)
		}
		if len(resp.Results) != n {
			t.Fatalf("CreateAcls with %d creations answered %d results", n, len(resp.Results))
		}
		for i, r := range resp.Results {
			if r.ErrorCode != errSecurityDisabled {
				t.Errorf("creation %d of %d answered %d, want SECURITY_DISABLED (%d)",
					i, n, r.ErrorCode, errSecurityDisabled)
			}
			if r.ErrorMessage == nil || *r.ErrorMessage != noAuthorizer {
				t.Errorf("creation %d of %d: message = %v, want %q",
					i, n, showPtr(r.ErrorMessage), noAuthorizer)
			}
		}
	}
}

// CHECK A5. DeleteAcls answers one result per filter, each with no matching
// ACLs — nothing matched because nothing exists to match. The two-filter case
// carries one specific filter and one ANY filter in the same request, which is
// the shape a UI's "delete all" sends beside a targeted revoke.
func TestDeleteAclsAnswersOneResultPerFilter(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)

	anyFilter := kmsg.NewDeleteACLsRequestFilter()
	anyFilter.ResourceType = kmsg.ACLResourceType(aclAny)
	anyFilter.ResourcePatternType = kmsg.ACLResourcePatternType(aclAnyPattern)
	anyFilter.Operation = kmsg.ACLOperation(aclAny)
	anyFilter.PermissionType = kmsg.ACLPermissionType(aclAny)

	for _, filters := range [][]kmsg.DeleteACLsRequestFilter{
		nil,
		{aclDeleteFilter(topic)},
		{aclDeleteFilter(topic), anyFilter},
	} {
		req := kmsg.NewPtrDeleteACLsRequest()
		req.Filters = filters
		resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
		if err != nil {
			t.Fatalf("DeleteAcls with %d filters: %v", len(filters), err)
		}
		if len(resp.Results) != len(filters) {
			t.Fatalf("DeleteAcls with %d filters answered %d results",
				len(filters), len(resp.Results))
		}
		for i, r := range resp.Results {
			if r.ErrorCode != errSecurityDisabled {
				t.Errorf("filter %d answered %d, want SECURITY_DISABLED (%d)",
					i, r.ErrorCode, errSecurityDisabled)
			}
			if r.ErrorMessage == nil || *r.ErrorMessage != noAuthorizer {
				t.Errorf("filter %d: message = %v, want %q",
					i, showPtr(r.ErrorMessage), noAuthorizer)
			}
			if len(r.MatchingACLs) != 0 {
				t.Errorf("filter %d matched %d ACLs on a broker that holds none",
					i, len(r.MatchingACLs))
			}
		}
	}
}

// CHECK A6. The refusal costs the connection nothing. An ACL command is a
// dead end, not a fault: the same client goes straight on to produce and
// consume, which is what "the refusal is IN the protocol" buys over closing the
// connection on an un-advertised key.
func TestTheConnectionSurvivesAnAclCommand(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	create := kmsg.NewPtrCreateACLsRequest()
	create.Creations = append(create.Creations, aclCreation(topic, aclRead))
	if _, err := create.RequestWith(ctxFor(t, 30*time.Second), cl); err != nil {
		t.Fatalf("CreateAcls: %v", err)
	}

	rec := &kgo.Record{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v")}
	produceSync(t, cl, []*kgo.Record{rec})

	if resp := describeACLs(t, cl, anyACLFilter()); resp.ErrorCode != errSecurityDisabled {
		t.Errorf("the second DescribeAcls answered %d", resp.ErrorCode)
	}
}

func showPtr(s *string) string {
	if s == nil {
		return "<null>"
	}
	return *s
}
