package compat

import (
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// M5, against a real client: the Cloud onboarding claim, which is that a tenant
// changes `bootstrap.servers` and its credentials and nothing else.
//
// Everything here goes through franz-go's own TLS dialler and its own
// SASL/PLAIN implementation — the handshake version it picks, the frame it
// wraps the token in, what it does with the answer — so it is the half the
// hand-rolled tests in `src/conn.rs` cannot reach: those drive the exchange
// with `kafka-protocol`'s encoder, which is the same crate the facade decodes
// with, and a shared misreading of the protocol would pass both sides.
//
// It runs only when the rig stands up the second listener
// (`queen-kafka/compat/rig.sh --m5`), because it needs a facade configured with
// TLS and SASL and the default rig deliberately has neither.
//
// The credential CHECK is real here, but the checker is not the broker: the
// rig's broker runs with JWT_ENABLED unset, so `GET /api/v1/resources/queues`
// answers 200 for any bearer and every password would be accepted. So under
// --m5 the rig puts `compat/authgate` in front of it — a reverse proxy whose
// only job is the 401 — and points the TLS facade at that. What the tests below
// therefore prove is the facade's half: that it forwards the password as the
// bearer, that a 401 becomes SASL_AUTHENTICATION_FAILED, and that the client
// treats it as fatal. What they do NOT prove is any particular auth layer's
// verdict; the difference between a refusal and an unreachable Queen is covered
// by the unit tests in `handlers::sasl_authenticate` and `conn`.
func TestTLSAndSaslPlainOnboarding(t *testing.T) {
	bootstrap := os.Getenv("QUEEN_KAFKA_TLS_BOOTSTRAP")
	if bootstrap == "" {
		t.Skip("no TLS+SASL listener: run queen-kafka/compat/rig.sh --m5")
	}
	// The name matters and an address would not do: Go sends no SNI for an IP
	// literal (RFC 6066 forbids it), so a rig that dialled 127.0.0.1 would be
	// testing TLS and nothing about routing.
	host := bootstrap[:strings.LastIndex(bootstrap, ":")]
	if host == "127.0.0.1" || host == "::1" {
		t.Fatalf("QUEEN_KAFKA_TLS_BOOTSTRAP=%s is an address; SNI needs a name", bootstrap)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		// The certificate is the self-signed one the rig writes out, so the
		// chain is not what is being tested; the handshake and the server name
		// are. `ServerName` is set explicitly rather than left to the dialler
		// so this is a statement about what goes on the wire.
		kgo.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a self-signed rig certificate
			ServerName:         host,
		}),
		// The whole of the credential change a tenant makes: a label, and the
		// bearer token as the password.
		kgo.SASL(plain.Auth{
			User: "compat-rig",
			Pass: saslToken(),
		}.AsMechanism()),
		kgo.DisableIdempotentWrite(),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequestRetries(3),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	topic := newTopic(t)
	ensureTopic(t, cl, topic)

	sent := []*kgo.Record{
		{Topic: topic, Partition: 0, Key: []byte("k1"), Value: []byte("over tls")},
		{Topic: topic, Partition: 1, Key: []byte("k2"), Value: []byte("and sasl")},
	}
	if err := cl.ProduceSync(ctxFor(t, 60*time.Second), sent...).FirstErr(); err != nil {
		t.Fatalf("produce over TLS+SASL: %v", err)
	}

	// ...and back out again, on a second authenticated connection: a consumer
	// re-dials the address Metadata advertised, so this also pins that the
	// facade advertised a name a TLS client can still verify and authenticate
	// against.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a self-signed rig certificate
			ServerName:         host,
		}),
		kgo.SASL(plain.Auth{User: "compat-rig", Pass: saslToken()}.AsMechanism()),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(0), 1: kgo.NewOffset().At(0)},
		}),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient (consumer): %v", err)
	}
	defer consumer.Close()

	ctx := ctxFor(t, 60*time.Second)
	got := map[string]string{}
	for len(got) < len(sent) {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err0(); err != nil && ctx.Err() != nil {
			t.Fatalf("consume over TLS+SASL: %v", err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			got[string(r.Key)] = string(r.Value)
		})
	}
	for _, r := range sent {
		if got[string(r.Key)] != string(r.Value) {
			t.Fatalf("key %q read back as %q, not %q", r.Key, got[string(r.Key)], r.Value)
		}
	}
}

// The gate itself: an unauthenticated connection to a SASL listener gets
// nothing. franz-go with no `kgo.SASL` option sends its Metadata straight
// after ApiVersions, and the facade closes on it — see the SASL gate in
// `conn::dispatch`.
func TestSaslListenerRefusesAnUnauthenticatedClient(t *testing.T) {
	bootstrap := os.Getenv("QUEEN_KAFKA_TLS_BOOTSTRAP")
	if bootstrap == "" {
		t.Skip("no TLS+SASL listener: run queen-kafka/compat/rig.sh --m5")
	}
	host := bootstrap[:strings.LastIndex(bootstrap, ":")]

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a self-signed rig certificate
			ServerName:         host,
		}),
		kgo.RequestRetries(0),
		kgo.RetryTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	// A short deadline, because the failure shape is a disconnect and franz-go
	// answers it by reconnecting: what is being asserted is that it never
	// succeeds, not how it gives up.
	ctx := ctxFor(t, 10*time.Second)
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{}
	if _, err := cl.Request(ctx, req); err == nil {
		t.Fatal("an unauthenticated client read metadata from a SASL listener")
	}
}

// CHECK 8. A WRONG password is refused, and the client is told so rather than
// left to retry.
//
// The distinction that matters is not "it failed" but HOW: a
// SASL_AUTHENTICATION_FAILED is fatal in every Kafka client — franz-go stops
// there and hands the error up — whereas a bare disconnect is retried forever.
// The facade reserves the disconnect for a Queen it could not reach, which is
// the opposite case, so an assertion that only checked for an error would pass
// on the wrong behaviour.
func TestSaslRefusesAWrongPassword(t *testing.T) {
	bootstrap := os.Getenv("QUEEN_KAFKA_TLS_BOOTSTRAP")
	if bootstrap == "" {
		t.Skip("no TLS+SASL listener: run queen-kafka/compat/rig.sh --m5")
	}
	if os.Getenv("QUEEN_KAFKA_SASL_TOKEN") == "" {
		t.Skip("no QUEEN_KAFKA_SASL_TOKEN: without the credential gate every password is accepted")
	}
	host := bootstrap[:strings.LastIndex(bootstrap, ":")]

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a self-signed rig certificate
			ServerName:         host,
		}),
		kgo.SASL(plain.Auth{User: "compat-rig", Pass: "not-the-token"}.AsMechanism()),
		kgo.RequestRetries(0),
		kgo.RetryTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{}
	_, err = cl.Request(ctxFor(t, 30*time.Second), req)
	if err == nil {
		t.Fatal("a wrong password read metadata from a SASL listener")
	}
	t.Logf("wrong password: %v", err)
	if !errors.Is(err, kerr.SaslAuthenticationFailed) {
		t.Errorf("the client was given %v, not SASL_AUTHENTICATION_FAILED: a client that is not told "+
			"a credential is wrong retries it forever", err)
	}

	// ...and the listener is still a listener: a refusal is one connection's
	// business, not the facade's.
	good, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a self-signed rig certificate
			ServerName:         host,
		}),
		kgo.SASL(plain.Auth{User: "compat-rig", Pass: saslToken()}.AsMechanism()),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient (good credential): %v", err)
	}
	defer good.Close()
	if _, err := good.Request(ctxFor(t, 30*time.Second), kmsg.NewPtrMetadataRequest()); err != nil {
		t.Fatalf("the right credential failed after a wrong one: %v", err)
	}
}

// CHECK 9. TLS is not optional on the TLS listener, and a client that ignores
// that gets a clean failure rather than a facade that falls over.
//
// A plaintext Kafka client sends an ApiVersions request, which rustls reads as
// a malformed ClientHello and refuses. The point of the second half is the
// facade's health afterwards: the listener keeps accepting, and the rig's own
// panic sweep over the log is what catches the other way this could go wrong.
func TestPlaintextClientAgainstTheTLSListener(t *testing.T) {
	bootstrap := os.Getenv("QUEEN_KAFKA_TLS_BOOTSTRAP")
	if bootstrap == "" {
		t.Skip("no TLS+SASL listener: run queen-kafka/compat/rig.sh --m5")
	}
	host := bootstrap[:strings.LastIndex(bootstrap, ":")]

	// A raw connection first, so the failure is observed at the socket and not
	// through a client's retry policy: a plaintext frame goes in, and what
	// comes back is a TLS alert or a close — never a Kafka response.
	raw, err := net.DialTimeout("tcp", bootstrap, 10*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", bootstrap, err)
	}
	// A 4-byte-BE-framed ApiVersions v0 request, which is what every Kafka
	// client opens with: api key 18, version 0, correlation 1, null client id.
	frame := []byte{
		0, 0, 0, 10,
		0, 18,
		0, 0,
		0, 0, 0, 1,
		0xff, 0xff,
	}
	_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := raw.Write(frame); err != nil {
		t.Logf("the plaintext write was refused outright: %v", err)
	}
	buf := make([]byte, 64)
	n, readErr := raw.Read(buf)
	_ = raw.Close()
	if readErr == nil && n >= 4 && buf[0] != 0x15 {
		// 0x15 is the TLS alert content type; anything else with a plausible
		// Kafka frame in it would mean the listener answered in the clear.
		t.Fatalf("the TLS listener answered a plaintext request with %d bytes: % x", n, buf[:n])
	}
	if n > 0 {
		t.Logf("plaintext on the TLS port: read % x (%d bytes, err %v)", buf[:n], n, readErr)
	} else {
		t.Logf("plaintext on the TLS port: nothing came back (err %v)", readErr)
	}

	// A plaintext CLIENT, for the shape a user would actually see.
	plainClient, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.RequestRetries(0),
		kgo.RetryTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient (plaintext): %v", err)
	}
	defer plainClient.Close()
	if _, err := plainClient.Request(ctxFor(t, 15*time.Second), kmsg.NewPtrMetadataRequest()); err == nil {
		t.Fatal("a plaintext client read metadata from a TLS listener")
	} else {
		t.Logf("plaintext client: %v", err)
	}

	// And the listener survived all of it.
	tlsClient, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a self-signed rig certificate
			ServerName:         host,
		}),
		kgo.SASL(plain.Auth{User: "compat-rig", Pass: saslToken()}.AsMechanism()),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient (tls): %v", err)
	}
	defer tlsClient.Close()
	if _, err := tlsClient.Request(ctxFor(t, 30*time.Second), kmsg.NewPtrMetadataRequest()); err != nil {
		t.Fatalf("the TLS listener stopped serving after a plaintext client hit it: %v", err)
	}
}

// saslToken is the password the M5 tests present: the token the rig's credential
// gate accepts, or the historical constant when the rig did not set one (a
// facade whose Queen checks nothing accepts any string, so the value only
// matters when the gate is up).
func saslToken() string {
	if v := os.Getenv("QUEEN_KAFKA_SASL_TOKEN"); v != "" {
		return v
	}
	return "rig-tenant-token"
}
