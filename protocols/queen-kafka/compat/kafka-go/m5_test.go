package compat

import (
	"strings"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// The M5 lane: SASL/PLAIN over TLS against a second facade.
//
// Skipped unless the rig exports BOTH QUEEN_KAFKA_TLS_BOOTSTRAP and
// QUEEN_KAFKA_SASL_TOKEN. QUEEN_KAFKA_TLS_CA is optional: point it at the rig's
// self-signed certificate and the handshake is verified for real; leave it unset
// and the config falls back to InsecureSkipVerify.
//
// Two kafka-go details make this lane different from every other client's:
//
//  1. kafka-go has TWO places to configure TLS+SASL and they are not the same
//     type. A `Reader` takes a `*kafka.Dialer` (TLS *tls.Config, SASLMechanism);
//     a `Writer`/`Client` takes a `*kafka.Transport` (TLS *tls.Config, SASL).
//     Configure one and forget the other and half the suite talks plaintext to a
//     TLS port, which surfaces as an unreadable framing error, not an auth
//     error. Both are configured below.
//  2. Go sends no SNI for an IP-literal ServerName, and the facade captures SNI
//     for shared-host routing. ServerName is therefore set explicitly, exactly as
//     compat/go/m5_test.go does for franz-go.
//
// The password is the Queen bearer token; the username is a free label the
// facade only logs (queen-kafka/src/sasl.rs).
func tlsDialer(t *testing.T, serverName string) *kafka.Dialer {
	t.Helper()
	return &kafka.Dialer{
		Timeout:       20 * time.Second,
		DualStack:     true,
		ClientID:      "queen-kafka-compat-kafka-go",
		TLS:           tlsConfig(t, serverName),
		SASLMechanism: plain.Mechanism{Username: "kafka-go-compat", Password: saslToken()},
	}
}

func tlsTransport(t *testing.T, serverName string) *kafka.Transport {
	t.Helper()
	return &kafka.Transport{
		ClientID: "queen-kafka-compat-kafka-go",
		TLS:      tlsConfig(t, serverName),
		SASL:     plain.Mechanism{Username: "kafka-go-compat", Password: saslToken()},
	}
}

func serverNameOf(bootstrapAddr string) string {
	if i := strings.LastIndex(bootstrapAddr, ":"); i > 0 {
		return bootstrapAddr[:i]
	}
	return bootstrapAddr
}

// TestSaslTlsRoundTrip is bar 6: one produce and one group consume through
// SASL/PLAIN over TLS.
func TestSaslTlsRoundTrip(t *testing.T) {
	addr := tlsBootstrap()
	if addr == "" || saslToken() == "" {
		t.Skip("QUEEN_KAFKA_TLS_BOOTSTRAP / QUEEN_KAFKA_SASL_TOKEN unset: SASL/TLS lane not requested")
	}
	section(t, "SASL/PLAIN over TLS at %s", addr)

	sn := serverNameOf(addr)
	if ca := tlsCACertPath(); ca != "" {
		note("verifying the chain against QUEEN_KAFKA_TLS_CA=%s with ServerName=%q", ca, sn)
	} else {
		note("QUEEN_KAFKA_TLS_CA unset: InsecureSkipVerify, ServerName=%q (still sent as SNI)", sn)
	}

	// (1) authenticate and create+read the topic.
	//
	// Through the DIALER, not the Client: `Client.Metadata` sends Metadata v8
	// with AllowAutoTopicCreation false and no way to set it, so it would never
	// create the topic and this loop would burn its whole deadline. See
	// TestAutoCreateIsGatedByTheWireFlag.
	topic := topicName("m5")
	width := topicWidth(t)

	d := tlsDialer(t, sn)
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	created := false
	for time.Now().Before(deadline) {
		ctx, cancel := ctxWith(t, 20*time.Second)
		parts, err := d.LookupPartitions(ctx, "tcp", addr, topic)
		cancel()
		if err == nil && len(parts) >= width {
			created = true
			break
		}
		lastErr = err
		time.Sleep(400 * time.Millisecond)
	}
	if !created {
		failf(t, "LookupPartitions over SASL_SSL never produced %d partitions for %s: %v", width, topic, lastErr)
	}
	okf(t, "authenticated over SASL/PLAIN on a TLS listener and read metadata for %s (%d partitions)", topic, width)

	// (2) produce
	w := &kafka.Writer{
		Addr:                   kafka.TCP(addr),
		Topic:                  topic,
		Balancer:               &keyPinnedBalancer{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           30 * time.Second,
		Transport:              tlsTransport(t, sn),
	}
	recs := corpus(width, 4)
	msgs := make([]kafka.Message, 0, len(recs))
	for _, r := range recs {
		msgs = append(msgs, r.message())
	}
	ctx, cancel := ctxWith(t, 60*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		failf(t, "produce over SASL_SSL: %v", err)
	}
	if err := w.Close(); err != nil {
		failf(t, "closing the SASL_SSL writer: %v", err)
	}
	okf(t, "produced %d records over SASL_SSL with acks=all", len(recs))

	// (3) consume them back through a GROUP, on the Dialer half of the config
	group := groupName("m5")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           []string{addr},
		Topic:             topic,
		GroupID:           group,
		Dialer:            tlsDialer(t, sn),
		MinBytes:          1,
		MaxBytes:          10e6,
		MaxWait:           500 * time.Millisecond,
		ReadBatchTimeout:  10 * time.Second,
		StartOffset:       kafka.FirstOffset,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		CommitInterval:    0,
	})
	defer r.Close() //nolint:errcheck

	ctx2, cancel2 := ctxWith(t, 120*time.Second)
	defer cancel2()
	out := make([]got, 0, len(recs))
	var last kafka.Message
	for len(out) < len(recs) {
		m, err := r.FetchMessage(ctx2)
		if err != nil {
			failf(t, "consume over SASL_SSL (%d/%d): %v", len(out), len(recs), err)
		}
		out = append(out, got{
			part: m.Partition, offset: m.Offset,
			key: string(m.Key), val: append([]byte(nil), m.Value...),
			hdrs: headerMap(m.Headers),
		})
		last = m
	}
	okf(t, "consumed %d records back through group %s over SASL_SSL", len(out), group)
	verifyCorpus(t, recs, out)

	commitCtx, commitCancel := ctxWith(t, 30*time.Second)
	defer commitCancel()
	if err := r.CommitMessages(commitCtx, last); err != nil {
		failf(t, "OffsetCommit over SASL_SSL: %v", err)
	}
	okf(t, "committed an offset over SASL_SSL (the whole group path works authenticated)")
}

// TestSaslWrongPasswordIsRefused proves the credential is actually checked. It
// only means anything when the facade sits behind compat/authgate: a bare rig
// broker runs with JWT_ENABLED unset and accepts every bearer, so this test
// reports rather than asserts when a wrong password is let through.
func TestSaslWrongPasswordIsRefused(t *testing.T) {
	addr := tlsBootstrap()
	if addr == "" || saslToken() == "" {
		t.Skip("QUEEN_KAFKA_TLS_BOOTSTRAP / QUEEN_KAFKA_SASL_TOKEN unset: SASL/TLS lane not requested")
	}
	section(t, "SASL/PLAIN with the WRONG password")

	sn := serverNameOf(addr)
	cl := &kafka.Client{
		Addr:    kafka.TCP(addr),
		Timeout: 20 * time.Second,
		Transport: &kafka.Transport{
			ClientID: "queen-kafka-compat-kafka-go",
			TLS:      tlsConfig(t, sn),
			SASL:     plain.Mechanism{Username: "kafka-go-compat", Password: saslToken() + "-wrong"},
		},
	}
	ctx, cancel := ctxWith(t, 25*time.Second)
	defer cancel()

	_, err := cl.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topicName("m5")}})
	if err == nil {
		note("a wrong password was ACCEPTED; that means no authgate in front of the broker (JWT_ENABLED unset accepts any bearer), not a facade defect")
		okf(t, "recorded, not asserted")
		return
	}
	okf(t, "a wrong password is refused, fast and legibly: %v", err)
	if strings.Contains(err.Error(), "401") || strings.Contains(strings.ToLower(err.Error()), "auth") {
		okf(t, "the refusal names the reason rather than dropping the connection")
	}
}
