// =============================================================================
// Sasl.cs — the M5 lane: SASL/PLAIN over TLS.
//
// Two things are being proved and they are different:
//
//   1. Confluent.Kafka can produce and consume through the TLS + SASL/PLAIN
//      listener, which means the .NET binding's OpenSSL plumbing works against
//      rustls and the SaslHandshake/SaslAuthenticate round trip lands.
//
//   2. A WRONG password is actually REFUSED. That only means anything if the
//      broker behind the facade checks credentials; a rig broker runs with
//      JWT_ENABLED unset and answers 200 to any bearer. compat/authgate is the
//      one exact-match check that makes the negative case real, so this
//      section reports whether the negative case was meaningful.
//
// THE PASSWORD IS THE QUEEN BEARER TOKEN. sasl.username is a free label the
// facade only logs.
//
// THE CERTIFICATE HAS NO host.docker.internal SAN. rig.sh's self-signed cert
// carries kafka.example.com / shared.queenmq.cloud / localhost / 127.0.0.1, so
// a client dialling any other name must switch hostname verification off while
// still verifying the chain: SslEndpointIdentificationAlgorithm = None with
// SslCaLocation pointed at the PEM. That is the tighter of the two escapes and
// the one this file uses; QK_SSL_INSECURE=1 falls back to disabling
// verification entirely.
//
// Environment:
//   KAFKA_TLS_BOOTSTRAP      host:port of the SASL_SSL listener   (required)
//   QUEEN_KAFKA_SASL_TOKEN   the bearer token = the password      (required)
//   QUEEN_KAFKA_TLS_CA       path to the listener's cert in PEM   (recommended)
//   QK_SSL_INSECURE=1        skip chain verification too
// =============================================================================

using Confluent.Kafka;

namespace QueenKafkaCompat;

public static class Sasl
{
    public static void Run(string runId)
    {
        var bootstrap = Environment.GetEnvironmentVariable("KAFKA_TLS_BOOTSTRAP");
        var token = Environment.GetEnvironmentVariable("QUEEN_KAFKA_SASL_TOKEN");
        var ca = Environment.GetEnvironmentVariable("QUEEN_KAFKA_TLS_CA");

        Program.Section("M5: SASL/PLAIN over TLS");
        if (string.IsNullOrEmpty(bootstrap) || string.IsNullOrEmpty(token))
        {
            Program.Note("KAFKA_TLS_BOOTSTRAP / QUEEN_KAFKA_SASL_TOKEN unset: skipping the SASL/TLS lane");
            return;
        }
        Program.Note($"bootstrap {bootstrap}, ca {(string.IsNullOrEmpty(ca) ? "<none>" : ca)}");

        var topic = $"dnet-{runId}-tls";
        var group = $"dnet-{runId}-tlsg";

        // ------------------------------------------------------ right password
        var pc = Core.BaseProducer(bootstrap, "dnet-tls-prod");
        Secure(pc, token, ca);

        var sentOffsets = new List<TopicPartitionOffset>();
        try
        {
            using var prod = new ProducerBuilder<byte[], byte[]>(pc)
                .SetLogHandler((_, m) => Program.OnLog(m))
                .Build();
            for (var i = 0; i < 40; i++)
            {
                var idx = i;
                var r = Program.Deadline($"tls produce {idx}", 60, () =>
                    prod.ProduceAsync(new TopicPartition(topic, new Partition(idx % 4)),
                        new Message<byte[], byte[]>
                        {
                            Key = Core.Key(idx),
                            Value = Program.U8($"tls|{idx}"),
                            Headers = Core.HeadersFor(idx),
                        }).GetAwaiter().GetResult());
                sentOffsets.Add(r.TopicPartitionOffset);
            }
            prod.Flush(TimeSpan.FromSeconds(30));
            Program.Ok($"produced {sentOffsets.Count} records over SASL_SSL with the right token");
        }
        catch (Exception e)
        {
            Program.Fail($"SASL_SSL produce threw {e.GetType().Name}: {Trim(e.Message)}");
            return;
        }

        // ------------------------------------------------------- group consume
        var cc = Core.BaseConsumer(bootstrap, group, "dnet-tls-cons");
        Secure(cc, token, ca);

        var got = new List<ConsumeResult<byte[], byte[]>>();
        try
        {
            using var c = new ConsumerBuilder<byte[], byte[]>(cc)
                .SetLogHandler((_, m) => Program.OnLog(m))
                .Build();
            c.Subscribe(topic);
            var idle = 0;
            var deadline = DateTime.UtcNow.AddSeconds(150);
            while (got.Count < 40 && idle < 4 && DateTime.UtcNow < deadline)
            {
                var cr = c.Consume(TimeSpan.FromSeconds(5));
                if (cr?.Message != null) { got.Add(cr); idle = 0; } else idle++;
            }
            if (got.Count > 0)
            {
                var committed = c.Commit();
                Program.Ok($"committed {committed.Count} offsets over SASL_SSL");
            }
            c.Close();
        }
        catch (Exception e)
        {
            Program.Fail($"SASL_SSL consume threw {e.GetType().Name}: {Trim(e.Message)}");
        }

        Program.Check(got.Count == 40, $"consumed {got.Count}/40 over SASL_SSL through a group");
        var exact = got.All(cr =>
        {
            var i = int.Parse(Program.S8(cr.Message.Value).Split('|')[1]);
            return Program.BytesEq(cr.Message.Key, Core.Key(i))
                   && (cr.Message.Headers?.Count ?? 0) == 5;
        });
        Program.Check(exact, "keys and headers survived the TLS lane byte-exact");

        // ------------------------------------------------------ wrong password
        Program.Section("M5: a wrong password must be refused");
        var bad = Core.BaseProducer(bootstrap, "dnet-tls-wrong");
        Secure(bad, token + "-WRONG", ca);
        bad.MessageTimeoutMs = 12000;

        var errors = new List<string>();
        try
        {
            using var prod = new ProducerBuilder<byte[], byte[]>(bad)
                .SetLogHandler((_, m) => Program.OnLog(m))
                .SetErrorHandler((_, e) => { lock (errors) errors.Add($"{e.Code}: {Trim(e.Reason)}"); })
                .Build();

            var outcome = Program.Deadline("produce with a wrong password", 90, () =>
            {
                try
                {
                    var r = prod.ProduceAsync(topic, new Message<byte[], byte[]>
                    { Key = Program.U8("nope"), Value = Program.U8("nope") })
                        .GetAwaiter().GetResult();
                    return $"ACCEPTED at {r.TopicPartitionOffset}";
                }
                catch (ProduceException<byte[], byte[]> pe) { return $"{pe.Error.Code}: {Trim(pe.Error.Reason)}"; }
                catch (Exception ex) { return $"{ex.GetType().Name}: {Trim(ex.Message)}"; }
            });

            Program.Check(!outcome.StartsWith("ACCEPTED"),
                $"the wrong password did not get a record through: {outcome}");
            lock (errors)
                foreach (var e in errors.Distinct().Take(4)) Program.Note($"error cb: {e}");
            var sawAuth = errors.Any(e => e.Contains("Authentication", StringComparison.OrdinalIgnoreCase)
                                       || e.Contains("SaslAuthentication", StringComparison.OrdinalIgnoreCase)
                                       || e.Contains("401"));
            Program.Check(sawAuth,
                sawAuth
                    ? "the refusal was an AUTHENTICATION error, not a generic timeout"
                    : "the refusal did not name authentication (check whether the broker behind the facade checks credentials at all)");
        }
        catch (TimeoutException)
        {
            Program.Fail("the wrong-password producer HUNG instead of being refused");
        }
    }

    private static void Secure(ClientConfig cfg, string token, string ca)
    {
        cfg.SecurityProtocol = SecurityProtocol.SaslSsl;
        cfg.SaslMechanism = SaslMechanism.Plain;
        cfg.SaslUsername = "dnet";       // a free label; the facade only logs it
        cfg.SaslPassword = token;        // THE QUEEN BEARER TOKEN

        if (Environment.GetEnvironmentVariable("QK_SSL_INSECURE") == "1" || string.IsNullOrEmpty(ca))
        {
            cfg.EnableSslCertificateVerification = false;
        }
        else
        {
            cfg.SslCaLocation = ca;
            // The rig cert has no host.docker.internal SAN. Verify the chain,
            // not the name.
            cfg.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.None;
        }
    }

    private static string Trim(string s)
    {
        if (string.IsNullOrEmpty(s)) return s;
        var i = s.IndexOf('\n');
        var line = i < 0 ? s : s[..i];
        return line.Length > 240 ? line[..240] + "..." : line;
    }
}
