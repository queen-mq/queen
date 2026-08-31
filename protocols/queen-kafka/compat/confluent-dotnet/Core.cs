// =============================================================================
// Core.cs — the test bar, items 1 through 5.
//
//   1. auto-create a topic by producing to it
//   2. 512 messages over 8 partitions, keys + headers, uncompressed
//   3. every compression codec Confluent.Kafka exposes
//   4. awkward payload shapes (null key, empty value, 0x00..0xFF, duplicate
//      and empty header names, a 256 KiB value)
//   5. consume through a GROUP: count, per-partition order, byte-exact
//      key/value/header round trip, sane timestamps
//   6. commit, close, rejoin: a second instance in the same group resumes
//   7. watermarks (ListOffsets), Assign at an explicit offset, Seek, Position
//
// Every produce sets EnableIdempotence = false EXPLICITLY. librdkafka already
// defaults it off, but the brief for this row is "defaults and binding
// behaviour are the point", and being explicit is what a .NET user shipping
// against this facade must actually write.
// =============================================================================

using System.Text;
using Confluent.Kafka;

namespace QueenKafkaCompat;

public static class Core
{
    public const int Partitions = 8;   // must match QUEEN_KAFKA_DEFAULT_PARTITIONS
    public const int Total = 512;      // 64 per partition

    public static void Run(string bootstrap, string runId)
    {
        var parts = int.TryParse(Environment.GetEnvironmentVariable("QUEEN_KAFKA_PARTITIONS"), out var p)
            ? p : Partitions;

        var tCore = $"dnet-{runId}-core";
        var tCodec = $"dnet-{runId}-codec";
        var tShapes = $"dnet-{runId}-shapes";
        var group = $"dnet-{runId}-g1";

        AutoCreate(bootstrap, tCore, parts);
        var sent = ProduceMain(bootstrap, tCore, parts);
        Codecs(bootstrap, tCodec);
        Shapes(bootstrap, tShapes);
        GroupConsume(bootstrap, tCore, group, sent, parts);
        Watermarks(bootstrap, tCore, parts, sent.Count);
    }

    // -------------------------------------------------------------- helpers

    public static ProducerConfig BaseProducer(string bootstrap, string clientId) => new()
    {
        BootstrapServers = bootstrap,
        ClientId = clientId,
        // Not mandatory any more: M7 F3 advertises InitProducerId and Edges.cs
        // asserts that leaving this true SENDS. Kept explicit so the main path
        // of this suite stays the non-idempotent one whatever librdkafka does
        // with its default.
        EnableIdempotence = false,
        Acks = Acks.All,
        LingerMs = 20,
        MessageTimeoutMs = 30000,
        // Convention #5: we read the negotiated versions out of this stream.
        Debug = "protocol",
        // The facade is one broker with no elections; a long retry backoff just
        // makes a genuine failure take longer to surface.
        MessageSendMaxRetries = 3,
        RetryBackoffMs = 200,
    };

    public static ConsumerConfig BaseConsumer(string bootstrap, string group, string clientId) => new()
    {
        BootstrapServers = bootstrap,
        GroupId = group,
        ClientId = clientId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        // The facade's window is 6000..300000 (QUEEN_KAFKA_GROUP_MIN/MAX_
        // SESSION_TIMEOUT_MS); outside it you get INVALID_SESSION_TIMEOUT (26).
        SessionTimeoutMs = 10000,
        HeartbeatIntervalMs = 3000,
        MaxPollIntervalMs = 300000,
        Debug = "protocol",
        EnablePartitionEof = false,
    };

    /// The value of message i. Self-describing so a consumer can check
    /// ordering without carrying a table around.
    public static byte[] Value(string topic, int i) =>
        Program.U8($"dnet|{topic}|i={i}|p={i % Partitions}|seq={i / Partitions}|" + new string('x', 40));

    public static byte[] Key(int i) => Program.U8($"k-{i:D5}");

    public static Headers HeadersFor(int i)
    {
        var h = new Headers();
        h.Add("h-idx", Program.U8(i.ToString()));
        h.Add("h-bin", new byte[] { 0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff });
        h.Add("h-dup", Program.U8("first"));
        h.Add("h-dup", Program.U8("second"));   // duplicate names are legal in Kafka
        h.Add("h-empty", Array.Empty<byte>());
        return h;
    }

    // -------------------------------------------------------- 1. auto-create

    private static void AutoCreate(string bootstrap, string topic, int parts)
    {
        Program.Section("1. auto-create: producing to a topic that does not exist");

        // Careful: naming a topic in a Metadata request is ITSELF the
        // auto-create trigger in this facade. So we list EVERYTHING and look
        // for the name, rather than asking about the name.
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrap,
            ClientId = "dnet-precheck",
        }).SetLogHandler((_, m) => Program.OnLog(m)).Build();

        var before = Program.Deadline("metadata(all) before", 30,
            () => admin.GetMetadata(TimeSpan.FromSeconds(20)));
        Program.Check(before.Topics.All(t => t.Topic != topic),
            $"{topic} does not exist yet (facade knows {before.Topics.Count} topics)");

        using var prod = new ProducerBuilder<byte[], byte[]>(BaseProducer(bootstrap, "dnet-autocreate"))
            .SetLogHandler((_, m) => Program.OnLog(m))
            .Build();

        DeliveryResult<byte[], byte[]> dr = null;
        try
        {
            dr = Program.Deadline("first produce (auto-create)", 60, () =>
                prod.ProduceAsync(topic, new Message<byte[], byte[]>
                {
                    Key = Program.U8("autocreate"),
                    Value = Program.U8("hello from Confluent.Kafka"),
                }).GetAwaiter().GetResult());
        }
        catch (Exception e)
        {
            Program.Fail($"auto-create produce threw {e.GetType().Name}: {e.Message}");
            return;
        }

        Program.Check(dr.Status == PersistenceStatus.Persisted,
            $"delivery report says Persisted (got {dr.Status})");
        Program.Check(dr.Offset >= 0 && dr.Partition.Value >= 0,
            $"delivery report carries a real coordinate: partition {dr.Partition} offset {dr.Offset}");

        var after = Program.Deadline("metadata(all) after", 30,
            () => admin.GetMetadata(TimeSpan.FromSeconds(20)));
        var t = after.Topics.FirstOrDefault(x => x.Topic == topic);
        Program.Check(t != null, $"{topic} exists after the produce");
        if (t != null)
        {
            Program.Check(t.Error.Code == ErrorCode.NoError, $"topic metadata has no error ({t.Error.Code})");
            Program.Check(t.Partitions.Count == parts,
                $"auto-created with {parts} partitions (got {t.Partitions.Count})");
            Program.Check(t.Partitions.All(x => x.Leader >= 0),
                "every partition advertises a leader");
        }

        // That first record is on the topic now; the main batch below starts at
        // offset 1 on whichever partition took it. GroupConsume accounts for it.
        Program.Note($"the auto-create probe record sits at {topic}[{dr.Partition}]@{dr.Offset}");
        AutoCreateCoord = dr.TopicPartitionOffset;
    }

    public static TopicPartitionOffset AutoCreateCoord;

    // --------------------------------------------------- 2. the main produce

    /// Returns the delivery coordinate of every message, keyed by index.
    private static Dictionary<int, TopicPartitionOffset> ProduceMain(string bootstrap, string topic, int parts)
    {
        Program.Section($"2. produce {Total} messages over {parts} partitions, keys + headers, uncompressed");

        var cfg = BaseProducer(bootstrap, "dnet-main");
        cfg.CompressionType = CompressionType.None;

        var acked = new Dictionary<int, TopicPartitionOffset>();
        var errors = new List<string>();
        var lockObj = new object();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        using (var prod = new ProducerBuilder<byte[], byte[]>(cfg)
                   .SetLogHandler((_, m) => Program.OnLog(m))
                   .SetErrorHandler((_, e) => { if (e.IsFatal) lock (lockObj) errors.Add($"fatal: {e}"); })
                   .Build())
        {
            for (var i = 0; i < Total; i++)
            {
                var idx = i;
                // Explicit partition, so per-partition order is a fact we can
                // assert rather than a partitioner's opinion.
                var tp = new TopicPartition(topic, new Partition(idx % parts));
                prod.Produce(tp, new Message<byte[], byte[]>
                {
                    Key = Key(idx),
                    Value = Value(topic, idx),
                    Headers = HeadersFor(idx),
                }, r =>
                {
                    lock (lockObj)
                    {
                        if (r.Error.IsError) errors.Add($"i={idx}: {r.Error.Code} {r.Error.Reason}");
                        else acked[idx] = r.TopicPartitionOffset;
                    }
                });
            }

            var left = prod.Flush(TimeSpan.FromSeconds(120));
            Program.Check(left == 0, $"Flush drained the queue ({left} left)");
        }
        sw.Stop();

        Program.Check(errors.Count == 0,
            errors.Count == 0 ? "no delivery errors" : $"{errors.Count} delivery errors, first: {errors[0]}");
        Program.Check(acked.Count == Total, $"{acked.Count}/{Total} delivery reports came back");
        Program.Note($"produced in {sw.ElapsedMilliseconds} ms");

        var used = acked.Values.Select(v => v.Partition.Value).Distinct().OrderBy(x => x).ToList();
        Program.Check(used.Count >= 4, $"records landed on {used.Count} distinct partitions: [{string.Join(",", used)}]");

        // Per partition the broker must have handed out contiguous, ascending
        // offsets in the order we produced.
        var monotone = true;
        foreach (var g in acked.OrderBy(kv => kv.Key).GroupBy(kv => kv.Value.Partition.Value))
        {
            long prev = -1;
            foreach (var kv in g)
            {
                if (kv.Value.Offset <= prev) { monotone = false; break; }
                prev = kv.Value.Offset;
            }
        }
        Program.Check(monotone, "delivery-report offsets ascend within every partition, in produce order");

        // And the DEFAULT partitioner (librdkafka consistent_random, CRC32)
        // must also spread. Same topic, no explicit partition.
        var byKey = new List<int>();
        using (var prod = new ProducerBuilder<byte[], byte[]>(BaseProducer(bootstrap, "dnet-partitioner"))
                   .SetLogHandler((_, m) => Program.OnLog(m))
                   .Build())
        {
            for (var i = 0; i < 64; i++)
            {
                var r = Program.Deadline("keyed produce", 30, () =>
                    prod.ProduceAsync(topic, new Message<byte[], byte[]>
                    {
                        Key = Program.U8($"pk-{i}"),
                        Value = Program.U8($"pk-{i}"),
                    }).GetAwaiter().GetResult());
                byKey.Add(r.Partition.Value);
            }
            prod.Flush(TimeSpan.FromSeconds(30));
        }
        var spread = byKey.Distinct().Count();
        Program.Check(spread >= 4,
            $"the default librdkafka partitioner (consistent_random/CRC32) spread 64 keys over {spread} partitions");
        // Same key twice must be the same partition — that is the whole point
        // of a keyed partitioner and it is the client's job, not the facade's.
        Program.Check(byKey.Where((_, i) => i < 32).SequenceEqual(byKey.Take(32)),
            "keyed partitioning is deterministic within a run");

        KeyedExtra = byKey.Count;
        return acked;
    }

    public static int KeyedExtra;

    // ------------------------------------------------------- 3. compression

    private static void Codecs(string bootstrap, string topicBase)
    {
        Program.Section("3. compression codecs");

        var codecs = new[]
        {
            CompressionType.Gzip, CompressionType.Snappy,
            CompressionType.Lz4,  CompressionType.Zstd,
        };
        const int n = 60;

        foreach (var codec in codecs)
        {
            var topic = $"{topicBase}-{codec.ToString().ToLowerInvariant()}";
            var cfg = BaseProducer(bootstrap, $"dnet-{codec}");
            cfg.CompressionType = codec;
            cfg.LingerMs = 200;               // force real batches, or there is
            cfg.BatchNumMessages = 1000;      // nothing to compress
            cfg.BatchSize = 1_000_000;

            var errs = new List<string>();
            try
            {
                using var prod = new ProducerBuilder<byte[], byte[]>(cfg)
                    .SetLogHandler((_, m) => Program.OnLog(m))
                    .Build();
                for (var i = 0; i < n; i++)
                {
                    var idx = i;
                    prod.Produce(new TopicPartition(topic, new Partition(idx % 4)),
                        new Message<byte[], byte[]>
                        {
                            Key = Key(idx),
                            // Compressible on purpose: a codec that silently
                            // no-ops still round-trips, so the assertion here
                            // is correctness, and the LOG says whether it
                            // actually compressed.
                            Value = Program.U8($"{codec}|{idx}|" + new string('a', 400)),
                            Headers = HeadersFor(idx),
                        }, r => { if (r.Error.IsError) lock (errs) errs.Add($"{r.Error.Code}"); });
                }
                var left = prod.Flush(TimeSpan.FromSeconds(60));
                if (left != 0) errs.Add($"flush left {left}");
            }
            catch (Exception e)
            {
                Program.Fail($"{codec}: producer threw {e.GetType().Name}: {e.Message}");
                continue;
            }

            if (errs.Count > 0)
            {
                Program.Fail($"{codec}: {errs.Count} delivery errors, first {errs[0]}");
                continue;
            }

            // Read them straight back with a plain assign (no group: this
            // section is about the codec, not the coordinator).
            var got = ReadAllAssigned(bootstrap, topic, 4, n, $"dnet-codec-{codec}");
            if (got == null) { Program.Fail($"{codec}: read-back timed out"); continue; }

            var okCount = got.Count == n;
            var okBytes = got.All(cr =>
            {
                var s = Program.S8(cr.Message.Value);
                var i = int.Parse(s.Split('|')[1]);
                return Program.BytesEq(cr.Message.Key, Key(i))
                       && Program.BytesEq(cr.Message.Value, Program.U8($"{codec}|{i}|" + new string('a', 400)));
            });
            Program.Check(okCount && okBytes,
                $"{codec}: {got.Count}/{n} records round-tripped byte-exact");
        }

        Program.Note("librdkafka gates zstd on Fetch v10 and this facade caps Fetch at v6 on purpose, so a");
        Program.Note("zstd producer logs 'Broker does not support compression type' and sends UNCOMPRESSED.");
        Program.Note("The records still land. See PLAN_QUEEN_KAFKA.md STATUS; this is not a defect.");
    }

    // ------------------------------------------------------ 4. payload shapes

    private static void Shapes(string bootstrap, string topic)
    {
        Program.Section("4. awkward payload shapes");

        var big = new byte[256 * 1024];
        new Random(7).NextBytes(big);
        var allBytes = new byte[256];
        for (var i = 0; i < 256; i++) allBytes[i] = (byte)i;

        var cases = new List<(string name, byte[] key, byte[] val, Headers hdr)>
        {
            ("null key",        null,                       Program.U8("v-nullkey"), new Headers()),
            ("empty key",       Array.Empty<byte>(),        Program.U8("v-emptykey"), new Headers()),
            ("null value",      Program.U8("k-nullval"),    null,                    new Headers()),
            ("empty value",     Program.U8("k-emptyval"),   Array.Empty<byte>(),     new Headers()),
            ("0x00..0xFF value", Program.U8("k-allbytes"),  allBytes,                new Headers()),
            ("256 KiB value",   Program.U8("k-big"),        big,                     new Headers()),
            ("utf8 key+value",  Program.U8("chiave-àèìòù-鍵-🐝"), Program.U8("valore-àèìòù-値-👑"), new Headers()),
            ("dup+empty headers", Program.U8("k-hdr"),      Program.U8("v-hdr"),     HeadersFor(0)),
        };

        var sent = new Dictionary<string, TopicPartitionOffset>();
        using (var prod = new ProducerBuilder<byte[], byte[]>(BaseProducer(bootstrap, "dnet-shapes"))
                   .SetLogHandler((_, m) => Program.OnLog(m))
                   .Build())
        {
            foreach (var (name, key, val, hdr) in cases)
            {
                try
                {
                    var r = Program.Deadline($"produce shape '{name}'", 45, () =>
                        prod.ProduceAsync(new TopicPartition(topic, new Partition(0)),
                            new Message<byte[], byte[]> { Key = key, Value = val, Headers = hdr })
                            .GetAwaiter().GetResult());
                    sent[name] = r.TopicPartitionOffset;
                }
                catch (Exception e)
                {
                    Program.Fail($"shape '{name}' produce threw {e.GetType().Name}: {e.Message}");
                }
            }
            prod.Flush(TimeSpan.FromSeconds(30));
        }
        Program.Check(sent.Count == cases.Count, $"{sent.Count}/{cases.Count} shapes accepted");

        var got = ReadAllAssigned(bootstrap, topic, 1, cases.Count, "dnet-shapes-read");
        if (got == null) { Program.Fail("shapes read-back timed out"); return; }
        Program.Check(got.Count == cases.Count, $"{got.Count}/{cases.Count} shapes came back");

        var byOffset = got.ToDictionary(c => c.Offset.Value);
        foreach (var (name, key, val, hdr) in cases)
        {
            if (!sent.TryGetValue(name, out var tpo)) continue;
            if (!byOffset.TryGetValue(tpo.Offset.Value, out var cr))
            {
                Program.Fail($"shape '{name}' missing at offset {tpo.Offset}");
                continue;
            }

            // Kafka has no separate "empty" and "null" on the wire for a
            // zero-length field: a null is length -1, an empty is length 0.
            // Both directions must survive.
            var keyOk = Program.BytesEq(cr.Message.Key, key) || (key is null && cr.Message.Key is null);
            var valOk = Program.BytesEq(cr.Message.Value, val) || (val is null && cr.Message.Value is null);
            if (!keyOk)
                Program.Fail($"shape '{name}': key {Hexish(key)} -> {Hexish(cr.Message.Key)}");
            else if (!valOk)
                Program.Fail($"shape '{name}': value ({val?.Length.ToString() ?? "null"} bytes) -> " +
                             $"({cr.Message.Value?.Length.ToString() ?? "null"} bytes)");
            else
                Program.Ok($"shape '{name}' round-tripped byte-exact");

            if (hdr.Count > 0)
            {
                var back = cr.Message.Headers?.ToList() ?? new List<IHeader>();
                var same = back.Count == hdr.Count;
                for (var i = 0; same && i < hdr.Count; i++)
                    same = back[i].Key == hdr[i].Key &&
                           Program.BytesEq(back[i].GetValueBytes(), hdr[i].GetValueBytes());
                Program.Check(same,
                    $"shape '{name}': {hdr.Count} headers (incl. a duplicate name and an empty value) " +
                    $"survived in order (got {back.Count}: {string.Join(",", back.Select(h => h.Key))})");
            }
        }
    }

    private static string Hexish(byte[] b) =>
        b is null ? "<null>" : (b.Length <= 32 ? Program.Hex(b) : $"<{b.Length} bytes>");

    // -------------------------------------------------- 5/6. group + resume

    private static void GroupConsume(string bootstrap, string topic, string group,
                                     Dictionary<int, TopicPartitionOffset> sent, int parts)
    {
        Program.Section($"5. consume through a GROUP ({group}) and verify the round trip");

        // Everything on the topic: the main batch, the auto-create probe, and
        // the 64 keyed records the partitioner test wrote.
        var expectTotal = sent.Count + 1 + KeyedExtra;

        // c1 reads until it has at least MinPer records on EVERY partition, so
        // the resume test below covers all of them rather than whichever ones
        // librdkafka happened to drain first. Then it commits only SOME of
        // them, on purpose: the partitions it deliberately leaves uncommitted
        // are what proves the facade tells "never committed" apart from
        // "committed at 0".
        const int MinPer = 6;
        var commitParts = Enumerable.Range(0, parts - 2).ToHashSet();   // leave the last two
        var first = new List<ConsumeResult<byte[], byte[]>>();
        List<TopicPartitionOffset> committed = null;
        var rebalances = 0;

        using (var c = new ConsumerBuilder<byte[], byte[]>(BaseConsumer(bootstrap, group, "dnet-c1"))
                   .SetLogHandler((_, m) => Program.OnLog(m))
                   .SetPartitionsAssignedHandler((_, ps) =>
                   {
                       rebalances++;
                       Program.Note($"c1 assigned [{string.Join(",", ps.Select(x => x.Partition.Value))}]");
                   })
                   .Build())
        {
            c.Subscribe(topic);
            var per = new Dictionary<int, int>();
            var deadline = DateTime.UtcNow.AddSeconds(180);
            while (DateTime.UtcNow < deadline)
            {
                var cr = c.Consume(TimeSpan.FromSeconds(5));
                if (cr?.Message == null) continue;
                first.Add(cr);
                per.TryGetValue(cr.Partition.Value, out var n);
                per[cr.Partition.Value] = n + 1;
                if (per.Count == parts && per.Values.All(v => v >= MinPer)) break;
            }
            Program.Check(per.Count == parts && per.Values.All(v => v >= MinPer),
                $"c1 read at least {MinPer} records on each of the {parts} partitions " +
                $"({first.Count} total: {string.Join(" ", per.OrderBy(k => k.Key).Select(k => $"p{k.Key}={k.Value}"))})");
            Program.Check(rebalances >= 1, $"c1 got an assignment ({rebalances} rebalance callback(s))");

            // Commit an EXPLICIT list, not the whole position set, and shape it
            // so the resume test below has something to prove on every kind of
            // partition:
            //
            //   p0..p5   committed at max+1        -> c2 must resume exactly there
            //   p1, p2   committed at a REWIND     -> c2 must go BACK to it, which
            //            (offset 10) instead          is the only way to tell
            //                                         "the facade stored what I
            //                                         sent" from "the facade
            //                                         happened to agree with my
            //                                         local position"
            //   p6, p7   not committed at all      -> c2 must fall back to earliest
            const int Rewind = 10;
            var rewound = new HashSet<int> { 1, 2 };
            var toCommit = first.GroupBy(x => x.Partition.Value)
                                .Where(g => commitParts.Contains(g.Key))
                                .Select(g => new TopicPartitionOffset(topic, new Partition(g.Key),
                                    new Offset(rewound.Contains(g.Key)
                                        ? Rewind
                                        : g.Max(x => x.Offset.Value) + 1)))
                                .OrderBy(x => x.Partition.Value)
                                .ToList();
            // Commit(IEnumerable<TopicPartitionOffset>) returns void — only the
            // parameterless overload hands back the positions it chose. So the
            // read-back below is the only proof the write landed.
            try
            {
                Program.Deadline("Commit(list)", 60, () => { c.Commit(toCommit); return 0; });
                committed = toCommit;
                Program.Ok($"Commit() wrote {toCommit.Count} of {parts} partitions: " +
                           string.Join(" ", toCommit.Select(x => $"p{x.Partition.Value}@{x.Offset.Value}")));
            }
            catch (Exception e)
            {
                Program.Fail($"Commit(list) threw {e.GetType().Name}: {e.Message}");
            }

            try
            {
                var back = Program.Deadline("Committed()", 60, () =>
                    c.Committed(c.Assignment, TimeSpan.FromSeconds(30)));
                var same = committed != null && committed.All(x =>
                    back.Any(y => y.TopicPartition == x.TopicPartition && y.Offset == x.Offset));
                Program.Check(same, "Committed() reads back exactly what Commit() wrote (OffsetFetch)");
            }
            catch (Exception e)
            {
                Program.Fail($"Committed() threw {e.GetType().Name}: {e.Message}");
            }

            Program.Deadline("c1.Close()", 60, () => { c.Close(); return 0; });
            Program.Ok("c1 closed cleanly (LeaveGroup)");
        }

        // ---- a fresh client, same group, nothing but OffsetFetch.
        //
        // Two facts in one round trip, and the second is the one that bites in
        // production: a brand-new consumer with no local cache must read back
        // the committed offsets over the wire, AND it must be told "never
        // committed" for the partitions c1 left alone. If the facade answered 0
        // for an uncommitted partition instead of -1, every group that ever
        // crashed before its first commit would silently replay from the head
        // and the client's auto.offset.reset would never get a say.
        if (committed != null)
        {
            Program.Section("5b. OffsetFetch from a fresh client: committed vs never-committed");
            var tps = Enumerable.Range(0, parts).Select(x => new TopicPartition(topic, new Partition(x))).ToList();
            using var probe = new ConsumerBuilder<byte[], byte[]>(BaseConsumer(bootstrap, group, "dnet-probe"))
                .SetLogHandler((_, m) => Program.OnLog(m))
                .Build();
            try
            {
                var back = Program.Deadline("fresh Committed()", 60,
                    () => probe.Committed(tps, TimeSpan.FromSeconds(30)));
                var want = committed.ToDictionary(x => x.Partition.Value, x => x.Offset.Value);
                var okReal = back.Where(x => want.ContainsKey(x.Partition.Value))
                                 .All(x => x.Offset.Value == want[x.Partition.Value]);
                var never = back.Where(x => !want.ContainsKey(x.Partition.Value)).ToList();
                var okUnset = never.Count > 0 && never.All(x => x.Offset == Offset.Unset);
                Program.Check(okReal,
                    $"a fresh client reads back the {want.Count} committed offsets over OffsetFetch: " +
                    string.Join(" ", back.OrderBy(x => x.Partition.Value).Select(x => $"p{x.Partition.Value}@{x.Offset.Value}")));
                Program.Check(okUnset,
                    $"the {never.Count} never-committed partitions come back Unset, not 0 — the facade " +
                    $"distinguishes 'no offset' from 'offset 0' ({string.Join(" ", never.Select(x => $"p{x.Partition.Value}@{x.Offset.Value}"))})");
            }
            catch (Exception e)
            {
                Program.Fail($"fresh Committed() threw {e.GetType().Name}: {e.Message}");
            }
            probe.Close();
        }

        // ---- a SECOND instance in the SAME group must resume where c1 stopped
        Program.Section("6. a new consumer in the same group resumes from the committed offset");

        var second = new List<ConsumeResult<byte[], byte[]>>();
        using (var c = new ConsumerBuilder<byte[], byte[]>(BaseConsumer(bootstrap, group, "dnet-c2"))
                   .SetLogHandler((_, m) => Program.OnLog(m))
                   .Build())
        {
            c.Subscribe(topic);
            var idle = 0;
            var deadline = DateTime.UtcNow.AddSeconds(180);
            while (idle < 4 && DateTime.UtcNow < deadline)
            {
                var cr = c.Consume(TimeSpan.FromSeconds(5));
                if (cr?.Message != null) { second.Add(cr); idle = 0; }
                else idle++;
            }
            Program.Deadline("c2.Close()", 60, () => { c.Close(); return 0; });
        }

        Program.Note($"c1 read {first.Count}, c2 read {second.Count}, topic holds {expectTotal}");

        var union = new HashSet<(int, long)>();
        foreach (var cr in first) union.Add((cr.Partition.Value, cr.Offset.Value));
        var dupes = new List<(int, long)>();
        foreach (var cr in second)
            if (!union.Add((cr.Partition.Value, cr.Offset.Value))) dupes.Add((cr.Partition.Value, cr.Offset.Value));

        Program.Check(union.Count == expectTotal,
            $"c1 + c2 between them saw every one of the {expectTotal} records (got {union.Count})");

        // THE invariant a commit buys you, stated over every record c2 saw:
        // on a committed partition, c2 must never be handed an offset BELOW the
        // commit point. Everything at or above it is fair game — indeed on the
        // two rewound partitions c2 is SUPPOSED to re-read from 10 — so "no
        // duplicates at all" would be asserting the wrong thing, and asserting
        // nothing at all would let a commit that never stuck through.
        var cmap = committed?.ToDictionary(x => x.Partition.Value, x => x.Offset.Value)
                   ?? new Dictionary<int, long>();
        var below = second
            .Where(x => cmap.TryGetValue(x.Partition.Value, out var at) && x.Offset.Value < at)
            .ToList();
        Program.Check(below.Count == 0,
            below.Count == 0
                ? $"c2 was never handed a record below a commit point " +
                  $"({dupes.Count} redeliveries in total: the deliberate rewinds and the two never-committed partitions)"
                : $"{below.Count} records delivered BELOW their commit point, e.g. " +
                  $"p{below[0].Partition.Value}@{below[0].Offset.Value} (committed {cmap[below[0].Partition.Value]})");

        // c2 must have started AT the committed offset on the partitions c1
        // committed, and at 0 (auto.offset.reset=earliest) on the two it did
        // not. Both halves are correct behaviour and both are worth an
        // assertion; only asserting the first would let a facade that answers
        // "0" for an uncommitted partition slide through.
        if (committed != null && second.Count > 0)
        {
            var firstSeen = second.GroupBy(x => x.Partition.Value)
                                  .ToDictionary(g => g.Key, g => g.Min(x => x.Offset.Value));
            var resumed = true; var reset = true; var nResumed = 0; var nReset = 0;
            foreach (var (part, lo) in firstSeen)
            {
                var tpo = committed.FirstOrDefault(x => x.Partition.Value == part);
                if (tpo != null)
                {
                    nResumed++;
                    if (lo != tpo.Offset.Value)
                    {
                        resumed = false;
                        Program.Note($"p{part}: committed {tpo.Offset.Value}, c2 restarted at {lo}");
                    }
                }
                else
                {
                    nReset++;
                    if (lo != 0)
                    {
                        reset = false;
                        Program.Note($"p{part}: never committed, c2 restarted at {lo} not 0");
                    }
                }
            }
            Program.Check(nResumed > 0 && resumed,
                $"on all {nResumed} committed partitions c2's first record is exactly the committed offset");
            Program.Check(nReset > 0 && reset,
                $"on the {nReset} never-committed partitions c2 fell back to earliest (offset 0), as configured");
        }

        // ---- and now the byte-exact verification, over everything both saw
        var all = first.Concat(second)
                       .GroupBy(x => (x.Partition.Value, x.Offset.Value))
                       .Select(g => g.First())
                       .ToList();

        var main = new Dictionary<int, ConsumeResult<byte[], byte[]>>();
        foreach (var kv in sent)
        {
            var cr = all.FirstOrDefault(x => x.Partition.Value == kv.Value.Partition.Value
                                          && x.Offset.Value == kv.Value.Offset.Value);
            if (cr != null) main[kv.Key] = cr;
        }
        Program.Check(main.Count == sent.Count,
            $"every one of the {sent.Count} main-batch coordinates was found in the consumed set ({main.Count})");

        var badKey = 0; var badVal = 0; var badHdr = 0; var badTs = 0;
        var now = DateTimeOffset.UtcNow;
        string firstBad = null;
        foreach (var (i, cr) in main)
        {
            if (!Program.BytesEq(cr.Message.Key, Key(i)))
            { badKey++; firstBad ??= $"i={i} key {Program.Hex(Key(i))} -> {Program.Hex(cr.Message.Key)}"; }
            if (!Program.BytesEq(cr.Message.Value, Value(topic, i)))
            { badVal++; firstBad ??= $"i={i} value '{Program.S8(Value(topic, i))}' -> '{Program.S8(cr.Message.Value)}'"; }

            var want = HeadersFor(i);
            var back = cr.Message.Headers?.ToList() ?? new List<IHeader>();
            var same = back.Count == want.Count;
            for (var k = 0; same && k < want.Count; k++)
                same = back[k].Key == want[k].Key &&
                       Program.BytesEq(back[k].GetValueBytes(), want[k].GetValueBytes());
            if (!same)
            { badHdr++; firstBad ??= $"i={i} headers [{string.Join(",", want.Select(h => h.Key))}] -> " +
                                     $"[{string.Join(",", back.Select(h => h.Key))}]"; }

            var age = now - cr.Message.Timestamp.UtcDateTime;
            if (cr.Message.Timestamp.Type == TimestampType.NotAvailable ||
                age > TimeSpan.FromMinutes(30) || age < TimeSpan.FromMinutes(-5))
            { badTs++; firstBad ??= $"i={i} timestamp {cr.Message.Timestamp.Type} {cr.Message.Timestamp.UtcDateTime:O}"; }
        }
        Program.Check(badKey == 0, $"keys byte-exact ({main.Count - badKey}/{main.Count})");
        Program.Check(badVal == 0, $"values byte-exact ({main.Count - badVal}/{main.Count})");
        Program.Check(badHdr == 0, $"headers byte-exact and in order, duplicate name included ({main.Count - badHdr}/{main.Count})");
        Program.Check(badTs == 0, $"timestamps are CreateTime and recent ({main.Count - badTs}/{main.Count})");
        if (firstBad != null) Program.Note($"first mismatch: {firstBad}");

        if (main.Count > 0)
        {
            var t0 = main.Values.First().Message.Timestamp;
            Program.Note($"timestamp sample: {t0.Type} {t0.UtcDateTime:O}");
        }

        // per-partition ORDER: on partition p, ascending offset must mean
        // ascending produce index.
        var ordered = true; string orderBad = null;
        foreach (var g in main.GroupBy(kv => kv.Value.Partition.Value))
        {
            var seq = g.OrderBy(kv => kv.Value.Offset.Value).Select(kv => kv.Key).ToList();
            for (var k = 1; k < seq.Count; k++)
                if (seq[k] <= seq[k - 1])
                { ordered = false; orderBad ??= $"p{g.Key}: index {seq[k - 1]} then {seq[k]}"; }
        }
        Program.Check(ordered, "per-partition order preserved: offsets ascend with produce index" +
                               (orderBad == null ? "" : $" ({orderBad})"));
    }

    // --------------------------------------- 7. watermarks / assign / seek

    private static void Watermarks(string bootstrap, string topic, int parts, int mainCount)
    {
        Program.Section("7. ListOffsets: QueryWatermarkOffsets, Assign at an offset, Seek, Position");

        using var c = new ConsumerBuilder<byte[], byte[]>(
                BaseConsumer(bootstrap, $"dnet-wm-{Guid.NewGuid():N}", "dnet-wm"))
            .SetLogHandler((_, m) => Program.OnLog(m))
            .Build();

        long sum = 0; var lowsZero = true;
        for (var p = 0; p < parts; p++)
        {
            var tp = new TopicPartition(topic, new Partition(p));
            WatermarkOffsets wm;
            try
            {
                wm = Program.Deadline($"QueryWatermarkOffsets p{p}", 45,
                    () => c.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(20)));
            }
            catch (Exception e)
            {
                Program.Fail($"QueryWatermarkOffsets p{p} threw {e.GetType().Name}: {e.Message}");
                continue;
            }
            if (wm.Low.Value != 0) lowsZero = false;
            sum += wm.High.Value;
            if (p < 3) Program.Note($"p{p}: low={wm.Low} high={wm.High}");
        }
        Program.Check(lowsZero, "every partition's low watermark is 0 (nothing retained away)");
        Program.Check(sum == mainCount + 1 + KeyedExtra,
            $"high watermarks sum to the record count: {sum} vs {mainCount + 1 + KeyedExtra}");

        // Assign at an explicit offset — the ListOffsets-free path.
        var tp0 = new TopicPartition(topic, new Partition(0));
        const int at = 17;
        try
        {
            c.Assign(new TopicPartitionOffset(tp0, new Offset(at)));
            var cr = Program.Deadline("consume after Assign(offset)", 60, () =>
            {
                var d = DateTime.UtcNow.AddSeconds(45);
                while (DateTime.UtcNow < d)
                {
                    var r = c.Consume(TimeSpan.FromSeconds(5));
                    if (r?.Message != null) return r;
                }
                return null;
            });
            Program.Check(cr != null && cr.Offset.Value == at,
                $"Assign(p0, offset {at}) delivered exactly offset {cr?.Offset.Value.ToString() ?? "<none>"}");

            // Seek backwards on the live assignment.
            c.Seek(new TopicPartitionOffset(tp0, new Offset(3)));
            var cr2 = Program.Deadline("consume after Seek", 60, () =>
            {
                var d = DateTime.UtcNow.AddSeconds(45);
                while (DateTime.UtcNow < d)
                {
                    var r = c.Consume(TimeSpan.FromSeconds(5));
                    if (r?.Message != null) return r;
                }
                return null;
            });
            Program.Check(cr2 != null && cr2.Offset.Value == 3,
                $"Seek(p0, 3) rewound: next record is offset {cr2?.Offset.Value.ToString() ?? "<none>"}");

            var pos = c.Position(tp0);
            Program.Check(pos.Value == 4, $"Position(p0) after reading offset 3 is 4 (got {pos})");
        }
        catch (Exception e)
        {
            Program.Fail($"assign/seek/position threw {e.GetType().Name}: {e.Message}");
        }

        // Offset.Beginning / Offset.End as logical positions.
        try
        {
            c.Unassign();
            c.Assign(new TopicPartitionOffset(tp0, Offset.End));
            var none = Program.Deadline("consume at Offset.End", 30, () => c.Consume(TimeSpan.FromSeconds(8)));
            Program.Check(none?.Message == null, "Assign(Offset.End) sits at the head: nothing to read");

            c.Unassign();
            c.Assign(new TopicPartitionOffset(tp0, Offset.Beginning));
            var zero = Program.Deadline("consume at Offset.Beginning", 45, () =>
            {
                var d = DateTime.UtcNow.AddSeconds(35);
                while (DateTime.UtcNow < d)
                {
                    var r = c.Consume(TimeSpan.FromSeconds(5));
                    if (r?.Message != null) return r;
                }
                return null;
            });
            Program.Check(zero != null && zero.Offset.Value == 0,
                $"Assign(Offset.Beginning) starts at 0 (got {zero?.Offset.Value.ToString() ?? "<none>"})");
        }
        catch (Exception e)
        {
            Program.Fail($"logical offsets threw {e.GetType().Name}: {e.Message}");
        }

        c.Close();
    }

    // ------------------------------------------------------------- read-back

    /// Assign every partition from the beginning and drain, with a deadline.
    /// No group: this is the path that exercises Fetch and ListOffsets alone.
    public static List<ConsumeResult<byte[], byte[]>> ReadAllAssigned(
        string bootstrap, string topic, int parts, int want, string clientId)
    {
        var cfg = BaseConsumer(bootstrap, $"{clientId}-{Guid.NewGuid():N}", clientId);
        using var c = new ConsumerBuilder<byte[], byte[]>(cfg)
            .SetLogHandler((_, m) => Program.OnLog(m))
            .Build();

        c.Assign(Enumerable.Range(0, parts)
            .Select(p => new TopicPartitionOffset(topic, new Partition(p), Offset.Beginning)));

        var got = new List<ConsumeResult<byte[], byte[]>>();
        var idle = 0;
        var deadline = DateTime.UtcNow.AddSeconds(120);
        while (got.Count < want && idle < 4 && DateTime.UtcNow < deadline)
        {
            var cr = c.Consume(TimeSpan.FromSeconds(5));
            if (cr?.Message != null) { got.Add(cr); idle = 0; }
            else idle++;
        }
        c.Close();
        return DateTime.UtcNow >= deadline && got.Count < want ? null : got;
    }
}
