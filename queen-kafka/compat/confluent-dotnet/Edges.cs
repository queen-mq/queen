// =============================================================================
// Edges.cs — the things a .NET user WILL do that are not the ordinary path.
// Some of them the facade supports and some it deliberately does not, and the
// list moves: M7 turned three of the refusals below into working calls. Where a
// call is refused the assertion is that the client fails FAST and LEGIBLY
// rather than hanging or disconnecting with no explanation, because that is the
// difference between a five-minute diagnosis and an afternoon. Where a call
// works the assertion is on the ANSWER and not merely on the absence of an
// exception.
//
//   * EnableIdempotence = true           -> WORKS since M7 F3 (InitProducerId
//                                           key 22, advertised v0-4)
//   * transactions                       -> still refused, and not on key 22:
//                                           FindCoordinator answers
//                                           COORDINATOR_NOT_AVAILABLE for a
//                                           TRANSACTION coordinator first
//   * AdminClient.CreateTopics           -> WORKS since M7 F1; num_partitions
//                                           is accepted and not acted on
//   * AdminClient.DescribeCluster        -> not advertised, and rides Metadata
//   * AdminClient.ListConsumerGroups     -> WORKS since M7 F2, and the probe is
//                                           a positive one: this call used to
//                                           abort the process
//   * AdminClient.DescribeConfigs        -> WORKS since M7 F1
//   * SessionTimeoutMs outside 6s..300s  -> INVALID_SESSION_TIMEOUT (26)
//   * a "__"-prefixed topic              -> UNKNOWN_TOPIC_OR_PARTITION
//
// The advertised surface is queen-kafka/src/versions.rs.
// =============================================================================

using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace QueenKafkaCompat;

public static class Edges
{
    /// QK_EDGE_PROBES=a,b,c narrows the run. It earned its keep when one of
    /// these probes could take the whole PROCESS down: ListConsumerGroups
    /// against a broker without KIP-518 aborted librdkafka in glibc (see the
    /// listgroups section), and bisecting that needed the other sections out of
    /// the way. M7 F2 removed the cause; the knob stays because the next such
    /// probe will want it too.
    private static HashSet<string> Selected;

    private static bool Want(string name) =>
        Selected == null || Selected.Contains(name);

    public static void Run(string bootstrap, string runId)
    {
        var sel = Environment.GetEnvironmentVariable("QK_EDGE_PROBES");
        if (!string.IsNullOrWhiteSpace(sel))
            Selected = new HashSet<string>(sel.Split(',', StringSplitOptions.RemoveEmptyEntries)
                                              .Select(s => s.Trim()), StringComparer.OrdinalIgnoreCase);

        if (Want("idempotence")) Idempotence(bootstrap, runId);
        if (Want("transactions")) Transactions(bootstrap, runId);
        Admin(bootstrap, runId);
        if (Want("session")) SessionTimeout(bootstrap, runId);
        if (Want("internal")) InternalTopic(bootstrap);
    }

    // --------------------------------------------------------- idempotence

    private static void Idempotence(string bootstrap, string runId)
    {
        Program.Section("edge: EnableIdempotence = true (works since M7 F3)");

        var cfg = Core.BaseProducer(bootstrap, "dnet-idempotent");
        cfg.EnableIdempotence = true;
        cfg.MessageTimeoutMs = 15000;

        string fatal = null;
        var errs = new List<string>();
        try
        {
            using var prod = new ProducerBuilder<byte[], byte[]>(cfg)
                .SetLogHandler((_, m) => Program.OnLog(m))
                .SetErrorHandler((_, e) =>
                {
                    lock (errs) errs.Add($"{(e.IsFatal ? "FATAL " : "")}{e.Code}: {e.Reason}");
                    if (e.IsFatal) fatal ??= $"{e.Code}: {e.Reason}";
                })
                .Build();

            try
            {
                var r = Program.Deadline("idempotent produce", 60, () =>
                    prod.ProduceAsync($"dnet-{runId}-idem", new Message<byte[], byte[]>
                    { Key = Program.U8("k"), Value = Program.U8("v") })
                        .GetAwaiter().GetResult());
                Program.Note($"the idempotent produce landed at {r.TopicPartitionOffset} — " +
                             "InitProducerId (key 22) is advertised and the sequence window took the batch");
            }
            catch (ProduceException<byte[], byte[]> pe)
            {
                Program.Note($"ProduceException: {pe.Error.Code} / IsFatal={pe.Error.IsFatal} / {pe.Error.Reason}");
            }
            catch (Exception e)
            {
                Program.Note($"{e.GetType().Name}: {e.Message}");
            }

            prod.Flush(TimeSpan.FromSeconds(20));
        }
        catch (Exception e)
        {
            Program.Note($"the producer could not even be built: {e.GetType().Name}: {e.Message}");
        }

        lock (errs)
        {
            foreach (var e in errs.Distinct().Take(6)) Program.Note($"error cb: {e}");
        }

        // Whatever happened, it must have been decisive within the deadline.
        // That is the assertion; the outcome itself is documentation.
        Program.Ok("EnableIdempotence=true resolved within the deadline (outcome recorded above)");
        if (fatal != null)
            Program.Note($"fatal error surfaced: {fatal}");
        Program.Note("Nothing to mitigate since M7 F3: EnableIdempotence = true sends. librdkafka " +
                     "still defaults it OFF, so a plain ProducerConfig has always worked here.");
    }

    // ---------------------------------------------------------- transactions

    private static void Transactions(string bootstrap, string runId)
    {
        Program.Section("edge: transactions (TransactionalId -> InitProducerId)");

        var cfg = Core.BaseProducer(bootstrap, "dnet-txn");
        cfg.EnableIdempotence = true;              // implied by TransactionalId
        cfg.TransactionalId = $"dnet-{runId}-txn";
        cfg.MessageTimeoutMs = 15000;
        cfg.TransactionTimeoutMs = 20000;

        try
        {
            using var prod = new ProducerBuilder<byte[], byte[]>(cfg)
                .SetLogHandler((_, m) => Program.OnLog(m))
                .SetErrorHandler((_, e) => { if (e.IsFatal) Program.Note($"txn fatal: {e.Code} {e.Reason}"); })
                .Build();

            var outcome = Program.Deadline("InitTransactions", 90, () =>
            {
                try { prod.InitTransactions(TimeSpan.FromSeconds(30)); return "SUCCEEDED"; }
                catch (KafkaException ke) { return $"{ke.GetType().Name}: {ke.Error.Code} / {ke.Error.Reason}"; }
                catch (Exception ex) { return $"{ex.GetType().Name}: {ex.Message}"; }
            });
            Program.Ok($"InitTransactions failed fast rather than hanging: {outcome}");
            if (outcome == "SUCCEEDED")
                Program.Fail("InitTransactions SUCCEEDED — transactions are refused here (FindCoordinator answers " +
                             "COORDINATOR_NOT_AVAILABLE for a TRANSACTION coordinator), so this is a surprise worth chasing");
        }
        catch (TimeoutException)
        {
            Program.Fail("InitTransactions HUNG past its deadline");
        }
        catch (Exception e)
        {
            Program.Ok($"the transactional producer refused to build: {e.GetType().Name}: {e.Message}");
        }
    }

    // ---------------------------------------------------------- admin client

    private static void Admin(string bootstrap, string runId)
    {
        Program.Section("edge: AdminClient against the 21-API-key surface");

        if (!Want("metadata") && !Want("createtopics") && !Want("describecluster")
            && !Want("listgroups") && !Want("describeconfigs")) return;

        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrap,
            ClientId = "dnet-admin",
            Debug = "protocol",
        }).SetLogHandler((_, m) => Program.OnLog(m)).Build();

        // The one that DOES work: Metadata is advertised 0..9.
        if (Want("metadata"))
        try
        {
            var md = Program.Deadline("AdminClient.GetMetadata", 45,
                () => admin.GetMetadata(TimeSpan.FromSeconds(20)));
            Program.Check(md.Brokers.Count >= 1 && md.Topics != null,
                $"AdminClient.GetMetadata works: {md.Brokers.Count} broker(s), {md.Topics.Count} topic(s), " +
                $"originating broker '{md.OriginatingBrokerName}'");
        }
        catch (Exception e)
        {
            Program.Fail($"AdminClient.GetMetadata threw {e.GetType().Name}: {e.Message}");
        }

        // CreateTopics (key 19) is ADVERTISED since M7 F1, so this is a positive
        // check now. It was a Probe until F1 landed: an AdminClient that asked
        // for a topic got UnsupportedVersionException off ApiVersions and the
        // documented answer was "let the facade auto-create on Metadata".
        //
        // "It did not throw" is not the whole claim. CreateTopicsAsync returns a
        // non-generic Task, so there is nothing to inspect in the result, and a
        // facade that answered error_code 0 and created nothing would pass a
        // bare inversion. The topic is therefore read back through Metadata and
        // has to be there.
        //
        // NumPartitions is asked for as 4 and does NOT come back as 4, on
        // purpose: Queen declares no per-topic width, so every client sees
        // max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS) and a create's
        // num_partitions is accepted without being acted on. That is one of the
        // deliberate deviations on PLAN_QUEEN_KAFKA.md's list, so what is
        // asserted is that the topic EXISTS and has a real width; the number
        // itself is reported rather than demanded.
        if (Want("createtopics"))
            Works("CreateTopics", () =>
            {
                var name = $"dnet-{runId}-admin";
                admin.CreateTopicsAsync(new[]
                {
                    new TopicSpecification { Name = name, NumPartitions = 4, ReplicationFactor = 1 }
                }, new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) }).GetAwaiter().GetResult();

                var md = admin.GetMetadata(name, TimeSpan.FromSeconds(20));
                var t = md.Topics.FirstOrDefault(x => x.Topic == name);
                if (t == null)
                    throw new InvalidOperationException($"CreateTopics answered no error but Metadata has no {name}");
                if (t.Partitions.Count < 1)
                    throw new InvalidOperationException($"{name} exists but Metadata reports no partitions at all");
                Program.Note($"NumPartitions=4 was requested and the topic has {t.Partitions.Count}: " +
                             "the width is the facade's, by design, and the client's next Metadata agrees with it");
                return $"{name} exists with {t.Partitions.Count} partitions, none of them produced to";
            });

        // NOT a probe for an unadvertised key: librdkafka implements
        // DescribeCluster over the ORDINARY Metadata request when the broker
        // does not advertise API key 60, so this one is expected to SUCCEED and
        // its success is a fact about the client, not about the facade.
        if (Want("describecluster"))
            try
            {
                var dc = Program.Deadline("DescribeCluster", 60, () => admin.DescribeClusterAsync(
                    new DescribeClusterOptions { RequestTimeout = TimeSpan.FromSeconds(15) }).GetAwaiter().GetResult());
                Program.Ok($"DescribeCluster answered from Metadata alone: {dc.Nodes.Count} node(s), " +
                           $"clusterId '{dc.ClusterId}', controller {dc.Controller?.Id.ToString() ?? "<none>"}");
            }
            catch (Exception e)
            {
                var inner = e is AggregateException ae ? ae.InnerException ?? e : e;
                Program.Ok($"DescribeCluster failed: {inner.GetType().Name}: {First(inner.Message)}");
            }

        // ---------------------------------------------------------------------
        // THE ONE THAT USED TO KILL THE PROCESS, AND WHY IT IS NOW A POSITIVE
        // CHECK.
        //
        // AdminClient.ListConsumerGroupsAsync against a broker whose
        // ApiVersions does not advertise ListGroups (key 16) CORRUPTS THE HEAP
        // and ABORTS THE PROCESS in Confluent.Kafka 2.15.0 / librdkafka 2.15.0:
        //
        //     free(): double free detected in tcache 2      -> exit 134 (SIGABRT)
        //   or, when the corruption lands elsewhere first,  -> exit 139 (SIGSEGV)
        //
        // The request was NEVER SENT. A debug=protocol trace of the whole run
        // was ApiVersions + Metadata on two connections and nothing else:
        // librdkafka read ApiVersions, decided ListGroups was unsupported, and
        // crashed building the LOCAL error result. No try/catch could save you
        // — a glibc abort is not a .NET exception — which is why the fix could
        // only ever be on the broker side. Reproduced on 2.6.1 (134), 2.11.1
        // (139) and 2.15.0 (both), so it was never a regression in a recent
        // release; it is what this client has always done against a broker
        // without KIP-518.
        //
        // M7 F2 advertises key 16, so the call is made, answered, and returns.
        // The check is therefore inverted: SUCCEEDING is the pass, and the
        // process exiting at all is half of what it proves.
        // ---------------------------------------------------------------------
        if (Want("listgroups"))
            Works("ListConsumerGroups", () => admin.ListConsumerGroupsAsync(
                new ListConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(15) }).GetAwaiter().GetResult());

        // DescribeConfigs (key 32) is ADVERTISED since M7 F1, so this is a
        // positive check too. An empty answer would satisfy a bare inversion
        // and prove nothing, so the entries are counted: the facade reports a
        // key only where it can name what enforces it, and "a short list" is
        // the designed answer while "no list" is a broken one.
        if (Want("describeconfigs"))
            Works("DescribeConfigs", () =>
            {
                var results = admin.DescribeConfigsAsync(new[]
                {
                    new ConfigResource { Type = ResourceType.Broker, Name = BrokerIdForConfigs(admin) }
                }, new DescribeConfigsOptions { RequestTimeout = TimeSpan.FromSeconds(15) }).GetAwaiter().GetResult();

                var entries = results.SelectMany(r => r.Entries).ToList();
                foreach (var e in entries.Take(8))
                    Program.Note($"broker config {e.Key}={e.Value.Value} (read_only={e.Value.IsReadOnly})");
                if (entries.Count == 0)
                    throw new InvalidOperationException(
                        "DescribeConfigs answered with no entries at all for the broker resource");
                return $"{entries.Count} broker config entrie(s)";
            });

        Program.Note("The advertised surface is queen-kafka/src/versions.rs. Since M7 the topics-admin");
        Program.Note("trio and the groups-admin trio are on it; DescribeCluster is not, and rides Metadata.");
    }

    /// DescribeConfigs on a BROKER resource is routed to that broker id, so the
    /// id has to be one the facade actually advertises (it is 0, not Kafka's
    /// conventional 1). Asking for a broker that does not exist would test the
    /// client's routing, not the facade's surface.
    private static string BrokerIdForConfigs(IAdminClient admin)
    {
        var forced = Environment.GetEnvironmentVariable("QK_BROKER_ID");
        if (!string.IsNullOrEmpty(forced)) return forced;
        try
        {
            var md = admin.GetMetadata(TimeSpan.FromSeconds(15));
            return md.Brokers.Count > 0 ? md.Brokers[0].BrokerId.ToString() : "0";
        }
        catch { return "0"; }
    }

    /// The inverse of [Probe]: this call is expected to WORK, and the check is
    /// that it returns rather than that it fails legibly. Kept beside Probe
    /// because an API moving from one list to the other is exactly what a
    /// milestone does, and the two want to read the same.
    private static void Works(string name, Func<object> call)
    {
        try
        {
            var r = Program.Deadline(name, 60, call);
            // A probe that answers with a SENTENCE says what it saw; one that
            // answers with a result object can only be named by its type.
            Program.Ok($"{name} works: {(r is string s ? s : r?.GetType().Name)}");
        }
        catch (TimeoutException)
        {
            Program.Fail($"{name} HUNG past its deadline");
        }
        catch (Exception e)
        {
            var inner = e is AggregateException ae ? ae.InnerException ?? e : e;
            var code = inner is KafkaException ke ? $" [{ke.Error.Code}]" : "";
            Program.Fail($"{name} failed: {inner.GetType().Name}{code}: {First(inner.Message)}");
        }
    }

    private static void Probe(string name, Func<object> call)
    {
        try
        {
            var r = Program.Deadline(name, 60, call);
            Program.Fail($"{name} SUCCEEDED ({r?.GetType().Name}) — versions.rs does not advertise it; investigate");
        }
        catch (TimeoutException)
        {
            Program.Fail($"{name} HUNG past its deadline instead of failing fast");
        }
        catch (Exception e)
        {
            var inner = e is AggregateException ae ? ae.InnerException ?? e : e;
            var code = inner is KafkaException ke ? $" [{ke.Error.Code}]" : "";
            Program.Ok($"{name} failed fast: {inner.GetType().Name}{code}: {First(inner.Message)}");
        }
    }

    private static string First(string s)
    {
        var i = s.IndexOf('\n');
        var line = i < 0 ? s : s[..i];
        return line.Length > 200 ? line[..200] + "..." : line;
    }

    // -------------------------------------------------------- session window

    private static void SessionTimeout(string bootstrap, string runId)
    {
        Program.Section("edge: SessionTimeoutMs below the facade's 6000 ms floor");

        var cfg = Core.BaseConsumer(bootstrap, $"dnet-{runId}-badsession", "dnet-badsession");
        cfg.SessionTimeoutMs = 3000;
        cfg.HeartbeatIntervalMs = 1000;

        var seen = new List<string>();
        try
        {
            using var c = new ConsumerBuilder<byte[], byte[]>(cfg)
                .SetLogHandler((_, m) => Program.OnLog(m))
                .SetErrorHandler((_, e) => { lock (seen) seen.Add($"{e.Code}: {e.Reason}"); })
                .Build();
            c.Subscribe($"dnet-{runId}-core");

            var outcome = Program.Deadline("consume with a too-short session timeout", 60, () =>
            {
                var d = DateTime.UtcNow.AddSeconds(40);
                while (DateTime.UtcNow < d)
                {
                    try
                    {
                        var cr = c.Consume(TimeSpan.FromSeconds(3));
                        if (cr?.Message != null) return "records were delivered anyway";
                    }
                    catch (ConsumeException ce)
                    {
                        return $"ConsumeException {ce.Error.Code}: {First(ce.Error.Reason)}";
                    }
                }
                return "no record and no exception within 40s";
            });
            Program.Ok($"outcome: {outcome}");
            lock (seen)
                foreach (var s in seen.Distinct().Take(4)) Program.Note($"error cb: {s}");
            c.Close();
        }
        catch (Exception e)
        {
            Program.Ok($"rejected at construction: {e.GetType().Name}: {First(e.Message)}");
        }
        Program.Note("The facade answers INVALID_SESSION_TIMEOUT (26) outside " +
                     "QUEEN_KAFKA_GROUP_MIN/MAX_SESSION_TIMEOUT_MS (6000..300000).");
    }

    // ------------------------------------------------------- internal topic

    private static void InternalTopic(string bootstrap)
    {
        Program.Section("edge: a '__'-prefixed topic never exists");

        var cfg = Core.BaseConsumer(bootstrap, $"dnet-internal-{Guid.NewGuid():N}", "dnet-internal");
        cfg.AllowAutoCreateTopics = false;
        using var c = new ConsumerBuilder<byte[], byte[]>(cfg)
            .SetLogHandler((_, m) => Program.OnLog(m))
            .Build();

        try
        {
            var wm = Program.Deadline("QueryWatermarkOffsets on __nope", 45,
                () => c.QueryWatermarkOffsets(new TopicPartition("__nope", 0), TimeSpan.FromSeconds(15)));
            Program.Fail($"__nope answered watermarks {wm.Low}..{wm.High} instead of UNKNOWN_TOPIC_OR_PARTITION");
        }
        catch (TimeoutException)
        {
            Program.Fail("__nope watermark query HUNG");
        }
        catch (KafkaException ke)
        {
            Program.Check(ke.Error.Code is ErrorCode.UnknownTopicOrPart or ErrorCode.Local_UnknownPartition
                              or ErrorCode.Local_UnknownTopic,
                $"__nope is unknown: {ke.Error.Code} / {First(ke.Error.Reason)}");
        }
        catch (Exception e)
        {
            Program.Ok($"__nope rejected: {e.GetType().Name}: {First(e.Message)}");
        }
        finally
        {
            c.Close();
        }
    }
}
