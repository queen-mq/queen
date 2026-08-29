// =============================================================================
// Confluent.Kafka (.NET) against queen-kafka — the M6 matrix row for the
// official Confluent client for .NET.
//
// WHAT THIS SUITE PROVES
//
//   Confluent.Kafka is a thin P/Invoke binding over librdkafka: the wire is
//   librdkafka's, already proven in compat/librdkafka via kcat and
//   confluent-kafka-python. So the wire is NOT what this suite is about. What
//   IS new here is everything the BINDING owns and every DEFAULT the .NET
//   package chooses on your behalf:
//
//     * ProducerConfig / ConsumerConfig defaults, which are not always
//       librdkafka's defaults and are certainly not Java's;
//     * the DeliveryReport<TKey,TValue> / ConsumeResult<TKey,TValue> object
//       model, and whether Offset / Partition / Timestamp / Headers survive
//       the marshalling byte-exact;
//     * IProducer / IConsumer lifecycle — Flush, Close, Commit, Seek,
//       Assign, QueryWatermarkOffsets — each of which is a distinct librdkafka
//       entry point the other suites never call;
//     * AdminClient, which .NET users reach for first and which lands on the
//       admin API keys M7 added to the advertised table;
//     * the ISerializer/IDeserializer path for a null key and an empty value.
//
// WHAT IS THE CLIENT'S FAULT, NOT THE FACADE'S
//
//   1. EnableIdempotence. librdkafka defaults it OFF, so Confluent.Kafka has
//      always worked out of the box here where the Java client did not. We
//      still set it false EXPLICITLY on every producer so the main path is the
//      non-idempotent one, and Edges.cs turns it ON once: since M7 F3
//      advertised InitProducerId (key 22, v0-4) that send LANDS, where it used
//      to be refused before a byte went out.
//
//   2. Compression. librdkafka gates zstd on Fetch v10 and this facade caps
//      Fetch at v6 on purpose (fetch sessions, KIP-227, are out of scope), so
//      a zstd producer silently sends the batch UNCOMPRESSED after logging
//      "Broker does not support compression type zstd". The records land and
//      round-trip fine. That is librdkafka's feature gate, not a facade
//      defect — see PLAN_QUEEN_KAFKA.md STATUS.
//
//   3. AdminClient. The facade advertises 21 API keys since M7: CreateTopics,
//      DeleteTopics, DescribeConfigs, ListGroups, DescribeGroups and
//      DeleteGroups are on the table and Edges.cs asserts they ANSWER.
//      DescribeCluster (key 60) is still absent, and librdkafka answers it from
//      the ordinary Metadata request anyway. Where a call IS refused, the
//      assertion is that the client fails fast on ApiVersions with a legible
//      error rather than a hang or a bare disconnect.
//
//   4. The default partitioner. librdkafka's is `consistent_random` (CRC32),
//      Java's is murmur2. Same key, different partition, across clients. It
//      does not affect this facade — but do not expect a key produced here to
//      land where the Java suite put it.
//
// USAGE
//   dotnet run -- [bootstrap] [runId] [scenario]
//   scenarios: core | edges | sasl | all      (default: core + edges)
//   sasl needs KAFKA_TLS_BOOTSTRAP, QUEEN_KAFKA_SASL_TOKEN, QUEEN_KAFKA_TLS_CA.
//
// A HANG IS A RESULT. Every scenario runs under a watchdog that hard-exits
// 124; every blocking call carries its own TimeSpan.
// =============================================================================

using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;
using Confluent.Kafka;

namespace QueenKafkaCompat;

public static class Program
{
    public const string DefaultBootstrap = "127.0.0.1:19092";

    public static int Failures;
    public static int Checks;

    // ---------------------------------------------------------------- report

    public static void Section(string title) =>
        Console.WriteLine($"\n=== {title}");

    public static void Ok(string what)
    {
        Checks++;
        Console.WriteLine($"  ok   {what}");
    }

    public static void Fail(string what)
    {
        Checks++;
        Failures++;
        Console.WriteLine($"  FAIL {what}");
    }

    public static void Note(string what) => Console.WriteLine($"  --   {what}");

    public static void Check(bool cond, string what)
    {
        if (cond) Ok(what); else Fail(what);
    }

    // ------------------------------------------------- librdkafka log capture
    //
    // Convention #5 of compat/: PRINT the API versions the client actually
    // NEGOTIATED, read out of the client's own debug stream, never assumed.
    // With debug=protocol librdkafka emits, per request:
    //   ...|SEND|rdkafka#producer-1| [thrd:...]: host:port/0: Sent ProduceRequest (v9, 812 bytes @ 0, CorrId 7)
    // and on the ApiVersions handshake it prints the broker's whole table:
    //   ...|APIVERSION|...:   ApiKey Produce (0) Versions 3..9
    // We keep both. Everything else is dropped on the floor unless VERBOSE.

    public static readonly ConcurrentDictionary<string, int> Negotiated = new();
    public static readonly ConcurrentBag<string> BrokerApiTable = new();
    public static readonly ConcurrentBag<string> Notable = new();
    public static bool Verbose = Environment.GetEnvironmentVariable("QK_VERBOSE") == "1";

    private static readonly Regex SentRe =
        new(@"Sent (\w+)Request \(v(\d+)", RegexOptions.Compiled);
    private static readonly Regex ApiKeyRe =
        new(@"ApiKey (\w+)\s*\((\d+)\)\s*Versions (\d+)\.\.(\d+)", RegexOptions.Compiled);

    public static void OnLog(LogMessage m)
    {
        var s = m.Message;

        var sent = SentRe.Match(s);
        if (sent.Success)
        {
            var api = sent.Groups[1].Value;
            var v = int.Parse(sent.Groups[2].Value);
            Negotiated.AddOrUpdate(api, v, (_, old) => Math.Max(old, v));
        }

        var ak = ApiKeyRe.Match(s);
        if (ak.Success)
            BrokerApiTable.Add($"{ak.Groups[1].Value}({ak.Groups[2].Value}) {ak.Groups[3].Value}..{ak.Groups[4].Value}");

        // The lines a human actually wants to see out of a librdkafka run.
        if (s.Contains("does not support", StringComparison.OrdinalIgnoreCase) ||
            s.Contains("Disabling idempotence", StringComparison.OrdinalIgnoreCase) ||
            s.Contains("compression type", StringComparison.OrdinalIgnoreCase) ||
            s.Contains("UNSUPPORTED", StringComparison.OrdinalIgnoreCase))
            Notable.Add($"[{m.Level}] {s}");

        if (Verbose || (int)m.Level <= 4) // <= WARNING
            Console.WriteLine($"  rdk[{m.Level}] {m.Name}: {s}");
    }

    public static void PrintNegotiated()
    {
        Section("API versions this client actually negotiated (from librdkafka debug=protocol)");
        foreach (var kv in Negotiated.OrderBy(k => k.Key))
            Note($"{kv.Key,-22} v{kv.Value}");

        var table = BrokerApiTable.Distinct().OrderBy(x => x).ToList();
        if (table.Count > 0)
        {
            Section("ApiVersions table as librdkafka parsed it from the facade");
            foreach (var t in table) Note(t);
        }

        var notable = Notable.Distinct().ToList();
        if (notable.Count > 0)
        {
            Section("Notable librdkafka log lines");
            foreach (var n in notable) Note(n);
        }
    }

    // ------------------------------------------------------------- utilities

    public static byte[] U8(string s) => Encoding.UTF8.GetBytes(s);
    public static string S8(byte[] b) => b is null ? null : Encoding.UTF8.GetString(b);

    public static bool BytesEq(byte[] a, byte[] b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a is null || b is null) return false;
        return a.AsSpan().SequenceEqual(b);
    }

    public static string Hex(byte[] b) =>
        b is null ? "<null>" : Convert.ToHexString(b);

    /// A deadline wrapper for anything the client could sit on forever.
    /// It unwraps the AggregateException Task.Wait would otherwise put in front
    /// of every KafkaException, because "One or more errors occurred" is not a
    /// diagnosis and this suite exists to produce diagnoses.
    public static T Deadline<T>(string what, int seconds, Func<T> f)
    {
        var t = Task.Run(f);
        if (!t.Wait(TimeSpan.FromSeconds(seconds)))
            throw new TimeoutException($"{what} exceeded {seconds}s");
        try
        {
            return t.Result;
        }
        catch (AggregateException ae) when (ae.InnerExceptions.Count == 1)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo
                .Capture(ae.InnerExceptions[0]).Throw();
            throw; // unreachable
        }
    }

    // ------------------------------------------------------------------ main

    public static int Main(string[] argv)
    {
        var bootstrap = argv.Length > 0 && argv[0].Length > 0 ? argv[0] : DefaultBootstrap;
        var runId = argv.Length > 1 && argv[1].Length > 0
            ? argv[1]
            : DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        var scenario = argv.Length > 2 && argv[2].Length > 0 ? argv[2] : "default";

        var budget = int.TryParse(Environment.GetEnvironmentVariable("QK_BUDGET_S"), out var b) ? b : 900;

        // A hang is a result. This thread is the only thing that can end the
        // process without a RESULT line, and it says so loudly.
        var watchdog = new Thread(() =>
        {
            Thread.Sleep(TimeSpan.FromSeconds(budget));
            Console.Error.WriteLine($"\n!!   WATCHDOG: the suite exceeded {budget}s. Killing it.");
            Console.Out.Flush();
            Environment.Exit(124);
        })
        { IsBackground = true };
        watchdog.Start();

        Section("Confluent.Kafka (.NET) vs queen-kafka");
        Note($"bootstrap        {bootstrap}");
        Note($"runId            {runId}");
        Note($"scenario         {scenario}");
        var asm = typeof(ProducerBuilder<,>).Assembly;
        var info = asm.GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
                      .Cast<System.Reflection.AssemblyInformationalVersionAttribute>()
                      .FirstOrDefault()?.InformationalVersion;
        Note($"Confluent.Kafka  {info ?? asm.GetName().Version?.ToString()} (assembly {asm.GetName().Version})");
        Note($"librdkafka       {Library.VersionString} (0x{Library.Version:x})");
        Note($".NET             {Environment.Version} on {System.Runtime.InteropServices.RuntimeInformation.OSDescription} " +
             $"{System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}");

        try
        {
            switch (scenario)
            {
                case "core":
                    Core.Run(bootstrap, runId);
                    break;
                case "edges":
                    Edges.Run(bootstrap, runId);
                    break;
                case "sasl":
                    Sasl.Run(runId);
                    break;
                case "all":
                    Core.Run(bootstrap, runId);
                    Edges.Run(bootstrap, runId);
                    Sasl.Run(runId);
                    break;
                default:
                    Core.Run(bootstrap, runId);
                    Edges.Run(bootstrap, runId);
                    break;
            }
        }
        catch (Exception e)
        {
            Fail($"unhandled exception: {e.GetType().Name}: {e.Message}");
            Console.WriteLine(e.StackTrace);
        }

        PrintNegotiated();

        Console.WriteLine();
        if (Failures == 0)
            Console.WriteLine($"RESULT: PASS ({Checks} checks)");
        else
            Console.WriteLine($"RESULT: FAIL ({Failures} of {Checks} checks)");

        return Failures == 0 ? 0 : 1;
    }
}
