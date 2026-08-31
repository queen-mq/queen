%%%-----------------------------------------------------------------------------
%%% brod (Erlang/OTP) against the queen-kafka facade.
%%%
%%% WHAT THIS SUITE PROVES
%%%
%%%   versions  what brod's ApiVersions handshake actually negotiated, read out
%%%             of kpro_connection's own parsed table -- not assumed, not
%%%             scraped from a log line
%%%   produce   auto-create by producing to a topic that does not exist, then
%%%             512 messages over 8 partitions with keys and headers, and the
%%%             same payload set again through each of brod's four compression
%%%             codecs
%%%   consume   a real brod_group_subscriber_v2: join, assignment, per-message
%%%             callback, byte-exact key/value/header round trip, per-partition
%%%             order
%%%   commit    offsets committed through the group coordinator, read back with
%%%             OffsetFetch, then a SECOND subscriber in the SAME group that
%%%             must resume from the committed offset -- no replay of what was
%%%             committed, no loss of what came after
%%%   offsets   ListOffsets earliest/latest, and brod:fetch/5 used as a seek to
%%%             an arbitrary mid-log offset
%%%   probes    the edges that are interesting but are NOT pass/fail here: null
%%%             vs empty key, duplicate header keys, acks=0. These print `note`
%%%             and never fail the run, because what the facade does with them
%%%             is a documented product decision, not a brod question.
%%%   sasl      the same produce/consume through SASL/PLAIN over TLS
%%%
%%% WHAT IS THE CLIENT'S FAULT, NOT THE FACADE'S
%%%
%%%   * brod's consumer defaults `begin_offset` to LATEST. A group subscriber
%%%     with no committed offset and no `begin_offset` therefore reads nothing
%%%     from an existing topic and looks like a broken facade. Every consumer
%%%     config here sets `begin_offset => earliest` deliberately.
%%%   * brod ships gzip and gzip only. snappy, lz4 and zstd are reached through
%%%     `kpro_compress`, which calls snappyer / lz4b_frame / ezstd IF THE APP
%%%     IS LOADED and crashes if it is not. Those three are named in this
%%%     project's rebar.config; a stock brod project cannot send them.
%%%   * kafka_protocol pins OffsetFetch to v1..v2 client-side
%%%     (kpro_api_vsn:range/1), well under the v7 the facade advertises. A low
%%%     negotiated OffsetFetch is brod's ceiling, not the facade's.
%%%   * brod never sends InitProducerId unless you explicitly ask for a
%%%     transactional producer, so the idempotence trap that kills the Java
%%%     console producer does not exist on this client's default path.
%%%
%%% Entry point is main/1, called by run.sh as
%%%   erl -run qk_brod main <bootstrap> <runId> <scenario>
%%%-----------------------------------------------------------------------------
-module(qk_brod).

-export([main/1]).

-include_lib("brod/include/brod.hrl").

-define(CLIENT, qk_brod_client).
-define(PARTS, 8).            %% partitions the facade was booted with
-define(PER_PART, 64).        %% 8 * 64 = 512 messages on the main lane
-define(CODEC_PARTS, 4).
-define(CODEC_PER_PART, 32).  %% 4 * 32 = 128 messages per codec

%%%=============================================================================
%%% entry point
%%%=============================================================================

main(Args) ->
    process_flag(trap_exit, true),
    put(fails, 0),
    {Bootstrap, RunId, Scenario} = parse_args(Args),
    Endpoints = parse_endpoints(Bootstrap),
    io:format("brod compat suite~n"
              "  bootstrap ~s~n"
              "  runId     ~s~n"
              "  scenario  ~s~n",
              [Bootstrap, RunId, Scenario]),
    banner(),
    Ctx = #{endpoints => Endpoints,
            bootstrap => Bootstrap,
            run_id => RunId,
            conn_config => conn_config(Scenario)},
    Blocked =
        try
            run(Scenario, Ctx),
            none
        catch
            throw:{qk_blocked, Why} ->
                Why;
            Class:Reason:Stack ->
                fail("suite aborted: ~p:~p~n~p", [Class, Reason, Stack]),
                none
        end,
    Fails = get(fails),
    io:format("~n"),
    case {Blocked, Fails} of
        {none, 0} ->
            io:format("RESULT: PASS~n"), erlang:halt(0);
        {none, N} ->
            io:format("RESULT: FAIL (~p)~n", [N]), erlang:halt(1);
        {Why2, N2} ->
            io:format("RESULT: FAIL (~p) BLOCKED BY ~p~n", [N2, Why2]),
            erlang:halt(1)
    end.

parse_args([B]) -> {B, default_run_id(), "all"};
parse_args([B, R]) -> {B, R, "all"};
parse_args([B, R, S | _]) -> {B, R, S};
parse_args([]) -> {"127.0.0.1:19092", default_run_id(), "all"}.

default_run_id() -> integer_to_list(erlang:system_time(second)).

parse_endpoints(S) ->
    [begin
         case string:split(HostPort, ":", trailing) of
             [H, P] -> {H, list_to_integer(P)};
             [H] -> {H, 9092}
         end
     end || HostPort <- string:lexemes(S, ",")].

%% The SASL/TLS lane reads its credential and CA out of the environment, same
%% names the other suites in compat/ use. `verify_none` is not laziness: the
%% rig's self-signed certificate carries SANs for localhost / 127.0.0.1 /
%% kafka.example.com / shared.queenmq.cloud and NOT for the host alias a
%% container has to dial, so hostname verification cannot succeed from inside a
%% container no matter what the CA says.
conn_config("sasl") ->
    Token = os:getenv("QUEEN_KAFKA_SASL_TOKEN", ""),
    User = os:getenv("QUEEN_KAFKA_SASL_USER", "brod"),
    #{ssl => [{verify, verify_none}],
      sasl => {plain, list_to_binary(User), list_to_binary(Token)}};
conn_config(_) ->
    #{}.

run("all", Ctx) ->
    scenario_versions(Ctx),
    scenario_produce_consume(Ctx),
    scenario_codecs(Ctx),
    scenario_offsets(Ctx),
    scenario_resume(Ctx),
    scenario_probes(Ctx);
run("versions", Ctx) -> scenario_versions(Ctx);
run("produce", Ctx) -> scenario_produce_consume(Ctx);
run("codecs", Ctx) -> scenario_codecs(Ctx);
run("offsets", Ctx) -> scenario_offsets(Ctx);
run("resume", Ctx) -> scenario_resume(Ctx);
run("probes", Ctx) -> scenario_probes(Ctx);
run("sasl", Ctx) ->
    scenario_versions(Ctx),
    scenario_produce_consume(Ctx),
    scenario_offsets(Ctx);
run(Other, _Ctx) ->
    fail("unknown scenario ~s", [Other]).

banner() ->
    ok = ensure_started(),
    {ok, BrodVsn} = application:get_key(brod, vsn),
    {ok, KproVsn} = application:get_key(kafka_protocol, vsn),
    io:format("  erlang    OTP ~s / erts ~s~n"
              "  brod      ~s~n"
              "  kpro      ~s~n"
              "  codecs    ~s~n",
              [erlang:system_info(otp_release), erlang:system_info(version),
               BrodVsn, KproVsn, codec_report()]).

ensure_started() ->
    {ok, _} = application:ensure_all_started(brod),
    %% Loading these is what makes kpro_compress able to reach them; they are
    %% libraries with no supervision tree, so `load` is the whole story.
    _ = [application:load(A) || A <- [snappyer, lz4b, ezstd]],
    _ = [code:ensure_loaded(M) || M <- [snappyer, lz4b_frame, ezstd]],
    ok.

codec_report() ->
    Avail = [atom_to_list(C) || {C, true} <- codec_availability()],
    string:join(["gzip" | Avail], ",").

codec_availability() ->
    [{snappy, is_mod(snappyer)},
     {lz4, is_mod(lz4b_frame)},
     {zstd, is_mod(ezstd)}].

is_mod(M) ->
    case code:ensure_loaded(M) of
        {module, M} -> true;
        _ -> false
    end.

%%%=============================================================================
%%% 1. ApiVersions -- what brod actually negotiated
%%%=============================================================================

scenario_versions(Ctx) ->
    section("versions: what brod negotiated with ApiVersions"),
    #{endpoints := [Endpoint | _], conn_config := CC} = Ctx,
    case kpro:connect(Endpoint, CC) of
        {ok, Conn} ->
            {ok, Vsns} = kpro_connection:get_api_vsns(Conn),
            ok(true, "ApiVersions handshake completed, ~p APIs advertised",
               [maps:size(Vsns)]),
            print_versions(Vsns),
            check(maps:is_key(produce, Vsns), "Produce advertised", []),
            check(maps:is_key(fetch, Vsns), "Fetch advertised", []),
            check(maps:is_key(join_group, Vsns), "JoinGroup advertised", []),
            note("InitProducerId advertised: ~p (brod only needs it for "
                 "transactional producers, which this suite does not use)",
                 [maps:is_key(init_producer_id, Vsns)]),
            kpro:close_connection(Conn);
        {error, Reason} ->
            fail("could not connect to ~p: ~p", [Endpoint, Reason])
    end.

%% For each API the broker advertised, print the broker's window, brod's own
%% window, and the version brod will therefore USE -- the top of the
%% intersection, which is brod's selection rule (brod_kafka_apis:pick_version/2).
%%
%% THE CLIENT WINDOW HAS TWO SOURCES and using the wrong one gives a table that
%% looks right and is not. brod keeps its OWN table in
%% brod_kafka_apis:supported_versions/0, deliberately narrower than the codec's
%% (`%% Do not change range without verification.`), and it is that table --
%% not kpro_api_vsn:range/1 -- that decides what goes on the wire for the
%% fifteen APIs it names. The `src` column says which one answered, so a
%% surprising number can be traced.
print_versions(Vsns) ->
    io:format("       ~-20s ~-10s ~-10s ~-9s ~s~n",
              ["api", "broker", "brod", "will use", "src"]),
    lists:foreach(
      fun({Api, {BMin, BMax}}) ->
              case client_range(Api) of
                  {ok, {Lo, Hi}, Src} ->
                      Use = case min(BMax, Hi) >= max(BMin, Lo) of
                                true -> integer_to_list(min(BMax, Hi));
                                false -> "NONE"
                            end,
                      io:format("       ~-20s ~-10s ~-10s ~-9s ~s~n",
                                [atom_to_list(Api), range_str(BMin, BMax),
                                 range_str(Lo, Hi), Use, Src]);
                  error ->
                      io:format("       ~-20s ~-10s ~-10s ~-9s ~s~n",
                                [atom_to_list(Api), range_str(BMin, BMax),
                                 "-", "unknown", "-"])
              end
      end, lists:sort(maps:to_list(Vsns))).

range_str(Lo, Hi) -> integer_to_list(Lo) ++ ".." ++ integer_to_list(Hi).

client_range(Api) ->
    case maps:find(Api, brod_kafka_apis:supported_versions()) of
        {ok, {Lo, Hi}} ->
            {ok, {Lo, Hi}, "brod"};
        error ->
            try kpro_api_vsn:range(Api) of
                {Lo, Hi} -> {ok, {Lo, Hi}, "kpro"}
            catch
                _:_ -> error
            end
    end.

%%%=============================================================================
%%% 2 + 3 + 4. auto-create, produce with keys/headers, group consume
%%%=============================================================================

scenario_produce_consume(Ctx) ->
    #{run_id := RunId} = Ctx,
    Topic = topic("main", RunId),
    Group = group("main", RunId),

    section("auto-create: produce to a topic that does not exist"),
    Before = list_topics(Ctx),
    check(not lists:member(Topic, Before),
          "~s absent from cluster metadata before any produce", [Topic]),

    ok = start_client(Ctx, []),
    {ProdRes, Tries} = start_producer_retry(Topic, [{required_acks, -1}], 10),
    check(ProdRes =:= ok,
          "brod:start_producer on an unknown topic succeeded after ~p "
          "attempt(s) -- the Metadata it sends is the auto-create", [Tries]),

    Expected = build_expected(Topic, ?PARTS, ?PER_PART),
    Total = ?PARTS * ?PER_PART,
    ProduceRes = produce_all(Topic, ?PARTS, ?PER_PART, Expected),
    check(ProduceRes =:= ok, "produced ~p messages over ~p partitions "
                             "(keys + 4 headers each, uncompressed)",
          [Total, ?PARTS]),
    ok = abort_if_txn_id_refused(ProduceRes),

    After = list_topics(Ctx),
    check(lists:member(Topic, After),
          "~s present in cluster metadata after produce (auto-created)",
          [Topic]),
    case topic_partition_count(Ctx, Topic) of
        {ok, N} ->
            check(N =:= ?PARTS, "auto-created width is ~p partitions", [N]);
        {error, E} ->
            fail("metadata for ~s: ~p", [Topic, E])
    end,

    section("group consume: brod_group_subscriber_v2"),
    flush(),
    {ok, Sub} = start_subscriber(Group, Topic, commit),
    Got = collect_until(Total, 90000, 8000),
    ok(true, "collected ~p/~p messages", [length(Got), Total]),
    check(length(Got) =:= Total,
          "group subscriber received exactly ~p messages", [Total]),
    verify_payloads(Got, Expected),
    verify_order(Got),
    verify_partition_spread(Got, ?PARTS),

    %% Give the coordinator its commit interval, then read the offsets back
    %% over the wire. This is the OffsetCommit -> OffsetFetch round trip, not
    %% brod's opinion of what it committed.
    timer:sleep(2500),
    ok = brod_group_subscriber_v2:stop(Sub),
    timer:sleep(500),
    Committed = committed_offsets(Group),
    check(maps:size(Committed) =:= ?PARTS,
          "OffsetFetch returns a committed offset for all ~p partitions "
          "(~p)", [?PARTS, maps:size(Committed)]),
    check(lists:all(fun(O) -> O =:= ?PER_PART end, maps:values(Committed)),
          "every committed offset is ~p, the end of its partition: ~p",
          [?PER_PART, lists:sort(maps:to_list(Committed))]),
    put({topic, main}, Topic),
    put({group, main}, Group),
    put({expected, main}, Expected),
    ok.

%%%=============================================================================
%%% compression codecs
%%%=============================================================================

scenario_codecs(Ctx) ->
    section("codecs: the same payload set through each compression method"),
    #{run_id := RunId} = Ctx,
    ok = start_client(Ctx, []),
    %% gzip is unconditional: kpro_compress calls OTP's own zlib for it, so
    %% unlike the other three it needs no extra application to be present.
    Codecs = [{no_compression, true}, {gzip, true} | codec_availability()],
    lists:foreach(
      fun({Codec, Available}) ->
              case Available of
                  false ->
                      note("~p: NOT AVAILABLE in this build "
                           "(kpro_compress needs its library app); skipped",
                           [Codec]);
                  true ->
                      codec_lane(Ctx, RunId, Codec)
              end
      end, Codecs).

codec_lane(_Ctx, RunId, Codec) ->
    Topic = topic("codec-" ++ atom_to_list(Codec), RunId),
    {ok, _} = start_producer_retry(
                Topic, [{required_acks, -1}, {compression, Codec}], 10),
    Expected = build_expected(Topic, ?CODEC_PARTS, ?CODEC_PER_PART),
    Total = ?CODEC_PARTS * ?CODEC_PER_PART,
    case produce_all(Topic, ?CODEC_PARTS, ?CODEC_PER_PART, Expected) of
        ok ->
            %% Read it back with plain Fetch (no group): what matters here is
            %% that the facade DECOMPRESSED what brod sent and that the bytes
            %% survived the round trip.
            Got = fetch_all(Topic, ?CODEC_PARTS, Total),
            check(length(Got) =:= Total,
                  "~p: produced and fetched back ~p/~p messages",
                  [Codec, length(Got), Total]),
            verify_payloads_tagged(atom_to_list(Codec), Got, Expected);
        {error, R} ->
            fail("~p: produce failed: ~p", [Codec, R])
    end.

%%%=============================================================================
%%% offsets: ListOffsets watermarks and a seek
%%%=============================================================================

scenario_offsets(Ctx) ->
    section("offsets: ListOffsets watermarks and a mid-log seek"),
    #{endpoints := Eps, conn_config := CC, run_id := RunId} = Ctx,
    Topic = case get({topic, main}) of
                undefined -> topic("main", RunId);
                T -> T
            end,
    ok = start_client(Ctx, []),
    Earliest = [begin
                    {ok, O} = brod:resolve_offset(Eps, Topic, P, earliest, CC),
                    O
                end || P <- lists:seq(0, ?PARTS - 1)],
    Latest = [begin
                  {ok, O} = brod:resolve_offset(Eps, Topic, P, latest, CC),
                  O
              end || P <- lists:seq(0, ?PARTS - 1)],
    check(lists:all(fun(O) -> O =:= 0 end, Earliest),
          "earliest offset is 0 on every partition: ~lp", [Earliest]),
    check(lists:all(fun(O) -> O =:= ?PER_PART end, Latest),
          "latest offset is ~p on every partition: ~lp", [?PER_PART, Latest]),

    %% brod has no seek() on a bare consumer; the equivalent is fetching from
    %% an explicit offset, which is what a seek compiles down to on the wire.
    Mid = ?PER_PART div 2,
    case brod:fetch(?CLIENT, Topic, 0, Mid, #{max_wait_time => 2000,
                                              min_bytes => 1,
                                              max_bytes => 1048576}) of
        {ok, {Hw, [#kafka_message{offset = First} | _] = Msgs}} ->
            check(First =:= Mid,
                  "fetch from offset ~p returns offset ~p first (seek)",
                  [Mid, First]),
            check(Hw =:= ?PER_PART,
                  "fetch reports high watermark ~p", [Hw]),
            check(length(Msgs) =< ?PER_PART - Mid andalso length(Msgs) > 0,
                  "fetch returned ~p messages from the seek point", [length(Msgs)]);
        {ok, {_, []}} ->
            fail("fetch from offset ~p returned no messages", [Mid]);
        {error, R} ->
            fail("fetch from offset ~p: ~p", [Mid, R])
    end,

    %% Out-of-range read past the end: Kafka answers OFFSET_OUT_OF_RANGE.
    case brod:fetch(?CLIENT, Topic, 0, ?PER_PART + 5000,
                    #{max_wait_time => 1000, min_bytes => 1}) of
        {error, RErr} ->
            ok(true, "fetch past the end is refused: ~p", [RErr]);
        {ok, {_, []}} ->
            note("fetch past the end returned an empty set rather than "
                 "OFFSET_OUT_OF_RANGE", []);
        {ok, {_, Other}} ->
            fail("fetch past the end returned ~p messages", [length(Other)])
    end.

%%%=============================================================================
%%% resume: a second subscriber in the same group
%%%=============================================================================

scenario_resume(Ctx) ->
    section("resume: a NEW subscriber in the same group after a commit"),
    #{run_id := RunId} = Ctx,
    Topic = case get({topic, main}) of
                undefined -> topic("main", RunId);
                T -> T
            end,
    Group = case get({group, main}) of
                undefined -> group("main", RunId);
                G -> G
            end,
    ok = start_client(Ctx, []),

    %% Everything up to ?PER_PART is committed by scenario_produce_consume.
    %% A fresh subscriber in that group must therefore see NOTHING until new
    %% records arrive -- that is the whole no-replay assertion.
    flush(),
    {ok, Sub1} = start_subscriber(Group, Topic, commit),
    Replayed = collect_until(1, 12000, 9000),
    check(Replayed =:= [],
          "a new member of ~s replays nothing already committed "
          "(~p messages seen)", [Group, length(Replayed)]),

    %% Now append and prove it picks up exactly the tail.
    Extra = 40,
    ExtraExpected = build_expected_one(Topic, 0, ?PER_PART, Extra),
    ok = produce_range(Topic, 0, ExtraExpected),
    Got = collect_until(Extra, 60000, 8000),
    check(length(Got) =:= Extra,
          "resumed subscriber received exactly the ~p new messages (~p)",
          [Extra, length(Got)]),
    verify_payloads_tagged("resume", Got, ExtraExpected),
    Offsets = [O || {_, _, #kafka_message{offset = O}} <- Got],
    check(Offsets =:= lists:seq(?PER_PART, ?PER_PART + Extra - 1),
          "offsets resume exactly at the committed point ~p and are "
          "contiguous", [?PER_PART]),
    timer:sleep(2500),
    ok = brod_group_subscriber_v2:stop(Sub1),
    timer:sleep(500),
    Committed = committed_offsets(Group),
    check(maps:get(0, Committed, -1) =:= ?PER_PART + Extra,
          "partition 0 committed offset advanced to ~p (~p)",
          [?PER_PART + Extra, maps:get(0, Committed, -1)]).

%%%=============================================================================
%%% probes: interesting edges that are NOT pass/fail
%%%=============================================================================

scenario_probes(Ctx) ->
    section("probes: edges recorded, never asserted"),
    #{run_id := RunId} = Ctx,
    Topic = topic("probe", RunId),
    ok = start_client(Ctx, []),
    {ok, _} = start_producer_retry(Topic, [{required_acks, -1}], 10),

    %% (a) null key vs empty key. Kafka's wire format distinguishes them;
    %% whether the facade's base64 envelope does is a product question.
    Batch = [#{key => <<>>, value => <<"null-key-probe">>, headers => [],
               ts => now_ms_wall()}],
    {ok, O1} = brod:produce_sync_offset(?CLIENT, Topic, 0, <<>>, Batch),
    %% (b) duplicate header keys, and a header with an empty value.
    Dup = [#{key => <<"dup">>, value => <<"dup-header-probe">>,
             headers => [{<<"x">>, <<"1">>}, {<<"x">>, <<"2">>},
                         {<<"empty">>, <<>>}],
             ts => now_ms_wall()}],
    {ok, O2} = brod:produce_sync_offset(?CLIENT, Topic, 0, <<"dup">>, Dup),
    timer:sleep(300),
    case brod:fetch(?CLIENT, Topic, 0, O1, #{max_wait_time => 2000,
                                             min_bytes => 1}) of
        {ok, {_, Msgs}} ->
            case [M || M = #kafka_message{offset = O} <- Msgs, O =:= O1] of
                [#kafka_message{key = K1}] ->
                    note("a produced EMPTY key comes back as ~p", [K1]);
                _ -> note("could not read back the null-key probe", [])
            end,
            case [M || M = #kafka_message{offset = O} <- Msgs, O =:= O2] of
                [#kafka_message{headers = H2}] ->
                    note("duplicate header keys come back as ~p", [H2]),
                    note("both copies of the duplicate key survived: ~p",
                         [length([1 || {<<"x">>, _} <- H2]) =:= 2]);
                _ -> note("could not read back the duplicate-header probe", [])
            end;
        {error, R} ->
            note("probe fetch failed: ~p", [R])
    end,

    %% (c) acks=0. The facade writes no response frame at all, so brod cannot
    %% be told about a failure and the offset it reports is its own invention.
    %% Asserting on it would be asserting on a fiction; we only prove the call
    %% does not hang or crash the producer. It needs its OWN topic: brod's
    %% producer config is fixed at start_producer time and a second call for a
    %% topic that already has a producer is a no-op, so reusing the topic above
    %% would silently test acks=-1 again.
    AckTopic = topic("probe-acks0", RunId),
    {ok, _} = start_producer_retry(AckTopic, [{required_acks, 0}], 10),
    Res0 = brod:produce_sync(?CLIENT, AckTopic, 1, <<"a0">>,
                             [#{key => <<"a0">>, value => <<"acks-zero">>,
                                headers => [], ts => now_ms_wall()}]),
    note("acks=0 produce returned ~p (no response frame exists to carry an "
         "error; see PLAN_QUEEN_KAFKA.md deliberate deviations)", [Res0]),

    %% (d) auto-create cannot be REFUSED at the Metadata version brod speaks.
    %% allow_auto_topic_creation is a Metadata v4 field. brod pins Metadata at
    %% v2 (brod_kafka_apis:supported_versions/0), so a brod client configured
    %% with allow_topic_auto_creation => false has no way to say so on the
    %% wire, and a bare Metadata request creates the topic anyway. The facade's
    %% "cannot be refused on Metadata v0-v3" deviation is therefore not
    %% theoretical for this client -- it is the only behaviour brod can get.
    #{endpoints := Eps2, conn_config := CC2} = Ctx,
    Ghost = topic("ghost", RunId),
    _ = brod:get_metadata(Eps2, [Ghost], CC2),
    timer:sleep(300),
    Created = lists:member(Ghost, list_topics(Ctx)),
    note("a bare Metadata request for a never-produced topic created it: ~p "
         "(brod pins Metadata at v2, below the v4 that carries "
         "allow_auto_topic_creation, so this is unrefusable from brod)",
         [Created]).

%%%=============================================================================
%%% client, producer, subscriber plumbing
%%%=============================================================================

start_client(Ctx, Extra) ->
    #{endpoints := Eps, conn_config := CC} = Ctx,
    case whereis(?CLIENT) of
        undefined ->
            Config = [{auto_start_producers, true},
                      {allow_topic_auto_creation, true},
                      {query_api_versions, true},
                      %% brod caches an unknown-topic error for TWO MINUTES by
                      %% default. On a broker whose auto-create is asynchronous
                      %% that cache outlives any sane retry loop and the topic
                      %% stays invisible to this client long after the broker
                      %% made it. 1s is what makes the auto-create test mean
                      %% what it says on BOTH brokers.
                      {unknown_topic_cache_ttl, 1000},
                      {default_producer_config, [{required_acks, -1}]}
                      | Extra] ++ maps:to_list(CC),
            case brod:start_client(Eps, ?CLIENT, Config) of
                ok -> ok;
                {error, {already_started, _}} -> ok;
                {error, R} -> throw({start_client, R})
            end;
        _ -> ok
    end.

%% Auto-create is not the same shape on every broker and a client must cope.
%% Apache Kafka creates the topic as a SIDE EFFECT of the Metadata request and
%% answers that same request UNKNOWN_TOPIC_OR_PARTITION, so the first
%% start_producer always loses the race; queen-kafka creates it synchronously
%% and the first attempt wins. Retrying is what every production Kafka client
%% does here, so the suite does it too and reports the attempt count instead of
%% pretending either behaviour is the only correct one.
start_producer_retry(Topic, Config, Attempts) ->
    start_producer_retry(Topic, Config, Attempts, 1).

start_producer_retry(Topic, Config, Attempts, N) ->
    case brod:start_producer(?CLIENT, Topic, Config) of
        ok ->
            {ok, N};
        {error, Reason} when N < Attempts ->
            _ = Reason,
            timer:sleep(500),
            start_producer_retry(Topic, Config, Attempts, N + 1);
        {error, Reason} ->
            {{error, Reason}, N}
    end.

start_subscriber(Group, Topic, Mode) ->
    brod_group_subscriber_v2:start_link(
      #{client => ?CLIENT,
        group_id => Group,
        topics => [Topic],
        cb_module => qk_brod_cb,
        init_data => #{sink => self(), mode => Mode},
        message_type => message,
        %% begin_offset is the brod gotcha: without it a group with no
        %% committed offset starts at LATEST and reads nothing.
        consumer_config => [{begin_offset, earliest},
                            {offset_reset_policy, reset_to_earliest},
                            {max_wait_time, 500},
                            {prefetch_count, 200}],
        group_config => [{offset_commit_policy, commit_to_kafka_v2},
                         {offset_commit_interval_seconds, 1},
                         {session_timeout_seconds, 30},
                         {heartbeat_rate_seconds, 2},
                         {rejoin_delay_seconds, 1},
                         {partition_assignment_strategy, roundrobin_v2}]}).

committed_offsets(Group) ->
    case brod:fetch_committed_offsets(?CLIENT, Group) of
        {ok, Structs} ->
            lists:foldl(
              fun(TS, Acc) ->
                      Parts = maps:get(partitions, TS, []),
                      lists:foldl(
                        fun(PS, A) ->
                                P = get_any(PS, [partition_index, partition]),
                                O = get_any(PS, [committed_offset, offset]),
                                case is_integer(P) andalso is_integer(O)
                                     andalso O >= 0 of
                                    true -> A#{P => O};
                                    false -> A
                                end
                        end, Acc, Parts)
              end, #{}, Structs);
        {error, R} ->
            fail("fetch_committed_offsets: ~p", [R]),
            #{}
    end.

%%%=============================================================================
%%% payload construction and verification
%%%=============================================================================

%% Expected is #{{Partition, Index} => {Key, Value, Headers}}, the single map
%% shape every verifier reads. Two builders because the main lane fills a whole
%% grid of partitions and the resume lane appends to ONE partition at an offset
%% base; they are separate names rather than guarded clauses of one name
%% because both arguments are integers and no guard could tell them apart.
build_expected(Topic, Parts, PerPart) ->
    maps:from_list(
      [{{P, I}, record_for(Topic, P, I)}
       || P <- lists:seq(0, Parts - 1), I <- lists:seq(0, PerPart - 1)]).

build_expected_one(Topic, P, Base, Count) ->
    maps:from_list(
      [{{P, I}, record_for(Topic, P, I)}
       || I <- lists:seq(Base, Base + Count - 1)]).

record_for(_Topic, P, I) ->
    PB = integer_to_binary(P),
    IB = integer_to_binary(I),
    %% The key carries the coordinates so a received message can be looked up,
    %% and then three bytes that are not valid UTF-8 so a facade that round
    %% trips through a text encoding cannot pass by accident.
    Key = <<"qk|", PB/binary, "|", IB/binary, "|", 255, 0, 254>>,
    %% Redundant filler so the compressed lanes actually compress, plus the
    %% same hostile bytes. 0xC3 0xA9 is 'e-acute' in UTF-8, included as raw
    %% bytes rather than a source literal so this file stays pure ASCII.
    Filler = binary:copy(<<"queen-kafka-brod-compat.">>, 8),
    Value = <<"v|", PB/binary, "|", IB/binary, "|", Filler/binary,
              0, 255, 254, 253, 16#C3, 16#A9>>,
    Headers = [{<<"h-idx">>, IB},
               {<<"h-part">>, PB},
               {<<"h-bin">>, <<0, 1, 2, 253, 254, 255>>},
               {<<"h-empty">>, <<>>}],
    {Key, Value, Headers}.

%% THE ONE FAILURE THAT DESERVES A PARAGRAPH RATHER THAN A LINE.
%%
%% Stock brod cannot produce here, and if the suite just kept going it would
%% print a dozen consequential failures -- empty consume, zero committed
%% offsets, watermarks at 0, every codec lane -- and bury the single cause. So
%% the moment this specific error code appears, say exactly what it is and
%% stop.
abort_if_txn_id_refused(ok) ->
    ok;
abort_if_txn_id_refused({error, Errs}) ->
    case has_txn_id_error(Errs) of
        false ->
            ok;
        true ->
            io:format(
              "~n"
              "  ---------------------------------------------------------~n"
              "  BLOCKED: every produce is refused with~n"
              "           TRANSACTIONAL_ID_AUTHORIZATION_FAILED (code 53).~n"
              "~n"
              "  This is not a transaction. brod is producing normally; the~n"
              "  transactional id it sends is an EMPTY STRING rather than~n"
              "  null, because kafka_protocol hand-rolls the Produce request~n"
              "  and types that field `string` instead of `nullable_string`:~n"
              "~n"
              "    kpro_req_lib.erl:308  encode(string, transactional_id(..))~n"
              "    kpro_req_lib.erl:593  transactional_id(false) -> ?kpro_null~n"
              "    kpro_lib.erl:140      encode(string, ?null) -> \"\"~n"
              "~n"
              "  kafka_protocol's OWN schema disagrees with its encoder:~n"
              "    kpro_schema.erl:212   {transactional_id, nullable_string}~n"
              "~n"
              "  queen-kafka refuses any PRESENT transactional id at~n"
              "  src/handlers/produce.rs:195, and an empty string is present.~n"
              "  Apache Kafka 3.9.1 accepts the identical bytes.~n"
              "~n"
              "  Re-run with BROD_PATCH_TXNID=1 to correct that one field and~n"
              "  exercise the rest of the suite.~n"
              "  ---------------------------------------------------------~n~n",
              []),
            throw({qk_blocked, transactional_id_authorization_failed})
    end.

has_txn_id_error(Errs) when is_list(Errs) ->
    lists:any(fun(E) ->
                      string:find(io_lib:format("~p", [E]),
                                  "transactional_id_authorization_failed")
                          =/= nomatch
              end, Errs);
has_txn_id_error(_) ->
    false.

produce_all(Topic, Parts, PerPart, Expected) ->
    Results =
        [begin
             Batch = [begin
                          {K, V, H} = maps:get({P, I}, Expected),
                          #{key => K, value => V, headers => H,
                            ts => now_ms_wall()}
                      end || I <- lists:seq(0, PerPart - 1)],
             brod:produce_sync_offset(?CLIENT, Topic, P, <<>>, Batch)
         end || P <- lists:seq(0, Parts - 1)],
    case [R || R <- Results, element(1, R) =/= ok] of
        [] -> ok;
        Errs -> {error, Errs}
    end.

produce_range(Topic, P, Expected) ->
    Keys = lists:sort(maps:keys(Expected)),
    Batch = [begin
                 {K, V, H} = maps:get(Coord, Expected),
                 #{key => K, value => V, headers => H, ts => now_ms_wall()}
             end || Coord <- Keys],
    case brod:produce_sync_offset(?CLIENT, Topic, P, <<>>, Batch) of
        {ok, _} -> ok;
        Other -> Other
    end.

verify_payloads(Got, Expected) ->
    verify_payloads_tagged("main", Got, Expected).

verify_payloads_tagged(Tag, Got, Expected) ->
    {BadKey, BadVal, BadHdr, Unknown} =
        lists:foldl(
          fun({_T, P, #kafka_message{key = K, value = V, headers = H}},
              {A, B, C, D}) ->
                  case coord_of(K) of
                      {ok, {KP, I}} when KP =:= P ->
                          case maps:find({P, I}, Expected) of
                              {ok, {EK, EV, EH}} ->
                                  {A + b(K =/= EK), B + b(V =/= EV),
                                   C + b(not headers_equal(H, EH)), D};
                              error ->
                                  {A, B, C, D + 1}
                          end;
                      _ ->
                          {A, B, C, D + 1}
                  end
          end, {0, 0, 0, 0}, Got),
    check(Unknown =:= 0, "~s: every message maps to a produced coordinate "
                         "(~p unmatched)", [Tag, Unknown]),
    check(BadKey =:= 0, "~s: keys are byte-exact (~p mismatched)", [Tag, BadKey]),
    check(BadVal =:= 0, "~s: values are byte-exact (~p mismatched)", [Tag, BadVal]),
    check(BadHdr =:= 0, "~s: headers are byte-exact and ordered "
                        "(~p mismatched)", [Tag, BadHdr]).

b(true) -> 1;
b(false) -> 0.

headers_equal(Got, Expected) -> Got =:= Expected.

coord_of(K) when is_binary(K) ->
    case binary:split(K, <<"|">>, [global]) of
        [<<"qk">>, PB, IB | _] ->
            try {ok, {binary_to_integer(PB), binary_to_integer(IB)}}
            catch _:_ -> error end;
        [<<"v">>, PB, IB | _] ->
            try {ok, {binary_to_integer(PB), binary_to_integer(IB)}}
            catch _:_ -> error end;
        _ -> error
    end;
coord_of(_) -> error.

%% Per-partition order: within each partition, offsets must arrive strictly
%% increasing AND the payload index must increase with them. Across partitions
%% nothing is promised and nothing is checked.
verify_order(Got) ->
    ByPart = lists:foldl(
               fun({_T, P, M}, Acc) ->
                       maps:update_with(P, fun(L) -> [M | L] end, [M], Acc)
               end, #{}, Got),
    Bad = maps:fold(
            fun(P, Rev, Acc) ->
                    Msgs = lists:reverse(Rev),
                    Offs = [O || #kafka_message{offset = O} <- Msgs],
                    Idxs = [I || #kafka_message{key = K} <- Msgs,
                                 {ok, {_, I}} <- [coord_of(K)]],
                    case strictly_increasing(Offs)
                         andalso strictly_increasing(Idxs)
                         andalso length(Idxs) =:= length(Offs) of
                        true -> Acc;
                        false -> [P | Acc]
                    end
            end, [], ByPart),
    check(Bad =:= [],
          "per-partition order preserved on all ~p partitions "
          "(out of order: ~lp)", [maps:size(ByPart), Bad]).

strictly_increasing([]) -> true;
strictly_increasing([_]) -> true;
strictly_increasing([A, B | T]) when B > A -> strictly_increasing([B | T]);
strictly_increasing(_) -> false.

verify_partition_spread(Got, Parts) ->
    Seen = lists:usort([P || {_T, P, _M} <- Got]),
    check(length(Seen) =:= Parts,
          "messages arrived on all ~p partitions (~lp)", [Parts, Seen]).

%%%=============================================================================
%%% fetch loop (no group)
%%%=============================================================================

fetch_all(Topic, Parts, _Total) ->
    lists:flatten([fetch_partition(Topic, P, 0, []) || P <- lists:seq(0, Parts - 1)]).

fetch_partition(Topic, P, Offset, Acc) ->
    case brod:fetch(?CLIENT, Topic, P, Offset,
                    #{max_wait_time => 2000, min_bytes => 1,
                      max_bytes => 1048576}) of
        {ok, {Hw, []}} when Offset >= Hw ->
            lists:reverse(Acc);
        {ok, {_Hw, []}} ->
            lists:reverse(Acc);
        {ok, {Hw, Msgs}} ->
            Acc1 = lists:foldl(fun(M, A) -> [{Topic, P, M} | A] end, Acc, Msgs),
            #kafka_message{offset = Last} = lists:last(Msgs),
            case Last + 1 >= Hw of
                true -> lists:reverse(Acc1);
                false -> fetch_partition(Topic, P, Last + 1, Acc1)
            end;
        {error, R} ->
            fail("fetch ~s/~p at ~p: ~p", [Topic, P, Offset, R]),
            lists:reverse(Acc)
    end.

%%%=============================================================================
%%% metadata
%%%=============================================================================

list_topics(Ctx) ->
    #{endpoints := Eps, conn_config := CC} = Ctx,
    case brod:get_metadata(Eps, all, CC) of
        {ok, Meta} ->
            [topic_name(T) || T <- maps:get(topics, Meta, [])];
        {error, R} ->
            fail("get_metadata(all): ~p", [R]),
            []
    end.

topic_partition_count(Ctx, Topic) ->
    #{endpoints := Eps, conn_config := CC} = Ctx,
    case brod:get_metadata(Eps, [Topic], CC) of
        {ok, Meta} ->
            case [T || T <- maps:get(topics, Meta, []), topic_name(T) =:= Topic] of
                [T] -> {ok, length(maps:get(partitions, T, []))};
                _ -> {error, not_found}
            end;
        {error, R} -> {error, R}
    end.

topic_name(T) ->
    case get_any(T, [name, topic]) of
        B when is_binary(B) -> B;
        Other -> Other
    end.

get_any(_Map, []) -> undefined;
get_any(Map, [K | T]) ->
    case maps:find(K, Map) of
        {ok, V} -> V;
        error -> get_any(Map, T)
    end.

%%%=============================================================================
%%% mailbox collection
%%%=============================================================================

%% Collect up to N messages, giving up at the hard deadline or after QuietMs
%% with nothing arriving. A hang is a result, so every wait here is bounded.
collect_until(N, HardMs, QuietMs) ->
    End = now_ms() + HardMs,
    do_collect(N, End, QuietMs, now_ms(), [], 0).

do_collect(N, _End, _Quiet, _Last, Acc, Len) when Len >= N ->
    lists:reverse(Acc);
do_collect(N, End, Quiet, Last, Acc, Len) ->
    Now = now_ms(),
    Wait = min(End - Now, Last + Quiet - Now),
    case Wait =< 0 of
        true -> lists:reverse(Acc);
        false ->
            receive
                {qk_msg, T, P, M} ->
                    do_collect(N, End, Quiet, now_ms(), [{T, P, M} | Acc], Len + 1);
                {qk_assigned, _, _, _} ->
                    do_collect(N, End, Quiet, Last, Acc, Len);
                {qk_revoked, _, _} ->
                    do_collect(N, End, Quiet, Last, Acc, Len);
                {'EXIT', _, normal} ->
                    do_collect(N, End, Quiet, Last, Acc, Len);
                {'EXIT', Pid, Reason} ->
                    fail("a linked process died: ~p ~p", [Pid, Reason]),
                    lists:reverse(Acc)
            after Wait ->
                    lists:reverse(Acc)
            end
    end.

flush() ->
    receive _ -> flush() after 0 -> ok end.

now_ms() -> erlang:monotonic_time(millisecond).

%% Record timestamps go on the wire, so they are wall clock, not the monotonic
%% clock the deadlines above use.
now_ms_wall() -> erlang:system_time(millisecond).

%%%=============================================================================
%%% naming and output
%%%=============================================================================

topic(Kind, RunId) ->
    list_to_binary("brod-" ++ Kind ++ "-" ++ RunId).

group(Kind, RunId) ->
    list_to_binary("brod-grp-" ++ Kind ++ "-" ++ RunId).

section(Name) -> io:format("~n=== ~s~n", [Name]).

ok(true, Fmt, Args) -> io:format("  ok   " ++ Fmt ++ "~n", Args);
ok(false, Fmt, Args) -> fail(Fmt, Args).

check(true, Fmt, Args) -> io:format("  ok   " ++ Fmt ++ "~n", Args);
check(false, Fmt, Args) -> fail(Fmt, Args).

fail(Fmt, Args) ->
    put(fails, get(fails) + 1),
    io:format("  FAIL " ++ Fmt ++ "~n", Args).

note(Fmt, Args) -> io:format("  note " ++ Fmt ++ "~n", Args).
