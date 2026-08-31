%%% The brod_group_subscriber_v2 callback used by every group scenario.
%%%
%%% It does the least it possibly can: forward each message to a collector pid
%%% and tell brod what to do with the offset. Keeping the callback dumb is what
%%% makes the assertions in qk_brod live in ONE place, in the main process,
%%% where a failure is a printed line rather than a crashed worker whose exit
%%% reason nobody reads.
%%%
%%% One brod fact worth knowing before reading the scenarios: brod starts ONE
%%% instance of this module per assigned PARTITION, each in its own process,
%%% each with its own State. There is no cross-partition callback state to be
%%% had, which is why per-partition ordering is checked by the collector from
%%% the arrival sequence rather than accumulated here.
-module(qk_brod_cb).

-behaviour(brod_group_subscriber_v2).

-export([init/2, handle_message/2, terminate/2]).

-include_lib("brod/include/brod.hrl").

%% InitInfo carries #{group_id, topic, partition, commit_fun}.
init(#{topic := Topic, partition := Partition} = InitInfo, InitData) ->
    #{sink := Sink, mode := Mode} = InitData,
    Sink ! {qk_assigned, Topic, Partition, self()},
    {ok, #{topic => Topic,
           partition => Partition,
           sink => Sink,
           mode => Mode,
           commit_fun => maps:get(commit_fun, InitInfo, undefined)}}.

%% Mode is the offset disposition, and the two values are the two things a real
%% brod app does:
%%   commit -> {ok, commit, State}: ack the message to the consumer AND write
%%             the offset through the group coordinator.
%%   ack    -> {ok, ack, State}: flow-control only, offset stays uncommitted.
%%             This is how the "restart with nothing committed" case is built.
handle_message(#kafka_message{} = Msg, State) ->
    #{topic := Topic, partition := Partition, sink := Sink, mode := Mode} = State,
    Sink ! {qk_msg, Topic, Partition, Msg},
    case Mode of
        commit -> {ok, commit, State};
        ack    -> {ok, ack, State}
    end.

terminate(_Reason, #{topic := Topic, partition := Partition, sink := Sink}) ->
    Sink ! {qk_revoked, Topic, Partition},
    ok.
