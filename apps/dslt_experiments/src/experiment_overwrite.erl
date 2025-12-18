%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(experiment_overwrite).

%% behavior callbacks:
-export([defaults/0, init/1, loop/3, post_test/2]).

-include("loadtestds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(s,
        { from :: binary()
        , topic :: binary()
        , msg :: binary()
        }).

%% TODO: don't use this code
-record(message, {
    id :: binary(),
    qos = 0,
    from :: atom() | binary(),
    flags = #{},
    headers = #{},
    topic :: binary(),
    payload :: iodata(),
    timestamp :: integer(),
    extra = #{} :: term()
}).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

defaults() ->
  #{ payload_size => 100
   , n => 1000
   }.

init(Conf = #{payload_size := PS}) ->
  Conf#{payloads => [rand_bytes(PS) || _ <- lists:seq(1, 1000)]}.

loop(MyId, Opts = #{payloads := Payloads}, undefined) ->
  S = #s{ from = integer_to_binary(MyId)
        , msg = Payloads
        , topic = <<"t/", (integer_to_binary(MyId))/binary, "/foo">>
        },
  loop(MyId, Opts, S);
loop(MyId, #{db := DB, payload_size := PS}, S = #s{msg = [M | Rest]}) ->
  ?with_metric(t,
               MyId,
               begin
                 Msg = emqx_message:make(S#s.from, S#s.topic, M),
                 store_retained(DB, Msg)
               end),
  S#s{msg = Rest};
loop(MyId, Conf = #{payloads := Payloads = [_|_]}, S) ->
  loop(MyId, Conf, S#s{msg = Payloads}).


post_test(#{payload_size := PS, n := Nworkers, repeats := Repeats}, Time) ->
  BytesWritten = PS * Nworkers * Repeats,
  %% Note: B/μs = MB/S
  io:format("Written ~p MB~nTime: ~ps~n",
            [BytesWritten / 1_000_000, Time / 1_000_000]).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

store_retained(DB, Msg = #message{topic = Topic}) ->
    TTV = {emqx_ds:topic_words(Topic), 1, Msg#message{topic = <<>>, id = <<>>}},
    Ret = emqx_ds:trans(
        #{db => DB, shard => {auto, Topic}, generation => active_gen(), sync => true},
        fun() ->
            emqx_ds:tx_write(TTV)
        end
    ),
    case Ret of
        {atomic, _, _} ->
            ok;
        Error ->
            Error
    end.

active_gen() ->
  1.

rand_bytes(PS) ->
  list_to_binary([rand:uniform(256) - 1 || _ <- lists:seq(1, PS)]).
