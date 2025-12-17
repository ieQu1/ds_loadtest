%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(experiment_append).

%% API

%% behavior callbacks:
-export([name/0, defaults/0, init/1, loop/3, post_test/2, metric_columns/0, metric_prefix/1]).

-include("loadtestds.hrl").

%%================================================================================
%% API
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

name() ->
  "append".

defaults() ->
  #{ batch_size => 100
   , n => 1
   , n_nodes => 1
   , repeats => 100
   }.

init(Conf = #{payload_size := PS}) ->
  Payload = list_to_binary([0 || _ <- lists:seq(1, PS)]),
  Conf#{payload => Payload}.

metric_columns() ->
  ["DB", "PayloadSize", "BatchSize"].

metric_prefix(#{db := DB, payload_size := PSize, batch_size := BatchSize}) ->
  [DB, PSize, BatchSize].

loop(MyId, Opts = #{db := DB, batch_size := BS, payload := Payload}, undefined) ->
  Shards = emqx_ds:list_shards(DB),
  N = length(Shards),
  Shard = lists:nth(MyId rem N + 1, Shards),
  Topic = [<<"t">>, integer_to_binary(MyId)],
  Batch = [{Topic, ?ds_tx_ts_monotonic, Payload} || _ <- lists:seq(1, BS)],
  State = {Shard, Batch},
  loop(MyId, Opts, State);
loop(MyId, #{db := DB}, {Shard, Batch} = State) ->
  ?with_metric(t,
               begin
                 Ref = emqx_ds:dirty_append(#{db => DB, shard => Shard, reply => true}, Batch),
                 receive
                   ?ds_tx_commit_reply(Ref, Reply) ->
                     case emqx_ds:dirty_append_outcome(Ref, Reply) of
                       {ok, _} ->
                         ok;
                       Err ->
                         error(#{reason => Err, shard => Shard, worker => MyId})
                     end
                 end
               end),
  State.

post_test(#{payload_size := PS, batch_size := BS, n := Nworkers, repeats := Repeats}, Time) ->
  Nops = BS * Nworkers * Repeats,
  BytesWritten = PS * Nops,
  Throughput = BytesWritten / Time,
  %% Note: B/μs = MB/S
  io:format("Written ~p MB~nTime: ~ps~nThroughput ~p MB/s~n",
            [BytesWritten / 1_000_000, Time / 1_000_000, Throughput]),
  loadtestds:report_metric(throughput, Throughput),
  loadtestds:report_metric(tps, 1_000_000 * Nops / Time).

%%================================================================================
%% Internal functions
%%================================================================================
