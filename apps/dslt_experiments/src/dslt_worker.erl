%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(dslt_worker).

-export([start/6, worker_entrypoint/5, report_metric/3, with_metric/3]).

-include("loadtestds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API
%%================================================================================

with_metric(Metric, WorkerId, Fun) ->
  T0 = os:perf_counter(microsecond),
  try
    Fun()
  after
    T1 = os:perf_counter(microsecond),
    dslt_worker:report_metric(Metric, WorkerId, T1 - T0)
  end.

report_metric(Metric, MyId, Val) ->
  get(parent) ! #cast_metric{metric = Metric, t = os:system_time(microsecond), worker = MyId, val = Val},
  ok.

start(Node, CBM, Opts, N, Parent, Trigger) ->
  Fun = fun CBM:loop/3,
  Pid = proc_lib:spawn_link(Node, ?MODULE, worker_entrypoint, [Fun, Opts, N, Parent, Trigger]),
  {ok, Pid}.

%%================================================================================
%% Internal exports
%%================================================================================

worker_entrypoint(Fun, Opts = #{repeats := Repeats}, MyId, Parent, Trigger) ->
  MRef = monitor(process, Trigger),
  put(parent, Parent),
  receive
    {'DOWN', MRef, process, Trigger, _} ->
      try
        T0 = os:perf_counter(microsecond),
        loop(Fun, MyId, Opts, undefined, Repeats),
        T1 = os:perf_counter(microsecond),
        report_metric(run_time, MyId, T1 - T0),
        report_metric(w_start_time, MyId, T0),
        report_metric(w_stop_time, MyId, T1)
      catch EC:Err:Stack ->
          logger:error("Test worker ~p failed with reason ~p:~p~nStack: ~p", [MyId, EC, Err, Stack]),
          report_fail({EC, Err})
      after
        report_complete()
      end
  end.

%%================================================================================
%% Internal functions
%%================================================================================

report_fail(Reason) ->
  get(parent) ! {fail, Reason}.

report_complete() ->
  get(parent) ! worker_done.

loop(_Fun, _MyId, _Opts, _Acc, I) when I =< 0 ->
  ok;
loop(Fun, MyId, Opts, Acc0, I) ->
  Acc = Fun(MyId, Opts, Acc0),
  loop(Fun, MyId, Opts, Acc, I - 1).
