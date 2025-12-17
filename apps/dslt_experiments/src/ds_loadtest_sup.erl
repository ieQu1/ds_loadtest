%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(ds_loadtest_sup).


-behavior(supervisor).

%% API:
-export([start_link/4, stop/0, start_workers/1]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_workers/5]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-define(SUP, ?MODULE).

start_link(CBM, Opts = #{n := N}, Parent, Trigger) ->
  {ok, Top} = supervisor:start_link({local, ?SUP}, ?MODULE, {top, CBM, Opts, Parent, Trigger}),
  [{ok, _} = supervisor:start_child(Top, [I]) || I <- lists:seq(0, N - 1)],
  {ok, Top}.

stop() ->
  maybe
    Pid = whereis(?SUP),
    true ?= is_pid(Pid),
    unlink(Pid),
    exit(Pid, shutdown)
  end.

start_workers(I) when I =< 0 ->
  ok;
start_workers(I) ->
  supervisor:start_child(?SUP, [I]),
  start_workers(I - 1).

start_workers(CBM, Opts, Parent, Trigger, MyId) ->
  supervisor:start_link(?MODULE, {worker, CBM, Opts, Parent, Trigger, MyId}).

%%================================================================================
%% behavior callbacks
%%================================================================================

init({top, CBM, Opts, Parent, Trigger}) ->
  Children = [#{ id => worker
               , type => supervisor
               , shutdown => infinity
               , restart => temporary
               , start => {?MODULE, start_workers, [CBM, Opts, Parent, Trigger]}
               }
             ],
  SupFlags = #{ strategy      => simple_one_for_one
              , intensity     => 10
              , period        => 10
              },
  {ok, {SupFlags, Children}};
init({worker, CBM, Opts, Parent, Trigger, MyId}) ->
  #{n_nodes := NNodes, available_nodes := NodeAvail} = Opts,
  SupFlags = #{ strategy => one_for_one
              , intensity => 10
              , period => 1
              , auto_shutdown => all_significant
              },
  {Nodes, _} = lists:split(NNodes, shuffle(NodeAvail)),
  Children = [#{ id => Node
               , type => worker
               , restart => temporary
               , start => {loadtestds, start_worker, [Node, CBM, Opts, MyId, Parent, Trigger]}
               , shutdown => 100
               , significant => true
               }
              || Node <- Nodes],
  {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

shuffle(L) ->
    {_, Ret} = lists:unzip(lists:sort([{rand:uniform(), I} || I <- L])),
    Ret.
