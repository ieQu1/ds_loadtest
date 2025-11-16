%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(experiments).

-include("anvl.hrl").
-include("emqx_ds.hrl").

%% API:
-export([peer/1, append_test/0]).

-define(DLOC_SHARDS, 2).

%%================================================================================
%% API functions
%%================================================================================

?MEMO(append_test,
      begin
        precondition([append_test(I, dloc) || I <- [experiment, control]]),
        precondition([vs_graph("append", "PayloadSize"), append_throughput()])
      end).

%%================================================================================
%% Internal functions
%%================================================================================

?MEMO(vs_graph, Experiment, SweepBy,
      begin
        Script = filename:absname("analysis/histogram.py"),
        CSVs = csv_files(Experiment),
        newer([Script | CSVs], Experiment ++ ".png") andalso
          anvl_lib:exec(Script, [Experiment, SweepBy | CSVs])
      end).

?MEMO(append_throughput,
      begin
        Script = filename:absname("analysis/append_throughput.py"),
        Out = "append_throughput.png",
        CSVs = csv_files("append"),
        newer([Script | CSVs], Out) andalso
          anvl_lib:exec(Script, [Out | CSVs])
      end).

?MEMO(append_test, Release, DB,
      begin
        PSizes = [0, 100, 1_000, 100_000],
        precondition(
          [run_test(
             Release,
             experiment_append,
             #{ db => DB
              , payload_size => PS
              , repeats => 100
              , n => ?DLOC_SHARDS
              , batch_size => 1000
              })
           || PS <- PSizes])
      end).

?MEMO(local_system, Release,
      begin
        precondition(sut_compiled()),
        Dir = release_dir(Release),
        case file:del_dir_r(filename:join(Dir, "data")) of
          ok -> ok;
          {error, enoent} -> ok
        end,
        NodeName = get_node_name(),
        EmuFlags = "+JPperf true -pz " ++ loadgen_app_path(),
        Env = [ {"EMQX_NODE__NAME", NodeName}
              , {"ERL_FLAGS", EmuFlags}
              , {"EMQX_dashboard__listeners__http__bind", "0"}
              ] ++
              [{"EMQX_listeners__" ++ L ++ "__default__enable", "false"}
               || L <- ["tcp", "ssl", "ws", "wss"]],
        {ok, Peer, _} = peer:start(#{ connection => standard_io
                                    , exec => {filename:join(Dir, "bin/emqx"), ["foreground"]}
                                    , env => Env
                                    , wait_boot => 30_000
                                    }),
        anvl_condition:set_result({emqx_system, Release}, Peer),
        load_test_code(Peer),
        ok = peer:call(Peer, application, start, [loadtestds]),
        true
      end).

%% TODO: application controller doesn't do that automatically?
load_test_code(Peer) ->
  Beams = filelib:wildcard(filename:join(loadgen_app_path(), "*.beam")),
  [peer:call(Peer, code, load_abs, [filename:rootname(I)]) || I <- Beams],
  ok.

?MEMO(db_created, Release, DB,
      begin
        precondition(local_system(Release)),
        Conf = maps:merge(db_conf(DB), #{db => DB}),
        ok = peer:call(peer(Release), loadtestds, create_db, [Conf], infinity),
        true
      end).

db_conf(dloc) ->
  #{ backend => builtin_local
   , type => ds
   , subscriptions => #{n_workers_per_shard => 0}
   , n_shards => ?DLOC_SHARDS
   }.

?MEMO(run_test, Release, CBM, Cfg = #{db := DB},
      begin
        Name = CBM:name(),
        need_retest(Release, Name) andalso
          begin
            precondition(db_created(Release, DB)),
            anvl_resource:with(
              loadgen,
              fun() ->
                  logger:warning("Running test ~p/~p", [Release, Cfg]),
                  Result = peer:call(
                             peer(Release),
                             loadtestds,
                             exec_test,
                             [CBM, Cfg],
                             infinity),
                  logger:warning("Complete test with result ~p", [Result])
              end),
            true
          end
      end).

get_node_name() ->
  Ctr = persistent_term:get(node_name_ctr),
  NC = atomics:add_get(Ctr, 1, 1),
  io_lib:format("emqx~p@127.0.0.1", [NC]).

%% This function compares mtime of the output CSV file against all the
%% sources that go into the experiment, namely loadgen and EMQX
%% build artifacts.
need_retest(Release, Experiment) ->
  Sources = filelib:wildcard(filename:join(loadgen_app_path(), "**")) ++
    filelib:wildcard(filename:join(release_dir(Release), "{bin,lib,etc,plugins,releases}/**")),
  newer(Sources, csv_file(Release, Experiment)).

loadgen_app_path() ->
  #{ebin_dir := Dir} = anvl_erlc:app_info(default, loadtestds),
  filename:absname(filename:join(Dir, "ebin")).

peer(Release) ->
  anvl_condition:get_result({emqx_system, Release}).

csv_file(Release, Experiment) ->
  anvl_fn:workdir([Release, "data", Experiment ++ ".csv"]).

csv_files(Experiment) ->
  [csv_file(I, Experiment) || I <- [experiment, control]].

release_dir(Release) ->
  anvl_fn:ensure_type(filename:join(root_dir(Release), "_build/emqx-enterprise/rel/emqx"),
                      list).

root_dir(experiment) ->
  Dir = filename:join(anvl_project:root(), "SUT"),
  filelib:is_dir(Dir) orelse
    ?UNSAT("Symlink EMQX under development to ./SUT", []),
  Dir;
root_dir(control) ->
  _ = precondition(anvl_locate:located(?MODULE, control)),
  anvl_locate:dir(?MODULE, control).

?MEMO(sut_compiled,
      precondition([sut_compiled(control), sut_compiled(experiment)])).

?MEMO(sut_compiled, Release,
      begin
        Dir = root_dir(Release),
        Dst = filename:join(Dir, ".compiled@"),
        {ok, Sources} = anvl_git:ls_files(Dir, [other, {x, ".compiled@"}]),
        newer(Sources, Dst) andalso
          begin
            anvl_lib:exec("make", [], [{cd, Dir}]),
            file:write_file(Dst, []),
            true
          end
      end).
