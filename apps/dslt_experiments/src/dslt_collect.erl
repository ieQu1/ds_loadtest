%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(dslt_collect).

-behavior(gen_server).

%% API:
-export([start_experiment/4, complete_experiment/1, db_conf/0]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/0]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(call_decl_experiment,
        { scenario :: string()
        , release :: string()
        , db_conf :: map()
        , experiment :: map()
        }).

-record(call_complete_experiment,
        { time :: integer()
        }).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec start_experiment(string(), string(), map(), map()) -> integer().
start_experiment(Scenario, Release, DBConf, Exp) ->
  Req = #call_decl_experiment{scenario = Scenario, release = Release, db_conf = DBConf, experiment = Exp},
  gen_server:call(?SERVER, Req, infinity).

-spec complete_experiment(integer()) -> ok.
complete_experiment(Time) ->
  gen_server:call(?SERVER, #call_complete_experiment{time = Time}, infinity).

db_conf() ->
  #{ host => anvl_plugin:conf([dslt, results_db, host])
   , username => anvl_plugin:conf([dslt, results_db, username])
   , port => anvl_plugin:conf([dslt, results_db, port])
   , database => anvl_plugin:conf([dslt, results_db, db])
   , password => anvl_plugin:conf([dslt, results_db, password])
   }.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { db :: pid()
        , current_experiment :: integer()
        }).

init(_) ->
  process_flag(trap_exit, true),
  {ok, Pid} = epgsql:connect(db_conf()),
  %% FIXME: use priv dir normally
  Filename = filename:join([anvl_project:root(), "apps", "dslt_experiments", "priv", "schema.sql"]),
  {ok, Schema} = file:read_file(Filename),
  _ = epgsql:squery(Pid, binary_to_list(Schema)),
  S = #s{db = Pid},
  {ok, S}.

handle_call(#call_decl_experiment{scenario = Scenario, release = Release, db_conf = DBSpec, experiment = Experiment}, _From, S = #s{db = Pid}) ->
  #{db := DB, backend := Backend, n_shards := Nshards} = DBSpec,
  Nrepl = maps:get(replication_factor, DBSpec, 0),
  ConfigStr = iolist_to_binary(io_lib:format("~0p", [maps:without([backend, db, n_shards, replication_factor], DBSpec)])),
  {ok, _, [{Id}]} = epgsql:equery(
                      Pid,
                      "SELECT new_experiment($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                      [ atom_to_binary(DB)
                      , atom_to_binary(Backend)
                      , Nshards
                      , Nrepl
                      , ConfigStr
                      , atom_to_binary(Scenario)
                      , atom_to_binary(Release)
                      , maps:get(repeats, Experiment)
                      , maps:get(n, Experiment)
                      , maps:get(payload_size, Experiment, undefined)
                      , maps:get(batch_size, Experiment, undefined)
                      ]),
  {reply, Id, S#s{current_experiment = Id}};
handle_call(#call_complete_experiment{time = At}, _From, S = #s{db = Pid, current_experiment = Exp}) ->
  {ok, _, _} = epgsql:equery(
                 Pid,
                 "CALL complete_experiment($1, $2)",
                 [Exp, At]),
  {reply, ok, S#s{current_experiment = undefined}};
handle_call(Call, _From, S) ->
  logger:warning("Unknown call ~p", [Call]),
  {reply, {error, unknown_call}, S}.

handle_cast(Cast, S) ->
  logger:warning("Unknown cast ~p", [Cast]),
  {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  logger:warning("Unknown info ~p", [Info]),
  {noreply, S}.

terminate(_Reason, _S) ->
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
