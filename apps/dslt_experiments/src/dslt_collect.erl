%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(dslt_collect).

-behavior(gen_server).

%% API:
-export([decl_db/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/0]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec decl_db(map()) -> integer().
decl_db(DBSpec) ->
  gen_server:call(?SERVER, {decl_db, DBSpec}, infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { odbc :: pid()
        }).

init(_) ->
  process_flag(trap_exit, true),
  {ok, Pid} = odbc:connect(anvl_plugin:conf([dslt, results_db]), [{binary_strings, on}]),
  %% FIXME: use priv dir normally
  Filename = filename:join([anvl_project:root(), "apps", "dslt_experiments", "priv", "schema.sql"]),
  {ok, Schema} = file:read_file(Filename),
  _ = odbc:sql_query(Pid, binary_to_list(Schema)),
  S = #s{ odbc = Pid
        },
  {ok, S}.

handle_call({decl_db, DBSpec}, _From, S = #s{odbc = Pid}) ->
  #{db := DB, backend := Backend, n_shards := Nshards} = DBSpec,
  Nrepl = maps:get(replication_factor, DBSpec, 0),
  DBstr = atom_to_binary(DB),
  BackendStr = atom_to_binary(Backend),
  Config = iolist_to_binary(io_lib:format("~p", [DBSpec])),
  Q = """
      SELECT db_id(?, ?, ?, ?, ?)
      """,
  {selected, _, [{Id}]} = odbc:param_query(
                            Pid,
                            Q,
                            [ {{sql_char, size(DBstr)}, [DBstr]}
                            , {{sql_char, size(BackendStr)}, [BackendStr]}
                            , {sql_smallint, [Nshards]}
                            , {sql_smallint, [Nrepl]}
                            , {{sql_char, size(Config)}, [Config]}
                            ]),
  {reply, Id, S};
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
  {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, _S) ->
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
