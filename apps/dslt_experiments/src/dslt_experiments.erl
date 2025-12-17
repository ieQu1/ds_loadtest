%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(dslt_experiments).

-behavior(anvl_plugin).

%% API:
-export([]).

%% behavior callbacks:
-export([init/0, init_for_project/1, model/0, project_model/0]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

init() ->
  ok.

init_for_project(_) ->
  {ok, _} = application:ensure_all_started(dslt_experiments),
  ok.

model() ->
  #{ dslt =>
       #{ results_db =>
            {[value, os_env],
             #{ type => typerefl:string()
              , default => "DSN=PostgreSQL;UID=postgres"
              }}
        , data_dir =>
            {[value, os_env],
             #{ oneliner => "EMQX workdir"
              , type => typerefl:string()
              , default => anvl_fn:workdir(["dslt"])
              }}
        }
   }.

project_model() ->
  #{}.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
