%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(dslt_analysis).

-export([vs_graph/3, throughput/3]).

-include("anvl.hrl").

%%================================================================================
%% API functions
%%================================================================================

?MEMO(vs_graph, Experiment, SweepBy, Releases,
      begin
        CSVs = [experiments:csv_file(I, Experiment) || I <- Releases],
        Args = lists:append([[atom_to_list(I), experiments:csv_file(I, Experiment)] || I <- Releases]),
        Script = filename:absname("apps/dslt_analysis/histogram.py", anvl_project:root()),
        newer([Script | CSVs], Experiment ++ ".png") andalso
          anvl_lib:exec(Script, [Experiment, SweepBy | Args])
      end).

?MEMO(throughput, Experiment, SweepBy, Releases,
      begin
        CSVs = [experiments:csv_file(I, Experiment) || I <- Releases],
        Script = filename:absname("apps/dslt_analysis/append_throughput.py", anvl_project:root()),
        Out = Experiment ++ "_throughput.png",
        newer([Script | CSVs], Out) andalso
          anvl_lib:exec(Script, [Experiment, Out, SweepBy | CSVs])
      end).

%%================================================================================
%% Internal functions
%%================================================================================
