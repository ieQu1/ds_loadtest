-module(loadtestds).

-behavior(supervisor).

%% API:
-export([ create_db/1
        , test_dbs/0
        , create_dbs/0
        %% , counter_test/1
        %% , owned_counter_test/1
        , exec_test/2
        , with_metric/2
        , report_metric/2
        ]).

%% Test setup and supervisor callbacks:
-export([init/1, start_worker/6, worker_entrypoint/5]).

-include("loadtestds.hrl").

-define(MRIA_SHARD, otx_test_shard).

with_metric(Metric, Fun) ->
  T0 = erlang:system_time(microsecond),
  try
    Fun()
  after
    T1 = erlang:system_time(microsecond),
    report_metric(Metric, T1 - T0)
  end.

create_db(UserOpts = #{db := DB, type := ds}) ->
    DB = maps:get(db, UserOpts, t),
    Defaults = #{
        backend => builtin_raft,
        n_shards => 16,
        replication_options => #{},
        n_sites => 5,
        replication_factor => 5,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}},
        transaction => #{flush_interval => 1, idle_flush_interval => 0, conflict_window => 10_000},
        reads => local_preferred
    },
    Opts = emqx_utils_maps:deep_merge(Defaults, maps:remove(db, UserOpts)),
    multicall(fun() -> emqx_ds:open_db(DB, Opts), emqx_ds:wait_db(DB, all, infinity) end);
create_db(UserOpts = #{db := DB, type := mria}) ->
    Opts = maps:merge(
        #{type => ordered_set, storage => rocksdb_copies, rlog_shard => ?MRIA_SHARD},
        maps:without([db, rlog_shard, type], UserOpts)
    ),
    multicall(
        fun() ->
            ok = mria:create_table(DB, maps:to_list(Opts)),
            ok = mria:wait_for_tables([DB])
        end
    ).

test_dbs() ->
  [ #{type => mria, db => md, storage => disc_copies}
  , #{type => mria, db => mr, storage => rocksdb_copies}
  , #{type => ds, db => d3l, replication_factor => 3, reads => local_preferred}
  %% TODO: Excluded due to some weird outliers
  %% , #{type => ds, db => d3L, replication_factor => 3, reads => leader_preferred}
  , #{type => ds, db => d5l, replication_factor => 5, reads => local_preferred}
  , #{type => ds, db => d5L, replication_factor => 5, reads => leader_preferred}
  , #{type => ds, db => dl, backend => builtin_local}
  ].

create_dbs() ->
  [create_db(I) || I <- test_dbs()].

open_csv(Experiment, ColumnNames) ->
  Filename = filename:join(emqx:data_dir(), Experiment ++ ".csv"),
  IsNew = not filelib:is_file(Filename),
  {ok, FD} = file:open(Filename, [append]),
  %% Insert CSV header:
  IsNew andalso
    io:format(FD, "~s;Metric;Val~n", [lists:join(";", ColumnNames)]),
  FD.

%% do_inc_counter(MyId, Opts = #{type := ds}) ->
%%   TxOpts = maps:with([db, timeout, retries, retry_interval], Opts),
%%   Result = emqx_ds:trans(
%%              TxOpts#{shard => {auto, MyId}, generation => 1},
%%              fun() ->
%%                  Key = [<<"cnt">>, <<MyId:64>>],
%%                  case emqx_ds:tx_read(Key) of
%%                    [{_, _, <<Val:64>>}] ->
%%                      ok;
%%                    [] ->
%%                      Val = 0
%%                  end,
%%                  emqx_ds:tx_write({Key, 0, <<(Val + 1):64>>})
%%              end
%%             ),
%%   case Result of
%%     {atomic, _, _} ->
%%       ok;
%%     _ ->
%%       Result
%%   end;

%% do_inc_counter(MyId, #{type := mria, db := DB}) ->
%%     Result = mria:transaction(
%%         ?MRIA_SHARD,
%%         fun() ->
%%             Key = {<<"cnt">>, MyId},
%%             case mnesia:read(DB, Key) of
%%                 [{DB, _, <<Val:64>>}] ->
%%                     ok;
%%                 [] ->
%%                     Val = 0
%%             end,
%%             mnesia:write({DB, Key, <<(Val + 1):64>>})
%%         end
%%     ),
%%     case Result of
%%         {atomic, _} ->
%%             ok;
%%         _ ->
%%             Result
%%     end.

%% inc_counter_loop(MyId, Opts = #{sleep := Sleep}, State) ->
%%   ok = ?with_metric(t, do_inc_counter(MyId, Opts)),
%%   (Sleep > 0) andalso timer:sleep(Sleep),
%%   State.

%% counter_test(UserOpts = #{db := DB, type := _}) ->
%%   Defaults = #{ repeats => 1
%%               , n => 1
%%               , sleep => 0
%%               , n_nodes => 1
%%               , retries => 10
%%               , retry_interval => 10
%%               },
%%   #{ sleep := Sleep
%%    , n := N
%%    , n_nodes := NNodes
%%    , repeats := Repeats
%%    , retries := TxRetries
%%    } = Opts = maps:merge(Defaults, UserOpts),
%%   io:format("Cleanup..."),
%%   clear_table(Opts),
%%   timer:sleep(1000),
%%   Success = exec_test(Opts,
%%                       fun inc_counter_loop/3,
%%                       "counter",
%%                       ["DB", "N", "Nnodes", "Sleep", "Retries"],
%%                       [DB, N, NNodes, Sleep, TxRetries]
%%                      ),
%%   case Success of
%%     true ->
%%       io:format("Verifying results...~n"),
%%       ExpectedValue = <<(NNodes * Repeats):64>>,
%%       verify_counters(Opts, ExpectedValue);
%%     false ->
%%       io:format("Run wasn't successful...~n"),
%%       false
%%   end;
%% counter_test(UserOpts) ->
%%   [?FUNCTION_NAME(maps:merge(UserOpts, maps:with([db, type], I))) || I <- test_dbs()].

%% verify_counters(#{db := _DB, n := _N, type := mria}, _ExpectedVal) ->
%%     io:format("Ignored~n"),
%%     ok;
%% verify_counters(#{db := DB, n := N, type := ds}, ExpectedVal) ->
%%     timer:sleep(2000),
%%     NVerified = emqx_ds:fold_topic(
%%         fun(_Slab, _Stream, {Topic, _, Bin}, Acc) ->
%%             case Bin of
%%                 ExpectedVal ->
%%                     Acc + 1;
%%                 Other ->
%%                     io:format("Mismatch for topic ~p, got ~p expected ~p~n", [
%%                         Topic, Other, ExpectedVal
%%                     ]),
%%                     Acc + 1
%%             end
%%         end,
%%         0,
%%         [<<"cnt">>, '+'],
%%         #{db => DB}
%%     ),
%%     case NVerified of
%%         N ->
%%             ok;
%%         _ ->
%%             io:format("Number of counters is ~p, expected ~p~n", [NVerified, N])
%%     end.

%% do_own_counter(MyId, Opts = #{type := ds}) ->
%%   TxOpts = maps:with([db, timeout, retries, retry_interval], Opts),
%%   Result = emqx_ds:trans(
%%              TxOpts#{shard => {auto, MyId}, generation => 1},
%%              fun() ->
%%                  emqx_ds:tx_write({[<<"g">>, <<MyId:64>>], 0, ?ds_tx_serial}),
%%                  case emqx_ds:tx_read([<<"d">>, <<MyId:64>>]) of
%%                    [{_, _, <<Val:64>>}] ->
%%                      Val;
%%                    [] ->
%%                      0
%%                  end
%%              end),
%%     case Result of
%%       {atomic, Guard, Val} ->
%%         {ok, Guard, Val};
%%       _ ->
%%         Result
%%     end;
%% do_own_counter(MyId, Opts = #{type := mria}) ->
%%     #{db := DB} = Opts,
%%     Guard = make_ref(),
%%     Result = mria:transaction(
%%         ?MRIA_SHARD,
%%         fun() ->
%%             mnesia:write({DB, {g, MyId}, Guard}),
%%             case mnesia:read(DB, {d, MyId}) of
%%                 [{DB, _, <<Val:64>>}] ->
%%                     Val;
%%                 _ ->
%%                     0
%%             end
%%         end
%%     ),
%%     case Result of
%%         {atomic, Val} ->
%%             {ok, Guard, Val};
%%         _ ->
%%             Result
%%     end.

%% do_inc_owned_counter(MyId, Val0, Guard, Opts = #{type := ds}) ->
%%   TxOpts = maps:with([db, timeout, retries, retry_interval], Opts),
%%   Result = emqx_ds:trans(
%%              TxOpts#{shard => {auto, MyId}, generation => 1},
%%              fun() ->
%%                  Val = Val0 + 1,
%%                  emqx_ds:tx_ttv_assert_present([<<"g">>, <<MyId:64>>], 0, Guard),
%%                  emqx_ds:tx_write({[<<"cnt">>, <<MyId:64>>], 0, <<Val:64>>}),
%%                  {ok, Val}
%%              end),
%%   case Result of
%%     {atomic, _, Ret} ->
%%       Ret;
%%     ?err_unrec({precondition_failed, _}) ->
%%       lost_ownership;
%%     _ ->
%%       Result
%%   end;
%% do_inc_owned_counter(MyId, Val0, Guard, Opts = #{type := mria}) ->
%%   Val = Val0 + 1,
%%   #{db := DB} = Opts,
%%   Result = mria:transaction(
%%              ?MRIA_SHARD,
%%              fun() ->
%%                  case mnesia:read(DB, {g, MyId}) of
%%                    [{DB, _, Guard}] ->
%%                      mnesia:write({DB, {cnt, MyId}, <<Val:64>>}),
%%                      {ok, Val};
%%                    _ ->
%%                      lost_ownership
%%                  end
%%              end),
%%     case Result of
%%         {atomic, R} ->
%%             R;
%%         _ ->
%%             Result
%%     end.

%% inc_owned_counter_loop(MyId, Opts, S0) ->
%%   case S0 of
%%     undefined ->
%%       {ok, Guard, Val0} = ?with_metric(o, do_own_counter(MyId, Opts));
%%     {Guard, Val0} ->
%%       ok
%%   end,
%%   {ok, Val} = ?with_metric(i, do_inc_owned_counter(MyId, Val0, Guard, Opts)),
%%   {Guard, Val}.

%% owned_counter_test(UserOpts = #{db := DB, type := _}) ->
%%   Defaults = #{ repeats => 1
%%               , n => 1
%%               , n_nodes => 1
%%               , timeout => 10_000
%%               , retries => 10
%%               },
%%   Opts = #{n := N, n_nodes := NNodes, repeats := Repeats, retries := TxRetries} = maps:merge(Defaults, UserOpts),
%%   io:format("Cleanup..."),
%%   clear_table(Opts),
%%   timer:sleep(1000),
%%   Success = exec_test(Opts,
%%                       fun inc_owned_counter_loop/3,
%%                       "owned_counter",
%%                       ["DB", "N", "Nnodes", "Retries"],
%%                       [DB, N, NNodes, TxRetries]
%%                      ),
%%   case Success of
%%     true ->
%%       io:format("Verifying results...~n"),
%%       ExpectedValue = <<(NNodes * Repeats):64>>,
%%       verify_counters(Opts, ExpectedValue);
%%     false ->
%%       io:format("Run wasn't successful...~n"),
%%       false
%%   end;
%% owned_counter_test(UserOpts) ->
%%   [?FUNCTION_NAME(maps:merge(UserOpts, maps:with([db, type], I))) || I <- test_dbs()].

%%-----------------------------------------------------------------------------------------------------------
%% Test harness
%%-----------------------------------------------------------------------------------------------------------

-record(s,
        { success = true :: boolean()
        , csv_fd :: file:iodevice()
        , csv_prefix :: binary()
        , t0 :: integer()
        , mref :: reference() | undefined
        , timeout :: timeout()
        , run_time :: non_neg_integer() | undefined
        , opts :: map()
        , cbm :: module()
        }).

%% 1. Start a supervision tree with `n_nodes' copies on random nodes
%% in the cluster for each integer between 1 and 'n'.
%%
%% 2. Once all processes are ready, execute `Fun' in each of them
%%
%% 3. Wait until all processes are done.
-spec exec_test(
    module(),
    #{
        n := pos_integer(),
        n_nodes => pos_integer(),
        available_nodes => [node()],
        test_timeout => timeout(),
        repeats => pos_integer()
    }
) ->
    boolean().
exec_test(CBM, UserOpts) ->
  Defaults = #{ available_nodes => [node() | nodes()]
              , n_nodes => 1
              , test_timeout => infinity
              },
  #{test_timeout := TestTimeout} = Opts =
    maps:merge(Defaults, maps:merge(CBM:defaults(), UserOpts)),
  ExperimentName = CBM:name(),
  ColumNames = CBM:metric_columns(),
  MeasurementFields = CBM:metric_prefix(Opts),
  CSV = open_csv(ExperimentName, ColumNames),
  DatapointPrefix = iolist_to_binary(
                      lists:join(";", [io_lib:format("~p", [I]) || I <- MeasurementFields])),
  io:format("=== Running ~s/~s ===~n", [ExperimentName, DatapointPrefix]),
  put(parent, self()),
  %% Spawn a temporary process that will be monitored by all worker
  %% processes. Its termination signals start of the test:
  Trigger = spawn_link(fun() ->
                           receive
                             pull -> ok
                           end
                       end),
  %% Start the workers:
  {ok, Top} = supervisor:start_link(?MODULE, {top, CBM, Opts, self(), Trigger}),
  io:format("Ensemble is ready: ~p~n", [Top]),
  MRef = monitor(process, Top),
  unlink(Top),
  %% Now when the setup is complete, let's broadcast that it's time
  %% to start the test:
  Trigger ! pull,
  %% Start collecting messages until supervisor terminates:
  #s{ success = Success
    , run_time = RunTime
    } = collect_replies(#s{ csv_fd = CSV
                          , csv_prefix = DatapointPrefix
                          , t0 = erlang:system_time(microsecond)
                          , mref = MRef
                          , timeout = TestTimeout
                          , opts = Opts
                          , cbm = CBM
                          }),
  io:format(CSV, "~s;~p;~p~n", [DatapointPrefix, run_time, RunTime]),
  %% Shutdown the sup in case of timeout:
  exit(Top, shutdown),
  file:close(CSV),
  Success.

collect_replies(#s{ timeout = Timeout
                  , mref = MRef
                  , t0 = T0
                  , csv_fd = FD
                  , csv_prefix = Prefix
                  , opts = Opts
                  , cbm = CBM
                  } = S) ->
  receive
    {'DOWN', MRef, process, _, Reason} ->
      %% Supervisor has stopped, everything's done:
      Dt = erlang:system_time(microsecond) - T0,
      io:format("Complete in ~p s~n~p~n", [Dt / 1_000_000, Reason]),
      CBM:post_test(Opts, Dt),
      %% Wait a little more to collect the rest of the messages:
      collect_replies(S#s{timeout = 100, run_time = Dt});
    {metric, M, Val} ->
      io:format(FD, "~s;~p;~p~n", [Prefix, M, Val]),
      collect_replies(S);
    {fail, _} ->
      collect_replies(S#s{success = false})
  after Timeout ->
      S
  end.

report_metric(Metric, Val) ->
  get(parent) ! {metric, Metric, Val}.

report_fail(Reason) ->
  get(parent) ! {fail, Reason}.

%%-----------------------------------------------------------------------------------------------------------
%% Supervisor
%%-----------------------------------------------------------------------------------------------------------

init({top, CBM, Opts0 = #{n := N}, Parent, Trigger}) ->
  Opts = CBM:init(Opts0),
  SupFlags = #{ strategy => one_for_one
              , intensity => 10
              , period => 1
              , auto_shutdown => all_significant
              },
  Children = [#{ id => I
               , type => supervisor
               , shutdown => infinity
               , restart => temporary
               , start => {supervisor, start_link, [?MODULE, {worker, CBM, Opts, Parent, Trigger, I}]}
               , significant => true
               }
              || I <- lists:seq(0, N - 1)],
    {ok, {SupFlags, Children}};
init({worker, CBM, Opts, Parent, Trigger, MyId}) ->
  #{n_nodes := NNodes, available_nodes := NodeAvail} = Opts,
  SupFlags = #{
               strategy => one_for_one,
               intensity => 10,
               period => 1,
               auto_shutdown => all_significant
              },
  {Nodes, _} = lists:split(NNodes, shuffle(NodeAvail)),
  Children = [#{ id => Node
               , type => worker
               , restart => temporary
               , start => {?MODULE, start_worker, [Node, CBM, Opts, MyId, Parent, Trigger]}
               , shutdown => 100
               , significant => true
               }
              || Node <- Nodes],
  {ok, {SupFlags, Children}}.

start_worker(Node, CBM, Opts, N, Parent, Trigger) ->
  Fun = fun CBM:loop/3,
  Pid = proc_lib:spawn_link(Node, ?MODULE, worker_entrypoint, [Fun, Opts, N, Parent, Trigger]),
  {ok, Pid}.

worker_entrypoint(Fun, Opts = #{repeats := Repeats}, MyId, Parent, Trigger) ->
    MRef = monitor(process, Trigger),
    put(parent, Parent),
    receive
        {'DOWN', MRef, process, Trigger, _} ->
            try
              lists:foldl(
                fun(_, Acc) -> Fun(MyId, Opts, Acc) end,
                undefined,
                lists:seq(1, Repeats)
               )
            catch EC:Err:Stack ->
                logger:error("Test worker ~p failed with reason ~p:~p~nStack: ~p", [MyId, EC, Err, Stack]),
                report_fail({EC, Err})
            end
    end.

shuffle(L) ->
    {_, Ret} = lists:unzip(lists:sort([{rand:uniform(), I} || I <- L])),
    Ret.

clear_table(#{type := mria, db := DB}) ->
    mria:clear_table(DB);
clear_table(#{db := DB}) ->
    maps:foreach(
        fun({Shard, Gen}, _Val) ->
            {atomic, _, _} = emqx_ds:trans(
                #{db => DB, generation => Gen, shard => Shard, timeout => 120_000},
                fun() ->
                    emqx_ds:tx_del_topic(['#'])
                end
            )
        end,
        emqx_ds:list_slabs(DB)
    ).

multicall(Fun) ->
  Nodes = [node() | nodes()],
  {_, []} = rpc:multicall(Nodes, erlang, apply, [Fun, []]),
  ok.
