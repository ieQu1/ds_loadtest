-include("anvl.hrl").

conf() ->
  #{ plugins => [anvl_erlc, anvl_git]
   , erlang =>
       #{ includes => [ "../emqx/apps/emqx_durable_storage/include"
                      , "${src_root}/include"
                      , "${src_root}/src"
                      , anvl_project:anvl_includes_dir()
                      ]
        }
   , conditions => [append_test, overwrite_test]
   , [deps, git] =>
       [#{ id => control
         , repo => "https://github.com/emqx/emqx.git"
         , ref => {branch, "release-60"}
         }]
   }.

init() ->
  precondition([anvl_erlc:app_compiled(default, dslt_analysis), anvl_erlc:app_compiled(default, dslt_experiments)]),
  persistent_term:put(node_name_ctr, atomics:new(1, [])),
  %% Resource that excludes CPU-intensive tasks while running test
  ok = anvl_resource:declare(cleanroom, 1).

append_test() ->
  experiments:append_test().

overwrite_test() ->
  experiments:overwrite_test().
