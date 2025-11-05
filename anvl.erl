-include("anvl.hrl").

conf() ->
  #{ plugins => [anvl_erlc]
   , erlang =>
       #{ includes => [ "../emqx/apps/emqx_durable_storage/include"
                      , "${src_root}/include"
                      , "${src_root}/src"
                      , anvl_project:anvl_includes_dir()
                      ]
        }
   , conditions => [append_test]
   }.

init() ->
  precondition(anvl_erlc:app_compiled(default, loadtestds)),
  persistent_term:put(node_name_ctr, atomics:new(1, [])),
  ok = anvl_resource:declare(loadgen, 1).

append_test() ->
  experiments:append_test().
