-include("anvl.hrl").

conf() ->
  #{ plugins => [anvl_erlc, anvl_git, dslt_experiments]
   , erlang =>
       #{ includes => [ "../emqx/apps/emqx_durable_storage/include"
                      , "${src_root}/include"
                      , "${src_root}/src"
                      , anvl_project:anvl_includes_dir()
                      ]
        }
   , conditions => [append_test, overwrite_test]
   , [deps, git] =>
       [ #{ id => control
          , repo => "https://github.com/emqx/emqx.git"
          , ref => {branch, "release-60"}
          }
       , #{ id => epgsql
          , repo => "https://github.com/epgsql/epgsql.git"
          , ref => {tag, "4.8.0"}
          }
       ]
   }.

conf_override(_) ->
  [{set, [plugins], [anvl_erlc, anvl_git]}].

append_test() ->
  experiments:append_test().

overwrite_test() ->
  experiments:overwrite_test().
