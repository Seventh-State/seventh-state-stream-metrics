%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%%
%%% @end
%%% Created : 17 Jul 2025 by Seventh State <contact@seventhstate.io>
-module(seven_hello_plugin_tests).

%%%===================================================================
%%% Includes, defines, types and records
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%%  Test descriptions
%%====================================================================

core_functionality_test_() ->
    {"Core Functionality Tests",
     {setup,
      fun core_functionality_setup/0,
      fun core_functionality_cleanup/1,
      [{"Core Functionality First Use Case",
        ?_test(core_functionality_first_use_case())}
      ]}}.

%%====================================================================
%%  Setup and cleanup
%%====================================================================

core_functionality_setup() ->
    ok.

core_functionality_cleanup(_FromSetup) ->
    ok.

%%====================================================================
%%  Unit tests
%%====================================================================

%%--------------------------------------------------------------------
%% Test group: core_functionality
%%
%% Core Functionality Tests
%%--------------------------------------------------------------------

%% Core Functionality First Use Case
core_functionality_first_use_case() ->
    ?assertMatch([_|_], seven_hello_plugin:module_info()).

invalid_entry_omitted_test() ->
    Metrics = seven_stream_metrics_collector:build_metrics([
        #{vhost => <<"/">>, stream => <<"orders">>, role => leader, counters => #{offset => <<"x">>}},
        #{vhost => <<"/">>, stream => <<"payments">>, role => replica}
    ]),
    ?assertEqual(#{}, maps:get(field_samples, Metrics)).

optional_reader_metric_omission_test() ->
    Metrics = seven_stream_metrics_collector:build_metrics([
        #{vhost => <<"/">>, stream => <<"orders">>, role => leader, counters => #{offset => 120}},
        #{vhost => <<"/">>, stream => <<"payments">>, role => replica, counters => #{offset => 55}}
    ]),
    FieldSamples = maps:get(field_samples, Metrics),
    ?assertEqual(
        sort_samples([
            #{
                vhost => <<"/">>,
                stream => <<"orders">>,
                node_role => <<"leader">>,
                value => 120
            },
            #{
                vhost => <<"/">>,
                stream => <<"payments">>,
                node_role => <<"replica">>,
                value => 55
            }
        ]),
        sort_samples(maps:get(offset, FieldSamples))
    ),
    ?assertEqual(undefined, maps:get(readers, FieldSamples, undefined)).

reader_metric_present_when_available_test() ->
    Metrics = seven_stream_metrics_collector:build_metrics([
        #{vhost => <<"/">>, stream => <<"orders">>, role => leader, counters => #{offset => 120, readers => 3}},
        #{vhost => <<"/">>, stream => <<"payments">>, role => replica, counters => #{offset => 55}}
    ]),
    FieldSamples = maps:get(field_samples, Metrics),
    ?assertEqual(
        [
            #{
                vhost => <<"/">>,
                stream => <<"orders">>,
                node_role => <<"leader">>,
                value => 3
            }
        ],
        maps:get(readers, FieldSamples)
    ).

overview_shape_parsing_test() ->
    Overview = #{
        {osiris_writer, {resource, <<"/">>, queue, <<"streamtest">>}} =>
            #{
                offset => 0,
                chunks => 1,
                epoch => 1,
                committed_offset => 0,
                readers => 0,
                first_offset => 0,
                first_timestamp => 0,
                segments => 1
            },
        {osiris_replica, {resource, <<"/">>, queue, <<"replica_stream">>}} =>
            #{
                offset => 11,
                committed_offset => 9
            }
    },
    RawEntries = seven_stream_metrics_collector:entries_from_overview(Overview),
    Metrics = seven_stream_metrics_collector:build_metrics(RawEntries),
    FieldSamples = maps:get(field_samples, Metrics),
    ?assertEqual(
        sort_samples([
            #{
                vhost => <<"/">>,
                stream => <<"streamtest">>,
                node_role => <<"leader">>,
                value => 0
            },
            #{
                vhost => <<"/">>,
                stream => <<"replica_stream">>,
                node_role => <<"replica">>,
                value => 11
            }
        ]),
        sort_samples(maps:get(offset, FieldSamples))
    ),
    ?assertEqual(
        sort_samples([
            #{
                vhost => <<"/">>,
                stream => <<"streamtest">>,
                node_role => <<"leader">>,
                value => 0
            }
        ]),
        sort_samples(maps:get(readers, FieldSamples))
    ),
    ?assertEqual(
        sort_samples([
            #{
                vhost => <<"/">>,
                stream => <<"streamtest">>,
                node_role => <<"leader">>,
                value => 0
            },
            #{
                vhost => <<"/">>,
                stream => <<"replica_stream">>,
                node_role => <<"replica">>,
                value => 9
            }
        ]),
        sort_samples(maps:get(committed_offset, FieldSamples))
    ).

%%====================================================================
%%  Helper functions
%%====================================================================
sort_samples(Samples) ->
    lists:sort(
        fun(A, B) ->
            maps:get(stream, A) =< maps:get(stream, B)
        end,
        Samples
    ).
