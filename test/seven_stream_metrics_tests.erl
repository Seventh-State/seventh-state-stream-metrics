%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%%
%%% @end
%%% Created : 17 Jul 2025 by Seventh State <contact@seventhstate.io>
-module(seven_stream_metrics_tests).

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
    ?assertMatch([_|_], seven_stream_metrics:module_info()).

invalid_entry_omitted_test() ->
    Metrics = seven_stream_metrics_collector:build_metrics([
        #{vhost => <<"/">>, stream => <<"orders">>, role => writer, counters => #{offset => <<"x">>}},
        #{vhost => <<"/">>, stream => <<"payments">>, role => replica}
    ]),
    ?assertEqual(#{}, maps:get(field_samples, Metrics)).

optional_reader_metric_omission_test() ->
    Metrics = seven_stream_metrics_collector:build_metrics([
        #{vhost => <<"/">>, stream => <<"orders">>, role => writer, counters => #{offset => 120}},
        #{vhost => <<"/">>, stream => <<"payments">>, role => replica, counters => #{offset => 55}}
    ]),
    FieldSamples = maps:get(field_samples, Metrics),
    ?assertEqual(
        sort_samples([
            #{
                vhost => <<"/">>,
                stream => <<"orders">>,
                role => <<"writer">>,
                value => 120
            },
            #{
                vhost => <<"/">>,
                stream => <<"payments">>,
                role => <<"replica">>,
                value => 55
            }
        ]),
        sort_samples(maps:get(offset, FieldSamples))
    ),
    ?assertEqual(undefined, maps:get(readers, FieldSamples, undefined)).

reader_metric_present_when_available_test() ->
    Metrics = seven_stream_metrics_collector:build_metrics([
        #{vhost => <<"/">>, stream => <<"orders">>, role => writer, counters => #{offset => 120, readers => 3}},
        #{vhost => <<"/">>, stream => <<"payments">>, role => replica, counters => #{offset => 55}}
    ]),
    FieldSamples = maps:get(field_samples, Metrics),
    ?assertEqual(
        [
            #{
                vhost => <<"/">>,
                stream => <<"orders">>,
                role => <<"writer">>,
                value => 3
            }
        ],
        maps:get(readers, FieldSamples)
    ).

overview_shape_parsing_test() ->
    Pid = self(),
    Resource = {resource, <<"/">>, queue, <<"streamtest">>},
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
            },
        {rabbit_stream_reader, Resource, 0, Pid} =>
            #{
                offset => 42,
                chunks => 1
            }
    },
    ConsumerMap = #{{Resource, Pid} => <<"stream.subid-0">>},
    RawEntries = seven_stream_metrics_collector:entries_from_overview(Overview, ConsumerMap),
    Metrics = seven_stream_metrics_collector:build_metrics(RawEntries),
    FieldSamples = maps:get(field_samples, Metrics),
    ?assertEqual(
        sort_samples([
            #{
                vhost => <<"/">>,
                stream => <<"streamtest">>,
                role => <<"writer">>,
                value => 0
            },
            #{
                vhost => <<"/">>,
                stream => <<"replica_stream">>,
                role => <<"replica">>,
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
                role => <<"writer">>,
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
                role => <<"writer">>,
                value => 0
            },
            #{
                vhost => <<"/">>,
                stream => <<"replica_stream">>,
                role => <<"replica">>,
                value => 9
            }
        ]),
        sort_samples(maps:get(committed_offset, FieldSamples))
    ),
    ?assertEqual(
        [
            #{
                vhost => <<"/">>,
                stream => <<"streamtest">>,
                consumer => <<"stream.subid-0">>,
                connection => list_to_binary(erlang:pid_to_list(Pid)),
                value => 42
            }
        ],
        maps:get(consumer_offset, FieldSamples)
    ).

family_filtering_by_source_tag_test() ->
    Pid = self(),
    Overview = #{
        {osiris_writer, {resource, <<"/">>, queue, <<"stream_a">>}} =>
            #{
                offset => 10,
                committed_offset => 9,
                readers => 1
            },
        {osiris_replica, {resource, <<"/">>, queue, <<"stream_b">>}} =>
            #{
                offset => 5,
                committed_offset => 4
            },
        {rabbit_stream_reader, {resource, <<"/">>, queue, <<"stream_a">>}, 0, Pid} =>
            #{
                offset => 11
            }
    },
    Tab = ets:new(consumer_created, [named_table, public]),
    ConnTab = ets:new(connection_created, [named_table, public]),
    ets:insert(Tab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, <<"stream.subid-0">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"c1">>}]}),
    ets:insert(ConnTab, {Pid, [{user_provided_name, <<"conn-a">>},
                               {name, <<"127.0.0.1:59107 -> 127.0.0.1:5552">>}]}),

    StreamMetrics = seven_stream_metrics_collector:collect(
        [stream_metrics],
        fun() -> Overview end
    ),
    StreamFields = maps:get(field_samples, StreamMetrics),
    ?assert(maps:is_key(offset, StreamFields)),
    ?assert(maps:is_key(committed_offset, StreamFields)),
    ?assert(maps:is_key(readers, StreamFields)),
    ?assertEqual(false, maps:is_key(consumer_offset, StreamFields)),

    Consumers = seven_stream_metrics_collector:collect(
        [consumers],
        fun() -> Overview end
    ),
    ConsumerFields = maps:get(field_samples, Consumers),
    ?assertEqual([consumer_offset], maps:keys(ConsumerFields)),
    ?assertEqual(
        [
            #{
                vhost => <<"/">>,
                stream => <<"stream_a">>,
                consumer => <<"c1">>,
                connection => <<"conn-a">>,
                value => 11
            }
        ],
        maps:get(consumer_offset, ConsumerFields)
    ),
    ets:delete(ConnTab),
    ets:delete(Tab).

connection_name_fallback_and_truncation_test() ->
    Pid = self(),
    Resource = {resource, <<"/">>, queue, <<"stream_a">>},
    Overview = #{
        {rabbit_stream_reader, Resource, 0, Pid} =>
            #{
                offset => 11
            }
    },
    Tab = ets:new(consumer_created, [named_table, public]),
    ConnTab = ets:new(connection_created, [named_table, public]),
    ets:insert(Tab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, <<"stream.subid-0">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"c1">>}]}),
    LongName = list_to_binary(lists:duplicate(120, $a)),
    ets:insert(ConnTab, {Pid, [{user_provided_name, LongName},
                               {name, <<"fallback-name">>}]}),
    Consumers = seven_stream_metrics_collector:collect(
        [consumers],
        fun() -> Overview end
    ),
    [Sample] = maps:get(consumer_offset, maps:get(field_samples, Consumers)),
    ?assertEqual(binary:part(LongName, 0, 100), maps:get(connection, Sample)),
    ets:delete(ConnTab),
    ets:delete(Tab),

    Pid2 = spawn(fun() -> receive after 10 -> ok end end),
    Tab2 = ets:new(consumer_created, [named_table, public]),
    ConnTab2 = ets:new(connection_created, [named_table, public]),
    ets:insert(Tab2, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid2, <<"stream.subid-1">>},
                      false,false,0,true,up,[{<<"name">>, longstr, <<"c2">>}]}),
    ets:insert(ConnTab2, {Pid2, [{name, <<"fallback-name">>}]}),
    Overview2 = #{
        {rabbit_stream_reader, Resource, 1, Pid2} =>
            #{
                offset => 12
            }
    },
    Consumers2 = seven_stream_metrics_collector:collect(
        [consumers],
        fun() -> Overview2 end
    ),
    [Sample2] = maps:get(consumer_offset, maps:get(field_samples, Consumers2)),
    ?assertEqual(<<"fallback-name">>, maps:get(connection, Sample2)),
    ets:delete(ConnTab2),
    ets:delete(Tab2),
    exit(Pid2, kill).

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
