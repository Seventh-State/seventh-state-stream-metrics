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
    Metrics = seven_stream_metrics_collector:collect(
        [stream_metrics],
        fun() ->
            #{
                {osiris_writer, {resource, <<"/">>, queue, <<"orders">>}} =>
                    #{offset => <<"x">>},
                {osiris_replica, {resource, <<"/">>, queue, <<"payments">>}} =>
                    not_a_counter_map
            }
        end
    ),
    ?assertEqual(#{}, to_field_samples(Metrics)).

optional_reader_metric_omission_test() ->
    Metrics = seven_stream_metrics_collector:collect(
        [stream_metrics],
        fun() ->
            #{
                {osiris_writer, {resource, <<"/">>, queue, <<"orders">>}} =>
                    #{offset => 120},
                {osiris_replica, {resource, <<"/">>, queue, <<"payments">>}} =>
                    #{offset => 55}
            }
        end
    ),
    FieldSamples = to_field_samples(Metrics),
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
    Metrics = seven_stream_metrics_collector:collect(
        [stream_metrics],
        fun() ->
            #{
                {osiris_writer, {resource, <<"/">>, queue, <<"orders">>}} =>
                    #{offset => 120, readers => 3},
                {osiris_replica, {resource, <<"/">>, queue, <<"payments">>}} =>
                    #{offset => 55}
            }
        end
    ),
    FieldSamples = to_field_samples(Metrics),
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

forced_gc_metric_omitted_test() ->
    Metrics = seven_stream_metrics_collector:collect(
        [stream_metrics],
        fun() ->
            #{
                {osiris_writer, {resource, <<"/">>, queue, <<"orders">>}} =>
                    #{offset => 120, forced_gc => 7, forced_gcs => 9}
            }
        end
    ),
    FieldSamples = to_field_samples(Metrics),
    ?assertEqual(false, maps:is_key(forced_gc, FieldSamples)),
    ?assertEqual(false, maps:is_key(forced_gcs, FieldSamples)),
    ?assertEqual(
        [
            #{
                vhost => <<"/">>,
                stream => <<"orders">>,
                role => <<"writer">>,
                value => 120
            }
        ],
        maps:get(offset, FieldSamples)
    ).

overview_shape_parsing_test() ->
    Pid = self(),
    Resource = {resource, <<"/">>, queue, <<"streamtest">>},
    ensure_table_deleted(consumer_created),
    Tab = ets:new(consumer_created, [named_table, public]),
    ets:insert(Tab, {{Resource, Pid, <<"stream.subid-0">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"stream.subid-0">>}]}),
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
                },
            {rabbit_stream_queue, Resource, <<"amq.ctag-1">>, Pid} =>
                #{
                    offset => 99,
                    chunks => 2
                }
    },
    Metrics = seven_stream_metrics_collector:collect(
        [stream_metrics, consumers],
        fun() -> Overview end
    ),
    FieldSamples = to_field_samples(Metrics),
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
        sort_samples([
            #{
                vhost => <<"/">>,
                stream => <<"streamtest">>,
                consumer => <<"stream.subid-0">>,
                connection => list_to_binary(erlang:pid_to_list(Pid)),
                pid => list_to_binary(erlang:pid_to_list(Pid)),
                protocol => <<"stream">>,
                value => 42
            },
            #{
                vhost => <<"/">>,
                stream => <<"streamtest">>,
                consumer => <<"amq.ctag-1">>,
                connection => list_to_binary(erlang:pid_to_list(Pid)),
                pid => list_to_binary(erlang:pid_to_list(Pid)),
                protocol => <<"amqp">>,
                value => 99
            }
        ]),
        sort_samples(maps:get(consumer_offset, FieldSamples))
    ),
    ets:delete(Tab).

family_filtering_by_source_tag_test() ->
    Pid = self(),
    Overview = #{
        {osiris_writer, {resource, <<"/">>, queue, <<"stream_a">>}} =>
            #{
                offset => 10,
                committed_offset => 9,
                epoch => 2,
                packets => 25,
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
            },
        {rabbit_stream_queue, {resource, <<"/">>, queue, <<"stream_a">>}, <<"amq.ctag-a">>, Pid} =>
            #{
                offset => 12
            }
    },
    ensure_table_deleted(consumer_created),
    ensure_table_deleted(connection_created),
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
    StreamFields = to_field_samples(StreamMetrics),
    ?assert(maps:is_key(offset, StreamFields)),
    ?assert(maps:is_key(committed_offset, StreamFields)),
    ?assert(maps:is_key(readers, StreamFields)),
    ?assertEqual(false, maps:is_key(epoch, StreamFields)),
    ?assertEqual(false, maps:is_key(packets, StreamFields)),
    ?assertEqual(false, maps:is_key(consumer_offset, StreamFields)),

    StreamMisc = seven_stream_metrics_collector:collect(
        [stream_misc],
        fun() -> Overview end
    ),
    StreamMiscFields = to_field_samples(StreamMisc),
    ?assertEqual(
        [epoch, packets],
        lists:sort(maps:keys(StreamMiscFields))
    ),

    Consumers = seven_stream_metrics_collector:collect(
        [consumers],
        fun() -> Overview end
    ),
    ConsumerFields = to_field_samples(Consumers),
    ?assertEqual([consumer_offset], maps:keys(ConsumerFields)),
    ?assertEqual(
        sort_samples([
            #{
                vhost => <<"/">>,
                stream => <<"stream_a">>,
                consumer => <<"c1">>,
                connection => <<"conn-a">>,
                pid => list_to_binary(erlang:pid_to_list(Pid)),
                protocol => <<"stream">>,
                value => 11
            },
            #{
                vhost => <<"/">>,
                stream => <<"stream_a">>,
                consumer => <<"amq.ctag-a">>,
                connection => <<"conn-a">>,
                pid => list_to_binary(erlang:pid_to_list(Pid)),
                protocol => <<"amqp">>,
                value => 12
            }
        ]),
        sort_samples(maps:get(consumer_offset, ConsumerFields))
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
    ensure_table_deleted(consumer_created),
    ensure_table_deleted(connection_created),
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
    [Sample] = maps:get(consumer_offset, to_field_samples(Consumers)),
    ?assertEqual(binary:part(LongName, 0, 100), maps:get(connection, Sample)),
    ?assertEqual(<<"stream">>, maps:get(protocol, Sample)),
    ets:delete(ConnTab),
    ets:delete(Tab),

    Pid2 = spawn(fun() -> receive after 10 -> ok end end),
    ensure_table_deleted(consumer_created),
    ensure_table_deleted(connection_created),
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
    [Sample2] = maps:get(consumer_offset, to_field_samples(Consumers2)),
    ?assertEqual(<<"fallback-name">>, maps:get(connection, Sample2)),
    ?assertEqual(list_to_binary(erlang:pid_to_list(Pid2)), maps:get(pid, Sample2)),
    ?assertEqual(<<"stream">>, maps:get(protocol, Sample2)),
    ets:delete(ConnTab2),
    ets:delete(Tab2),
    exit(Pid2, kill).

consumer_lag_family_filtering_test() ->
    Pid = self(),
    Overview = #{
        {rabbit_stream_reader, {resource, <<"/">>, queue, <<"stream_a">>}, 0, Pid} =>
            #{offset => 100}
    },
    ensure_table_deleted(consumer_created),
    ensure_table_deleted(rabbit_stream_consumer_created),
    Tab = ets:new(consumer_created, [named_table, public]),
    ConsumerTab = ets:new(rabbit_stream_consumer_created, [named_table, public]),
    ets:insert(Tab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, <<"stream.subid-0">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"c1">>}]}),
    ets:insert(ConsumerTab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, 0},
                             [{offset_lag, 500}]}),
    Consumers = seven_stream_metrics_collector:collect(
        [consumer_lag],
        fun() -> Overview end
    ),
    FieldSamples = to_field_samples(Consumers),
    ?assertEqual([consumer_offset_lag], maps:keys(FieldSamples)),
    [Sample] = maps:get(consumer_offset_lag, FieldSamples),
    ?assertEqual(500, maps:get(value, Sample)),
    ?assertEqual(<<"stream">>, maps:get(protocol, Sample)),
    ets:delete(ConsumerTab),
    ets:delete(Tab).

consumer_offset_lag_metric_extraction_test() ->
    Pid = self(),
    Overview = #{
        {rabbit_stream_reader, {resource, <<"/">>, queue, <<"stream_a">>}, 0, Pid} =>
            #{offset => 100},
        {rabbit_stream_queue, {resource, <<"/">>, queue, <<"stream_b">>}, <<"amq.ctag-1">>, Pid} =>
            #{offset => 50}
    },
    ensure_table_deleted(consumer_created),
    ensure_table_deleted(rabbit_stream_consumer_created),
    ensure_table_deleted(connection_created),
    Tab = ets:new(consumer_created, [named_table, public]),
    ConsumerTab = ets:new(rabbit_stream_consumer_created, [named_table, public]),
    ConnTab = ets:new(connection_created, [named_table, public]),
    
    % Insert stream reader consumer with offset_lag
    ets:insert(Tab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, <<"stream.subid-0">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"reader-c1">>}]}),
    ets:insert(ConsumerTab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, 0},
                             [{offset_lag, 1000}]}),
    
    % Insert AMQP consumer with offset_lag
    ets:insert(Tab, {{{resource, <<"/">>, queue, <<"stream_b">>}, Pid, <<"amq.ctag-1">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"queue-c1">>}]}),
    ets:insert(ConsumerTab, {{{resource, <<"/">>, queue, <<"stream_b">>}, Pid, <<"amq.ctag-1">>},
                             [{offset_lag, 2000}]}),
    ets:insert(ConnTab, {Pid, [{user_provided_name, <<"test-conn">>}]}),
    
    Metrics = seven_stream_metrics_collector:collect(
        [consumer_lag],
        fun() -> Overview end
    ),
    Samples = sort_samples(maps:get(consumer_offset_lag, to_field_samples(Metrics))),
    ?assertEqual(2, length(Samples)),
    
    [Sample1, Sample2] = Samples,
    ?assertEqual(2000, maps:get(value, Sample1)),
    ?assertEqual(<<"amqp">>, maps:get(protocol, Sample1)),
    
    ?assertEqual(1000, maps:get(value, Sample2)),
    ?assertEqual(<<"stream">>, maps:get(protocol, Sample2)),
    
    ets:delete(ConnTab),
    ets:delete(ConsumerTab),
    ets:delete(Tab).

consumer_offset_lag_omitted_when_missing_test() ->
    Pid = self(),
    Overview = #{
        {rabbit_stream_reader, {resource, <<"/">>, queue, <<"stream_a">>}, 0, Pid} =>
            #{offset => 100}
    },
    ensure_table_deleted(consumer_created),
    ensure_table_deleted(rabbit_stream_consumer_created),
    Tab = ets:new(consumer_created, [named_table, public]),
    ConsumerTab = ets:new(rabbit_stream_consumer_created, [named_table, public]),
    ets:insert(Tab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, <<"stream.subid-0">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"c1">>}]}),
    % No offset_lag entry in consumer data
    ets:insert(ConsumerTab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, 0},
                             [{other_field, 123}]}),
    Metrics = seven_stream_metrics_collector:collect(
        [consumer_lag],
        fun() -> Overview end
    ),
    FieldSamples = to_field_samples(Metrics),
    ?assertEqual(false, maps:is_key(consumer_offset_lag, FieldSamples)),
    ets:delete(ConsumerTab),
    ets:delete(Tab).

consumer_lag_with_invalid_offset_lag_values_test() ->
    Pid = self(),
    Overview = #{
        {rabbit_stream_reader, {resource, <<"/">>, queue, <<"stream_a">>}, 0, Pid} =>
            #{offset => 100},
        {rabbit_stream_reader, {resource, <<"/">>, queue, <<"stream_b">>}, 1, Pid} =>
            #{offset => 50}
    },
    ensure_table_deleted(consumer_created),
    ensure_table_deleted(rabbit_stream_consumer_created),
    Tab = ets:new(consumer_created, [named_table, public]),
    ConsumerTab = ets:new(rabbit_stream_consumer_created, [named_table, public]),
    
    % Valid offset_lag
    ets:insert(Tab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, <<"stream.subid-0">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"c1">>}]}),
    ets:insert(ConsumerTab, {{{resource, <<"/">>, queue, <<"stream_a">>}, Pid, 0},
                             [{offset_lag, 500}]}),
    
    % Invalid offset_lag values (should be omitted)
    ets:insert(Tab, {{{resource, <<"/">>, queue, <<"stream_b">>}, Pid, <<"stream.subid-1">>},
                     false,false,0,true,up,[{<<"name">>, longstr, <<"c2">>}]}),
    ets:insert(ConsumerTab, {{{resource, <<"/">>, queue, <<"stream_b">>}, Pid, 1},
                             [{offset_lag, <<"not_integer">>}]}),
    
    Metrics = seven_stream_metrics_collector:collect(
        [consumer_lag],
        fun() -> Overview end
    ),
    Samples = maps:get(consumer_offset_lag, to_field_samples(Metrics)),
    ?assertEqual(1, length(Samples)),
    [Sample] = Samples,
    ?assertEqual(500, maps:get(value, Sample)),
    
    ets:delete(ConsumerTab),
    ets:delete(Tab).

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

to_field_samples(FieldSamples) when is_list(FieldSamples) ->
    maps:from_list(FieldSamples);
to_field_samples(FieldSamples) when is_map(FieldSamples) ->
    FieldSamples;
to_field_samples(_) ->
    #{}.

ensure_table_deleted(Table) ->
    case ets:info(Table) of
        undefined -> ok;
        _ -> ets:delete(Table)
    end.
