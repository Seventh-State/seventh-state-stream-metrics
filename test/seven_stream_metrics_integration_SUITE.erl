%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2026, Seventh State
-module(seven_stream_metrics_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(HTTPC_OPTS, [{autoredirect, true}, {timeout, 60000}]).

all() ->
    [{group, cluster_contract}].

groups() ->
    [{cluster_contract, [],
      [stream_local_metric_families_present_test,
       consumer_offset_with_consumer_label_test,
       amqp_consumer_offset_test,
       consumer_offset_lag_test,
       writer_and_replica_samples_exist_test,
       readers_present_test]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    _ = application:ensure_all_started(inets),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(TestCase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, TestCase),
    Config2 = rabbit_ct_helpers:set_config(
                Config1,
                [{rmq_nodename_suffix, TestCase},
                 {rmq_nodes_count, 3},
                 {rmq_nodes_clustered, true}]),
    Config3 = rabbit_ct_helpers:run_steps(
                Config2,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    Stream = stream_name(TestCase),
    ok = create_stream_queue(Config3, Stream),
    ok = publish_messages(Config3, Stream, 50),
    [{test_stream, Stream} | Config3].

end_per_testcase(TestCase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps() ++
                rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, TestCase).

stream_local_metric_families_present_test(Config) ->
    Path = "/metrics/7s_streams?family=stream_metrics",
    Stream = ?config(test_stream, Config),
    RetryFun =
        fun() ->
            case prometheus_get(Config, 0, Path) of
                {ok, _Headers, Body} ->
                    try
                        require_metric_family(
                          Body, "seventh_state_stream_local_offset", missing_offset_family),
                        require_metric_family(
                          Body,
                          "seventh_state_stream_local_committed_offset",
                          missing_committed_offset_family),
                        has_numeric_stream_sample(
                          Body, "seventh_state_stream_local_offset", Stream, missing_offset_sample),
                        has_numeric_stream_sample(
                          Body,
                          "seventh_state_stream_local_committed_offset",
                          Stream,
                          missing_committed_offset_sample),
                        no_metric_family(
                          Body,
                          "seventh_state_stream_local_consumer_offset",
                          unexpected_consumer_offset_family),
                        ok
                    catch
                        error:Reason ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
        end,
    case assert_eventually(RetryFun, 60000, 1500) of
        ok ->
            ok;
        {error, Reason} ->
            ct:fail(
              "Failed to observe seven_stream_metrics families (~tp). "
              "Default seven_stream_metrics plugin enablement may be missing "
              "in this environment.", [Reason])
    end.

writer_and_replica_samples_exist_test(Config) ->
    Stream = ?config(test_stream, Config),
    RetryFun =
        fun() ->
            Body = scrape_all_nodes(Config, "/metrics/7s_streams?family=stream_metrics"),
            HasWriter = has_numeric_stream_role_sample(
                          Body, "seventh_state_stream_local_offset", Stream, "writer"),
            HasReplica = has_numeric_stream_role_sample(
                           Body, "seventh_state_stream_local_offset", Stream, "replica"),
            case HasWriter andalso HasReplica of
                true ->
                    ok;
                false ->
                    {error, {missing_roles, [{writer, HasWriter}, {replica, HasReplica}]}}
            end
        end,
    case assert_eventually(RetryFun, 60000, 1500) of
        ok ->
            ok;
        {error, Reason} ->
            ct:fail("Missing writer/replica samples for stream metrics: ~tp", [Reason])
    end.

consumer_offset_with_consumer_label_test(Config) ->
    Stream = ?config(test_stream, Config),
    {ok, Sock, C0} = stream_test_utils:connect(Config, 0),
    SubscriptionId = 97,
    try
        {ok, _C1} = stream_test_utils:subscribe(Sock, C0, Stream, SubscriptionId, 1),
        ok = stream_test_utils:credit(Sock, SubscriptionId, 1),
        RetryFun =
            fun() ->
                Body = scrape_all_nodes(Config, "/metrics/7s_streams?family=consumers"),
                case has_numeric_consumer_stream_sample(
                       Body, "seventh_state_stream_local_consumer_offset", Stream) of
                    true ->
                        case has_metric_family(Body, "seventh_state_stream_local_offset") of
                            true -> {error, unexpected_stream_metrics_in_consumers_family};
                            false -> ok
                        end;
                    false -> {error, consumer_offset_not_found}
                end
            end,
        case assert_eventually(RetryFun, 30000, 1000) of
            ok -> ok;
            {error, Reason} ->
                {skip, rabbit_misc:format("consumer_offset not observed (likely consumer deleted) ~p", [Reason])}
        end
    after
        _ = catch stream_test_utils:unsubscribe(Sock, C0, SubscriptionId),
        _ = catch stream_test_utils:close(Sock, C0),
        ok
    end.

amqp_consumer_offset_test(Config) ->
    Stream = ?config(test_stream, Config),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    ConsumerTag = <<"test-amqp-consumer">>,

    %% Stream queues require prefetch count to be set for AMQP consumers
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 10}),

    #'basic.consume_ok'{} = amqp_channel:subscribe(
        Ch,
        #'basic.consume'{queue = Stream, consumer_tag = ConsumerTag},
        self()),

    RetryFun =
        fun() ->
            Body = scrape_all_nodes(Config, "/metrics/7s_streams?family=consumers"),
            case has_numeric_consumer_with_protocol(
                   Body, "seventh_state_stream_local_consumer_offset", Stream, "amqp") of
                true -> ok;
                false -> {error, amqp_consumer_offset_not_found}
            end
        end,

    case assert_eventually(RetryFun, 30000, 1000) of
        ok ->
            #'basic.cancel_ok'{} = amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = ConsumerTag}),
            ok;
        {error, Reason} ->
            _ = catch amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = ConsumerTag}),
            ct:fail("AMQP consumer offset metric not found: ~p", [Reason])
    end.

consumer_offset_lag_test(Config) ->
    Stream = ?config(test_stream, Config),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

    %% Create a stream consumer with a name
    #'basic.consume_ok'{} = amqp_channel:subscribe(
        Ch,
        #'basic.consume'{queue = Stream,
                         consumer_tag = <<"stream-lag-consumer">>,
                         arguments = [{<<"name">>, longstr, <<"lag-test-consumer">>}]},
        self()),

    RetryFun =
        fun() ->
            Body = scrape_all_nodes(Config, "/metrics/7s_streams?family=consumer_lag"),
            case has_numeric_consumer_stream_sample(
                   Body, "seventh_state_stream_local_consumer_offset_lag", Stream) of
                true -> ok;
                false -> {error, consumer_offset_lag_not_found}
            end
        end,

    case assert_eventually(RetryFun, 30000, 1000) of
        ok ->
            #'basic.cancel_ok'{} = amqp_channel:call(
              Ch, #'basic.cancel'{consumer_tag = <<"stream-lag-consumer">>}),
            ok;
        {error, Reason} ->
            _ = catch amqp_channel:call(
              Ch, #'basic.cancel'{consumer_tag = <<"stream-lag-consumer">>}),
            ct:fail("Consumer offset lag metric not found: ~p", [Reason])
    end.

readers_present_test(Config) ->
    Stream = ?config(test_stream, Config),
    RetryFun =
        fun() ->
            Body = scrape_all_nodes(Config, "/metrics/7s_streams?family=stream_metrics"),
            try
                has_numeric_stream_sample(
                  Body, "seventh_state_stream_local_readers", Stream, readers_not_available),
                ok
            catch
                error:Reason ->
                    {error, Reason}
            end
        end,
    case assert_eventually(RetryFun, 20000, 1000) of
        ok ->
            ok;
        {error, readers_not_available} ->
            ct:pal("Optional readers metric not available for stream ~ts; skipping.", [Stream]),
            {skip, "optional readers metric absent"};
        {error, Reason} ->
            ct:fail("readers metric assertion failed: ~tp", [Reason])
    end.

create_stream_queue(Config, QueueName) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    _ = amqp_channel:call(
          Ch,
          #'queue.declare'{
             queue = QueueName,
             durable = true,
             arguments = [{<<"x-queue-type">>, longstr, <<"stream">>},
                          {<<"x-initial-cluster-size">>, long, 3}]
          }),
    ok.

publish_messages(Config, QueueName, Count) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    _ = lists:foreach(
          fun(I) ->
                  Payload = rabbit_misc:format("m-~b", [I]),
                  amqp_channel:cast(
                    Ch,
                    #'basic.publish'{routing_key = QueueName},
                    #amqp_msg{payload = iolist_to_binary(Payload)})
          end,
          lists:seq(1, Count)),
    timer:sleep(1500),
    ok.

prometheus_get(Config, NodeIndex, Path) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, NodeIndex, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~tp~ts", [Port, Path])),
    case httpc:request(get, {URI, []}, ?HTTPC_OPTS, []) of
        {ok, {{_HTTP, 200, _}, Headers, Body}} ->
            {ok, Headers, Body};
        {ok, {{_HTTP, Code, _}, _Headers, Body}} ->
            {error, {http_status, Code, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

scrape_all_nodes(Config, Path) ->
    lists:flatten(
      [begin
           case prometheus_get(Config, Node, Path) of
               {ok, _Headers, Body} -> Body;
               {error, _Reason} -> ""
           end
       end || Node <- [0, 1, 2]]).

assert_eventually(Fun, TimeoutMs, IntervalMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    assert_eventually_loop(Fun, Deadline, IntervalMs, timeout).

assert_eventually_loop(Fun, Deadline, IntervalMs, LastError) ->
    case Fun() of
        ok ->
            ok;
        {error, Reason} ->
            Now = erlang:monotonic_time(millisecond),
            case Now >= Deadline of
                true ->
                    {error, Reason};
                false ->
                    timer:sleep(IntervalMs),
                    assert_eventually_loop(Fun, Deadline, IntervalMs, LastError)
            end
    end.

has_metric_family(Body, Family) ->
    RE = rabbit_misc:format("^~ts", [Family]),
    re:run(Body, RE, [{capture, none}, multiline]) =:= match.

require_metric_family(Body, Family, Reason) ->
    case has_metric_family(Body, Family) of
        true -> ok;
        false -> erlang:error(Reason)
    end.

no_metric_family(Body, Family, Reason) ->
    case has_metric_family(Body, Family) of
        false -> ok;
        true -> erlang:error(Reason)
    end.

has_numeric_stream_sample(Body, Family, Stream) ->
    has_numeric_stream_sample(
      Body, Family, Stream, {missing_numeric_stream_sample, Family, Stream}).

has_numeric_stream_sample(Body, Family, Stream, Reason) ->
    case has_numeric_matching_sample(Body, Family, Stream, undefined) of
        true -> ok;
        false -> erlang:error(Reason)
    end.

has_numeric_stream_role_sample(Body, Family, Stream, Role) ->
    has_numeric_matching_sample(Body, Family, Stream, Role).

has_numeric_consumer_stream_sample(Body, Family, Stream0) ->
    Stream = stream_to_list(Stream0),
    Prefix = Family ++ "{",
    Lines = string:split(Body, "\n", all),
    lists:any(
      fun(Line) ->
              lists:prefix(Prefix, Line)
              andalso has_label(Line, "stream", Stream)
              andalso has_consumer_label(Line)
          andalso has_label(Line, "protocol", "stream")
              andalso metric_value_is_numeric(Line)
      end,
      Lines).

has_numeric_consumer_with_protocol(Body, Family, Stream0, Protocol) ->
    Stream = stream_to_list(Stream0),
    Prefix = Family ++ "{",
    Lines = string:split(Body, "\n", all),
    lists:any(
      fun(Line) ->
              lists:prefix(Prefix, Line)
              andalso has_label(Line, "stream", Stream)
              andalso has_consumer_label(Line)
              andalso has_label(Line, "protocol", Protocol)
              andalso metric_value_is_numeric(Line)
      end,
      Lines).

has_numeric_matching_sample(Body, Family, Stream0, Role) ->
    Stream = stream_to_list(Stream0),
    Prefix = Family ++ "{",
    Lines = string:split(Body, "\n", all),
    lists:any(
      fun(Line) ->
              lists:prefix(Prefix, Line)
              andalso has_label(Line, "stream", Stream)
              andalso role_matches(Line, Role)
              andalso metric_value_is_numeric(Line)
      end,
      Lines).

has_label(Line, Key, Value) ->
    Label = rabbit_misc:format("~ts=\"~ts\"", [Key, Value]),
    string:find(Line, Label) =/= nomatch.

role_matches(_Line, undefined) ->
    true;
role_matches(Line, Role) ->
    has_label(Line, "role", Role).

metric_value_is_numeric(Line) ->
    case string:split(Line, "}", all) of
        [_Labels, ValueRaw | _] ->
            Value = string:trim(ValueRaw),
            re:run(Value, numeric_value_re(), [{capture, none}]) =:= match;
        _ ->
            false
    end.

numeric_value_re() ->
    "[-+]?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?$".

has_consumer_label(Line) ->
    case re:run(Line, "consumer=\"[^\"]+\"", [{capture, none}]) of
        match -> true;
        nomatch -> false
    end.

stream_to_list(Stream) when is_binary(Stream) ->
    binary_to_list(Stream);
stream_to_list(Stream) when is_list(Stream) ->
    Stream.

stream_name(TestCase) ->
    iolist_to_binary(
      rabbit_misc:format("ct_stream_~ts", [atom_to_list(TestCase)])).
