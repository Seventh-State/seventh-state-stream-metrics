%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
-module(seven_stream_metrics_prometheus).

-include("include/seven_stream_metrics.hrl").

-define(METRIC_PREFIX, <<"seventh_state_stream_local_">>).
-define(REGISTRY_7S_STREAMS, '7s_streams').
-define(SCRAPE_DURATION, telemetry_scrape_duration_seconds).
-define(SCRAPE_SIZE, telemetry_scrape_size_bytes).

%% API
-export([register_collector/0, unregister_collector/0]).

%% Prometheus collector callbacks
-export([collect_mf/2, collect_metrics/2]).


% This is how current RabbitMQ places the family filter into the collector.
-spec get_requested_families() -> [atom()].
get_requested_families() ->
    case get(prometheus_mf_filter) of
        undefined -> [];
        Families when is_list(Families) -> Families;
        _ -> []
    end.

collect_mf(_Registry, Callback) ->
    FieldSamples = seven_stream_metrics_collector:collect(get_requested_families()),
    maps:foreach(
        fun(Field, FieldData) ->
            MetricName = metric_name(Field),
            MF = prometheus_model_helpers:create_mf(
                MetricName,
                metric_help(Field),
                gauge,
                ?MODULE,
                {Field, FieldData}
            ),
            Callback(MF)
        end,
        FieldSamples
    ).

collect_metrics(_MetricName, {_Field, Samples}) ->
    [prometheus_model_helpers:gauge_metric(
         labels_for_sample(Sample),
         maps:get(value, Sample)
     ) || Sample <- Samples].

labels_for_sample(#{consumer := Consumer, connection := Connection, pid := Pid, protocol := Protocol} = Sample) ->
    [
        {vhost, maps:get(vhost, Sample)},
        {stream, maps:get(stream, Sample)},
        {consumer, Consumer},
        {connection, Connection},
        {pid, Pid},
        {protocol, Protocol}
    ];
labels_for_sample(Sample) ->
    [
        {vhost, maps:get(vhost, Sample)},
        {stream, maps:get(stream, Sample)},
        {role, maps:get(role, Sample)}
    ].

metric_name(offset) -> <<"seventh_state_stream_local_offset">>;
metric_name(first_offset) -> <<"seventh_state_stream_local_first_offset">>;
metric_name(first_timestamp) -> <<"seventh_state_stream_local_first_timestamp">>;
metric_name(committed_offset) -> <<"seventh_state_stream_local_committed_offset">>;
metric_name(segments) -> <<"seventh_state_stream_local_segments">>;
metric_name(readers) -> <<"seventh_state_stream_local_readers">>;
metric_name(consumer_offset) -> <<"seventh_state_stream_local_consumer_offset">>;
metric_name(consumer_offset_lag) -> <<"seventh_state_stream_local_consumer_offset_lag">>;
metric_name(packets) -> <<"seventh_state_stream_local_packets">>;
metric_name(epoch) -> <<"seventh_state_stream_local_epoch">>;
metric_name(Field) when is_atom(Field) ->
    <<?METRIC_PREFIX/binary, (atom_to_binary(Field, utf8))/binary>>;
metric_name(_) ->
    <<"seventh_state_stream_local_unknown">>.

metric_help(offset) -> <<"Local stream counter field from osiris_counters:overview(): offset.">>;
metric_help(first_offset) -> <<"Local stream counter field from osiris_counters:overview(): first_offset.">>;
metric_help(first_timestamp) -> <<"Local stream counter field from osiris_counters:overview(): first_timestamp.">>;
metric_help(committed_offset) -> <<"Local stream counter field from osiris_counters:overview(): committed_offset.">>;
metric_help(segments) -> <<"Local stream counter field from osiris_counters:overview(): segments.">>;
metric_help(readers) -> <<"Local stream counter field from osiris_counters:overview(): readers.">>;
metric_help(consumer_offset) -> <<"Local stream counter field from osiris_counters:overview(): consumer_offset.">>;
metric_help(consumer_offset_lag) -> <<"Consumer offset lag from rabbit_stream_consumer_created ETS table.">>;
metric_help(packets) -> <<"Local stream counter field from osiris_counters:overview(): packets.">>;
metric_help(epoch) -> <<"Local stream counter field from osiris_counters:overview(): epoch.">>;
metric_help(Field) when is_atom(Field) ->
    <<"Local stream counter field from osiris_counters:overview(): ",
      (atom_to_binary(Field, utf8))/binary,
      ".">>;
metric_help(_) ->
    <<"Local stream counter field from osiris_counters:overview().">>.

register_collector() ->
    case ensure_registry(?REGISTRY_7S_STREAMS) of
        ok ->
            case setup_registry_metrics(?REGISTRY_7S_STREAMS) of
                ok ->
                    case safe_apply(
                           prometheus_registry,
                           register_collector,
                           [?REGISTRY_7S_STREAMS, ?MODULE]
                         ) of
                        ok ->
                            ?INF(
                              "Registered stream metrics collector in registry ~p.",
                              [?REGISTRY_7S_STREAMS]),
                            ok;
                        {error, already_exists} ->
                            ok;
                        {error, _} = Error ->
                            Error;
                        Other ->
                            {error,
                             {unexpected_result,
                              {prometheus_registry, register_collector, Other}}}
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

unregister_collector() ->
    case safe_apply(prometheus_registry, deregister_collector, [?REGISTRY_7S_STREAMS, ?MODULE]) of
        ok ->
            ok;
        {error, not_found} ->
            ok;
        {error, _} = Error ->
            Error;
        Other ->
            {error, {unexpected_result, {prometheus_registry, deregister_collector, Other}}}
    end.

setup_registry_metrics(Registry) ->
    ScrapeDuration = [
        {name, ?SCRAPE_DURATION},
        {help, "Scrape duration"},
        {labels, ["registry", "content_type"]},
        {registry, Registry}
    ],
    ScrapeSize = [
        {name, ?SCRAPE_SIZE},
        {help, "Scrape size, not encoded"},
        {labels, ["registry", "content_type"]},
        {registry, Registry}
    ],
    case declare_summary_metric(ScrapeDuration) of
        ok ->
            declare_summary_metric(ScrapeSize);
        {error, _} = Error ->
            Error
    end.

ensure_registry(Registry) ->
    case safe_apply(prometheus_registry, register_collectors, [Registry, []]) of
        ok ->
            ok;
        true ->
            ok;
        {error, already_exists} ->
            ok;
        {error, _} = Error ->
            Error;
        Other ->
            {error, {unexpected_result, {prometheus_registry, register_collectors, Other}}}
    end.

declare_summary_metric(Options) ->
    case safe_apply(prometheus_summary, declare, [Options]) of
        ok ->
            ok;
        true ->
            ok;
        false ->
            ok;
        {error, already_exists} ->
            ok;
        {error, _} = Error ->
            Error;
        Other ->
            {error, {unexpected_result, {prometheus_summary, declare, Other}}}
    end.

safe_apply(Module, Function, Args) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            try
                apply(Module, Function, Args)
            catch
                Class:Reason:Stacktrace ->
                    {error, {apply_failed, Module, Function, Class, Reason, Stacktrace}}
            end;
        {error, Reason} ->
            {error, {module_not_loaded, Module, Reason}}
    end.
