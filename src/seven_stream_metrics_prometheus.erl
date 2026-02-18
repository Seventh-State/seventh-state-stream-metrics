%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
-module(seven_stream_metrics_prometheus).

-include("include/seven_hello_plugin.hrl").

-define(METRIC_PREFIX, "rabbitmq_stream_local_").
-define(REGISTRY_7S_STREAMS, '7s_streams').
-define(SCRAPE_DURATION, telemetry_scrape_duration_seconds).
-define(SCRAPE_SIZE, telemetry_scrape_size_bytes).

%% API
-export([register_collector/0, unregister_collector/0]).

%% Prometheus collector callbacks
-export([collect_mf/2, collect_metrics/2]).

collect_mf(_Registry, Callback) ->
    Samples = seven_stream_metrics_collector:collect(),
    FieldSamples = maps:get(field_samples, Samples, #{}),
    lists:foreach(
        fun({Field, FieldData}) ->
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
        maps:to_list(FieldSamples)
    ).

collect_metrics(_MetricName, {_Field, Samples}) ->
    [prometheus_model_helpers:gauge_metric(
         [
             {vhost, maps:get(vhost, Sample)},
             {stream, maps:get(stream, Sample)},
             {node_role, maps:get(node_role, Sample)}
         ],
         maps:get(value, Sample)
     ) || Sample <- Samples].

metric_name(Field) ->
    list_to_atom(?METRIC_PREFIX ++ atom_to_list(Field)).

metric_help(Field) ->
    iolist_to_binary([
        "Local stream counter field from osiris_counters:overview(): ",
        atom_to_list(Field),
        "."
    ]).

register_collector() ->
    _ = safe_apply(prometheus_registry, register_collectors, [?REGISTRY_7S_STREAMS, []]),
    _ = setup_registry_metrics(?REGISTRY_7S_STREAMS),
    case safe_apply(prometheus_registry, register_collector, [?REGISTRY_7S_STREAMS, ?MODULE]) of
        ok ->
            ?INF("Registered stream metrics collector in registry ~p.", [?REGISTRY_7S_STREAMS]),
            ok;
        {error, already_exists} ->
            ok;
        _ ->
            case safe_apply(prometheus_registry, register_collector, [?MODULE]) of
                ok -> ok;
                {error, already_exists} -> ok;
                Error -> Error
            end
    end.

unregister_collector() ->
    _ = safe_apply(prometheus_registry, deregister_collector, [?REGISTRY_7S_STREAMS, ?MODULE]),
    _ = safe_apply(prometheus_registry, deregister_collector, [?MODULE]),
    ok.

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
    _ = safe_apply(prometheus_summary, declare, [ScrapeDuration]),
    _ = safe_apply(prometheus_summary, declare, [ScrapeSize]),
    ok.

safe_apply(Module, Function, Args) ->
    _ = code:ensure_loaded(Module),
    try
        apply(Module, Function, Args)
    catch
        error:undef -> error;
        _:_ -> error
    end.
