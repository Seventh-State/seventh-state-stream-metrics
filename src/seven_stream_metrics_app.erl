%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%%
%%% @end
%%% Created : 17 Jul 2025 by Seventh State <contact@seventhstate.io>
-module(seven_stream_metrics_app).

-behaviour(application).
-export([start/2, stop/1]).

-include("include/seven_stream_metrics.hrl").


start(_StartType, _StartArgs) ->
    case ensure_prometheus_plugin_enabled() of
        ok ->
            case seven_stream_metrics_prometheus:register_collector() of
                ok ->
                    case seven_stream_metrics_sup:start_link() of
                        {ok, Pid} ->
                            {ok, Pid};
                        Error ->
                            _ = seven_stream_metrics_prometheus:unregister_collector(),
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

stop(_State) ->
    _ = seven_stream_metrics_prometheus:unregister_collector(),
    ok.

ensure_prometheus_plugin_enabled() ->
    case rabbit_plugins:is_enabled(rabbitmq_prometheus) of
        true ->
            ok;
        false ->
            ?ERR("Required plugin rabbitmq_prometheus is not enabled.", []),
            {error, {missing_required_plugin, rabbitmq_prometheus}}
    end.
