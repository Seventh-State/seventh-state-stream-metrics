%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
%%% @doc 
%%%
%%% @end
%%% Created : 17 Jul 2025 by Seventh State <contact@seventhstate.io>
-module(seven_stream_metrics_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupervisorSpecification = #{
        strategy => one_for_one, % one_for_one | one_for_all | rest_for_one | simple_one_for_one
        intensity => 10,
        period => 60},

    ChildSpecifications = [
        #{
            id => seven_stream_metrics,
            start => {seven_stream_metrics, start_link, []},
            restart => permanent, % permanent | transient | temporary
            shutdown => 2000, % use 'infinity' for supervisor child
            type => worker, % worker | supervisor
            modules => [seven_stream_metrics]
        }
    ],

    {ok, {SupervisorSpecification, ChildSpecifications}}.
