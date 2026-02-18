%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
-module(seven_stream_metrics_collector).

-export([
    collect/0,
    collect/1,
    discover_local_streams/0,
    entries_from_overview/1,
    build_metrics/1
]).

collect() ->
    collect(fun discover_local_streams/0).

collect(DiscoveryFun) when is_function(DiscoveryFun, 0) ->
    Raw = safe_call(DiscoveryFun),
    build_metrics(Raw).

discover_local_streams() ->
    entries_from_overview(safe_apply(osiris_counters, overview, [])).

entries_from_overview(Overview) when is_map(Overview) ->
    lists:reverse(
        maps:fold(fun overview_item_to_entry/3, [], Overview)
    );
entries_from_overview(_) ->
    [].

overview_item_to_entry({RoleTag, Resource}, Counters, Acc) when is_map(Counters) ->
    case {normalize_overview_role(RoleTag), parse_resource(Resource)} of
        {{ok, Role}, {ok, VHost, Stream}} ->
            [#{
                vhost => VHost,
                stream => Stream,
                role => Role,
                counters => filter_counters_for_role(Role, Counters)
            } | Acc];
        _ ->
            Acc
    end;
overview_item_to_entry(_Key, _Counters, Acc) ->
    Acc.

normalize_overview_role(osiris_writer) ->
    {ok, leader};
normalize_overview_role(osiris_replica) ->
    {ok, replica};
normalize_overview_role(_) ->
    error.

parse_resource({resource, VHost, queue, Stream}) ->
    {ok, VHost, Stream};
parse_resource(#{virtual_host := VHost, kind := queue, name := Stream}) ->
    {ok, VHost, Stream};
parse_resource(_) ->
    error.

filter_counters_for_role(leader, Counters) ->
    Counters;
filter_counters_for_role(replica, Counters) ->
    maps:remove(readers, Counters);
filter_counters_for_role(_, Counters) ->
    Counters.

build_metrics(RawEntries) ->
    FieldSamples = lists:foldl(fun entry_to_field_samples/2, #{}, RawEntries),
    #{
        field_samples => finalize_field_samples(FieldSamples)
    }.
entry_to_field_samples(
    #{vhost := VHost, stream := Stream, role := Role, counters := Counters},
    Acc0
) when is_map(Counters) ->
    BaseSample = #{
        vhost => VHost,
        stream => Stream,
        node_role => role_label(Role)
    },
    maps:fold(
        fun(Field, Value, Acc) when is_atom(Field), is_integer(Value) ->
            Sample = BaseSample#{value => Value},
            Existing = maps:get(Field, Acc, []),
            Acc#{Field => [Sample | Existing]};
           (_Field, _Value, Acc) ->
            Acc
        end,
        Acc0,
        Counters
    );
entry_to_field_samples(_Entry, Acc) ->
    Acc.

finalize_field_samples(FieldSamples) ->
    maps:map(fun(_Field, Samples) -> lists:reverse(Samples) end, FieldSamples).

role_label(leader) ->
    <<"leader">>;
role_label(replica) ->
    <<"replica">>;
role_label(Other) ->
    iolist_to_binary(io_lib:format("~p", [Other])).

safe_call(Fun) ->
    try
        Fun()
    catch
        _:_ -> []
    end.

safe_apply(Module, Function, Args) ->
    try
        apply(Module, Function, Args)
    catch
        _:_ -> []
    end.
