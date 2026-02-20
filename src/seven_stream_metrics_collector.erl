%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
-module(seven_stream_metrics_collector).

-export([
    collect/0,
    collect/1,
    collect/2,
    discover_local_streams/0,
    discover_local_streams/1,
    discover_local_streams/2,
    entries_from_overview/1,
    entries_from_overview/2,
    build_metrics/1
]).

collect() ->
    collect([], fun discover_local_streams/0).

collect(Families) when is_list(Families) ->
    ConsumerMap = lookup_context(Families),
    collect(Families, fun() -> discover_local_streams(Families, ConsumerMap) end);

collect(DiscoveryFun) when is_function(DiscoveryFun, 0) ->
    collect([], DiscoveryFun).

collect(Families, DiscoveryFun)
  when is_list(Families), is_function(DiscoveryFun, 0) ->
    Raw = safe_call(DiscoveryFun),
    build_metrics(raw_entries_for_families(Raw, Families)).

discover_local_streams() ->
    discover_local_streams([]).

discover_local_streams(Families) when is_list(Families) ->
    discover_local_streams(Families, #{}).

discover_local_streams(Families, ConsumerMap) when is_list(Families), is_map(ConsumerMap) ->
    Overview = safe_apply(osiris_counters, overview, []),
    FilteredOverview = filter_overview_for_families(Overview, Families),
    entries_from_overview(FilteredOverview, ConsumerMap).

entries_from_overview(Overview) ->
    entries_from_overview(Overview, #{}).

entries_from_overview(Overview, ConsumerMap) when is_map(Overview), is_map(ConsumerMap) ->
    lists:reverse(
        maps:fold(fun(Key, Counters, Acc) ->
                          overview_item_to_entry(Key, Counters, Acc, ConsumerMap)
                  end, [], Overview)
    );
entries_from_overview(_, _) ->
    [].

overview_item_to_entry({RoleTag, Resource}, Counters, Acc, _ConsumerMap) when is_map(Counters) ->
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
overview_item_to_entry(
    {rabbit_stream_reader, Resource, _SubscriptionId, Pid},
    Counters,
    Acc,
    ConsumerMap
) when is_map(Counters) ->
    case {parse_resource(Resource),
          maps:get(offset, Counters, undefined),
          maps:get({Resource, Pid}, ConsumerMap, not_found)} of
        {{ok, VHost, Stream}, Offset, Consumer} when is_integer(Offset), Offset >= 0, Consumer =/= not_found ->
            [#{
                vhost => VHost,
                stream => Stream,
                consumer => Consumer,
                connection => connection_label(Pid),
                counters => #{consumer_offset => Offset}
            } | Acc];
        _ ->
            logger:error(
                "Unexpected reader entry in osiris_counters:overview(): ~p with counters ~p",
                [{rabbit_stream_reader, Resource, _SubscriptionId, Pid}, Counters]
            ),
            Acc
    end;
overview_item_to_entry(_Key, _Counters, Acc, _ConsumerMap) ->
    Acc.

normalize_overview_role(osiris_writer) ->
    {ok, writer};
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

filter_counters_for_role(writer, Counters) ->
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
        role => role_label(Role)
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
entry_to_field_samples(
    #{
        vhost := VHost,
        stream := Stream,
        consumer := Consumer,
        connection := Connection,
        counters := Counters
     },
    Acc0
) when is_map(Counters) ->
    BaseSample = #{
        vhost => VHost,
        stream => Stream,
        consumer => Consumer,
        connection => Connection
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

raw_entries_for_families(RawEntries, _Families) when is_list(RawEntries) ->
    RawEntries;
raw_entries_for_families(Overview, Families) when is_map(Overview) ->
    entries_from_overview(filter_overview_for_families(Overview, Families));
raw_entries_for_families(_, _Families) ->
    [].

filter_overview_for_families(Overview, []) when is_map(Overview) ->
    Overview;
filter_overview_for_families(Overview, Families) when is_map(Overview), is_list(Families) ->
    AllowedTags = allowed_source_tags(Families),
    maps:filter(
      fun(Key, _Value) ->
              lists:member(source_tag_from_key(Key), AllowedTags)
      end,
      Overview);
filter_overview_for_families(_Overview, _Families) ->
    #{}.

allowed_source_tags(Families) ->
    lists:usort(
      lists:flatten([source_tags_for_family(Family) || Family <- Families])).

source_tags_for_family(stream_metrics) ->
    [osiris_writer, osiris_replica];
source_tags_for_family(consumers) ->
    [rabbit_stream_reader];
source_tags_for_family(_) ->
    [].

source_tag_from_key({Tag, _}) when is_atom(Tag) ->
    Tag;
source_tag_from_key({Tag, _, _, _}) when is_atom(Tag) ->
    Tag;
source_tag_from_key(_) ->
    undefined.

role_label(writer) ->
    <<"writer">>;
role_label(replica) ->
    <<"replica">>;
role_label(Other) ->
    iolist_to_binary(io_lib:format("~p", [Other])).

lookup_context(Families) ->
    case lists:member(consumers, Families) of
        true ->
            build_consumer_map();
        false ->
            #{}
    end.

build_consumer_map() ->
    try
        Entries = ets:tab2list(consumer_created),
        lists:foldl(
          fun({{{resource, VHost, queue, Stream}, Pid, ConsumerTag}, _F1,_F2,_F3,_F4,_F5, Args}, Acc) ->
                  ConsumerName = consumer_name(ConsumerTag, Args),
                  maps:put({{resource, VHost, queue, Stream}, Pid}, ConsumerName, Acc)
          end,
          #{},
          Entries)
    catch
        error:badarg ->
            #{}
    end.

connection_name_from_props(Props) when is_list(Props) ->
    case proplists:get_value(user_provided_name, Props, undefined) of
        Name when is_binary(Name), Name =/= <<>> ->
            {ok, Name};
        _ ->
            case proplists:get_value(name, Props, undefined) of
                Name when is_binary(Name), Name =/= <<>> ->
                    {ok, Name};
                _ ->
                    error
            end
    end;
connection_name_from_props(_) ->
    error.

connection_label(Pid) ->
    case connection_name_for_pid(Pid) of
        {ok, Name} ->
            truncate_binary(Name, 100);
        error ->
            pid_to_binary(Pid)
    end.

connection_name_for_pid(Pid) ->
    try
        case ets:lookup(connection_created, Pid) of
            [{Pid, Props}] ->
                connection_name_from_props(Props);
            _ ->
                error
        end
    catch
        error:badarg ->
            error
    end.

consumer_name(ConsumerTag, Args) ->
    case consumer_name_from_args(Args) of
        {ok, Name} -> Name;
        error -> ConsumerTag
    end.

consumer_name_from_args(Args) when is_list(Args) ->
    case lists:keyfind(<<"name">>, 1, Args) of
        {<<"name">>, _Type, Name} when is_binary(Name), Name =/= <<>> ->
            {ok, Name};
        {<<"name">>, Name} when is_binary(Name), Name =/= <<>> ->
            {ok, Name};
        _ ->
            error
    end;
consumer_name_from_args(Args) when is_map(Args) ->
    case maps:get(<<"name">>, Args, undefined) of
        Name when is_binary(Name), Name =/= <<>> ->
            {ok, Name};
        _ ->
            error
    end;
consumer_name_from_args(_) ->
    error.

truncate_binary(Bin, MaxBytes) when is_binary(Bin), byte_size(Bin) =< MaxBytes ->
    Bin;
truncate_binary(Bin, MaxBytes) when is_binary(Bin), MaxBytes >= 0 ->
    binary:part(Bin, 0, MaxBytes);
truncate_binary(Other, MaxBytes) ->
    truncate_binary(iolist_to_binary(io_lib:format("~p", [Other])), MaxBytes).

pid_to_binary(Pid) when is_pid(Pid) ->
    list_to_binary(erlang:pid_to_list(Pid));
pid_to_binary(Bin) when is_binary(Bin) ->
    Bin;
pid_to_binary(Other) ->
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
