%%% @author Seventh State <contact@seventhstate.io>
%%% @copyright (C) 2025, Seventh State
-module(seven_stream_metrics_collector).

-define(EXCLUDED_COUNTER_FIELDS, [forced_gcs]).
-define(STREAM_METRICS_FIELDS,
        [offset, first_offset, first_timestamp, committed_offset, segments, readers]).
-define(CONSUMERS_FIELDS, [consumer_offset]).
-define(STREAM_MISC_FIELDS, [packets, epoch]).

-type osiris_resource() :: {resource, binary(), queue, binary()}.
-type osiris_role_key() :: {osiris_writer | osiris_replica, osiris_resource()}.
-type osiris_reader_key() :: {rabbit_stream_reader, osiris_resource(), non_neg_integer(), pid()}.
-type osiris_overview_key() :: osiris_role_key() | osiris_reader_key().
-type osiris_counter_key() ::
    offset
    | chunks
    | epoch
    | committed_offset
    | readers
    | first_offset
    | first_timestamp
    | segments
    | packets
    | forced_gcs.
-type osiris_counters() :: #{osiris_counter_key() => integer()}.
-type osiris_overview() :: #{osiris_overview_key() => osiris_counters()}.

-export([
    collect/1,
    collect/2
]).

collect(Families) when is_list(Families) ->
    collect(Families, fun get_osiris_overview/0).

collect(Families, DiscoveryFun)
  when is_list(Families), is_function(DiscoveryFun, 0) ->
    ConsumerMap = lookup_context(Families),
    Overview = DiscoveryFun(),
    FilteredOverview = filter_overview_for_families(Overview, Families),
    Entries = entries_from_overview(FilteredOverview, ConsumerMap),
    build_metrics(Entries, Families).

-spec get_osiris_overview() -> osiris_overview().
get_osiris_overview() ->
    case osiris_counters:overview() of
        Overview when is_map(Overview) -> Overview;
        _ -> #{}
    end.

entries_from_overview(Overview, ConsumerMap) when is_map(Overview), is_map(ConsumerMap) ->
    maps:fold(fun(Key, Counters, Acc) ->
                        overview_item_to_entry(Key, Counters, Acc, ConsumerMap)
               end, [], Overview).
overview_item_to_entry({RoleTag, Resource}, Counters, Acc, _ConsumerMap) when
    (RoleTag =:= osiris_writer orelse RoleTag =:= osiris_replica),
    is_map(Counters) ->
    case {role_label(RoleTag), parse_resource(Resource)} of
        {RoleLabel, {ok, VHost, Stream}} when is_binary(RoleLabel) ->
            [#{
                vhost => VHost,
                stream => Stream,
                role => RoleLabel,
                counters => Counters
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
                pid => pid_to_binary(Pid),
                counters => #{consumer_offset => Offset}
            } | Acc];
        _ ->
            Acc
    end;
% we don't want to display:
% - osiris_replica_reader, it is an internal reader used for replication
overview_item_to_entry(_Key, _Counters, Acc, _ConsumerMap) ->
    Acc.

parse_resource({resource, VHost, queue, Stream}) ->
    {ok, VHost, Stream};
parse_resource(_) ->
    error.

remove_excluded_counters(Counters) ->
    maps:without(?EXCLUDED_COUNTER_FIELDS, Counters).

build_metrics(RawEntries, Families) ->
    FieldSamples = lists:foldl(fun entry_to_field_samples/2, #{}, RawEntries),
    filter_field_samples_for_families(FieldSamples, Families).
entry_to_field_samples(
    #{vhost := VHost, stream := Stream, role := RoleLabel, counters := Counters},
    Acc0
) when is_map(Counters) ->
    FilteredCounters = remove_excluded_counters(Counters),
    BaseSample = #{
        vhost => VHost,
        stream => Stream,
        role => RoleLabel
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
        FilteredCounters
    );
entry_to_field_samples(
    #{
        vhost := VHost,
        stream := Stream,
        consumer := Consumer,
        connection := Connection,
        pid := Pid,
        counters := Counters
     },
    Acc0
) when is_map(Counters) ->
    FilteredCounters = remove_excluded_counters(Counters),
    BaseSample = #{
        vhost => VHost,
        stream => Stream,
        consumer => Consumer,
        connection => Connection,
        pid => Pid
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
        FilteredCounters
    );
entry_to_field_samples(_Entry, Acc) ->
    Acc.

filter_overview_for_families(Overview, Families) when is_map(Overview), is_list(Families) ->
    AllowedTags = allowed_source_tags(Families),
    maps:filter(
      fun(Key, _Value) ->
              maps:is_key(source_tag_from_key(Key), AllowedTags)
      end,
      Overview);
filter_overview_for_families(_Overview, _Families) ->
    #{}.

allowed_source_tags(Families) ->
    Tags = lists:flatten([source_tags_for_family(Family) || Family <- Families]),
    maps:from_keys(Tags, true).

source_tags_for_family(stream_metrics) ->
    [osiris_writer, osiris_replica];
source_tags_for_family(stream_misc) ->
    [osiris_writer, osiris_replica];
source_tags_for_family(consumers) ->
    [rabbit_stream_reader];
source_tags_for_family(_) ->
    [].

filter_field_samples_for_families(FieldSamples, Families)
  when is_map(FieldSamples), is_list(Families) ->
    AllowedFields = allowed_fields_for_families(Families),
    maps:with(AllowedFields, FieldSamples);
filter_field_samples_for_families(_FieldSamples, _Families) ->
    #{}.

allowed_fields_for_families(Families) ->
    lists:flatten([family_fields(Family) || Family <- Families]).

family_fields(stream_misc) ->
    ?STREAM_MISC_FIELDS;
family_fields(consumers) ->
    ?CONSUMERS_FIELDS;
family_fields(stream_metrics) ->
    ?STREAM_METRICS_FIELDS;
family_fields(_) ->
    [].

source_tag_from_key({Tag, _}) when is_atom(Tag) ->
    Tag;
source_tag_from_key({Tag, _, _, _}) when is_atom(Tag) ->
    Tag;
source_tag_from_key(_) ->
    undefined.

role_label(osiris_writer) ->
    <<"writer">>;
role_label(osiris_replica) ->
    <<"replica">>;
role_label(_) ->
    error.

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
