-module(redis_ets_ffi).
-export([new_table/1]).

%% Create a new ETS table with options optimized for Redis use case
new_table(Name) ->
    ets:new(Name, [set, public, {read_concurrency, true}]).
