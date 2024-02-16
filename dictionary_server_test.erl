-module(dictionary_server_test).
-include_lib("eunit/include/eunit.hrl").

-export([run_tests/0, start_stop_test/0, put_get_test/0, rem_test/0, size_test/0]).

start_stop_test() ->
    {ok, Pid} = dictionary_server:start(test_server),
    ?assertNotEqual(undefined, whereis(test_server)),
    dictionary_server:stop(),
    ?assertEqual(undefined, whereis(test_server)).

put_get_test() ->
    {ok, Pid} = dictionary_server:start(test_server),
    dictionary_server:put(a, value1, 1),
    dictionary_server:put(b, value2, 2),
    dictionary_server:put(c, value3, 3),
    {ok, Value1, Timestamp1} = dictionary_server:get(a),
    ?assertEqual(value1, Value1),
    ?assertEqual(1, Timestamp1),
    {ok, Value2, Timestamp2} = dictionary_server:get(b),
    ?assertEqual(value2, Value2),
    ?assertEqual(2, Timestamp2),
    {ok, Value3, Timestamp3} = dictionary_server:get(c),
    ?assertEqual(value3, Value3),
    ?assertEqual(3, Timestamp3),
    dictionary_server:stop().

rem_test() ->
    {ok, Pid} = dictionary_server:start(test_server),
    dictionary_server:put(a, value1, 1),
    dictionary_server:put(b, value2, 2),
    dictionary_server:put(c, value3, 3),
    dictionary_server:remm(a, 0),
    {notfound, _} = dictionary_server:get(a),
    dictionary_server:remm(b, 3),
    {ok, _, _} = dictionary_server:get(b),
    dictionary_server:remm(c, 5),
    {notfound, _} = dictionary_server:get(c),
    dictionary_server:stop().

size_test() ->
    {ok, Pid} = dictionary_server:start(test_server),
    ?assertEqual(0, dictionary_server:size()),
    dictionary_server:put(a, value1, 1),
    ?assertEqual(1, dictionary_server:size()),
    dictionary_server:put(b, value2, 2),
    ?assertEqual(2, dictionary_server:size()),
    dictionary_server:put(c, value3, 3),
    ?assertEqual(3, dictionary_server:size()),
    dictionary_server:remm(b, 3),
    ?assertEqual(2, dictionary_server:size()),
    dictionary_server:stop().

run_tests() ->
    eunit:test([start_stop_test, put_get_test, rem_test, size_test]).