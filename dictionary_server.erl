-module(dictionary_server).
-behaviour(gen_server).

-export([start/1, stop/0, put/3, remm/2, get/1, size/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {dictionary = dict:new()}).

start(Name) ->
    gen_server:start({local, Name}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

put(Key, Value, Timestamp) ->
    gen_server:cast(?MODULE, {put, Key, Value, Timestamp}).

remm(Key, Timestamp) ->
    gen_server:call(?MODULE, {remm, Key, Timestamp}).

get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

size() ->
    gen_server:call(?MODULE, size).

init([]) ->
    {ok, #state{}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call({put, Key, Value, Timestamp}, _From, State) ->
    NewState = put_dict(Key, Value, Timestamp, State),
    {reply, ok, NewState};

handle_call({remm, Key, Timestamp}, _From, State) ->
    NewState = rem_dict(Key, Timestamp, State),
    {reply, ok, NewState};

handle_call({get, Key}, _From, State) ->
    Reply = get_dict(Key, State),
    {reply, Reply, State};

handle_call(size, _From, State) ->
    Size = dict:size(State#state.dictionary),
    {reply, Size, State}.

handle_cast(stop, State) ->
    {stop, normal, ok, State}.

handle_msg(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

put_dict(Key, Value, Timestamp, State) ->
    OldValue = dict:find(Key, State#state.dictionary),
    case OldValue of
        error -> NewDictionary = dict:store(Key, {Value, Timestamp}, State#state.dictionary);
        {OldVal, OldTimestamp} when OldTimestamp < Timestamp ->
            NewDictionary = dict:store(Key, {Value, Timestamp}, State#state.dictionary);
        _ -> NewDictionary = State#state.dictionary
    end,
    State#state{dictionary = NewDictionary}.

rem_dict(Key, Timestamp, State) ->
    OldValue = dict:find(Key, State#state.dictionary),
    case OldValue of
        {OldVal, OldTimestamp} when OldTimestamp < Timestamp ->
            NewDictionary = dict:erase(Key, State#state.dictionary);
        _ -> NewDictionary = State#state.dictionary
    end,
    State#state{dictionary = NewDictionary}.

get_dict(Key, State) ->
    case dict:find(Key, State#state.dictionary) of
        {Value, Timestamp} -> {ok, Value, Timestamp};
        error -> {notfound, undefined}
    end.