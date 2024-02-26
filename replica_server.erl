-module(replica_server).
-export([start/2, stop/1, putt/5, remm/4, gett/3, sizee/1, test/0]).
-export([init/1, handle_call/3]).

-record(state, {dict, replicas, actual_state}).

%%% Funciones publicas %%%
test() ->
    gen_server:start_link({local, server1}, ?MODULE, [server2, server3], []),
    gen_server:start_link({local, server2}, ?MODULE, [server1, server3], []),
    gen_server:start_link({local, server3}, ?MODULE, [server1, server2], []).

start(Name, Replicas) ->
    gen_server:start_link({local, Name}, ?MODULE, Replicas, []).

stop(Name) ->
    gen_server:call(Name, stop).

putt(Name, Key, Value, TimeStamp, Consistency) ->
    gen_server:call(Name, {putt, Key, Value, TimeStamp, Consistency}).

remm(Name, Key, TimeStamp, Consistency) ->
    gen_server:call(Name, {remm, Key, TimeStamp, Consistency}).

gett(Name, Key, Consistency) ->
    gen_server:call(Name, {gett, Key, Consistency}).

sizee(Name) ->
    gen_server:call(Name, sizee).

%%% Funciones privadas %%%
init(Replicas) ->
    {ok, #state{dict = dict:new(), replicas = Replicas, actual_state = "free"}}.

handle_call({putt, Key, Value, TimeStamp, Consistency}, _From, State) ->
    {_, NewState} = case Consistency of
        one ->
            do_putt_one(Key, Value, TimeStamp, State);
        quorum ->
            do_putt_quorum(Key, Value, TimeStamp, State);
        all ->
            do_putt_all(Key, Value, TimeStamp, State)
    end,
    {reply, ok, NewState};

handle_call({remm, Key, TimeStamp, Consistency}, _From, State) ->
    {_, NewState} = case Consistency of
        one ->
            do_remm_one(Key, TimeStamp, State);
        quorum ->
            do_remm_quorum(Key, TimeStamp, State);
        all ->
            do_remm_all(Key, TimeStamp, State)
    end,
    {reply, ok, NewState};

handle_call({gett, Key, Consistency}, _From, State) ->
    Val = do_gett(Key, Consistency, State),
    {reply, Val, State};

handle_call(sizee, _From, State) ->
    {reply, dict:size(State#state.dict), State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Msg , _From, _State ) ->
    {noreply, error}.
%%% Internal functions %%%
do_gett(Key, Consistency, State) ->
    case Consistency of
        one ->
            do_gett_one(Key, State);
        quorum ->
            do_gett_quorum(Key, State);
        all ->
            do_gett_all(Key, State)
    end.

do_putt_one(Key, Value, TimeStamp, State) ->
    Val = dict:find(Key, State#state.dict),
    case Val of
        error -> 
            NewDict = dict:store(Key, {Value, TimeStamp, true}, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        {_, {_Value1, _TimeStamp1, false}} ->
            NewDict = dict:update(Key, {Value, TimeStamp, true}, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        _ -> {error, State}
    end.

do_putt_quorum(Key, Value, TimeStamp, State) ->
    {Confirmed, _} = lists:foldl(
        fun(Replica, {Acc, Rejected}) ->
            case do_check_putt(Replica, Key, TimeStamp, State) of
                {accept, _} ->
                    {Acc + 1, Rejected};
                {reject, _} ->
                    {Acc, Rejected + 1}
            end
        end,
        {0, 0},
        State#state.replicas
    ),
    if
        Confirmed >= length(State#state.replicas) div 2 + 1 ->
            NewDict = dict:store(Key, {Value, TimeStamp, true}, State#state.dict),
            lists:map(
                fun(Replica) ->
                    gen_server:call(Replica, {putt, Key, Value, TimeStamp, one})
                end,
                State#state.replicas
            ),
            {noreply, State#state{dict=NewDict}};
        true ->
            {noreply, State}
    end.

do_putt_all(Key, Value, TimeStamp, State) ->
    {Confirmed, _Rejected} = lists:foldl(
        fun(Replica, {Acc, Rejected}) ->
            case do_check_putt(Replica, Key, TimeStamp, State) of
                {accept, _} ->
                    {Acc + 1, Rejected};
                {reject, _} ->
                    {Acc, Rejected + 1}
            end
        end,
        {0, 0},
        State#state.replicas
    ),
    if
        Confirmed == length(State#state.replicas) ->
            NewDict = dict:store(Key, {Value, TimeStamp, true}, State#state.dict),
            lists:map(
                fun(Replica) ->
                    gen_server:call(Replica, {putt, Key, Value, TimeStamp, one})
                end,
                State#state.replicas
            ),
            {noreply, State#state{dict=NewDict}};
        true ->
            {noreply, State}
    end.

do_remm_one(Key, TimeStamp, State) ->
    case dict:find(Key, State#state.dict) of
        {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
            % Si existe y está activa, actualiza el timestamp al actual y desactiva la tupla
            NewDict = dict:update(Key, fun({Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, false} end, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        {error, _} ->
            % Si no existe, crea una nueva tupla con valor null, el timestamp actual y desactivada
            NewDict = dict:store(Key, {null, TimeStamp, false}, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        _ ->
            % Si existe pero está desactivada, no se realiza ninguna acción
            {noreply, State}
    end.

do_remm_quorum(Key, TimeStamp, State) ->
    {Confirmed, _} = lists:foldl(
        fun(Replica, {Acc, Rejected}) ->
            case do_check_remm(Replica, Key, TimeStamp, State) of
                {accept, _} ->
                    {Acc + 1, Rejected};
                {reject, _} ->
                    {Acc, Rejected + 1}
            end
        end,
        {0, 0},
        State#state.replicas
    ),
    if
        Confirmed >= length(State#state.replicas) div 2 + 1 ->
            lists:map(
                fun(Replica) ->
                    gen_server:call(Replica, {remm, Key, TimeStamp, one})
                end,
                State#state.replicas
            ),
            case dict:find(Key, State#state.dict) of
                {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
                    % Si existe y está activa, actualiza el timestamp al actual y desactiva la tupla
                    NewDict = dict:update(Key, fun({Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, false} end, State#state.dict),
                    {noreply, State#state{dict=NewDict}};
                {error, _} ->
                    % Si no existe, crea una nueva tupla con valor null, el timestamp actual y desactivada
                    NewDict = dict:store(Key, {null, TimeStamp, false}, State#state.dict),
                    {noreply, State#state{dict=NewDict}};
                _ ->
                    % Si existe pero está desactivada, no se realiza ninguna acción
                    {noreply, State}
            end;
        true ->
            {noreply, State}
    end.

do_remm_all(Key, TimeStamp, State) ->
    {Confirmed, _Rejected} = lists:foldl(
        fun(Replica, {Acc, Rejected}) ->
            case do_check_remm(Replica, Key, TimeStamp, State) of
                {accept, _} ->
                    {Acc + 1, Rejected};
                {reject, _} ->
                    {Acc, Rejected + 1}
            end
        end,
        {0, 0},
        State#state.replicas
    ),
    if
        Confirmed == length(State#state.replicas) ->
            lists:map(
                fun(Replica) ->
                    gen_server:call(Replica, {remm, Key, TimeStamp, one})
                end,
                State#state.replicas
            ),
            case dict:find(Key, State#state.dict) of
                {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
                    % Si existe y está activa, actualiza el timestamp al actual y desactiva la tupla
                    NewDict = dict:update(Key, fun({_Value, _ExistingTimeStamp, _IsActive}) -> {null, TimeStamp, false} end, State#state.dict),
                    {noreply, State#state{dict=NewDict}};
                {error, _} ->
                    % Si no existe, crea una nueva tupla con valor null, el timestamp actual y desactivada
                    NewDict = dict:store(Key, {null, TimeStamp, false}, State#state.dict),
                    {noreply, State#state{dict=NewDict}};
                _ ->
                    % Si existe pero está desactivada, no se realiza ninguna acción
                    {noreply, State}
            end;
        true ->
            {noreply, State}
    end.

do_gett_one(Key, State) ->
    Val = dict:find(Key, State#state.dict),
    case Val of
        {_, {Value1, TimeStamp1, true}} -> {Value1, TimeStamp1};
        error -> notfound;
        _ -> notfound
    end.

do_gett_quorum(Key, State) ->
    Replies = lists:map(
        fun(Replica) ->
            case do_gett_one_replica(Replica, Key, State) of
                notfound -> notfound;
               {Value1, TimeStamp1} -> {Value1, TimeStamp1}
            end
        end,
        State#state.replicas
    ),
    RepliesFiltered = lists:filter(
        fun({_Value, _TimeStamp}) -> true;
           (_) -> false
        end,
        Replies
    ),
    case RepliesFiltered of
        [] -> empty;
        _ -> lists:max(RepliesFiltered)
    end.

do_gett_all(Key, State) ->
    Replies = lists:map(
        fun(Replica) ->
            do_gett_one_replica(Replica, Key, State)
        end,
        State#state.replicas
    ),
    case lists:any(fun(X) -> X == notfound end, Replies) of
        true -> notfound;
        false ->
            RepliesFiltered = lists:filter(
                fun({_Value, _TimeStamp}) -> true;
                   (_) -> false
                end,
                Replies
            ),
            case RepliesFiltered of
                [] -> empty;
                _ -> lists:max(RepliesFiltered)
            end
    end.

do_gett_one_replica(Replica, Key, State) ->
    case State#state.actual_state of
        "free" ->
            case gen_server:call(Replica, {gett, Key, one}) of
                {Value1, Reply} -> {Value1, Reply};
                _ -> notfound
            end;
        "occupied" ->
            notfound
    end.

do_check_putt(Replica, Key, TimeStamp, State) ->
    case State#state.actual_state of
        "free" ->
            case dict:find(Key, State#state.dict) of
                {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
                    {reject, Replica};
                _ ->
                    {accept, Replica}
            end;
        "occupied" ->
            {reject, Replica}
    end.

do_check_remm(Replica, Key, TimeStamp, State) ->
    case State#state.actual_state of
        "free" ->
                case dict:find(Key, State#state.dict) of
                    {ok, {_, ExistingTimeStamp, true}} when ExistingTimeStamp =< TimeStamp ->
                        % Si existe y está activa, actualiza el timestamp al actual y desactiva la tupla
                        {accept, Replica};
                    {error, _} ->
                        % Si no existe, crea una nueva tupla con valor null, el timestamp actual y desactivada
                        {accept, Replica};
                    _ ->
                        % Si existe pero está desactivada, no se realiza ninguna acción
                        {reject, Replica}
                end;
        "occupied" ->
            {reject, Replica}
    end.
