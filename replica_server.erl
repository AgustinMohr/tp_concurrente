-module(replica_server).
-behaviour(gen_server).
-export([start/2, stop/1, putt/5, remm/4, gett/3, sizee/1, test/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {dict, replicas, pending_puts_all, pending_puts_quorum, pending_rems_all, pending_rems_quorum, pending_gets_all, pending_gets_quorum}).

%%% Public API %%%
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

%%% GenServer callbacks %%%
init(Replicas) ->
    {ok, #state{dict = dict:store("1", {10, os:timestamp(), true}, dict:new()), replicas = Replicas, 
    pending_puts_all = [], 
    pending_puts_quorum = [], 
    pending_rems_all = [], 
    pending_rems_quorum = [], 
    pending_gets_all = [], 
    pending_gets_quorum = []}}.

handle_call({putt, Key, Value, TimeStamp, Consistency}, _From, State) ->
    NewState = do_putt(Key, Value, TimeStamp, Consistency, State),
    {reply, ok, NewState};

handle_call({remm, Key, TimeStamp, Consistency}, _From, State) ->
    NewState = do_remm(Key, TimeStamp, Consistency, State),
    {reply, ok, NewState};

handle_call({gett, Key, Consistency}, _From, State) ->
    {reply, do_gett(Key, Consistency, State), State};

handle_call(sizee, _From, State) ->
    {reply, dict:size(State#state.dict), State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({store_tuple, Key, Value, TimeStamp}, State) ->
    % Almacena la tupla en el diccionario
    case dict:find(Key, State#state.dict) of
        {ok, {_, ExistingTimeStamp, false}} when ExistingTimeStamp =< TimeStamp ->
            % Si existe y está activa, actualiza el timestamp al actual y desactiva la tupla
            NewDict = dict:update(Key, fun({_Value, _ExistingTimeStamp, _IsActive}) -> {Value, TimeStamp, true} end, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        {error, _} ->
            % Si no existe, crea una nueva tupla con valor null, el timestamp actual y desactivada
            NewDict = dict:store(Key, {Value, TimeStamp, true}, State#state.dict),
            {noreply, State#state{dict=NewDict}};
        _ ->
            % Si existe pero está desactivada, no se realiza ninguna acción
            {noreply, State}
    end;

handle_cast({remove_tuple, Key, TimeStamp}, State) ->
    % Verifica si la tupla existe y está activa
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
    end.

handle_info({putt_checks_completed_all, CanStore}, State) ->
    lists:foreach(fun({Pid, Key, Value, TimeStamp}) ->
                Pid ! {store_tuple, Key, Value, TimeStamp}
            end, State#state.pending_puts_all),
    {noreply, State#state{pending_puts_all=[]}};

handle_info({putt_checks_completed_quorum, CanStore}, State) ->
    % Si al menos la mitad de las réplicas pueden almacenar la tupla, se procede a almacenarla
    Quorum = length(State#state.replicas) div 2 + 1,
    ConfirmationCount = lists:count(fun({_Pid, true}) -> true; (_) -> false end, CanStore),
    if
        ConfirmationCount >= Quorum ->
            lists:foreach(fun({Pid, Key, Value, TimeStamp}) ->
                Pid ! {store_tuple, Key, Value, TimeStamp}
            end, State#state.pending_puts_quorum),
            {noreply, State#state{pending_puts_quorum=[]}};
        true ->
            {noreply, State#state{pending_puts_quorum=[]}}
    end;

handle_info({remm_checks_completed_all, CanRemove}, State) ->
    lists:foreach(fun({Pid, Key, TimeStamp}) ->
                Pid ! {remove_tuple, Key, TimeStamp}
            end, State#state.pending_rems_all),
    {noreply, State#state{pending_rems_all=[]}};

handle_info({remm_checks_completed_quorum, CanRemove}, State) ->
    % Si al menos la mitad de las réplicas pueden eliminar la tupla, se procede a eliminarla
    Quorum = length(State#state.replicas) div 2 + 1,
    ConfirmationCount = lists:count(fun({_Pid, true}) -> true; (_) -> false end, CanRemove),
    if
        ConfirmationCount >= Quorum ->
            lists:foreach(fun({Pid, Key, TimeStamp}) ->
                Pid ! {remove_tuple, Key, TimeStamp}
            end, State#state.pending_rems_quorum),
            {noreply, State#state{pending_rems_quorum=[]}};
        true ->
            {noreply, State#state{pending_rems_quorum=[]}}
    end;

handle_info({gett_checks_completed_all, Results}, State) ->
    Values = [Value || {_, Value} <- Results],
    case lists:member(not_found, Values) of
        true ->
            % Si al menos una réplica no pudo encontrar la tupla, se responde con "not_found"
            lists:foreach(fun({Pid, _Key}) -> Pid ! {reply, not_found} end, State#state.pending_gets_all);
        false ->
            % Si todas las réplicas encontraron la tupla, se responde con el valor de la tupla
            lists:foreach(fun({Pid, Key}) -> Pid ! {reply, {ok, dict:fetch(Key, State#state.dict)}} end, State#state.pending_gets_all)
    end,
    {noreply, State#state{pending_gets_all=[]}};

handle_info({gett_checks_completed_quorum, Results}, State) ->
    Values = [Value || {_, Value} <- Results],
    case lists:member(not_found, Values) of
        true ->
            % Si al menos una réplica no pudo encontrar la tupla, se responde con "not_found"
            lists:foreach(fun({Pid, _Key}) -> Pid ! {reply, not_found} end, State#state.pending_gets_quorum);
        false ->
            % Si al menos la mitad de las réplicas encontraron la tupla, se responde con el valor de la tupla
            Quorum = length(State#state.replicas) div 2 + 1,
            ReplyCount = length(Values),
            case ReplyCount >= Quorum of
                true ->
                    lists:foreach(fun({Pid, Key}) -> Pid ! {reply, {ok, dict:fetch(Key, State#state.dict)}} end, State#state.pending_gets_quorum);
                false ->
                    lists:foreach(fun({Pid, _Key}) -> Pid ! {reply, not_found} end, State#state.pending_gets_quorum)
            end
    end,
    {noreply, State#state{pending_gets_quorum=[]}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions %%%
do_putt(Key, Value, TimeStamp, Consistency, State) ->
    case Consistency of
        one ->
            NewDict = dict:store(Key, {Value, TimeStamp, true}, State#state.dict),
            State#state{dict=NewDict};
        quorum ->
            % Envía una solicitud a las réplicas para verificar si la tupla ya existe
            {noreply, State#state{pending_puts_quorum=[{self(), Key, Value, TimeStamp}|State#state.pending_puts_quorum]}};
        all ->
            % Envía una solicitud a todas las réplicas para verificar si la tupla ya existe
            {noreply, State#state{pending_puts_all=[{self(), Key, Value, TimeStamp}|State#state.pending_puts_all]}}
    end.

do_remm(Key, TimeStamp, Consistency, State) ->
    case Consistency of
        one ->
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
                end.
        quorum ->
            % Envía una solicitud a las réplicas para verificar si la tupla ya existe
            {noreply, State#state{pending_rems_quorum=[{self(), Key, TimeStamp} | State#state.pending_rems_quorum]}};
        all ->
            % Envía una solicitud a todas las réplicas para verificar si la tupla ya existe
            {noreply, State#state{pending_rems_all=[{self(), Key, TimeStamp} | State#state.pending_rems_all]}}
    end.

do_gett(Key, Consistency, State) ->
    case Consistency of
        one ->
            dict:find(Key, State#state.dict);
        quorum ->
            % Envía una solicitud a las réplicas para obtener el valor de la tupla
            {noreply, State#state{pending_gets_quorum=[{self(), Key} | State#state.pending_gets_quorum]}};
        all ->
            % Envía una solicitud a todas las réplicas para obtener el valor de la tupla
            {noreply, State#state{pending_gets_all=[{self(), Key} | State#state.pending_gets_all]}}
    end.