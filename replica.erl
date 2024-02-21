-module(replica).
-export([start/2, stop/1, putt/5, remm/4, gett/3, sizee/1, all/1]).
-import(string, [len/1, concat/2, chr/2, substr/3, str/2, 
				 to_lower/1, to_upper/1]).

start(Nombre, Replicas) ->
	Pid = whereis(Nombre),
	if 
		Pid /= undefined ->
			unregister(Nombre);
		true ->
			{true}
	end,
	register(Nombre, spawn(fun() -> init(Nombre, Replicas) end)).


init(Nombre, Replicas) ->
	Pid = whereis(servers),
	if
		Pid /= undefined ->
			unregister(servers);
		true ->
			{true}
	end,
	ListaReplicas = lists:append([Nombre], Replicas),
	register(servers, spawn(fun() -> replicas(ListaReplicas) end)),
	Dict = dict:new(),
	loop(Dict, Replicas).
	
replicas(Replicas) ->
	receive
		{consultaNombre, NombreReplica, Pid} ->
			Pred = fun(Replica) -> Replica =:= NombreReplica end,
			[Replic|_T] = lists:filter(Pred, Replicas),
			Pid ! {replyServer, Replic},
			replicas(Replicas)
	end.

stop(Nombre) -> call({stop}, Nombre).
putt(Key, Value, TimeStamp, NombreReplica, NivelConsistencia) -> call({putt, Key, Value, TimeStamp, NivelConsistencia}, NombreReplica).
remm(Key, TimeStamp, NombreReplica, NivelConsistencia) -> call({remm, Key, TimeStamp, NivelConsistencia},NombreReplica).
gett(Key, NombreReplica, NivelConsistencia) -> call({gett, Key, NivelConsistencia}, NombreReplica).
sizee(Nombre) -> call({sizee}, Nombre).
all(Nombre) -> call(all, Nombre).


call(M, NombreReplica) ->
	servers ! {consultaNombre, NombreReplica, self()},
	receive 
		{replyServer, Replica} -> R = Replica
	end,
	R ! {request, self(), M},
	receive 
		{reply, Reply} -> Reply
	end.

loop(Dict, Replicas) ->
	receive
		{request, Pid, {stop, Nombre}} -> 
			unregister(Nombre),
			reply(Pid, ok);
			
		{request, Pid, all} ->
			reply(Pid, Dict),
			loop(Dict, Replicas);
		
		{request,Pid, sizee} ->
			Size = dict:size(Dict),
			reply(Pid, Size),
			loop(Dict, Replicas);
			
		{request, Pid, {gett, Key, NivelConsistencia}} ->
			Val = dict:find(Key,Dict),
			if 
				Val =:= error ->
					reply(Pid, notfound),
					loop(Dict, Replicas);
				true ->
					{ok, {Value, TimeStamp, Activo}} = Val,
					case Activo of
						true ->
							reply(Pid, {ok, {Value,TimeStamp}}),
							loop(Dict, Replicas);
						false ->
							reply(Pid, {ko, TimeStamp}),
							loop(Dict, Replicas)
					end
				
			end;
				
		{request, Pid, {remm, Key, TimeStamp, NivelConsistencia}} ->
			Val = dict:find(Key,Dict),
			if
				Val =:= error ->
					Dict1 = dict:store(Key,{null,TimeStamp,false},Dict),
					reply(Pid, notfound),
					loop(Dict1, Replicas);
				true ->
					{ok, {_, TimeStamp1, Activo}} = Val,
					case Activo of
						true ->
							if
								TimeStamp > TimeStamp1 ->
									Fun = fun({V,_TS, _Activo}) -> {V,TimeStamp,false} end, 
									Dict1 = dict:update(Key, Fun, Dict),
									reply(Pid,ok),
									loop(Dict1, Replicas);
								true ->
									reply(Pid,ko),
									loop(Dict, Replicas)
							end;
						false ->
							reply(Pid,notfound),
							loop(Dict, Replicas)
					end
				
			end;
			
		{request, Pid, remall} ->
			Keys = dict:fetch_keys(Dict),
			Dict1 = remov(Keys, Dict),
			reply(Pid, Dict1),
			loop(Dict1, Replicas);
			
		{request, Pid, {putt, Key, Value, TimeStamp, NivelConsistencia}} -> 
			case NivelConsistencia of
				quorum -> 
					Respuestas1 = erlang:length(Replicas),
					Respuestas = round(Respuestas1/2),
					register('sincP', spawn(fun() -> sinc(Respuestas, self()) end)),
					Flag = true;
				all ->
					Respuestas = erlang:length(Replicas),
					register('sincP', spawn(fun() -> sinc(Respuestas, self()) end)),
					Flag = true;
				one ->
					Flag = false
			end,
			
			register(sender, spawn(fun() -> enviar_mensaje_replicas({requestFromReplic, 'sincP', {putt, Key, Value, TimeStamp}}, Replicas) end)),
			
			
			
			if
				%NivelConsistencia = one
				Flag =:= false -> 
					Val = dict:find(Key,Dict),
					if 
						Val =:= error ->  
							Dict1 = dict:store(Key,{Value,TimeStamp,true},Dict),
							
							reply(Pid, ok),
							loop(Dict1, Replicas);
						true ->
							{ok, {_, TimeStamp1, _}} = Val,
							if 
								TimeStamp > TimeStamp1 -> 
									Dict1 = dict:erase(Key,Dict),
									Dict2 = dict:store(Key,{Value,TimeStamp,true},Dict1),
									reply(Pid, ok),
									loop(Dict2, Replicas);
								true ->
									reply(Pid, ko),
									loop(Dict, Replicas)
							end
					end;
				%NivelConsistencia /= one
				true ->
					sincP ! {ready},
					receive
						{replies_confirmed} -> 
							Val = dict:find(Key,Dict),
							if 
								Val =:= error ->  
									Dict1 = dict:store(Key,{Value,TimeStamp,true},Dict),
									
									reply(Pid, ok),
									loop(Dict1, Replicas);
								true ->
									{ok, {_, TimeStamp1, _}} = Val,
									if 
										TimeStamp > TimeStamp1 -> 
											Dict1 = dict:erase(Key,Dict),
											Dict2 = dict:store(Key,{Value,TimeStamp,true},Dict1),
											reply(Pid, ok),
											loop(Dict2, Replicas);
										true ->
											reply(Pid, ko),
											loop(Dict, Replicas)
									end
							end
					end
			end;
		{requestFromReplic, Pid, {putt, Key, Value, TimeStamp}} -> 
			Val = dict:find(Key,Dict),
			if 
				Val =:= error ->  
					Dict1 = dict:store(Key,{Value,TimeStamp,true},Dict),
					reply(Pid, ok),
					loop(Dict1, Replicas);
				true ->
					{ok, {_, TimeStamp1, _}} = Val,
					if 
						TimeStamp > TimeStamp1 -> 
							Dict1 = dict:erase(Key,Dict),
							Dict2 = dict:store(Key,{Value,TimeStamp,true},Dict1),
							reply(Pid, ok),
							loop(Dict2, Replicas);
						true ->
							reply(Pid, ko),
							loop(Dict, Replicas)
					end
			end
	end.
	
sinc(0, Pid) -> 
	receive
		{ready} ->
			Pid ! {replies_confirmed}
	end;	

sinc(Respuestas, Pid) -> 
	receive
		{reply, ok} ->
			sender ! {replySinc, ok},
			sinc(Respuestas-1, Pid)
	end.
	
enviar_mensaje_replicas(_M, []) ->
	{mensajes_enviados};

enviar_mensaje_replicas(M, [Replic|Tail]) ->
	Replic ! M,
	receive
		{replySinc, ok} ->
			enviar_mensaje_replicas(M, Tail)
	end.
	
	

remov([H|T], Dict) ->
	Dict2 = dict:erase(H,Dict),
	remov(T,Dict2);

remov([], Dict) ->
	Dict = dict:new(),
	Dict.
	
reply(Pid,Reply) ->
	Pid ! {reply, Reply}.