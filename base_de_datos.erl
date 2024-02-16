-module(base_de_datos).
-export([start/2, stop/0,all/0, remall/0, putt/3, remm/2, gett/1, sizee/0]).
-import(string, [len/1, concat/2, chr/2, substr/3, str/2, 
				 to_lower/1, to_upper/1]).

start(Nombre, N) ->
	Nombre1 = atom_to_list(Nombre),
	Nombre2 = concat(Nombre1, "-"),
	Replicas = crear_lista_replicas(Nombre2, N, []),
	Fun = fun(A) -> replica:start(A, Replicas) end,
	Replicas1 = lists:map(Fun,Replicas).

stop() -> 
	stop.

crear_lista_replicas(_Nombre,0,List) ->
	List;
	
crear_lista_replicas(Nombre, N, List) ->
	N2 = N-1,
	N1 = integer_to_list(N),
	Nombre1 = concat(Nombre, N1),
	Nombre2 = [list_to_atom(Nombre1)],
	List1 = List ++ Nombre2,
	crear_lista_replicas(Nombre,N2,List1).
	
replicas(Replicas) -> 
	Replicas.
	
server(Server) ->
	receive
		{consultaServidor, Pid} -> 
			Pid ! {replyServer, Server},
			server(Server)
	end.

loop(Dict) ->
	receive
		{request, Pid, stop} -> 
			reply(Pid, ok);
			
		{request, Pid, all} ->
			reply(Pid, Dict),
			loop(Dict);
		
		{request,Pid, sizee} ->
			Size = dict:size(Dict),
			reply(Pid, Size),
			loop(Dict);
			
		{request, Pid, {gett, Key}} ->
			Val = dict:find(Key,Dict),
			if 
				Val =:= error ->
					reply(Pid, notfound),
					loop(Dict);
				true ->
					{ok, {Value, TimeStamp, Activo}} = Val,
					case Activo of
						true ->
							reply(Pid, {ok, {Value,TimeStamp}}),
							loop(Dict);
						false ->
							reply(Pid, {ko, TimeStamp}),
							loop(Dict)
					end
				
			end;
				
		{request, Pid, {remm, Key, TimeStamp}} ->
			Val = dict:find(Key,Dict),
			if
				Val =:= error ->
					Dict1 = dict:store(Key,{null,TimeStamp,false},Dict),
					reply(Pid, notfound),
					loop(Dict1);
				true ->
					{ok, {_, TimeStamp1, Activo}} = Val,
					case Activo of
						true ->
							if
								TimeStamp > TimeStamp1 ->
									Fun = fun({V,_TS, _Activo}) -> {V,TimeStamp,false} end, 
									Dict1 = dict:update(Key, Fun, Dict),
									reply(Pid,ok),
									loop(Dict1);
								true ->
									reply(Pid,ko),
									loop(Dict)
							end;
						false ->
							reply(Pid,notfound),
							loop(Dict)
					end
				
			end;
			
		{request, Pid, remall} ->
			Keys = dict:fetch_keys(Dict),
			Dict1 = remov(Keys, Dict),
			reply(Pid, Dict1),
			loop(Dict1);
			
		{request, Pid, {putt, Key, Value, TimeStamp}} -> 
			Val = dict:find(Key,Dict),
			if 
				Val =:= error ->  
					Dict1 = dict:store(Key,{Value,TimeStamp,true},Dict),
					reply(Pid, ok),
					loop(Dict1);
				true ->
					{ok, {_, TimeStamp1, _}} = Val,
					if 
						TimeStamp > TimeStamp1 -> 
							Dict1 = dict:erase(Key,Dict),
							Dict2 = dict:store(Key,{Value,TimeStamp,true},Dict1),
							reply(Pid, ok),
							loop(Dict2);
						true ->
							reply(Pid, ko),
							loop(Dict)
					end
			end
	end.
	
all() -> call(all).
remall() -> call(remall).
putt(Key, Value, TimeStamp) -> call({putt, Key, Value, TimeStamp}).
remm(Key, TimeStamp) -> call({remm, Key, TimeStamp}).
gett(Key) -> call({gett, Key}).
sizee() -> call({sizee}).

call(M) ->
	servers ! {consultaServidor, self()},
	receive 
		{replyServer, Server} -> S = Server
	end,
	S ! {request, self(), M},
	receive 
		{reply, Reply} -> Reply
	end.
	
reply(Pid,Reply) ->
	Pid ! {reply, Reply}.

remov([H|T], Dict) ->
	Dict2 = dict:erase(H,Dict),
	remov(T,Dict2);

remov([], Dict) ->
	Dict = dict:new(),
	Dict.
	
	