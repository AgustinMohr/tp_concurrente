-module(base_de_datos).
-export([start/2, stop/0]).
-import(string, [len/1, concat/2, chr/2, substr/3, str/2, 
				 to_lower/1, to_upper/1]).

start(Nombre, N) ->
	Pid = whereis(base_de_datos),
	if 
		Pid /= undefined ->
			unregister(base_de_datos);
		true ->
			{true}
	end,
	register(base_de_datos, spawn(fun() -> init(Nombre, N) end)).

stop() -> call(stop).

init(Nombre, N) ->
	Nombre1 = atom_to_list(Nombre),
	Nombre2 = concat(Nombre1, "-"),
	Replicas = crear_lista_replicas(Nombre2, N, []),
	start_replics(Replicas, Replicas),
	loop(Replicas).

crear_lista_replicas(_Nombre,0,List) ->
	List;
	
crear_lista_replicas(Nombre, N, List) ->
	N2 = N-1,
	N1 = integer_to_list(N),
	Nombre1 = concat(Nombre, N1),
	Nombre2 = [list_to_atom(Nombre1)],
	List1 = List ++ Nombre2,
	crear_lista_replicas(Nombre,N2,List1).

start_replics([], _Replicas) ->
	{replics_started};

start_replics([Replica|Tail], Replicas) ->
	replica:start(Replica, Replicas),
	start_replics(Tail, Replicas).

loop(Replicas) ->
	receive
		{request, Pid, stop} ->
			stop_all(Replicas),
			reply(Pid, ok)
	end.
	
stop_all([]) ->
	{stop_ok};
	
stop_all(Replicas) ->
	[Replic|Tail] = Replicas,
	replica:stop(Replic),
	stop_all(Tail).

call(M) ->
	base_de_datos ! {request, self(), M},
	receive 
		{reply, Reply} -> Reply
	end.
	
reply(Pid,Reply) ->
	Pid ! {reply, Reply}.