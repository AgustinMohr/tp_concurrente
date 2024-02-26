-module(base_de_datos).
-include_lib("eunit/include/eunit.hrl").
-export([test/0, test1/0, test2/0, test3/0, test4/0,start/2]).

start(Nombre, Cantidad) ->
    Aux = crear_lista(Nombre, Cantidad),
    lists:foreach( fun(Elem)-> 
                            Aux2 = lists:delete(Elem, Aux),
                            replica_server:start(Elem, Aux2)
                    end, Aux).

crear_lista(_, 0) -> % Caso base: cuando la cantidad llega a cero, termina la recursión
    [];

crear_lista(Nombre, Cantidad) when Cantidad > 0 -> % Caso recursivo: mientras la cantidad sea mayor que cero, sigue construyendo la lista
    [list_to_atom(atom_to_list(Nombre) ++ integer_to_list(Cantidad)) | crear_lista(Nombre, Cantidad - 1)].

test() ->
    start(server, 5).

%% incerciones de los 3 tipos
test1() ->
    replica_server:putt(server1, "1", 1234, os:timestamp(), one),
    replica_server:putt(server2, "2", 1264, os:timestamp(), quorum),
    replica_server:putt(server3, "3", 3456, os:timestamp(), all).


%% busqueda de los 3 tipos
test2()->    
    Result1 = replica_server:gett(server1, "1", one),
    Result2 = replica_server:gett(server3, "2", quorum),
    Result3 = replica_server:gett(server5, "3", all),
    {Result1, Result2, Result3}.

%% eliminacion de los 3 tipos
test3() ->
    replica_server:remm(server1, "1", os:timestamp(), one),
    replica_server:remm(server4, "15", os:timestamp(), quorum),
    replica_server:remm(server5, "3", os:timestamp(), all).

%% buscamos las claves anteriores
test4() ->
    Result1 = replica_server:gett(server1, "1", one),
    Result2 = replica_server:gett(server4, "15", quorum),
    Result3 = replica_server:gett(server5, "3", all),
    {Result1, Result2, Result3}.
