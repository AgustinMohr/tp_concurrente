-module(almacenamiento_replicado).
-export([init/0, put/3, get/2, rem/2]).
-record(kv_pair, {key, value}).


% Inicialización de los nodos del cluster
init() ->
    % Definir los nodos del cluster
    Node1 = spawn_link(fun() -> node("node1@localhost") end),
    Node2 = spawn_link(fun() -> node("node2@localhost") end),
    Node3 = spawn_link(fun() -> node("node3@localhost") end),

    % Configurar la replicación
    Node1 ! {join, Node2},
    Node1 ! {join, Node3},
    Node2 ! {join, Node1},
    Node2 ! {join, Node3},
    Node3 ! {join, Node1},
    Node3 ! {join, Node2},

    % Retornar los nodos del cluster
    [Node1, Node2, Node3].

% Función que define el comportamiento de un nodo
node(NodeName) ->
    receive
        {join, OtherNode} ->
            OtherNode ! {joined, self()},
            loop([OtherNode | other_nodes()]) % Añadir el nodo a la lista de nodos
    end.

% Función auxiliar para obtener los nodos existentes
other_nodes() ->
    registered() -- [self() | [init:whereis(n) || n <- nodes()]].

% Función auxiliar para obtener los nombres de los nodos
nodes() ->
    [Name || {Name, _} <- nodes_info()].

% Función auxiliar para obtener información de los nodos
nodes_info() ->
    init:get_nodes().
% Almacenar un par clave-valor
put(Key, Value, Nodes) ->
    Node = determine_node(Key),
    replicate_put(Key, Value, Node, Nodes).

% Recuperar el valor asociado a una clave
get(Key, Nodes) ->
    Node = determine_node(Key),
    get_value(Key, Node, Nodes).

% Eliminar una clave
rem(Key, Nodes) ->
    Node = determine_node(Key),
    replicate_remove(Key, Node, Nodes).

% Función para determinar el nodo responsable de una clave
determine_node(Key) ->
    % Aquí puedes implementar tu función de hash para determinar el nodo responsable.

% Almacenamiento replicado: almacenar el par clave-valor en varios nodos
replicate_put(Key, Value, Node, Nodes) ->
    lists:foreach(
        fun(N) -> send_put_message(Key, Value, N) end,
        Nodes).

% Recuperar el valor de un nodo, si no está disponible, buscar en otros nodos
get_value(Key, Node, Nodes) ->
    case get_value_from_node(Key, Node) of
        {ok, Value} -> {ok, Value};
        not_found -> get_value_from_replicas(Key, Nodes)
    end.

% Eliminar una clave de un nodo y replicar la eliminación
replicate_remove(Key, Node, Nodes) ->
    lists:foreach(
        fun(N) -> send_remove_message(Key, N) end,
        Nodes).

% Función para enviar un mensaje de almacenamiento a un nodo
send_put_message(Key, Value, Node) ->
    Node ! {put, Key, Value}.

% Función para enviar un mensaje de eliminación a un nodo
send_remove_message(Key, Node) ->
    Node ! {remove, Key}.

% Función para recuperar el valor de una clave de un nodo
get_value_from_node(Key, Node) ->
    Node ! {get, self(), Key},
    receive
        {value, Value} -> {ok, Value};
        not_found -> not_found
    end.

% Función para recuperar el valor de una clave de los nodos replicados
get_value_from_replicas(Key, Nodes) ->
    lists:foldl(
        fun(Node, Acc) ->
            case get_value_from_node(Key, Node) of
                {ok, Value} -> {ok, Value};
                not_found -> Acc
            end
        end,
        not_found,
        Nodes).