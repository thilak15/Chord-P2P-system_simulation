-module(p2p).
-import(fingertables,[finger_table_data/5,fingertable_data/4,fingertable_node_pass/3,fingertable_send/2]).
-import(servers,[node_listen/1,totalHops/0]).
-import(math,[log/1]).
-export([start/2, start_network/2, listen_task_completion/2, node/4,get_successor_i/4,near_node/3,next_node/2,prev_node/2]).

get_m(Noof_Nodes) ->
    trunc(math:ceil(math:log2(Noof_Nodes)))
.


randomNode(Nodeid, []) -> Nodeid;
randomNode(_, Existing_Nodes) -> lists:nth(rand:uniform(length(Existing_Nodes)), Existing_Nodes).


add_node_to_chord(Chord_Nodes, Total_Nodes, M, Network_state) ->
    NoRemaining_Hashes = lists:seq(0, Total_Nodes - 1, 1) -- Chord_Nodes,
    Hash = lists:nth(rand:uniform(length(NoRemaining_Hashes)), NoRemaining_Hashes),
    Pid = spawn(p2p, node, [Hash, M, Chord_Nodes, dict:new()]),
    % io:format("~n ~p ~p ~n", [Hash, Pid]),
    [Hash, dict:store(Hash, Pid, Network_state)]
.


get_forward_distance(Key, Key, _, Distance) ->
    Distance;
get_forward_distance(Key, NodeId, M, Distance) ->
    get_forward_distance(Key, (NodeId + 1) rem trunc(math:pow(2, M)), M, Distance + 1)
.

get_closest(_, [], Min_Node, _, _) ->
    Min_Node;
get_closest(Key, FingerNode_Ids, Min_Node, Min_Val, State) ->
    [First_| Rest] = FingerNode_Ids,
    Distance = get_forward_distance(Key, First_, dict:fetch(m, State), 0),
    if
        Distance < Min_Val ->
            get_closest(Key, Rest, First_, Distance, State);
        true -> 
            get_closest(Key, Rest, Min_Node, Min_Val, State)
    end
.

near_node(Key, FingerNode_Ids, State) ->
    case lists:member(Key, FingerNode_Ids) of
        true -> Key;
        _ -> get_closest(Key, FingerNode_Ids, -1, 10000000, State)
    end

.


is_in_range(From, To, Key, M) ->
    if 
        From < To -> 
            (From =< Key) and (Key =< To);
        trunc(From) == trunc(To) ->
            trunc(Key) == trunc(From);
        From > To ->
            ((Key >= 0) and (Key =< To)) or ((Key >= From) and (Key < trunc(math:pow(2, M))))
    end
.

closest_preceding_finger(_, NodeState, 0) -> NodeState;
closest_preceding_finger(Id, NodeState, M) -> 
    MthFinger = lists:nth(M, dict:fetch(finger_table, NodeState)),
    
    case is_in_range(dict:fetch(id, NodeState), Id, dict:fetch(node ,MthFinger), dict:fetch(m, NodeState)) of
        true -> 

            dict:fetch(pid ,MthFinger) ! {state, self()},
            receive
                {statereply, FingerNodeState} ->
                    FingerNodeState
            end,
            FingerNodeState;

        _ -> closest_preceding_finger(Id, NodeState, M - 1)
    end
.

prev_node(Id, NodeState) ->
    case 
        is_in_range(dict:fetch(id, NodeState) + 1, dict:fetch(id, dict:fetch(successor, NodeState)), Id, dict:fetch(m, NodeState)) of 
            true -> NodeState;
            _ -> prev_node(Id, closest_preceding_finger(Id, NodeState, dict:fetch(m, NodeState)))
    end
.

next_node(Id, NodeState) ->
    PredicessorNodeState = prev_node(Id, NodeState),
    dict:fetch(successor, PredicessorNodeState)
.





node(Hash, M, Chord_Nodes, _NodeState) -> 
    %io:format("Node is spawned with hash ~p",[Hash]),
    FingerTable = lists:duplicate(M, randomNode(Hash, Chord_Nodes)),
    NodeStateUpdated = dict:from_list([{id, Hash}, {predecessor, nil}, {finger_table, FingerTable}, {next, 0}, {m, M}]),
    node_listen(NodeStateUpdated)        
.


create_nodes(Chord_Nodes, _, _, 0, Network_state) -> 
    [Chord_Nodes, Network_state];
create_nodes(Chord_Nodes, Total_Nodes, M, Noof_Nodes, Network_state) ->
    [Hash, NewNetwork_state] = add_node_to_chord(Chord_Nodes, Total_Nodes,  M, Network_state),
    create_nodes(lists:append(Chord_Nodes, [Hash]), Total_Nodes, M, Noof_Nodes - 1, NewNetwork_state)
.



get_successor_i(Hash, Network_state, I,  M) -> 
    case dict:find((Hash + I) rem trunc(math:pow(2, M)), Network_state) of
        error ->
             get_successor_i(Hash, Network_state, I + 1, M);
        _ -> (Hash + I) rem trunc(math:pow(2, M))
    end
.



get_node_pid(Hash, Network_state) -> 
    case dict:find(Hash, Network_state) of
        error -> nil;
        _ -> dict:fetch(Hash, Network_state)
    end
.

send_message_to_node(_, [], _) ->
    ok;
send_message_to_node(Key, Chord_Nodes, Network_state) ->
    [First_ | Rest] = Chord_Nodes,
    Pid = get_node_pid(First_, Network_state),
    Pid ! {lookup, First_, Key, 0, self()},
    send_message_to_node(Key, Rest, Network_state)
.

send_messages_all_nodes(_, 0, _, _) ->
    ok;
send_messages_all_nodes(Chord_Nodes, NumRequest, M, Network_state) ->
    timer:sleep(1000),
    Key = lists:nth(rand:uniform(length(Chord_Nodes)), Chord_Nodes),
    send_message_to_node(Key, Chord_Nodes, Network_state),
    send_messages_all_nodes(Chord_Nodes, NumRequest - 1, M, Network_state)
.

kill_all_nodes([], _) ->
    ok;
kill_all_nodes(Chord_Nodes, Network_state) -> 
    [First_ | Rest] = Chord_Nodes,
    get_node_pid(First_, Network_state) ! {kill},
    kill_all_nodes(Rest, Network_state).



listen_task_completion(0, HopsCount) ->
    mainprocess ! {totalhops, HopsCount}
;

listen_task_completion(NumRequests, HopsCount) ->
    receive 
        {completed, _Pid, HopsCountForTask, _Key} ->
            % io:format("received completion from ~p, Number of Hops ~p, For Key ~p", [Pid, HopsCountForTask, Key]),
            listen_task_completion(NumRequests - 1, HopsCount + HopsCountForTask)
    end
.


send_messages_and_kill(Chord_Nodes, Noof_Nodes, NumRequest, M, Network_state) ->
    register(taskcompletionmonitor, spawn(p2p, listen_task_completion, [Noof_Nodes * NumRequest, 0])),

    send_messages_all_nodes(Chord_Nodes, NumRequest, M, Network_state),

    TotalHops = totalHops(),
    
    {ok, File} = file:open("./stats.txt", [append]),
    io:format(File, "~n Average Hops = ~p   TotalHops = ~p    Noof_Nodes = ~p    NumRequests = ~p  ~n", [TotalHops/(Noof_Nodes * NumRequest), TotalHops, Noof_Nodes , NumRequest]),
    io:format("~n Average Hops = ~p   TotalHops = ~p    Noof_Nodes = ~p    NumRequests = ~p  ~n", [TotalHops/(Noof_Nodes * NumRequest), TotalHops, Noof_Nodes , NumRequest]),
    io:format("~n log2(~p)=~p ~n",[Noof_Nodes,log(Noof_Nodes)]),
    kill_all_nodes(Chord_Nodes, Network_state)
.

start_network(Noof_Nodes, NumRequest) ->
    M = get_m(Noof_Nodes),
    [Chord_Nodes, Network_state] = create_nodes([], round(math:pow(2, M)), M, Noof_Nodes, dict:new()),
    
    fingertable_send(Network_state,M),
    send_messages_and_kill(Chord_Nodes, Noof_Nodes, NumRequest, M, Network_state)
.


start(Noof_Nodes, NumRequest) ->
    register(mainprocess, spawn(p2p, start_network, [Noof_Nodes, NumRequest]))
.
