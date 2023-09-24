-module(fingertables).
-import(p2p,[get_successor_i/4]).
-export([finger_table_data/5,fingertable_data/4,fingertable_node_pass/3,fingertable_send/2]).

finger_table_data(_, _, M, M,FingerList) ->
    FingerList;
finger_table_data(Node, Network_state, M, I, FingerList) ->
    Hash = element(1, Node),
    Ith_succesor = get_successor_i(Hash, Network_state, trunc(math:pow(2, I)), M),
    finger_table_data(Node, Network_state, M, I + 1, FingerList ++ [{Ith_succesor, dict:fetch(Ith_succesor, Network_state)}] )
.


fingertable_data(_, [], FTDict,_) ->
    FTDict;

fingertable_data(Network_state, NetList, FTDict,M) ->
    [First | Rest] = NetList,
    FingerTables = finger_table_data(First, Network_state,M, 0,[]),
    fingertable_data(Network_state, Rest, dict:store(element(1, First), FingerTables, FTDict), M)
.



fingertable_node_pass([], _, _) ->
    ok;
fingertable_node_pass(NodesToSend, Network_state, FingerTables) ->
    [First|Rest] = NodesToSend,
    Pid = dict:fetch(First ,Network_state),
    Pid ! {fix_fingers, dict:from_list(dict:fetch(First, FingerTables))},
    fingertable_node_pass(Rest, Network_state, FingerTables)
.


fingertable_send(Network_state,M) ->
    FingerTables = fingertable_data(Network_state, dict:to_list(Network_state), dict:new(),M),
    % io:format("~n~p~n", [FingerTables]),
    fingertable_node_pass(dict:fetch_keys(FingerTables), Network_state, FingerTables)
.