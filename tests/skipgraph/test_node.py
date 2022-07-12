from bami.skipgraph.membership_vector import MembershipVector
from bami.skipgraph.node import SGNode
from bami.skipgraph.routing_table import LEFT, RIGHT


def test_fast_join_all_two_nodes():
    node1 = SGNode(1, mv=MembershipVector(0))
    node2 = SGNode(2, mv=MembershipVector(1))
    assert node1.routing_table.height() == 0
    assert node2.routing_table.height() == 0

    # Fast join
    SGNode.fast_join_all([node1, node2])

    assert node1.routing_table.height() == 1
    assert node1.routing_table.size_per_level() == [2]

    assert node1.routing_table.num_unique_nodes() == 1
    assert node2.routing_table.num_unique_nodes() == 1


def test_fast_join_all_four_nodes():
    node1 = SGNode(1, mv=MembershipVector(0))  # mv: 00
    node2 = SGNode(2, mv=MembershipVector(1))  # mv: 10
    node3 = SGNode(3, mv=MembershipVector(2))  # mv: 01
    node4 = SGNode(4, mv=MembershipVector(3))  # mv: 11

    # Fast join
    SGNode.fast_join_all([node1, node2, node3, node4])

    assert node1.routing_table.height() == 2
    assert node1.routing_table.size_per_level() == [2, 2]
    assert node1.routing_table.num_unique_nodes() == 3

    # The routing table of node 1 on level 1 should have node 3 as left/right neighbour
    assert node1.routing_table.levels[1].neighbors[LEFT] == [node3]
    assert node1.routing_table.levels[1].neighbors[RIGHT] == [node3]
