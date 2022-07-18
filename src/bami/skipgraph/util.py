from bami.skipgraph import LEFT, RIGHT
from bami.skipgraph.membership_vector import MembershipVector


def verify_skip_graph_integrity(nodes) -> bool:
    """
    Verify the integrity of the Skip Graph by iterating through each level and verify the links.
    """
    keys_to_nodes = {}
    for node in nodes:
        if not node.overlay.routing_table:
            continue
        keys_to_nodes[node.overlay.routing_table.key] = node

    # At every level, the key of the left neighbour should be less than the node's key.
    # Vice versa, the key of the right neighbour should be larger than the node's key.

    # Verify the links
    for level in range(MembershipVector.LENGTH + 1):
        for node in nodes:
            if not node.overlay.routing_table:
                continue

            left_neighbour = node.overlay.routing_table.get(level, LEFT)
            if left_neighbour:
                if left_neighbour.key >= node.overlay.routing_table.key:
                    print("Left neighbour of node with key %d should be smaller (is: %d)!" %
                          (node.overlay.routing_table.key, left_neighbour.key))
                    return False

                n = keys_to_nodes[left_neighbour.key]
                rn = n.overlay.routing_table.get(level, RIGHT)
                if not rn:
                    print("Right neighbour of node with key %d on level %d not set while it should be" % (rn.key, level))
                    return False
                if rn.key != node.overlay.routing_table.key:
                    print("Right neighbour of node on level %d with key %d not as it should be (%d)" % (level, rn.key, left_neighbour.key))
                    return False

            right_neighbour = node.overlay.routing_table.get(level, RIGHT)
            if right_neighbour:
                if right_neighbour.key <= node.overlay.routing_table.key:
                    print("Right neighbour of node with key %d should be larger (is: %d)!" %
                          (node.overlay.routing_table.key, right_neighbour.key))
                    return False

                n = keys_to_nodes[right_neighbour.key]
                ln = n.overlay.routing_table.get(level, LEFT)
                if not ln:
                    print("Left neighbour of node on with key %d level %d not set while it should be" % (ln.key, level))
                    return False
                if ln.key != node.overlay.routing_table.key:
                    print("Left neighbour of node on level %d with key %d not as it should be (%d)" % (
                    level, ln.key, right_neighbour.key))
                    return False
    return True
