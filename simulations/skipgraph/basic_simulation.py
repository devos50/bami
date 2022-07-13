import random
from asyncio import ensure_future, sleep

from bami.skipgraph import LEFT, RIGHT
from bami.skipgraph.membership_vector import MembershipVector
from ipv8.configuration import ConfigBuilder

from simulations.settings import SimulationSettings
from simulations.simulation import BamiSimulation


class BasicSkipgraphSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("SkipGraphCommunity", "my peer", [], [], {}, [])
        return builder

    def ipv8_discover_peers(self) -> None:
        """
        Ignore peer discovery. We don't use IPv8 peers.
        """
        pass

    async def start_ipv8_nodes(self) -> None:
        await super().start_ipv8_nodes()

        # Initialize the routing tables of each node after starting the IPv8 nodes.
        for ind, node in enumerate(self.nodes):
            node.overlay.initialize_routing_table(ind)

    async def on_ipv8_ready(self) -> None:
        await sleep(5)  # Make sure peers have discovered each other

        # Nodes are now joining the Skip Graph.
        # For better performance and load balancing, we keep track of the nodes that have already joined, and the
        # nodes that still need to join. We choose a random node that already joined as introducer peer.
        node_ids_that_joined = [0]

        node_ids = list(range(1, len(self.nodes)))
        for ind in node_ids:
            introducer_id = random.choice(node_ids_that_joined)
            # Make sure this node knows about the introducer peer
            print("Node %d will join the Skip Graph using introducer peer %d" % (ind, introducer_id))
            self.nodes[ind].overlay.peers_info[self.nodes[introducer_id].overlay.my_peer] = self.nodes[introducer_id].overlay.get_my_node()
            await self.nodes[ind].overlay.join(introducer_peer=self.nodes[introducer_id].overlay.my_peer)
            node_ids_that_joined.append(ind)
            print("Node %d joined the Skip Graph..." % ind)

        # Verify the integrity of the Skip Graph
        if not self.verify_skip_graph_integrity():
            print("Skip Graph not valid!!")
            for ind, node in enumerate(self.nodes):
                print("=== node %d ===\n%s" % (ind, node.overlays[0].routing_table))
            exit(1)
        else:
            print("Skip Graph valid!")

    def verify_skip_graph_integrity(self) -> bool:
        """
        Verify the integrity of the Skip Graph by iterating through each level and verify the links.
        """
        keys_to_nodes = {}
        for node in self.nodes:
            keys_to_nodes[node.overlay.routing_table.key] = node

        for level in range(MembershipVector.LENGTH + 1):
            for node in self.nodes:
                left_neighbour = node.overlay.routing_table.get(level, LEFT)
                if left_neighbour:
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
                    n = keys_to_nodes[right_neighbour.key]
                    ln = n.overlay.routing_table.get(level, LEFT)
                    if not ln:
                        print("Left neighbour of node on with key %d level %d not set while it should be" % (ln.key, level))
                        return False
                    if ln.key != node.overlay.routing_table.key:
                        print("Left neighbour of node on level %d with key %d not as it should be (%d)" % (level, ln.key, right_neighbour.key))
                        return False
        return True


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 2000
    settings.duration = 20
    settings.logging_level = "ERROR"
    simulation = BasicSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
