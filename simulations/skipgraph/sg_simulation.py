import os
import random
from asyncio import sleep

from bami.skipgraph.util import verify_skip_graph_integrity
from ipv8.configuration import ConfigBuilder

from simulations.settings import SimulationSettings
from simulations.simulation import BamiSimulation


class SkipgraphSimulation(BamiSimulation):

    def __init__(self, settings: SimulationSettings) -> None:
        super().__init__(settings)
        self.node_keys_sorted = []

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("SkipGraphCommunity", "my peer", [], [], {}, [])
        return builder

    async def ipv8_discover_peers(self) -> None:
        """
        Ignore peer discovery. We don't use IPv8 peers.
        """
        pass

    async def start_ipv8_nodes(self) -> None:
        await super().start_ipv8_nodes()

        # Initialize the routing tables of each node after starting the IPv8 nodes.
        for ind, node in enumerate(self.nodes):
            pk = node.overlay.my_peer.public_key.key_to_bin()
            int_pk = int.from_bytes(pk, 'big') % (2 ** 32)
            self.node_keys_sorted.append(int_pk)
            node.overlay.initialize_routing_table(int_pk)

        self.node_keys_sorted = sorted(self.node_keys_sorted)

    async def on_ipv8_ready(self) -> None:
        # Nodes are now joining the Skip Graph.
        # For better performance and load balancing, we keep track of the nodes that have already joined, and the
        # nodes that still need to join. We choose a random node that already joined as introducer peer.
        node_ids_that_joined = [0]

        node_ids = list(range(1, len(self.nodes)))
        random.shuffle(node_ids)
        count = 0
        for ind in node_ids:
            if count % 100 == 0:
                print("%d nodes joined the Skip Graph!" % count)

            introducer_id = random.choice(node_ids_that_joined)
            # Make sure this node knows about the introducer peer
            self.logger.info("Node %d will join the Skip Graph using introducer peer %d", ind, introducer_id)
            self.nodes[ind].overlay.peers_info[self.nodes[introducer_id].overlay.my_peer] = self.nodes[introducer_id].overlay.get_my_node()
            await self.nodes[ind].overlay.join(introducer_peer=self.nodes[introducer_id].overlay.my_peer)
            node_ids_that_joined.append(ind)
            await sleep(0.1)
            self.logger.info("Node %d joined the Skip Graph..." % ind)
            count += 1

        # Verify the integrity of the Skip Graph
        if not verify_skip_graph_integrity(self.nodes):
            print("Skip Graph not valid!!")
            for ind, node in enumerate(self.nodes):
                print("=== node %d (key: %d) ===\n%s\n" % (ind, node.overlay.routing_table.key, node.overlay.routing_table))
            exit(1)
        else:
            print("Skip Graph valid!")

    def on_simulation_finished(self):
        """
        The experiment is finished. Write the results away.
        """

        # Bandwidth statistics
        with open(os.path.join(self.data_dir, "bw_usage.csv"), "w") as bw_file:
            bw_file.write("peer,bytes_up,bytes_down\n")
            for ind, node in enumerate(self.nodes):
                bw_file.write("%d,%d,%d\n" % (ind, node.overlay.endpoint.bytes_up, node.overlay.endpoint.bytes_down))

        # Message statistics
        if self.settings.enable_community_statistics:
            with open(os.path.join(self.data_dir, "msg_statistics.csv"), "w") as msg_stats_file:
                msg_stats_file.write("peer,msg_id,num_up,num_down,bytes_up,bytes_down\n")
                for ind, node in enumerate(self.nodes):
                    for msg_id, network_stats in node.endpoint.statistics[node.overlay.get_prefix()].items():
                        msg_stats_file.write("%d,%d,%d,%d,%d,%d\n" % (ind, msg_id, network_stats.num_up,
                                                                      network_stats.num_down, network_stats.bytes_up,
                                                                      network_stats.bytes_down))

        # Search statistics
        hops_freq = {}
        for node in self.nodes:
            for num_hops, freq in node.overlay.search_hops.items():
                if num_hops not in hops_freq:
                    hops_freq[num_hops] = 0
                hops_freq[num_hops] += freq

        with open(os.path.join(self.data_dir, "search_hops.csv"), "w") as search_hops_file:
            search_hops_file.write("peers,hops,freq\n")
            for num_hops, freq in hops_freq.items():
                search_hops_file.write("%d,%d,%d\n" % (self.settings.peers, num_hops, freq))
