import hashlib
import os
import random
from asyncio import sleep
from typing import Dict

from bami.skipgraph.util import verify_skip_graph_integrity
from ipv8.configuration import ConfigBuilder

from simulations.simulation import BamiSimulation
from simulations.skipgraph.settings import SkipGraphSimulationSettings


class SkipgraphSimulation(BamiSimulation):

    def __init__(self, settings: SkipGraphSimulationSettings) -> None:
        super().__init__(settings)
        self.node_keys_sorted = []
        self.searches_done = 0
        self.invalid_searches = 0
        self.target_frequency: Dict[int, int] = {}
        self.key_to_node_ind: Dict[int, int] = {}

    async def do_search(self, delay, node, search_key):
        await sleep(delay)
        res = await node.overlay.search(search_key)

        # Verify that this is the right search result
        if res.key not in self.node_keys_sorted:
            assert False, "We got a key result that is not registered in the Skip List!"

        search_result_is_correct = True
        if search_key < self.node_keys_sorted[0] and res.key != self.node_keys_sorted[0]:
            search_result_is_correct = False
        elif search_key >= self.node_keys_sorted[0]:
            res_ind = self.node_keys_sorted.index(res.key)
            if res_ind == len(self.node_keys_sorted) - 1:
                if search_key <= res_ind:
                    search_result_is_correct = False
            else:
                if self.node_keys_sorted[res_ind + 1] <= search_key:
                    search_result_is_correct = False

        self.target_frequency[self.key_to_node_ind[res.key]] += 1
        if not search_result_is_correct:
            self.invalid_searches += 1

        self.searches_done += 1
        if self.searches_done % 100 == 0:
            print("Completed %d searches..." % self.searches_done)

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("SkipGraphCommunity", "my peer", [], [], {}, [])
        return builder

    async def ipv8_discover_peers(self) -> None:
        """
        Ignore peer discovery. We don't use IPv8 peers.
        """
        pass

    def initialize_routing_table(self, node) -> None:
        # Create a hash from the PK
        h = hashlib.md5()
        h.update(node.overlay.my_peer.public_key.key_to_bin())
        # int_pk = (2 ** 32) // len(self.nodes) * ind
        # int_pk = random.randint(0, (2 ** 32))
        int_pk = int.from_bytes(h.digest(), 'big') % (2 ** 32)
        self.node_keys_sorted.append(int_pk)
        node.overlay.initialize_routing_table(int_pk)

    async def start_ipv8_nodes(self) -> None:
        await super().start_ipv8_nodes()

        # Initialize the routing tables of each node after starting the IPv8 nodes.
        for node in self.nodes:
            self.initialize_routing_table(node)

        # Apply the appropriate settings
        for node in self.nodes:
            node.overlay.cache_search_responses = self.settings.cache_intermediate_search_results

        self.node_keys_sorted = sorted(self.node_keys_sorted)

    async def on_ipv8_ready(self) -> None:
        # Initialize the key to node map and target frequency map
        for ind, node in enumerate(self.nodes):
            self.key_to_node_ind[node.overlay.routing_table.key] = ind
            self.target_frequency[ind] = 0

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
        caching = "yes" if self.settings.cache_intermediate_search_results else "no"

        # Bandwidth statistics
        # with open(os.path.join(self.data_dir, "bw_usage.csv"), "w") as bw_file:
        #     bw_file.write("peer,bytes_up,bytes_down\n")
        #     for ind, node in enumerate(self.nodes):
        #         bw_file.write("%d,%d,%d\n" % (ind, node.overlay.endpoint.bytes_up, node.overlay.endpoint.bytes_down))

        # Message statistics
        if self.settings.enable_community_statistics:
            with open(os.path.join(self.data_dir, "msg_statistics.csv"), "w") as msg_stats_file:
                msg_stats_file.write("peer,msg_id,num_up,num_down,bytes_up,bytes_down\n")
                for ind, node in enumerate(self.nodes):
                    for msg_id, network_stats in node.endpoint.statistics[node.overlay.get_prefix()].items():
                        msg_stats_file.write("%d,%d,%d,%d,%d,%d\n" % (ind, msg_id, network_stats.num_up,
                                                                      network_stats.num_down, network_stats.bytes_up,
                                                                      network_stats.bytes_down))

        # Search hops statistics
        hops_freq = {}
        for node in self.nodes:
            for num_hops, freq in node.overlay.search_hops.items():
                if num_hops not in hops_freq:
                    hops_freq[num_hops] = 0
                hops_freq[num_hops] += freq

        with open(os.path.join(self.data_dir, "search_hops.csv"), "w") as search_hops_file:
            search_hops_file.write("peers,hops,freq,caching\n")
            for num_hops, freq in hops_freq.items():
                search_hops_file.write("%d,%d,%d,%s\n" % (self.settings.peers, num_hops, freq, caching))

        # Search latencies
        with open(os.path.join(self.data_dir, "latencies.csv"), "w") as latencies_file:
            latencies_file.write("peers,operation,time,caching\n")
            for node in self.nodes:
                for latency in node.overlay.search_latencies:
                    latencies_file.write("%d,%s,%f,%s\n" % (self.settings.peers, "search", latency, caching))

                for latency in node.overlay.join_latencies:
                    latencies_file.write("%d,%s,%f,%s\n" % (self.settings.peers, "join", latency, caching))

                for latency in node.overlay.leave_latencies:
                    latencies_file.write("%d,%s,%f,%s\n" % (self.settings.peers, "leave", latency, caching))

        # Write statistics on search targets
        with open(os.path.join(self.data_dir, "search_targets.csv"), "w") as out_file:
            out_file.write("peer,target_count\n")
            for peer_id, target_count in self.target_frequency.items():
                out_file.write("%d,%d\n" % (peer_id, target_count))
