import hashlib
import os
import random
from asyncio import sleep
from typing import Dict

from bami.skipgraph import LEFT, RIGHT
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
        self.online_nodes = None
        self.offline_nodes = []

    def extend_skip_graph_neighbourhood(self, neighbourhood_size):
        # First, clear everything except the first NB
        for node in self.nodes:
            for level in range(node.overlay.routing_table.height()):
                for side in [LEFT, RIGHT]:
                    nbs = node.overlay.routing_table.levels[level].neighbors[side]
                    if not nbs:
                        continue  # We cannot extend it

                    num_nbs = len(nbs)
                    cur_nbs = nbs[0 if side == RIGHT else len(nbs) - 1]

                    # Clear everything except the first NB
                    node.overlay.routing_table.levels[level].neighbors[side] = [cur_nbs]

        # Extend the neighbourhoods
        for node in self.nodes:
            for level in range(node.overlay.routing_table.height()):
                for side in [LEFT, RIGHT]:
                    nbs = node.overlay.routing_table.levels[level].neighbors[side]
                    if not nbs:
                        continue  # We cannot extend it

                    cur_nbs = nbs[0 if side == RIGHT else len(nbs) - 1]

                    while len(nbs) < neighbourhood_size:
                        # Get the left/right neighbour of the current neighbour
                        ipv8_nb_node = self.nodes[self.key_to_node_ind[cur_nbs.key]]
                        nb_nbs = ipv8_nb_node.overlay.routing_table.levels[level].neighbors[side]
                        if not nb_nbs:
                            break  # Unable to extend further

                        nb_nb = ipv8_nb_node.overlay.routing_table.levels[level].neighbors[side][
                            0 if side == RIGHT else len(nb_nbs) - 1]
                        node.overlay.routing_table.set(level, side, nb_nb)
                        cur_nbs = nb_nb

    def invalidate_skip_graph(self, num_link_breaks):
        # Invalidate the Skip Graph by breaking some random links at random levels
        for node in random.sample(self.nodes, num_link_breaks):
            rand_level = 0
            do_left = (random.randint(0, 1) == 1)
            if do_left:
                ln = node.overlay.routing_table.get(rand_level, LEFT)
                if ln:
                    lln = self.nodes[self.key_to_node_ind[ln.key]].overlay.routing_table.get(rand_level, LEFT)
                    if lln:
                        node.overlay.routing_table.set(rand_level, LEFT, lln)
            else:
                rn = node.overlay.routing_table.get(rand_level, RIGHT)
                if rn:
                    rrn = self.nodes[self.key_to_node_ind[rn.key]].overlay.routing_table.get(rand_level, RIGHT)
                    if rrn:
                        node.overlay.routing_table.set(rand_level, RIGHT, rrn)

    def get_responsible_node_for_key(self, search_key: int):
        """
        Find the responsible node that is responsible for a particular key.
        """
        if search_key < self.node_keys_sorted[0]:
            target_node_key = self.node_keys_sorted[0]
        else:
            # Perform a search through the keys
            ind = len(self.node_keys_sorted) - 1
            while ind >= 0:
                if self.node_keys_sorted[ind] <= search_key:
                    target_node_key = self.node_keys_sorted[ind]
                    break
                ind -= 1

        return self.nodes[self.key_to_node_ind[target_node_key]]

    async def do_search(self, delay, node, search_key):
        await sleep(delay)
        res = await node.overlay.search(search_key)

        if not res:
            # Search failed
            self.invalid_searches += 1
            self.searches_done += 1
            return False, None

        # Verify that this is the right search result
        if res.key not in self.node_keys_sorted:
            print(res.key)
            assert False, "We got a key result that is not registered in the Skip List!"

        search_result_is_correct = True
        if search_key < self.node_keys_sorted[0] and res.key != self.node_keys_sorted[0]:
            search_result_is_correct = False
        elif search_key >= self.node_keys_sorted[0]:
            res_ind = self.node_keys_sorted.index(res.key)
            if res_ind == len(self.node_keys_sorted) - 1:
                if search_key <= self.node_keys_sorted[res_ind - 1]:
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

        return search_result_is_correct, res

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("SkipGraphCommunity", "my peer", [], [], {}, [])
        return builder

    async def ipv8_discover_peers(self) -> None:
        """
        Ignore peer discovery. We don't use IPv8 peers.
        """
        pass

    def initialize_routing_table(self, ind: int, node) -> None:
        int_pk = ind + 1
        if not self.settings.assign_sequential_sg_keys:
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
        for ind, node in enumerate(self.nodes):
            self.initialize_routing_table(ind, node)

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
            #node_ids_that_joined.append(ind)
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

        self.online_nodes = [n for n in self.nodes]

        # TODO bit of cheating here, add left/right neighbours to the routing tables with full knowledge
        print("Extending Skip Graph neighbourhood size to %d" % self.settings.nb_size)
        self.extend_skip_graph_neighbourhood(self.settings.nb_size)

    def on_simulation_finished(self):
        """
        The experiment is finished. Write the results away.
        """
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

            # Aggregated bw statistics
            with open(os.path.join(self.data_dir, "aggregate_msg_statistics.csv"), "w") as msg_stats_file:
                msg_stats_file.write("peers,peer,bytes_up,bytes_down\n")
                for ind, node in enumerate(self.nodes):
                    tot_up, tot_down = 0, 0
                    for msg_id, network_stats in node.endpoint.statistics[node.overlay.get_prefix()].items():
                        tot_up += network_stats.bytes_up
                        tot_down += network_stats.bytes_down
                    msg_stats_file.write("%d,%d,%d,%d\n" % (self.settings.peers, ind, tot_up, tot_down))

        # Search hops statistics
        hops_freq = {}
        for node in self.nodes:
            for num_hops, freq in node.overlay.search_hops.items():
                if num_hops not in hops_freq:
                    hops_freq[num_hops] = 0
                hops_freq[num_hops] += freq

        tot_count = 0
        tot = 0
        with open(os.path.join(self.data_dir, "search_hops.csv"), "w") as search_hops_file:
            search_hops_file.write("peers,nb_size,hops,freq\n")
            for num_hops, freq in hops_freq.items():
                tot += num_hops * freq
                tot_count += freq
                search_hops_file.write("%d,%d,%d,%d\n" % (self.settings.peers, self.settings.nb_size, num_hops, freq))

        if tot_count > 0:
            print("Average search hops: %f" % (tot / tot_count))

        # Search latencies
        with open(os.path.join(self.data_dir, "latencies.csv"), "w") as latencies_file:
            latencies_file.write("peers,nb_size,operation,time\n")
            for node in self.nodes:
                for latency in node.overlay.search_latencies:
                    latencies_file.write("%d,%d,%s,%f\n" % (self.settings.peers, self.settings.nb_size, "search", latency))

                for latency in node.overlay.join_latencies:
                    latencies_file.write("%d,%d,%s,%f\n" % (self.settings.peers, self.settings.nb_size, "join", latency))

                for latency in node.overlay.leave_latencies:
                    latencies_file.write("%d,%d,%s,%f\n" % (self.settings.peers, self.settings.nb_size, "leave", latency))

        # Write statistics on search targets
        with open(os.path.join(self.data_dir, "search_targets.csv"), "w") as out_file:
            out_file.write("peer,target_count\n")
            for peer_id, target_count in self.target_frequency.items():
                out_file.write("%d,%d\n" % (peer_id, target_count))
