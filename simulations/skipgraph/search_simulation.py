"""
Simulation that initiates a number of searches in the Skip Graph.
"""
import random
from asyncio import ensure_future, sleep

from bami.skipgraph import LEFT, RIGHT
from simulations.skipgraph.settings import SkipGraphSimulationSettings
from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class SearchSkipgraphSimulation(SkipgraphSimulation):

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

        # Reset all search hops statistics (since introduction will also conduct a search)
        for node in self.nodes:
            node.overlay.search_hops = {}
            node.overlay.search_latencies = []

        for node in random.sample(self.nodes[1:], 20):
            node.overlay.is_offline = True
            print("Offline node: %s" % node.overlay.get_my_node())
            self.online_nodes.remove(node)
            self.offline_nodes.append(node)
            self.node_keys_sorted.remove(node.overlay.routing_table.key)

        # TODO bit of cheating here, add left/right neighbours to the routing tables with full knowledge

        # for node in self.nodes:
        #     print(node.overlay.routing_table)

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

        # for node in self.nodes:
        #     print(node.overlay.routing_table)

        # Extend the neighbourhoods
        target_size = 2
        for node in self.nodes:
            for level in range(node.overlay.routing_table.height()):
                for side in [LEFT, RIGHT]:
                    nbs = node.overlay.routing_table.levels[level].neighbors[side]
                    if not nbs:
                        continue  # We cannot extend it

                    cur_nbs = nbs[0 if side == RIGHT else len(nbs) - 1]

                    while len(nbs) < target_size:
                        # Get the left/right neighbour of the current neighbour
                        ipv8_nb_node = self.nodes[self.key_to_node_ind[cur_nbs.key]]
                        nb_nbs = ipv8_nb_node.overlay.routing_table.levels[level].neighbors[side]
                        if not nb_nbs:
                            break  # Unable to extend further

                        nb_nb = ipv8_nb_node.overlay.routing_table.levels[level].neighbors[side][0 if side == RIGHT else len(nb_nbs) - 1]
                        node.overlay.routing_table.set(level, side, nb_nb)
                        cur_nbs = nb_nb

        for node in self.nodes:
            print(node.overlay.routing_table)

        # Schedule some searches
        print(self.node_keys_sorted)
        for _ in range(self.settings.num_searches):
            random_node = random.choice(self.online_nodes)
            search_target = random.randint(0, self.node_keys_sorted[-1])
            #print("Node %d initiates search for %d" % (random_node.overlay.routing_table.key, search_target))
            is_correct, res = await self.do_search(0, random_node, search_target)
            # if not is_correct:
            #     print("Search: %d, result: %d (correct? %s)" % (search_target, res.key, is_correct))

            count = 0
            if self.settings.track_failing_nodes_in_rts:
                offline_sg_nodes = [node.overlay.get_my_node() for node in self.offline_nodes]
                for online_node in self.online_nodes:
                    for node_in_rt in online_node.overlay.routing_table.get_all_nodes():
                        if node_in_rt in offline_sg_nodes:
                            count += 1

        print("Searches with incorrect result: %d" % self.invalid_searches)


if __name__ == "__main__":
    settings = SkipGraphSimulationSettings()
    settings.peers = 100
    settings.duration = 3600
    settings.logging_level = "ERROR"
    settings.profile = False
    settings.enable_community_statistics = True
    settings.num_searches = 2000
    settings.enable_ipv8_ticker = False
    settings.latencies_file = "data/latencies.txt"
    settings.cache_intermediate_search_results = True
    settings.track_failing_nodes_in_rts = False
    #settings.assign_sequential_sg_keys = True
    simulation = SearchSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
