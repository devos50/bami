"""
Simulation that initiates a number of searches in the Skip Graph.
"""
import random
from asyncio import ensure_future, sleep

from simulations.skipgraph.settings import SkipGraphSimulationSettings
from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class SearchSkipgraphSimulation(SkipgraphSimulation):

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

        # Reset all search hops statistics (since introduction will also conduct a search)
        for node in self.nodes:
            node.overlay.search_hops = {}
            node.overlay.search_latencies = []

        # First we do some searches to warm-up the cache
        self.logger.error("Starting caches warm-up searches")
        for _ in range(500):
            random_node = random.choice(self.online_nodes)
            search_target = random.randint(0, self.node_keys_sorted[-1])
            await self.do_search(0, random_node, search_target)

        self.searches_done = 0
        self.invalid_searches = 0

        for node in random.sample(self.nodes[1:], 500):
            node.overlay.is_offline = True
            print("Offline node: %s" % node.overlay.get_my_node())
            self.online_nodes.remove(node)
            self.offline_nodes.append(node)
            self.node_keys_sorted.remove(node.overlay.routing_table.key)

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
    settings.peers = 1000
    settings.duration = 3600
    settings.logging_level = "ERROR"
    settings.profile = False
    settings.enable_community_statistics = True
    settings.num_searches = 2000
    settings.enable_ipv8_ticker = False
    settings.latencies_file = "data/latencies.txt"
    settings.cache_intermediate_search_results = True
    settings.track_failing_nodes_in_rts = False
    settings.assign_sequential_sg_keys = True
    simulation = SearchSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
