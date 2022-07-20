"""
Simulation that initiates a number of searches in the Skip Graph.
"""
import random
from asyncio import ensure_future

from bami.skipgraph import LEFT, RIGHT
from simulations.skipgraph.settings import SkipGraphSimulationSettings
from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class SearchSkipgraphSimulation(SkipgraphSimulation):

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

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

        # Reset all search hops statistics (since introduction will also conduct a search)
        for node in self.nodes:
            node.overlay.search_hops = {}
            node.overlay.search_latencies = []

        online_nodes = [n for n in self.nodes]

        #self.invalidate_skip_graph(10)
        for node in random.sample(self.nodes, 10):
            node.overlay.is_offline = True
            online_nodes.remove(node)

        # Schedule some searches
        for _ in range(1000):
            random_node = random.choice(online_nodes)
            await self.do_search(0, random_node, random.randint(0, 2 ** 32))

        print("Searches with incorrect result: %d" % self.invalid_searches)


if __name__ == "__main__":
    settings = SkipGraphSimulationSettings()
    settings.peers = 100
    settings.duration = 3600
    settings.logging_level = "ERROR"
    settings.profile = False
    settings.enable_community_statistics = True
    settings.enable_ipv8_ticker = False
    settings.latencies_file = "data/latencies.txt"
    settings.cache_intermediate_search_results = False
    simulation = SearchSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
