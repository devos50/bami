"""
Simulation that initiates a number of searches in the Skip Graph.
"""
import random
from asyncio import ensure_future

from simulations.skipgraph.settings import SkipGraphSimulationSettings
from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class SearchSkipgraphSimulation(SkipgraphSimulation):

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

        # Reset all statistics (since introduction will also conduct a search)
        for node in self.nodes:
            node.overlay.search_hops = {}
            node.overlay.search_latencies = []
            node.endpoint.enable_community_statistics(node.overlay.get_prefix(), False)
            node.endpoint.enable_community_statistics(node.overlay.get_prefix(), True)

        # Take a few nodes offline
        if self.settings.offline_fraction > 0:
            num_offline: int = int(len(self.nodes) * (self.settings.offline_fraction / 100))
            print("Bringing %d nodes offline..." % num_offline)
            for node in random.sample(self.nodes, num_offline):
                node.overlay.is_offline = True
                self.online_nodes.remove(node)
                self.offline_nodes.append(node)
                self.node_keys_sorted.remove(node.overlay.routing_table.key)

        # for node in random.sample(self.nodes[1:], 40):
        #     node.overlay.is_offline = True
        #     print("Offline node: %s" % node.overlay.get_my_node())
        #     self.online_nodes.remove(node)
        #     self.offline_nodes.append(node)
        #     self.node_keys_sorted.remove(node.overlay.routing_table.key)

        # honest_nodes = [n for n in self.nodes]
        # for node in random.sample(self.nodes[1:], 50):
        #     node.overlay.do_censor = True
        #     honest_nodes.remove(node)

        # for node in self.online_nodes:
        #     print(node.overlay.routing_table)

        # Schedule some searches
        successful_searches = 0
        for _ in range(self.settings.num_searches):
            results = []
            random_node = random.choice(self.online_nodes)
            for _ in range(1):
                search_target = random.randint(0, self.node_keys_sorted[-1])
                is_correct, res = await self.do_search(0, random_node, search_target)
                results.append(is_correct)

            if any(results):
                successful_searches += 1

        print("Searches with incorrect result: %d" % (self.settings.num_searches - successful_searches))


if __name__ == "__main__":
    settings = SkipGraphSimulationSettings()
    settings.peers = 10
    settings.duration = 3600
    settings.logging_level = "DEBUG"
    settings.profile = False
    settings.offline_fraction = 10
    settings.nb_size = 4
    settings.enable_community_statistics = True
    settings.num_searches = 1000
    settings.enable_ipv8_ticker = False
    settings.latencies_file = "data/latencies.txt"
    settings.assign_sequential_sg_keys = True
    simulation = SearchSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
