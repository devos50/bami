"""
This basic simulation has nodes join the
"""
import os
import random
from asyncio import ensure_future, sleep
from typing import Dict

from simulations.settings import SimulationSettings
from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class BasicSkipgraphSimulation(SkipgraphSimulation):

    def __init__(self, settings: SimulationSettings):
        super().__init__(settings)
        self.searches_done = 0
        self.key_to_node_ind: Dict[int, int] = {}
        self.target_frequency: Dict[int, int] = {}

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

        # Initialize the key to node map and target frequency map
        for ind, node in enumerate(self.nodes):
            self.key_to_node_ind[node.overlay.routing_table.key] = ind
            self.target_frequency[ind] = 0

        # Reset all search hops statistics (since introduction will also conduct a search)
        for node in self.nodes:
            node.overlay.search_hops = {}
            node.overlay.search_latencies = []

        async def do_search(delay, node, search_key):
            await sleep(delay)
            res = await node.overlay.search(search_key)

            # Verify that this is the right search result
            if res.key not in self.node_keys_sorted:
                assert False, "We got a key result that is not registered in the Skip List!"

            if search_key < self.node_keys_sorted[0]:
                assert res.key == self.node_keys_sorted[0]
            else:
                res_ind = self.node_keys_sorted.index(res.key)
                if res_ind == len(self.node_keys_sorted) - 1:
                    assert search_key > res_ind
                else:
                    assert res.key <= search_key and self.node_keys_sorted[res_ind + 1] > search_key, \
                        "Result: %d search key: %d" % (res.key, search_key)

            self.target_frequency[self.key_to_node_ind[res.key]] += 1

            self.searches_done += 1
            if self.searches_done % 100 == 0:
                print("Completed %d searches..." % self.searches_done)

        # Schedule some searches
        for _ in range(10000):
            random_node = random.choice(self.nodes)
            ensure_future(do_search(random.random() * 20, random_node, random.randint(0, 2 ** 32)))

    def on_simulation_finished(self) -> None:
        super().on_simulation_finished()

        # Write statistics on search targets
        with open(os.path.join(self.data_dir, "search_targets.csv"), "w") as out_file:
            out_file.write("peer,target_count\n")
            for peer_id, target_count in self.target_frequency.items():
                out_file.write("%d,%d\n" % (peer_id, target_count))


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 100
    settings.duration = 30
    settings.logging_level = "ERROR"
    settings.profile = False
    settings.enable_community_statistics = True
    settings.enable_ipv8_ticker = False
    settings.latencies_file = "data/latencies.txt"
    simulation = BasicSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
