"""
Simulation that initiates a number of searches in the Skip Graph.
"""
import os
import random
from asyncio import ensure_future

from simulations.settings import SimulationSettings
from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class SearchSkipgraphSimulation(SkipgraphSimulation):

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

        # Schedule some searches
        for _ in range(1000):
            random_node = random.choice(self.nodes)
            ensure_future(self.do_search(random.random() * 20, random_node, random.randint(0, 2 ** 32)))


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 100
    settings.duration = 30
    settings.logging_level = "ERROR"
    settings.profile = False
    settings.enable_community_statistics = True
    settings.enable_ipv8_ticker = False
    settings.latencies_file = "data/latencies.txt"
    simulation = SearchSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
