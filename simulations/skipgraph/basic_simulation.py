"""
This basic simulation has nodes join the
"""
import random
from asyncio import ensure_future, sleep

from simulations.settings import SimulationSettings
from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class BasicSkipgraphSimulation(SkipgraphSimulation):

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

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

        # Schedule some searches
        for _ in range(100):
            random_node = random.choice(self.nodes)
            ensure_future(do_search(random.random() * 20, random_node, random.randint(0, 2 ** 32)))


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 100
    settings.duration = 30
    settings.logging_level = "ERROR"
    settings.profile = True
    settings.enable_community_statistics = True
    settings.enable_ipv8_ticker = False
    simulation = BasicSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
