from asyncio import ensure_future, get_event_loop, sleep

from ipv8.configuration import ConfigBuilder

from simulations.settings import SimulationSettings
from simulations.simulation import BamiSimulation


class BasicSkipgraphSimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("SkipGraphCommunity", "my peer", [], [], {}, [])
        return builder

    def ipv8_discover_peers(self) -> None:
        """
        Introduce every peer to node 0.
        """
        for node in self.nodes[1:]:
            self.nodes[0].overlay.walk_to(node.endpoint.wan_address)

    async def start_ipv8_nodes(self) -> None:
        await super().start_ipv8_nodes()

        # Initialize the routing tables of each node after starting the IPv8 nodes.
        for ind, node in enumerate(self.nodes):
            node.overlay.initialize_routing_table(ind)

    async def on_ipv8_ready(self) -> None:
        await sleep(10)  # Make sure peers have discovered each other
        for ind, node in enumerate(self.nodes[1:]):
            await node.overlay.join(introducer_peer=self.nodes[0].overlay.my_peer)
            print("Node %d joined the Skip Graph..." % ind)

        tot_traffic = 0
        for node in self.nodes:
            #print(node.overlays[0].routing_table)
            tot_traffic += node.endpoint.bytes_up

        print(tot_traffic)


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 600
    settings.duration = 20
    simulation = BasicSkipgraphSimulation(settings)
    simulation.MAIN_OVERLAY = "SkipGraphCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
