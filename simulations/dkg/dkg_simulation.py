import os
import random
from asyncio import sleep, ensure_future
from binascii import unhexlify
from typing import List

from bami.dkg.content import Content
from bami.dkg.rules.ptn import PTNRule
from ipv8.configuration import ConfigBuilder
from simulations.settings import SimulationSettings

from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class DKGSimulation(SkipgraphSimulation):

    def __init__(self, settings: SimulationSettings) -> None:
        super().__init__(settings)
        self.content_hashes: List[bytes] = []
        self.searches_done: int = 0

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("my peer", "curve25519", os.path.join(self.data_dir, f"ec{peer_id}.pem"))
        builder.add_overlay("DKGCommunity", "my peer", [], [], {}, [])
        return builder

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

        # Reset all search hops statistics (since introduction will also conduct a search)
        for node in self.nodes:
            node.overlay.search_hops = {}
            node.overlay.search_latencies = []

        # Give each node the PTN rule
        ptn_rule = PTNRule()
        for node in self.nodes:
            node.overlay.rules_db.add_rule(ptn_rule)

        # TODO hard-coded data file
        # Read the torrents data file and assign them to different nodes
        with open("data/torrents_1000.txt") as torrents_file:
            for ind, torrent_line in enumerate(torrents_file.readlines()):
                parts = torrent_line.strip().split("\t")
                content_hash = unhexlify(parts[0])
                self.content_hashes.append(content_hash)
                content_data = parts[1].encode()
                content = Content(content_hash, content_data)

                target_node = self.nodes[ind % len(self.nodes)]
                target_node.overlay.content_db.add_content(content)

                target_node.overlay.rule_execution_engine.process_queue.append(content)
                target_node.overlay.rule_execution_engine.process()

        await sleep(20)  # Give some time to store the edges in the network

        print("Starting edge searches")

        async def do_search(delay, node, content_hash):
            await sleep(delay)
            _ = await node.overlay.search_edges(content_hash)

            self.searches_done += 1
            if self.searches_done % 100 == 0:
                print("Completed %d searches..." % self.searches_done)

        for _ in range(10000):
            content_hash = random.choice(self.content_hashes)
            random_node = random.choice(self.nodes)
            ensure_future(do_search(random.random() * 20, random_node, content_hash))

    def on_simulation_finished(self):
        super().on_simulation_finished()

        # Write away the knowledge graph statistics per node
        with open(os.path.join(self.data_dir, "kg_stats.csv"), "w") as out_file:
            out_file.write("peer,num_edges\n")
            for ind, node in enumerate(self.nodes):
                out_file.write("%d,%d\n" % (ind, node.overlay.knowledge_graph.get_num_edges()))

        # Write away the edge search latencies
        with open(os.path.join(self.data_dir, "edge_search_latencies.csv"), "w") as latencies_file:
            latencies_file.write("peers,time\n")
            for node in self.nodes:
                for latency in node.overlay.edge_search_latencies:
                    latencies_file.write("%d,%f\n" % (self.settings.peers, latency))
