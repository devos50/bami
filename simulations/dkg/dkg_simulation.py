import os
import random
from asyncio import sleep, ensure_future
from binascii import unhexlify, hexlify
from typing import List

from bami.dkg.content import Content
from bami.dkg.rules.ptn import PTNRule
from ipv8.configuration import ConfigBuilder
from simulations.settings import SimulationSettings
from simulations.skipgraph.settings import SkipGraphSimulationSettings

from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class DKGSimulation(SkipgraphSimulation):

    def __init__(self, settings: SkipGraphSimulationSettings) -> None:
        super().__init__(settings)
        self.content_hashes: List[bytes] = []
        self.searches_done: int = 0
        self.failed_searches: int = 0

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
        print("Processing torrents...")
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

        # Take a few nodes offline
        for node in random.sample(self.nodes, 50):
            node.overlay.is_offline = True
            self.online_nodes.remove(node)

        # Determine content that has generated edges - we do not want to search for content that has no edges
        content_with_triplets = set()
        for node in self.online_nodes:
            content_with_triplets = content_with_triplets.union(node.overlay.knowledge_graph.stored_content)

        print("%d content items with triplets" % len(content_with_triplets))

        print("Starting edge searches")

        async def do_search(delay, node, content_hash):
            await sleep(delay)
            edges = await node.overlay.search_edges(content_hash)
            if not edges:
                self.failed_searches += 1

            self.searches_done += 1
            if self.searches_done % 100 == 0:
                print("Completed %d searches..." % self.searches_done)

        for _ in range(1000):
            content_hash = random.choice(list(content_with_triplets))
            random_node = random.choice(self.online_nodes)
            ensure_future(do_search(random.random() * 20, random_node, content_hash))

        await sleep(30)

        print("Failed searches: %d" % self.failed_searches)

    def on_simulation_finished(self):
        # Write away which node stores what
        with open(os.path.join(self.data_dir, "storage.csv"), "w") as out_file:
            out_file.write("content_id,peer_id\n")
            for ind, node in enumerate(self.nodes):
                for content_id in node.overlay.knowledge_graph.stored_content:
                    out_file.write("%s,%d\n" % (hexlify(content_id).decode(), ind))

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
