import os
from binascii import unhexlify

from bami.dkg.content import Content
from bami.dkg.rules.ptn import PTNRule
from ipv8.configuration import ConfigBuilder

from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class DKGSimulation(SkipgraphSimulation):

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
        with open("data/torrents_10000.txt") as torrents_file:
            for ind, torrent_line in enumerate(torrents_file.readlines()):
                parts = torrent_line.strip().split("\t")
                content_hash = unhexlify(parts[0])
                content_data = parts[1].encode()
                content = Content(content_hash, content_data)

                target_node = self.nodes[ind % len(self.nodes)]
                target_node.overlay.content_db.add_content(content)

        for node in self.nodes:
            node.overlay.rule_execution_engine.start()

    def on_simulation_finished(self):
        super().on_simulation_finished()

        # Write away the knowledge graph statistics per node
        with open(os.path.join(self.data_dir, "kg_stats.csv"), "w") as out_file:
            out_file.write("peer,num_edges\n")
            for ind, node in enumerate(self.nodes):
                out_file.write("%d,%d\n" % (ind, node.overlay.knowledge_graph.get_num_edges()))
