import json
import os
import random
from asyncio import sleep, ensure_future
from binascii import unhexlify, hexlify
from typing import List

from bami.dkg.content import Content
from bami.dkg.db.triplet import Triplet
from bami.dkg.rules.ethereum import EthereumBlockRule, EthereumTransactionRule
from bami.dkg.rules.ptn import PTNRule
from ipv8.configuration import ConfigBuilder
from simulations.dkg.settings import DKGSimulationSettings, Dataset

from simulations.skipgraph.sg_simulation import SkipgraphSimulation


class DKGSimulation(SkipgraphSimulation):

    def __init__(self, settings: DKGSimulationSettings) -> None:
        super().__init__(settings)
        self.searches_done: int = 0
        self.failed_searches: int = 0

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("my peer", "curve25519", os.path.join(self.data_dir, f"ec{peer_id}.pem"))
        builder.add_overlay("DKGCommunity", "my peer", [], [], {}, [])
        return builder

    def on_triplets_generated(self, content: Content, triplets: List[Triplet]):
        """
        We generated some triplets. Directly get the responsible node and store the triplets on that node.
        """
        content_keys = Content.get_keys(content.identifier, num_keys=self.settings.replication_factor)
        for ind in range(self.settings.replication_factor):
            responsible_node = self.get_responsible_node_for_key(content_keys[ind])
            for triplet in triplets:
                responsible_node.overlay.knowledge_graph.add_triplet(triplet)

    async def setup_tribler_experiment(self):
        # Give each node the PTN rule
        ptn_rule = PTNRule()
        for node in self.nodes:
            node.overlay.rules_db.add_rule(ptn_rule)

        # Read the torrents data file and assign them to different nodes
        print("Setting up Tribler experiment...")
        if self.settings.fast_data_injection:
            # First, change the callback of the rule execution engines so we don't spread edges in the network
            for node in self.nodes:
                node.overlay.rule_execution_engine.callback = self.on_triplets_generated

        with open(os.path.join("data", self.settings.data_file_name)) as torrents_file:
            for ind, torrent_line in enumerate(torrents_file.readlines()):
                if ind % 1000 == 0:
                    print("Processed %d torrents..." % ind)

                parts = torrent_line.strip().split("\t")
                content_hash = unhexlify(parts[0])
                content_data = parts[1].encode()
                content = Content(content_hash, content_data)

                target_node = self.nodes[ind % len(self.nodes)]
                target_node.overlay.content_db.add_content(content)

                target_node.overlay.rule_execution_engine.process_queue.append(content)
                target_node.overlay.rule_execution_engine.process()

        if not self.settings.fast_data_injection:
            await sleep(20)  # Give some time to store the edges in the network

    async def setup_ethereum_experiment(self):
        # Give each node the Ethereum rules
        eth_block_rule = EthereumBlockRule()
        eth_tx_rule = EthereumTransactionRule()
        for node in self.nodes:
            node.overlay.rules_db.add_rule(eth_block_rule)
            node.overlay.rules_db.add_rule(eth_tx_rule)

        print("Setting up Ethereum experiment...")
        if self.settings.fast_data_injection:
            # First, change the callback of the rule execution engines so we don't spread edges in the network
            for node in self.nodes:
                node.overlay.rule_execution_engine.callback = self.on_triplets_generated

        # Feed Ethereum blocks to the rule execution engines
        total_tx = 0
        blocks_processed = 0
        with open(os.path.join("data", self.settings.data_file_name)) as blocks_file:
            for ind, block_line in enumerate(blocks_file.readlines()):
                if self.settings.max_eth_blocks and blocks_processed >= self.settings.max_eth_blocks:
                    print("Done - processed %d ETH blocks (txs so far: %d)..." % (ind, total_tx))
                    break

                if ind % 100 == 0:
                    print("Processed %d ETH blocks (txs so far: %d)..." % (ind, total_tx))

                block_json = json.loads(block_line.strip())
                total_tx += len(block_json["transactions"])
                content_hash = unhexlify(block_json["hash"][2:])
                content_data = block_line.strip().encode()
                content = Content(content_hash, content_data)

                target_node = self.nodes[ind % len(self.nodes)]
                target_node.overlay.content_db.add_content(content)

                target_node.overlay.rule_execution_engine.process_queue.append(content)
                while target_node.overlay.rule_execution_engine.process_queue:
                    target_node.overlay.rule_execution_engine.process()

                blocks_processed += 1

        print("Total ETH transactions: %d" % total_tx)

    async def on_ipv8_ready(self) -> None:
        await super().on_ipv8_ready()

        # Set the replication factor
        for node in self.nodes:
            node.overlay.replication_factor = self.settings.replication_factor

        # Reset all search hops statistics (since introduction will also conduct a search)
        for node in self.nodes:
            node.overlay.search_hops = {}
            node.overlay.search_latencies = []

        if self.settings.dataset == Dataset.TRIBLER:
            await self.setup_tribler_experiment()
        elif self.settings.dataset == Dataset.ETHEREUM:
            await self.setup_ethereum_experiment()
        else:
            raise RuntimeError("Unknown dataset %s" % self.settings.dataset)

        # Take a few nodes offline
        if self.settings.offline_fraction > 0:
            num_offline: int = int(len(self.nodes) * (self.settings.offline_fraction / 100))
            print("Bringing %d nodes offline..." % num_offline)
            for node in random.sample(self.nodes, num_offline):
                node.overlay.is_offline = True
                self.online_nodes.remove(node)
                self.offline_nodes.append(node)

        # Determine content that has generated edges - we do not want to search for content that has no triplets
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

        for _ in range(self.settings.num_searches):
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
            out_file.write("peers,replication_factor,peer,key,num_edges,storage_costs\n")
            for ind, node in enumerate(self.nodes):
                num_edges = node.overlay.knowledge_graph.get_num_edges()
                storage_costs = node.overlay.knowledge_graph.get_storage_costs()
                out_file.write("%d,%d,%d,%d,%d,%d\n" %
                               (self.settings.peers, self.settings.replication_factor, ind,
                                node.overlay.routing_table.key, num_edges, storage_costs))

        # Aggregate search hops statistics across nodes
        hops_freq = {}
        for node in self.nodes:
            for num_hops, freq in node.overlay.search_hops.items():
                if num_hops not in hops_freq:
                    hops_freq[num_hops] = 0
                hops_freq[num_hops] += freq

        # Write away the search hops info
        caching = "yes" if self.settings.cache_intermediate_search_results else "no"
        with open(os.path.join(self.data_dir, "search_hops.csv"), "w") as search_hops_file:
            search_hops_file.write("peers,nb_size,hops,freq\n")
            for num_hops, freq in hops_freq.items():
                search_hops_file.write("%d,%d,%d,%s\n" % (self.settings.peers, num_hops, freq, caching))

        # Write away the edge search latencies
        with open(os.path.join(self.data_dir, "edge_search_latencies.csv"), "w") as latencies_file:
            latencies_file.write("peers,offline_fraction,replication_factor,with_cache,time\n")
            for node in self.nodes:
                for latency in node.overlay.edge_search_latencies:
                    latencies_file.write("%d,%d,%d,%d,%f\n" %
                                         (self.settings.peers, self.settings.offline_fraction,
                                          self.settings.replication_factor,
                                          int(self.settings.cache_intermediate_search_results), latency))

        # Write aggregated statistics away
        aggregated_file_path = os.path.join("data", "edge_searches_exp_%s.csv" % self.settings.name)
        if os.path.exists(aggregated_file_path):
            with open(aggregated_file_path, "a") as out_file:
                out_file.write("%d,%d,%d,%d,%d\n" % (len(self.nodes), self.settings.offline_fraction,
                                                     self.settings.replication_factor, self.searches_done,
                                                     self.failed_searches))

        aggregated_file_path = os.path.join("data", "edge_search_latencies_exp_%s.csv" % self.settings.name)
        if os.path.exists(aggregated_file_path):
            with open(aggregated_file_path, "a") as out_file:
                with open(os.path.join(self.data_dir, "edge_search_latencies.csv")) as latencies_file:
                    parsed_header = False
                    for line in latencies_file.readlines():
                        if not parsed_header:
                            parsed_header = True
                            continue

                        out_file.write(line)

        aggregated_file_path = os.path.join("data", "kg_stats_exp_%s.csv" % self.settings.name)
        if os.path.exists(aggregated_file_path):
            with open(aggregated_file_path, "a") as out_file:
                with open(os.path.join(self.data_dir, "kg_stats.csv")) as kg_stats_file:
                    parsed_header = False
                    for line in kg_stats_file.readlines():
                        if not parsed_header:
                            parsed_header = True
                            continue

                        out_file.write(line)

        aggregated_file_path = os.path.join("data", "search_hops_exp_%s.csv" % self.settings.name)
        if os.path.exists(aggregated_file_path):
            with open(aggregated_file_path, "a") as out_file:
                with open(os.path.join(self.data_dir, "search_hops.csv")) as kg_stats_file:
                    parsed_header = False
                    for line in kg_stats_file.readlines():
                        if not parsed_header:
                            parsed_header = True
                            continue

                        out_file.write(line)
