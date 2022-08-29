import asyncio
import json
import random
from asyncio import get_event_loop, ensure_future
from binascii import unhexlify, hexlify
from typing import List, Optional, Tuple

from bami.dkg.cache import StorageRequestCache, TripletsRequestCache, IsStoringQueryCache
from bami.dkg.content import Content
from bami.eva.protocol import EVAProtocol
from bami.skipgraph import LEFT, RIGHT
from bami.skipgraph.community import SkipGraphCommunity

from bami.dkg.payloads import StorageRequestPayload, StorageResponsePayload, TripletsRequestPayload, \
    SearchFailurePayload, IsStoringQueryPayload, IsStoringResponsePayload, TripletsPayload
from bami.dkg.db.content_database import ContentDatabase
from bami.dkg.db.knowledge_graph import KnowledgeGraph
from bami.dkg.db.rules_database import RulesDatabase
from bami.dkg.db.triplet import Triplet
from bami.dkg.rule_execution_engine import RuleExecutionEngine
from bami.skipgraph.node import SGNode
from ipv8.lazy_community import lazy_wrapper
from ipv8.types import Peer


class DKGCommunity(SkipGraphCommunity):
    community_id = unhexlify('d5889074c1e5b60423cdb6e9307ba0ca5695ead7')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content_db = ContentDatabase()
        self.rules_db = RulesDatabase()
        self.knowledge_graph = KnowledgeGraph()
        self.rule_execution_engine: RuleExecutionEngine = RuleExecutionEngine(self.content_db, self.rules_db,
                                                                              self.my_peer.key,
                                                                              self.on_new_triplets_generated)

        self.edge_search_latencies: List[Tuple[float, float]] = []  # Keep track of the latency of individual edge searches

        self.eva = EVAProtocol(self, self.on_eva_receive, self.on_eva_send_complete, self.on_eva_error)
        self.eva.settings.max_simultaneous_transfers = 10000

        self.add_message_handler(StorageRequestPayload, self.on_storage_request)
        self.add_message_handler(StorageResponsePayload, self.on_storage_response)
        self.add_message_handler(TripletsRequestPayload, self.on_triplets_request)
        self.add_message_handler(SearchFailurePayload, self.on_search_failure)
        self.add_message_handler(IsStoringQueryPayload, self.on_is_storing_query)
        self.add_message_handler(IsStoringResponsePayload, self.on_is_storing_response)

        self.replication_factor: int = 2
        self.should_verify_key: bool = True
        self.is_malicious: bool = False

        self.logger.info("The DKG community started!")

    async def on_eva_receive(self, result):
        self.logger.info(f'EVA Data has been received: {result}')
        info_json = json.loads(result.info.decode())
        if info_json["type"] == "store":
            triplets_payload = self.serializer.unpack_serializable(TripletsPayload, result.data)[0]
            for triplet_payload in triplets_payload.triplets:
                self.knowledge_graph.add_triplet(Triplet.from_payload(triplet_payload))
        elif info_json["type"] == "search_response":
            if not self.request_cache.has("triplets", info_json["id"]):
                self.logger.warning("triplets cache with id %s not found", info_json["id"])
                return

            cache: TripletsRequestCache = self.request_cache.pop("triplets", info_json["id"])
            triplets_payload = self.serializer.unpack_serializable(TripletsPayload, result.data)[0]
            triplets = [Triplet.from_payload(triplet_payload) for triplet_payload in triplets_payload.triplets]
            cache.future.set_result(triplets)

    async def on_eva_send_complete(self, result):
        self.logger.info(f'EVA transfer has been completed: {result}')

    async def on_eva_error(self, peer, exception):
        self.logger.error(f'EVA Error has occurred: {exception}')

    async def search_edges_with_key(self, key: int, content_hash: bytes):
        sg_search_start_time = get_event_loop().time()
        target_node = await self.search(key)
        sg_search_time = get_event_loop().time() - sg_search_start_time
        if not target_node:
            self.logger.warning("Search node with key %d failed and returned nothing.", key)
            self.edge_search_latencies.append((sg_search_time, 0))
            return key, None, []

        # Query the target node directly for the edges.
        if target_node.key == self.routing_table.key:
            triplets = self.knowledge_graph.get_triplets_of_node(content_hash)
            self.edge_search_latencies.append((sg_search_time, 0))
            return key, target_node, triplets
        else:
            # We send an outgoing query
            eva_start_time = get_event_loop().time()
            cache = TripletsRequestCache(self)
            self.request_cache.add(cache)
            self.ez_send(target_node.get_peer(), TripletsRequestPayload(cache.number, content_hash))
            triplets = await cache.future
            self.edge_search_latencies.append((sg_search_time, get_event_loop().time() - eva_start_time))
            return key, target_node, triplets

    async def search_edges(self, content_hash: bytes) -> List[Triplet]:
        """
        Query the network to fetch incoming/outgoing edges of the node labelled with the content hash.
        """
        content_keys: List[int] = Content.get_keys(content_hash, num_keys=self.replication_factor)
        key_to_ind = {}
        for ind, key in enumerate(content_keys):
            key_to_ind[key] = ind
        random.shuffle(content_keys)  # For load balancing

        failed_indices = []
        futures = []

        for attempt in range(self.replication_factor):
            self.logger.info("Peer %s searching for edges (request %d/%d)" %
                             (self.get_my_short_id(), attempt, self.replication_factor))
            key = content_keys[attempt]

            futures.append(self.search_edges_with_key(key, content_hash))

        search_results = await asyncio.gather(*futures)
        for search_key, target_node, node_search_results in search_results:
            if not node_search_results and target_node:
                failed_indices.append((key_to_ind[search_key], target_node))

        # Inform the target node about our prior failed searches
        for failed_index, target_node in failed_indices:
            self.ez_send(target_node.get_peer(), SearchFailurePayload(content_hash, failed_index))

        # Merge the search results and remove duplicates
        triplets = []
        for _, __, node_search_results in search_results:
            if node_search_results:
                for triplet in node_search_results:
                    if triplet not in triplets:
                        triplets.append(triplet)

        return triplets

    @lazy_wrapper(TripletsRequestPayload)
    def on_triplets_request(self, peer: Peer, payload: TripletsRequestPayload):
        triplets: List[Triplet] = []
        if not self.is_malicious:
            triplets = self.knowledge_graph.get_triplets_of_node(payload.content)

        triplets_payload = TripletsPayload([triplet.to_payload() for triplet in triplets])
        serialized_payload = self.serializer.pack_serializable(triplets_payload)
        info_json = {"type": "search_response", "id": payload.identifier, "cid": hexlify(payload.content).decode()}
        ensure_future(self.eva.send_binary(peer, json.dumps(info_json).encode(), serialized_payload))

    async def on_new_triplets_generated(self, content: Content, triplets: List[Triplet]):
        """
        The rule engine generated new triplets. We should store these triplets in the network now.
        """
        if not triplets:
            self.logger.info("Content generated no triplets - won't send out storage requests")
            return

        content_keys: List[int] = Content.get_keys(content.identifier, num_keys=self.replication_factor)
        print(content_keys)
        # TODO this should be done in parallel
        for ind in range(self.replication_factor):
            target_node: Optional[SGNode] = await self.search(content_keys[ind])
            if not target_node:
                self.logger.warning("Search node with key %d failed and returned nothing - bailing out.",
                                    content_keys[ind])
                return

            if target_node.key == self.routing_table.key:
                # I'm responsible for storing this data
                for triplet in triplets:
                    self.knowledge_graph.add_triplet(triplet)
                continue

            # Send a storage request to the target node.
            response = await self.send_storage_request(target_node, content.identifier, content_keys[ind])
            if response:
                # Store the triplets on this peer
                triplets_payload = TripletsPayload([triplet.to_payload() for triplet in triplets])
                serialized_payload = self.serializer.pack_serializable(triplets_payload)
                info_json = {"type": "store", "cid": hexlify(content.identifier).decode()}
                ensure_future(self.eva.send_binary(target_node.get_peer(), json.dumps(info_json).encode(), serialized_payload))
            else:
                self.logger.warning("Peer %s refused storage request for key %d",
                                    self.get_short_id(target_node.public_key), content_keys[ind])

    async def send_storage_request(self, target_node: SGNode, content_identifier: bytes, key: int) -> bool:
        cache = StorageRequestCache(self)
        self.request_cache.add(cache)
        self.ez_send(target_node.get_peer(), StorageRequestPayload(cache.number, content_identifier, key))
        response = await cache.future
        return response

    def should_store(self, content_identifier: bytes, content_key: int) -> bool:
        """
        Determine whether we should store this data element by checking the proximity in the Skip Graph.
        """

        # Check that the content key is derived from the content identifier
        if self.should_verify_key and not Content.verify_key(content_identifier, content_key, self.replication_factor):
            self.logger.warning("Key %d not generated from content with ID %s!",
                                content_key, hexlify(content_identifier).decode())
            return False

        # Verify whether we are responsible for this key
        ln: SGNode = self.routing_table.get(0, LEFT)
        if ln and content_key <= ln.key:
            return False  # A neighbour to the left should actually store this

        rn: SGNode = self.routing_table.get(0, RIGHT)
        if rn and content_key >= rn.key:
            return False  # A neighbour to the right should actually store this

        return True

    @lazy_wrapper(StorageRequestPayload)
    def on_storage_request(self, peer: Peer, payload: StorageRequestPayload):
        self.logger.info("Peer %s received storage request from peer %s for key %d",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), payload.key)
        response = self.should_store(payload.content_identifier, payload.key)
        self.ez_send(peer, StorageResponsePayload(payload.identifier, response))

    @lazy_wrapper(StorageResponsePayload)
    def on_storage_response(self, peer: Peer, payload: StorageResponsePayload):
        self.logger.info("Peer %s received storage response from peer %s (response: %s)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), bool(payload.response))

        if not self.request_cache.has("store", payload.identifier):
            self.logger.warning("store cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("store", payload.identifier)
        cache.future.set_result(payload.response)

    async def peer_is_storing_content(self, peer: Peer, content_hash: bytes) -> bool:
        cache = IsStoringQueryCache(self)
        self.request_cache.add(cache)
        self.ez_send(peer, IsStoringQueryPayload(cache.number, content_hash))
        response = await cache.future
        return response

    @lazy_wrapper(IsStoringQueryPayload)
    def on_is_storing_query(self, peer: Peer, payload: IsStoringQueryPayload):
        self.logger.info("Peer %s received storing query from peer %s for content %s",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()),
                         hexlify(payload.content).decode())
        is_storing = payload.content in self.knowledge_graph.stored_content
        self.ez_send(peer, IsStoringResponsePayload(payload.identifier, payload.content, is_storing))

    @lazy_wrapper(IsStoringResponsePayload)
    def on_is_storing_response(self, peer: Peer, payload: IsStoringResponsePayload):
        self.logger.info("Peer %s received is storing response from peer %s (response: %s)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()),
                         bool(payload.storing))

        if not self.request_cache.has("is-storing", payload.identifier):
            self.logger.warning("is-storing cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("is-storing", payload.identifier)
        cache.future.set_result(payload.storing)

    @lazy_wrapper(SearchFailurePayload)
    async def on_search_failure(self, peer: Peer, payload: SearchFailurePayload):
        """
        A peer has informed us that a search for some content has failed for a particular index.
        Check this and repair the damage if possible.
        """
        if payload.content not in self.knowledge_graph.stored_content:
            self.logger.warning("Peer %s informed us of a failed search for content %s but we're not storing it!",
                                self.get_short_id(peer.public_key.key_to_bin()), hexlify(payload.content).decode())
            return

        if payload.key_index >= self.replication_factor:
            self.logger.warning("Failed key index (%d) equal to or larger than the replication factor (%d)",
                                payload.key_index, self.replication_factor)
            return

        content_keys = Content.get_keys(payload.content, num_keys=self.replication_factor)
        failed_key = content_keys[payload.key_index]

        target_node = await self.search(failed_key)
        if not target_node:
            self.logger.warning("Search node with failing key %d failed and returned nothing.", failed_key)
            return

        is_storing = await self.peer_is_storing_content(target_node.get_peer(), payload.content)
        if not is_storing:
            # The node should store the triplets
            response = await self.send_storage_request(target_node, payload.content, failed_key)
            if response:
                triplets = self.knowledge_graph.get_triplets_of_node(payload.content)
                triplets_payload = TripletsPayload([triplet.to_payload() for triplet in triplets])
                serialized_payload = self.serializer.pack_serializable(triplets_payload)
                info_json = {"type": "store", "cid": hexlify(payload.content).decode()}
                ensure_future(self.eva.send_binary(target_node.get_peer(), json.dumps(info_json).encode(), serialized_payload))

    def start_rule_execution_engine(self):
        self.rule_execution_engine.start()

    async def unload(self):
        await super().unload()
        self.rule_execution_engine.shutdown()
