import random
from asyncio import get_event_loop
from binascii import unhexlify, hexlify
from typing import List, Optional

from bami.dkg.cache import StorageRequestCache, TripletsRequestCache
from bami.dkg.content import Content
from bami.skipgraph import LEFT, RIGHT
from bami.skipgraph.community import SkipGraphCommunity

from bami.dkg.payloads import TripletMessage, StorageRequestPayload, StorageResponsePayload, TripletsRequestPayload, \
    TripletsResponsePayload, TripletsEmptyResponsePayload
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

        self.edge_search_latencies: List[float] = []  # Keep track of the latency of individual edge searches

        self.add_message_handler(TripletMessage, self.on_triplet_message)
        self.add_message_handler(StorageRequestPayload, self.on_storage_request)
        self.add_message_handler(StorageResponsePayload, self.on_storage_response)
        self.add_message_handler(TripletsRequestPayload, self.on_triplets_request)
        self.add_message_handler(TripletsResponsePayload, self.on_triplets_response)
        self.add_message_handler(TripletsEmptyResponsePayload, self.on_empty_triplets_response)

        self.replication_factor: int = 2  # TODO this should be a setting
        self.search_parallelism: int = 2
        self.should_verify_key: bool = True  # TODO this should be a setting

        self.logger.info("The DKG community started!")

    async def search_edges(self, content_hash: bytes) -> List[Triplet]:
        """
        Query the network to fetch incoming/outgoing edges of the node labelled with the content hash.
        """
        start_time = get_event_loop().time()
        content_keys: List[int] = Content.get_keys(content_hash, self.replication_factor)
        random.shuffle(content_keys)  # For load balancing

        # TODO we probably want to send search queries in parallel
        for attempt in range(self.search_parallelism):
            self.logger.info("Peer %s searching for edges (attempt %d/%d)" %
                             (self.get_my_short_id(), attempt, self.search_parallelism))
            key = content_keys[attempt]
            target_node = await self.search(key)
            if not target_node:
                self.logger.warning("Search node with key %d failed and returned nothing.", key)
                self.edge_search_latencies.append(get_event_loop().time() - start_time)
                continue

            # Query the target node directly for the edges.
            # TODO could we integrate this directly in our SearchPayload at the lower layer? Might save a message?

            cache = TripletsRequestCache(self)
            self.request_cache.add(cache)
            self.ez_send(target_node.get_peer(), TripletsRequestPayload(cache.number, content_hash))
            triplets = await cache.future
            if triplets:
                return triplets
            self.edge_search_latencies.append(get_event_loop().time() - start_time)
        return []

    @lazy_wrapper(TripletsRequestPayload)
    def on_triplets_request(self, peer: Peer, payload: TripletsRequestPayload):
        triplets = self.knowledge_graph.get_triplets_of_node(payload.content)
        if not triplets:
            self.ez_send(peer, TripletsEmptyResponsePayload(payload.identifier, payload.content))
            return

        for triplet in triplets:
            rp = TripletsResponsePayload(payload.identifier, payload.content, len(triplets), triplet.to_payload())
            self.ez_send(peer, rp)

    @lazy_wrapper(TripletsResponsePayload)
    def on_triplets_response(self, peer: Peer, payload: TripletsResponsePayload):
        self.logger.info("Peer %s received triplets response from peer %s",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()))

        if not self.request_cache.has("triplets", payload.identifier):
            self.logger.warning("triplets cache with id %s not found", payload.identifier)
            return

        cache: TripletsRequestCache = self.request_cache.get("triplets", payload.identifier)
        cache.on_triplet_response(payload)

    @lazy_wrapper(TripletsEmptyResponsePayload)
    def on_empty_triplets_response(self, peer: Peer, payload: TripletsResponsePayload):
        self.logger.info("Peer %s received empty triplets response from peer %s",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()))

        if not self.request_cache.has("triplets", payload.identifier):
            self.logger.warning("triplets cache with id %s not found", payload.identifier)
            return

        cache: TripletsRequestCache = self.request_cache.pop("triplets", payload.identifier)
        cache.future.set_result([])

    async def on_new_triplets_generated(self, content: Content, triplets: List[Triplet]):
        """
        The rule engine generated new triplets. We should store these triplets in the network now.
        """
        if not triplets:
            self.logger.info("Content generated no triplets - won't send out storage requests")
            return

        content_keys: List[int] = Content.get_keys(content.identifier, self.replication_factor)
        # TODO this should be done in parallel
        for ind in range(self.replication_factor):
            target_node: Optional[SGNode] = await self.search(content_keys[ind])
            if not target_node:
                self.logger.warning("Search node with key %d failed and returned nothing - bailing out.",
                                    content_keys[ind])
                return

            # Send a storage request to the target node.
            response = await self.send_storage_request(target_node, content.identifier, content_keys[ind])
            if response:
                # Store the triplets on this peer
                # TODO we should probably use EVA here instead of sending individual Triplet messages
                for triplet in triplets:
                    self.ez_send(target_node.get_peer(), TripletMessage(triplet.to_payload()))
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

    @lazy_wrapper(TripletMessage)
    def on_triplet_message(self, peer: Peer, payload: TripletMessage):
        # TODO we should verify whether the triplets are valid, etc...
        self.logger.info("Peer %s received triplet message from peer %s",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()))
        self.knowledge_graph.add_triplet(payload.triplet)

    def start_rule_execution_engine(self):
        self.rule_execution_engine.start()

    async def unload(self):
        self.rule_execution_engine.shutdown()
