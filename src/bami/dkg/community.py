from binascii import unhexlify
from typing import List

from bami.dkg.cache import StorageRequestCache
from bami.dkg.content import Content
from bami.skipgraph.community import SkipGraphCommunity

from bami.dkg.payloads import TripletsMessage, StorageRequestPayload, StorageResponsePayload
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

        self.add_message_handler(TripletsMessage, self.on_triplet_message)
        self.add_message_handler(StorageRequestPayload, self.on_storage_request)
        self.add_message_handler(StorageResponsePayload, self.on_storage_response)

        self.logger.info("The DKG community started!")

    async def search_edges(self, content_hash: bytes) -> List[Triplet]:
        """
        Query the network to fetch incoming/outgoing edges of the node labelled with the content hash.
        """
        key = int.from_bytes(content_hash, 'big') % (2 ** 32)
        target_node = await self.search(key)
        if not target_node:
            self.logger.warning("Search node with key %d failed and returned nothing - bailing out.", key)
            return []

        # Query the target node directly for the edges.
        # TODO could we model it such that each node is responsible for multiple keys in the SG? Each node in the SG then represents a content item and could still have an IP attached.

    async def on_new_triplets_generated(self, content: Content, triplets: List[Triplet]):
        """
        The rule engine generated new triplets. We should store these triplets in the network now.
        """
        # TODO should we also store the other end of an edge on the respective node?
        target_node = await self.search(content.get_key())
        if not target_node:
            self.logger.warning("Search node with key %d failed and returned nothing - bailing out.", content.get_key())
            return

        # Send a storage request to the target node.
        # TODO send to parallel nodes for replication + let the receiver verify whether it is actually responsible
        response = await self.send_storage_request(target_node, content)
        if response:
            # Store the triplets on this peer
            # TODO we should probably use EVA here instead of sending individual Triplet messages
            for triplet in triplets:
                self.ez_send(target_node.get_peer(), TripletsMessage([triplet.to_payload()]))
        else:
            self.logger.warning("Peer %s refused storage request for key %d",
                                target_node.public_key.key_to_bin(), content.get_key())

    async def send_storage_request(self, target_node: SGNode, content: Content) -> bool:
        cache = StorageRequestCache(self)
        self.request_cache.add(cache)
        self.ez_send(target_node.get_peer(), StorageRequestPayload(cache.number, content.get_key()))
        response = await cache.future
        return response

    def should_store(self, content_key) -> bool:
        # TODO we should probably do some verification here, e.g., if this node is the designated node to store it (by checking the right neighbour in the SG)
        return True

    @lazy_wrapper(StorageRequestPayload)
    def on_storage_request(self, peer: Peer, payload: StorageRequestPayload):
        self.logger.info("Peer %s received storage request from peer %s for key %d",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), payload.key)
        response = self.should_store(payload.key)
        self.ez_send(peer, StorageResponsePayload(payload.identifier, response))

    @lazy_wrapper(StorageResponsePayload)
    def on_storage_response(self, peer: Peer, payload: StorageResponsePayload):
        self.logger.info("Peer %s received storage response from peer %s",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()))

        if not self.request_cache.has("store", payload.identifier):
            self.logger.warning("store cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("store", payload.identifier)
        cache.future.set_result(payload.response)

    @lazy_wrapper(TripletsMessage)
    def on_triplet_message(self, _: Peer, payload: TripletsMessage):
        # TODO we should verify whether the triplets are valid, etc...
        for triplet in payload.triplets:
            self.knowledge_graph.add_triplet(triplet)

    def start_rule_execution_engine(self):
        self.rule_execution_engine.start()

    async def unload(self):
        self.rule_execution_engine.shutdown()
