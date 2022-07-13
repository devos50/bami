import random
from binascii import unhexlify, hexlify
from typing import Optional, Dict

from bami.skipgraph import RIGHT, LEFT, Direction
from bami.skipgraph.cache import MaxLevelRequestCache, SearchRequestCache, NeighbourRequestCache, LinkRequestCache, \
    BuddyCache
from bami.skipgraph.membership_vector import MembershipVector
from bami.skipgraph.node import SGNode
from bami.skipgraph.payload import SearchPayload, SearchResponsePayload, NodeInfoPayload, MaxLevelRequestPayload, \
    MaxLevelResponsePayload, NeighbourRequestPayload, NeighbourResponsePayload, GetLinkPayload, SetLinkPayload, \
    BuddyPayload
from bami.skipgraph.routing_table import RoutingTable
from ipv8.community import Community
from ipv8.lazy_community import lazy_wrapper
from ipv8.messaging.payload import IntroductionRequestPayload, IntroductionResponsePayload
from ipv8.requestcache import RequestCache
from ipv8.types import Peer


class SkipGraphCommunity(Community):
    community_id = unhexlify("d37c847a628e2414dffb0a4646b7fa0999fba888")

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the Skip Graph community.
        """
        super(SkipGraphCommunity, self).__init__(*args, **kwargs)
        self.peers_info: Dict[Peer, SGNode] = {}
        self.routing_table: Optional[RoutingTable] = None

        self.request_cache = RequestCache()

        self.add_message_handler(SearchPayload.msg_id, self.on_search_request)
        self.add_message_handler(SearchResponsePayload, self.on_search_response)
        self.add_message_handler(MaxLevelRequestPayload, self.on_max_level_request)
        self.add_message_handler(MaxLevelResponsePayload, self.on_max_level_response)
        self.add_message_handler(NeighbourRequestPayload, self.on_neighbour_request)
        self.add_message_handler(NeighbourResponsePayload, self.on_neighbour_response)
        self.add_message_handler(GetLinkPayload, self.on_get_link)
        self.add_message_handler(SetLinkPayload, self.on_set_link)
        self.add_message_handler(BuddyPayload, self.on_buddy)

        self.logger.info("Skip Graph community initialized. Short ID: %s", self.get_my_short_id())

    def get_my_short_id(self) -> str:
        return hexlify(self.my_peer.public_key.key_to_bin()).decode()[-8:]

    def get_short_id(self, peer_pk: bytes) -> str:
        return hexlify(peer_pk).decode()[-8:]

    def initialize_routing_table(self, key: int, mv: Optional[MembershipVector] = None):
        self.logger.info("Node %s initializing routing table with key %d and MV %s", self.get_my_short_id(), key, mv)
        self.routing_table = RoutingTable(key, mv or MembershipVector())

    def get_my_node(self) -> SGNode:
        my_pk = self.my_peer.public_key.key_to_bin()
        return SGNode(self.my_estimated_wan, my_pk, self.routing_table.key, self.routing_table.mv)

    @lazy_wrapper(SearchPayload)
    def on_search_request(self, peer: Peer, payload: SearchPayload):
        self.logger.info("Peer %s received search request from peer %s for key %d (start at level %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()),
                         payload.search_key, payload.level)

        originator_node = SGNode.from_payload(payload.originator)

        if self.routing_table.key == payload.search_key:
            # Send this nodes' info back to the search originator
            response_payload = SearchResponsePayload(payload.identifier, self.get_my_node().to_payload())
            self.ez_send(originator_node.get_peer(), response_payload)
            return
        elif self.routing_table.key < payload.search_key:
            level = min(payload.level, self.routing_table.height() - 1)
            while level >= 0:
                neighbour = self.routing_table.levels[level].neighbors[RIGHT]
                if neighbour and neighbour.key <= payload.search_key:
                    response_payload = SearchPayload(payload.identifier, payload.originator, payload.search_key, level)
                    self.ez_send(neighbour.get_peer(), response_payload)
                    return
                else:
                    level -= 1

            # We exhausted our search to the right - return ourselves as result to the search originator
            self.logger.debug("Peer %s exhausted search to the right - returning self as search result",
                              self.get_my_short_id())
            response_payload = SearchResponsePayload(payload.identifier, self.get_my_node().to_payload())
            self.ez_send(originator_node.get_peer(), response_payload)
        else:
            level = min(payload.level, self.routing_table.height() - 1)
            while level >= 0:
                neighbour = self.routing_table.levels[level].neighbors[LEFT]
                if neighbour and neighbour.key >= payload.search_key:
                    response_payload = SearchPayload(payload.identifier, payload.originator, payload.search_key, level)
                    self.ez_send(neighbour.get_peer(), response_payload)
                    return
                else:
                    level -= 1

            # Search on the left level exhausted - return the left neighbour at level 0
            left_neighbour = self.routing_table.levels[0].neighbors[LEFT]
            if left_neighbour:
                self.logger.debug("Peer %s exhausted search - returning left neighbour as search result",
                                  self.get_my_short_id())
                response_payload = SearchResponsePayload(payload.identifier, left_neighbour.to_payload())
                self.ez_send(originator_node.get_peer(), response_payload)
            else:
                # We also don't have a left neighbour - return ourselves as last resort
                response_payload = SearchResponsePayload(payload.identifier, self.get_my_node().to_payload())
                self.ez_send(originator_node.get_peer(), response_payload)

    @lazy_wrapper(SearchResponsePayload)
    def on_search_response(self, peer: Peer, payload: SearchResponsePayload):
        self.logger.info("Peer %s received search response from peer %s (key: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), payload.response.key)

        if not self.request_cache.has("search", payload.identifier):
            self.logger.warning("search cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("search", payload.identifier)
        node = SGNode.from_payload(payload.response)
        cache.future.set_result(node)

    async def get_max_level(self, peer: Peer) -> Optional[int]:
        """
        Request the maximum level in the Skip Graph of a target peer.
        """
        cache = MaxLevelRequestCache(self)
        self.request_cache.add(cache)
        self.ez_send(peer, MaxLevelRequestPayload(cache.number))
        max_level = await cache.future
        return max_level

    @lazy_wrapper(MaxLevelRequestPayload)
    def on_max_level_request(self, peer: Peer, payload: MaxLevelRequestPayload):
        self.logger.info("Received max level request payload from peer %s", peer)
        if not self.routing_table:
            self.logger.warning("Routing table not initialized - not responding to max level request")
            return

        self.ez_send(peer, MaxLevelResponsePayload(payload.identifier, self.routing_table.max_level))

    @lazy_wrapper(MaxLevelResponsePayload)
    def on_max_level_response(self, peer: Peer, payload: MaxLevelResponsePayload):
        self.logger.info("Received max level response payload from peer %s (max lvl: %d)", peer, payload.max_level)
        if not self.request_cache.has("max-level", payload.identifier):
            self.logger.warning("max level cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("max-level", payload.identifier)
        cache.future.set_result(payload.max_level)

    async def get_neighbour(self, peer: Peer, side: Direction, level: int) -> Optional[SGNode]:
        """
        Query the left/right neighbour of a particular peer in its Skip Graph.
        """
        self.logger.info("Peer %s querying neighbour of peer %s (side: %d, level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), side, level)

        cache = NeighbourRequestCache(self)
        self.request_cache.add(cache)
        self.ez_send(peer, NeighbourRequestPayload(cache.number, side, level))
        node = await cache.future
        return node

    @lazy_wrapper(NeighbourRequestPayload)
    def on_neighbour_request(self, peer: Peer, payload: NeighbourRequestPayload):
        self.logger.info("Peer %s received neighbour request payload from peer %s (side: %d, level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()),
                         payload.side, payload.level)
        if not self.routing_table:
            self.logger.warning("Routing table not initialized - not responding to neighbour request")
            return

        if self.routing_table.height() < payload.level + 1:
            self.logger.warning("Neighbour at level %d not found - returning empty response", payload.level)
            self.ez_send(peer, NeighbourResponsePayload(payload.identifier, False, SGNode.empty().to_payload()))
            return

        neighbouring_node = self.routing_table.levels[payload.level].neighbors[payload.side]
        if neighbouring_node:
            self.ez_send(peer, NeighbourResponsePayload(payload.identifier, True, neighbouring_node.to_payload()))
            return

        self.ez_send(peer, NeighbourResponsePayload(payload.identifier, False, SGNode.empty().to_payload()))

    @lazy_wrapper(NeighbourResponsePayload)
    def on_neighbour_response(self, peer: Peer, payload: NeighbourResponsePayload):
        self.logger.info("Peer %s received neighbour response payload from peer %s (found? %s, key: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), bool(payload.found),
                         payload.neighbour.key)

        if not self.request_cache.has("neighbour", payload.identifier):
            self.logger.warning("neighbour cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("neighbour", payload.identifier)
        cache.future.set_result((payload.found, SGNode.from_payload(payload.neighbour)))

    async def search(self, key: int, introducer_peer: Optional[Peer] = None, other_max_lvl: Optional[int] = None) -> SGNode:
        cache = SearchRequestCache(self)
        self.request_cache.add(cache)
        if introducer_peer:
            self.ez_send(introducer_peer, SearchPayload(cache.number, self.get_my_node().to_payload(),
                                                        key, max(other_max_lvl - 1, 0)))
        else:
            # This is a regular search. Send a Search message to yourself to initiate the search.
            self.ez_send(self.my_peer, SearchPayload(cache.number, self.get_my_node().to_payload(),
                                                     key, self.routing_table.height() - 1))
            pass

        response = await cache.future
        return response

    async def get_link(self, peer: Peer, side: Direction, level: int):
        self.logger.info("Peer %s sending get link message to peer %s (side: %d, level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), side, level)

        cache = LinkRequestCache(self)
        self.request_cache.add(cache)
        self.ez_send(peer, GetLinkPayload(cache.number, self.get_my_node().to_payload(), side, level))
        node = await cache.future
        return node

    def change_neighbour(self, identifier: int, node: SGNode, side: Direction, level: int):
        side_str = "left" if side == LEFT else "right"
        self.logger.info("Peer %s changing %s neighbour at level %d to %s", self.get_my_short_id(), side_str, level, node)
        neighbour = self.routing_table.levels[level].neighbors[side]
        if (side == RIGHT and neighbour is not None and neighbour.key < node.key) or \
                (side == LEFT and neighbour is not None and neighbour.key > node.key):
            to_peer = self.routing_table.levels[level].neighbors[side].get_peer()
            self.ez_send(to_peer, GetLinkPayload(identifier, node.to_payload(), side, level))
        else:
            self.ez_send(node.get_peer(), SetLinkPayload(identifier, self.get_my_node().to_payload(), level))

        self.routing_table.levels[level].neighbors[side] = node

    @lazy_wrapper(GetLinkPayload)
    def on_get_link(self, peer: Peer, payload: GetLinkPayload):
        self.logger.info("Peer %s received get link message from peer %s (side: %d, level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()),
                         payload.side, payload.level)

        self.change_neighbour(payload.identifier, SGNode.from_payload(payload.originator), payload.side, payload.level)

    @lazy_wrapper(SetLinkPayload)
    def on_set_link(self, peer: Peer, payload: SetLinkPayload):
        self.logger.info("Received set link response payload from peer %s",
                         self.get_short_id(peer.public_key.key_to_bin()))
        cache = None

        if self.request_cache.has("link", payload.identifier):
            cache = self.request_cache.pop("link", payload.identifier)
        if self.request_cache.has("buddy", payload.identifier):
            cache = self.request_cache.pop("buddy", payload.identifier)

        if not cache:
            self.logger.warning("link/buddy cache with identifier %d not found!", payload.identifier)
            return

        node = SGNode.from_payload(payload.new_neighbour)
        if node.is_empty():
            node = None
        cache.future.set_result(node)

    async def do_buddy_request(self, peer: Peer, originator: SGNode, level: int, val: int, side: Direction) -> SGNode:
        self.logger.info("Peer %s sending buddy request to peer %s (level: %d, val: %d, side: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), level, val, side)
        cache = BuddyCache(self)
        self.request_cache.add(cache)
        self.ez_send(peer, BuddyPayload(cache.number, originator.to_payload(), level, val, side))
        node = await cache.future
        return node

    @lazy_wrapper(BuddyPayload)
    def on_buddy(self, peer: Peer, payload: BuddyPayload):
        self.logger.info("Peer %s received buddy message from peer %s (val: %d, side: %d, level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()),
                         payload.val, payload.side, payload.level)

        val: int = payload.val
        level: int = payload.level
        side: Direction = payload.side
        other_side: Direction = LEFT if side == RIGHT else RIGHT
        originator = SGNode.from_payload(payload.originator)

        if self.routing_table.mv.val[level] == val:
            self.change_neighbour(payload.identifier, originator, side, level + 1)
        else:
            if self.routing_table.levels[level].neighbors[other_side] is not None:
                to_peer = self.routing_table.levels[level].neighbors[other_side].get_peer()
                self.logger.info("Peer %s forwarding buddy request to peer %s (val: %d, side: %d, level: %d)",
                                 self.get_my_short_id(), self.get_short_id(to_peer.public_key.key_to_bin()), val, side,
                                 level)
                self.ez_send(to_peer, BuddyPayload(payload.identifier, originator.to_payload(), level, val, side))
            else:
                self.logger.info("Peer %s will not link at level %d", self.get_my_short_id(), level)
                self.ez_send(originator.get_peer(),
                             SetLinkPayload(payload.identifier, SGNode.empty().to_payload(), level))

    async def join(self, introducer_peer: Optional[Peer] = None) -> None:
        """
        Join the Skip Graph.
        """
        self.logger.info("Peer %s joining the Skip Graph (key: %d, mv: %s)",
                         self.get_my_short_id(), self.routing_table.key, self.routing_table.mv)
        if not introducer_peer:
            peers = self.get_peers()
            if not peers:
                self.logger.warning("Unable to join the Skip Graph - no introducer peers available!")
                return

            # Step 1. search for your own key to get the node with the largest key smaller than this node's key.
            introducer_peer = random.choice(peers)
            if introducer_peer not in self.peers_info:
                self.logger.warning("Unable to join the Skip Graph - no info on the introducer node")
                return

        introducer_peer_info = self.peers_info[introducer_peer]
        self.logger.debug("Peer %s selected peer %s for Skip Graph introduction (key: %d, mv: %s)",
                          self.get_my_short_id(), self.get_short_id(introducer_peer.public_key.key_to_bin()),
                          introducer_peer_info.key, introducer_peer_info.mv)

        if introducer_peer_info.key < self.routing_table.key:
            side = RIGHT
            other_side = LEFT
        else:
            side = LEFT
            other_side = RIGHT

        max_level = await self.get_max_level(introducer_peer)
        other_side_neighbour = await self.search(self.routing_table.key,
                                                 introducer_peer=introducer_peer, other_max_lvl=max_level)
        if other_side_neighbour.key == self.routing_table.key:
            self.logger.warning("Node with key %d is already registered in the Skip Graph!", other_side_neighbour.key)
            return

        self.logger.info("Peer %s established other side neighbour with key %s during join!",
                         self.get_my_short_id(), other_side_neighbour.key)

        found, side_neighbour = await self.get_neighbour(other_side_neighbour.get_peer(), side, 0)
        new_neighbour = await self.get_link(other_side_neighbour.get_peer(), side, 0)
        self.routing_table.levels[0].neighbors[other_side] = new_neighbour
        if found:
            new_neighbour = await self.get_link(side_neighbour.get_peer(), other_side, 0)
            self.routing_table.levels[0].neighbors[side] = new_neighbour

        # Now that we have set the neighbours in lvl 0, we continue and set the neighbours in higher levels.
        self.logger.info("Joining phase 2")
        level = 0
        while True:
            level += 1
            self.logger.info("Joining phase 2 - level %d", level)
            if level > MembershipVector.LENGTH:
                break

            if self.routing_table.levels[level - 1].neighbors[RIGHT] is not None:
                peer = self.routing_table.levels[level - 1].neighbors[RIGHT].get_peer()
                neighbour = await self.do_buddy_request(peer, self.get_my_node(), level - 1,
                                                        self.routing_table.mv.val[level - 1], LEFT)
                self.routing_table.levels[level].neighbors[RIGHT] = neighbour
            else:
                self.routing_table.levels[level].neighbors[RIGHT] = None

            if self.routing_table.levels[level - 1].neighbors[LEFT] is not None:
                peer = self.routing_table.levels[level - 1].neighbors[LEFT].get_peer()
                neighbour = await self.do_buddy_request(peer, self.get_my_node(), level - 1,
                                                        self.routing_table.mv.val[level - 1], RIGHT)
                self.routing_table.levels[level].neighbors[LEFT] = neighbour
            else:
                self.routing_table.levels[level].neighbors[LEFT] = None

            if self.routing_table.levels[level].neighbors[RIGHT] is None and \
                    self.routing_table.levels[level].neighbors[LEFT] is None:
                break

        self.routing_table.max_level = level
        self.logger.info("Peer %s has joined the Skip Graph!", self.get_my_short_id())

    def create_introduction_request(self, socket_address, extra_bytes=b'', new_style=False, prefix=None):
        extra_payload = SGNode(self.my_estimated_wan, self.my_peer.public_key.key_to_bin(),
                               self.routing_table.key, self.routing_table.mv).to_payload()
        extra_bytes = self.serializer.pack_serializable(extra_payload)
        return super(SkipGraphCommunity, self).create_introduction_request(socket_address, extra_bytes)

    def create_introduction_response(self, lan_socket_address, socket_address, identifier,
                                     introduction=None, extra_bytes=b'', prefix=None, new_style=False):
        extra_payload = SGNode(self.my_estimated_wan, self.my_peer.public_key.key_to_bin(),
                               self.routing_table.key, self.routing_table.mv).to_payload()
        extra_bytes = self.serializer.pack_serializable(extra_payload)
        return super(SkipGraphCommunity, self).create_introduction_response(lan_socket_address, socket_address,
                                                                            identifier, introduction, extra_bytes,
                                                                            prefix, new_style)

    def parse_node_info(self, peer: Peer, node_info_bytes: bytes):
        payload = self.serializer.unpack_serializable(NodeInfoPayload, node_info_bytes)[0]
        self.peers_info[peer] = SGNode.from_payload(payload)

    def introduction_request_callback(self, peer: Peer, _, payload: IntroductionRequestPayload) -> None:
        self.parse_node_info(peer, payload.extra_bytes)

    def introduction_response_callback(self, peer: Peer, _, payload: IntroductionResponsePayload) -> None:
        self.parse_node_info(peer, payload.extra_bytes)

    async def unload(self):
        await self.request_cache.shutdown()
        await super().unload()
