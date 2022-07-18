import random
from asyncio import get_event_loop
from binascii import unhexlify, hexlify
from typing import Optional, Dict, List

from ipv8.util import fail

from bami.skipgraph import RIGHT, LEFT, Direction
from bami.skipgraph.cache import SearchRequestCache, NeighbourRequestCache, LinkRequestCache, BuddyCache, DeleteCache, \
    FindNewNeighbourCache, SetNeighbourNilCache
from bami.skipgraph.membership_vector import MembershipVector
from bami.skipgraph.node import SGNode
from bami.skipgraph.payload import SearchPayload, SearchResponsePayload, NodeInfoPayload, NeighbourRequestPayload, \
    NeighbourResponsePayload, GetLinkPayload, SetLinkPayload, \
    BuddyPayload, DeletePayload, NoNeighbourPayload, FindNewNeighbourPayload, ConfirmDeletePayload, \
    FoundNewNeighbourPayload, SetNeighbourNilPayload
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

        # Join messages
        self.add_message_handler(NeighbourRequestPayload, self.on_neighbour_request)
        self.add_message_handler(NeighbourResponsePayload, self.on_neighbour_response)
        self.add_message_handler(GetLinkPayload, self.on_get_link)
        self.add_message_handler(SetLinkPayload, self.on_set_link)
        self.add_message_handler(BuddyPayload, self.on_buddy)

        # Leave messages
        self.add_message_handler(DeletePayload, self.on_delete)
        self.add_message_handler(NoNeighbourPayload, self.on_no_neighbour)
        self.add_message_handler(FindNewNeighbourPayload, self.on_find_new_neighbour)
        self.add_message_handler(FoundNewNeighbourPayload, self.on_found_new_neighbour)
        self.add_message_handler(ConfirmDeletePayload, self.on_confirm_delete)
        self.add_message_handler(SetNeighbourNilPayload, self.on_set_neighbour_nil)

        self.search_hops: Dict[int, int] = {}  # Keep track of the number of hops per search
        self.search_latencies: List[int] = []  # Keep track of the latency of individual searches

        self.is_leaving: bool = False  # Whether we are leaving the Skip Graph

        self.logger.info("Skip Graph community initialized. Short ID: %s", self.get_my_short_id())

    def get_my_short_id(self) -> str:
        return hexlify(self.my_peer.public_key.key_to_bin()).decode()[-8:]

    def get_short_id(self, peer_pk: bytes) -> str:
        return hexlify(peer_pk).decode()[-8:]

    def initialize_routing_table(self, key: int, mv: Optional[MembershipVector] = None):
        mv = mv or MembershipVector()
        self.logger.info("Node %s initializing routing table with key %d and MV %s", self.get_my_short_id(), key, mv)
        self.routing_table = RoutingTable(key, mv)

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
            response_payload = SearchResponsePayload(payload.identifier, self.get_my_node().to_payload(), payload.hops)
            self.ez_send(originator_node.get_peer(), response_payload)
            return
        elif self.routing_table.key < payload.search_key:
            level = min(payload.level, self.routing_table.height() - 1)
            while level >= 0:
                neighbour = self.routing_table.get(level, RIGHT)
                if neighbour and neighbour.key <= payload.search_key:
                    response_payload = SearchPayload(payload.identifier, payload.originator, payload.search_key,
                                                     level, payload.hops + 1)
                    self.ez_send(neighbour.get_peer(), response_payload)
                    return
                else:
                    level -= 1

            # We exhausted our search to the right - return ourselves as result to the search originator
            self.logger.debug("Peer %s exhausted search to the right - returning self as search result",
                              self.get_my_short_id())
            response_payload = SearchResponsePayload(payload.identifier, self.get_my_node().to_payload(), payload.hops)
            self.ez_send(originator_node.get_peer(), response_payload)
        else:
            level = min(payload.level, self.routing_table.height() - 1)
            while level >= 0:
                neighbour = self.routing_table.get(level, LEFT)
                if neighbour and neighbour.key >= payload.search_key:
                    response_payload = SearchPayload(payload.identifier, payload.originator, payload.search_key,
                                                     level, payload.hops + 1)
                    self.ez_send(neighbour.get_peer(), response_payload)
                    return
                else:
                    level -= 1

            # Search on the left level exhausted - return the left neighbour at level 0
            left_neighbour = self.routing_table.get(0, LEFT)
            if left_neighbour:
                self.logger.debug("Peer %s exhausted search - returning left neighbour as search result",
                                  self.get_my_short_id())
                response_payload = SearchResponsePayload(payload.identifier, left_neighbour.to_payload(), payload.hops)
                self.ez_send(originator_node.get_peer(), response_payload)
            else:
                # We also don't have a left neighbour - return ourselves as last resort
                response_payload = SearchResponsePayload(payload.identifier, self.get_my_node().to_payload(),
                                                         payload.hops)
                self.ez_send(originator_node.get_peer(), response_payload)

    @lazy_wrapper(SearchResponsePayload)
    def on_search_response(self, peer: Peer, payload: SearchResponsePayload):
        self.logger.info("Peer %s received search response from peer %s (resulting key: %d, hops: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()),
                         payload.response.key, payload.hops)

        if not self.request_cache.has("search", payload.identifier):
            self.logger.warning("search cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("search", payload.identifier)

        if payload.hops not in self.search_hops:
            self.search_hops[payload.hops] = 0
        self.search_hops[payload.hops] += 1
        self.search_latencies.append(get_event_loop().time() - cache.start_time)

        node = SGNode.from_payload(payload.response)
        cache.future.set_result(node)

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

        neighbouring_node = self.routing_table.get(payload.level, payload.side)
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

    async def search(self, key: int, introducer_peer: Optional[Peer] = None) -> SGNode:
        self.logger.info("Peer %s initiating search for key %d", self.get_my_short_id(), key)
        cache = SearchRequestCache(self)
        self.request_cache.add(cache)
        if introducer_peer:
            self.ez_send(introducer_peer, SearchPayload(cache.number, self.get_my_node().to_payload(),
                                                        key, self.routing_table.height() - 1, 0))
        else:
            if not self.routing_table:
                self.logger.error("Routing table not initialized! Failing search")
                return fail("Routing table not initialized!")

            # This is a regular search. Send a Search message to yourself to initiate the search.
            self.ez_send(self.my_peer, SearchPayload(cache.number, self.get_my_node().to_payload(),
                                                     key, self.routing_table.height() - 1, 0))
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
        neighbour = self.routing_table.get(level, side)
        if (side == RIGHT and neighbour is not None and neighbour.key < node.key) or \
                (side == LEFT and neighbour is not None and neighbour.key > node.key):
            self.ez_send(neighbour.get_peer(), GetLinkPayload(identifier, node.to_payload(), side, level))
        else:
            self.ez_send(node.get_peer(), SetLinkPayload(identifier, self.get_my_node().to_payload(), level))

        # Only update the neighbour if it's not set or when it's "better" than the current neighbour
        if not neighbour or (side == RIGHT and neighbour is not None and neighbour.key > node.key) or \
            (side == LEFT and neighbour is not None and neighbour.key < node.key):
            self.routing_table.set(level, side, node)

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
            neighbour = self.routing_table.get(level, other_side)
            if neighbour is not None:
                to_peer = neighbour.get_peer()
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

        closest_neighbour = await self.search(self.routing_table.key, introducer_peer=introducer_peer)
        if closest_neighbour.key == self.routing_table.key:
            self.logger.warning("Node with key %d is already registered in the Skip Graph!", closest_neighbour.key)
            return

        self.logger.info("Peer %s established closest neighbour with key %s during join!",
                         self.get_my_short_id(), closest_neighbour.key)

        # There are two possibilities. The closest neighbour has a key that is the greatest key smaller than ours.
        # Or we have the smallest key in the network.
        if closest_neighbour.key < self.routing_table.key:
            # We have to insert ourselves between the closest neighbour and the right neighbour of this closest
            # neighbour.
            found, closest_neighbour_right = await self.get_neighbour(closest_neighbour.get_peer(), RIGHT, 0)

            # Change the left neighbour of the right neighbour
            if found:
                new_neighbour = await self.get_link(closest_neighbour_right.get_peer(), LEFT, 0)
                self.routing_table.set(0, RIGHT, closest_neighbour_right)

            # Change the right neighbour of the left neighbour to point to ourselves
            new_neighbour = await self.get_link(closest_neighbour.get_peer(), RIGHT, 0)
            self.routing_table.set(0, LEFT, closest_neighbour)
        else:
            # We have the smallest key in the network. Simply update the left neighbour of the closest neighbour.
            new_neighbour = await self.get_link(closest_neighbour.get_peer(), LEFT, 0)
            self.routing_table.set(0, RIGHT, closest_neighbour)

        # Now that we have set the neighbours in lvl 0, we continue and set the neighbours in higher levels.
        self.logger.info("Joining phase 2")
        level = 0
        while True:
            level += 1
            self.logger.info("Joining phase 2 - level %d", level)
            if level > MembershipVector.LENGTH:
                break

            if self.routing_table.get(level - 1, RIGHT) is not None:
                peer = self.routing_table.get(level - 1, RIGHT).get_peer()
                neighbour = await self.do_buddy_request(peer, self.get_my_node(), level - 1,
                                                        self.routing_table.mv.val[level - 1], LEFT)
                self.routing_table.set(level, RIGHT, neighbour)
            else:
                self.routing_table.set(level, RIGHT, None)

            if self.routing_table.get(level - 1, LEFT) is not None:
                peer = self.routing_table.get(level - 1, LEFT).get_peer()
                neighbour = await self.do_buddy_request(peer, self.get_my_node(), level - 1,
                                                        self.routing_table.mv.val[level - 1], RIGHT)
                self.routing_table.set(level, LEFT, neighbour)
            else:
                self.routing_table.set(level, LEFT, None)

            if self.routing_table.get(level, RIGHT) is None and self.routing_table.get(level, LEFT) is None:
                break

        self.routing_table.max_level = level
        self.logger.info("Peer %s has joined the Skip Graph!", self.get_my_short_id())

    async def leave(self) -> bool:
        """
        Gracefully leave the skip graph by informing the neighbours at each level.
        """
        self.logger.info("Peer %s will leave the Skip Graph", self.get_my_short_id())
        self.is_leaving = True
        level = self.routing_table.height()
        while level >= 0:
            rn: Optional[SGNode] = self.routing_table.get(level, RIGHT)
            if rn:
                cache = DeleteCache(self)
                self.request_cache.add(cache)
                self.ez_send(rn.get_peer(), DeletePayload(cache.number, self.get_my_node().to_payload(), level))
                confirmed = await cache.future
                if not confirmed:  # We received a NoNeighbour message
                    ln: Optional[SGNode] = self.routing_table.get(level, LEFT)
                    if ln:
                        cache = SetNeighbourNilCache(self)
                        self.request_cache.add(cache)
                        payload = SetNeighbourNilPayload(cache.number, self.get_my_node().to_payload(), level)
                        self.ez_send(ln.get_peer(), payload)
                        confirmed = await cache.future
            else:
                ln: Optional[SGNode] = self.routing_table.get(level, LEFT)
                if ln:
                    # There is no right neighbour. Simply inform our left neighbour to set the right neighbour to nil
                    cache = SetNeighbourNilCache(self)
                    self.request_cache.add(cache)
                    payload = SetNeighbourNilPayload(cache.number, self.get_my_node().to_payload(), level)
                    self.ez_send(ln.get_peer(), payload)
                    confirmed = await cache.future
            self.logger.debug("Peer %s left the Skip Graph at level %d", self.get_my_short_id(), level)
            level -= 1

        self.logger.info("Peer %s left the Skip Graph", self.get_my_short_id())
        self.is_leaving = False
        self.routing_table = None
        return True

    async def find_new_neighbour(self, level: int) -> Optional[SGNode]:
        self.logger.info("Peer %s finding new neighbour (level: %d)", self.get_my_short_id(), level)

        ln: SGNode = self.routing_table.get(level, LEFT)
        cache = FindNewNeighbourCache(self)
        self.request_cache.add(cache)
        self.ez_send(ln.get_peer(), FindNewNeighbourPayload(cache.number, self.get_my_node().to_payload(), level))
        node = await cache.future
        return node

    @lazy_wrapper(DeletePayload)
    async def on_delete(self, peer: Peer, payload: DeletePayload):
        self.logger.info("Peer %s received delete message from peer %s",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()))

        originator_node: SGNode = SGNode.from_payload(payload.originator)

        if self.is_leaving:
            rn: Optional[SGNode] = self.routing_table.get(payload.level, RIGHT)
            if rn:
                # Recurse the delete payload to the right neighbour in the Skip Graph
                self.ez_send(rn.get_peer(), DeletePayload(payload.identifier, payload.originator, payload.level))
            else:
                self.ez_send(originator_node.get_peer(), NoNeighbourPayload(payload.identifier, payload.level))
        else:
            # Find your new left neighbour
            new_neighbour: Optional[SGNode] = await self.find_new_neighbour(payload.level)
            self.routing_table.set(payload.level, LEFT, new_neighbour)
            self.ez_send(originator_node.get_peer(), ConfirmDeletePayload(payload.identifier, payload.level))

    @lazy_wrapper(FindNewNeighbourPayload)
    def on_find_new_neighbour(self, peer: Peer, payload: FindNewNeighbourPayload):
        self.logger.info("Peer %s received find new neighbour message from peer %s (level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), payload.level)

        originator_node: SGNode = SGNode.from_payload(payload.originator)

        if self.is_leaving:
            ln: Optional[SGNode] = self.routing_table.get(payload.level, LEFT)
            if ln:
                new_payload = FindNewNeighbourPayload(payload.identifier, payload.originator, payload.level)
                self.ez_send(ln.get_peer(), new_payload)
            else:
                new_payload = FoundNewNeighbourPayload(payload.identifier, SGNode.empty().to_payload(), payload.level)
                self.ez_send(originator_node.get_peer(), new_payload)
        else:
            new_payload = FoundNewNeighbourPayload(payload.identifier, self.get_my_node().to_payload(), payload.level)
            self.ez_send(originator_node.get_peer(), new_payload)
            self.routing_table.set(payload.level, RIGHT, originator_node)

    @lazy_wrapper(FoundNewNeighbourPayload)
    def on_found_new_neighbour(self, peer: Peer, payload: FoundNewNeighbourPayload):
        self.logger.info("Peer %s received found new neighbour message from peer %s (level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), payload.level)

        if not self.request_cache.has("find-new-neighbour", payload.identifier):
            self.logger.warning("find-new-neighbour cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("find-new-neighbour", payload.identifier)
        new_neighbour = SGNode.from_payload(payload.neighbour)
        if new_neighbour.is_empty():
            new_neighbour = None
        cache.future.set_result(new_neighbour)

    @lazy_wrapper(NoNeighbourPayload)
    def on_no_neighbour(self, peer: Peer, payload: NoNeighbourPayload):
        self.logger.info("Peer %s received no neighbour message from peer %s (level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), payload.level)

        if not self.request_cache.has("delete", payload.identifier):
            self.logger.warning("delete cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("delete", payload.identifier)
        cache.future.set_result(False)

    @lazy_wrapper(ConfirmDeletePayload)
    def on_confirm_delete(self, peer: Peer, payload: ConfirmDeletePayload):
        self.logger.info("Peer %s received confirm delete message from peer %s (level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), payload.level)

        if self.request_cache.has("delete", payload.identifier):
            cache = self.request_cache.pop("delete", payload.identifier)
            cache.future.set_result(True)
        if self.request_cache.has("set-neighbour-nil", payload.identifier):
            cache = self.request_cache.pop("set-neighbour-nil", payload.identifier)
            cache.future.set_result(True)

    @lazy_wrapper(SetNeighbourNilPayload)
    def on_set_neighbour_nil(self, peer: Peer, payload: SetNeighbourNilPayload):
        self.logger.info("Peer %s received set neighbour nil message from peer %s (level: %d)",
                         self.get_my_short_id(), self.get_short_id(peer.public_key.key_to_bin()), payload.level)

        originator_node: SGNode = SGNode.from_payload(payload.originator)

        if self.is_leaving:
            ln: Optional[SGNode] = self.routing_table.get(payload.level, LEFT)
            if ln:
                payload = SetNeighbourNilPayload(payload.identifier, payload.originator, payload.level)
                self.ez_send(ln.get_peer(), payload)
            else:
                payload = ConfirmDeletePayload(payload.identifier, payload.level)
                self.ez_send(originator_node.get_peer(), payload)
        else:
            payload = ConfirmDeletePayload(payload.identifier, payload.level)
            self.ez_send(originator_node.get_peer(), payload)
            self.routing_table.set(payload.level, RIGHT, None)

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
