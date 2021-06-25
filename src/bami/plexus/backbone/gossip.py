from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from asyncio import Queue, sleep
from binascii import hexlify
from logging import Logger
from typing import Union

from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.plexus.backbone.datastore.frontiers import Frontier, FrontierDiff
from bami.plexus.backbone.payload import (
    BlocksRequestPayload,
    FrontierPayload,
    FrontierResponsePayload,
)
from bami.plexus.backbone.peer_selection import NextPeerSelectionStrategy, SmartPeerSelectionStrategy

from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer


class GossipRoutines(ABC):
    @property
    @abstractmethod
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        pass

    @abstractmethod
    def frontier_gossip_sync_task(self, subcom_id: bytes) -> None:
        """Start of the gossip state machine"""
        pass

    @abstractmethod
    def incoming_frontier_queue(self, subcom_id: bytes) -> Queue:
        pass


class GossipFrontiersMixin(
    GossipRoutines, MessageStateMachine, CommunityRoutines, metaclass=ABCMeta,
):
    COMMUNITY_CACHE = "gossip_cache"
    logger: Logger

    def frontier_gossip_sync_task(self, subcom_id: bytes) -> None:
        """
        Periodic task that gossips the frontier to connected peers in the community.
        """
        chain = self.persistence.get_chain(subcom_id)
        if not chain:
            self.logger.debug(
                "Skipping the gossip round - no chain for %s", hexlify(subcom_id).decode()
            )
        else:
            frontier = chain.frontier
            # Select next peers for the gossip round
            next_peers = self.gossip_strategy.get_next_gossip_peers(
                subcom_id,
                subcom_id,
                frontier,
                self.settings.frontier_gossip_fanout,
            )
            for peer in next_peers:
                self.logger.debug(
                    "Sending frontier %s to peer %s",
                    frontier,
                    peer,
                )
                self.send_packet(
                    peer, FrontierPayload(subcom_id, frontier.to_bytes())
                )

    async def process_frontier_queue(self, subcom_id: bytes) -> None:
        while True:
            peer, frontier, should_respond = await self.incoming_frontier_queue(
                subcom_id
            ).get()
            self.persistence.store_last_frontier(
                subcom_id, peer.public_key.key_to_bin(), frontier
            )
            frontier_diff = self.persistence.reconcile(
                subcom_id, frontier, peer.public_key.key_to_bin()
            )
            if frontier_diff.is_empty():
                # Move to the next
                await sleep(0.001)
            else:
                # Request blocks and wait for some time
                self.logger.debug(
                    "Sending frontier diff %s to peer %s",
                    frontier_diff,
                    peer,
                )
                self.send_packet(
                    peer, BlocksRequestPayload(subcom_id, frontier_diff.to_bytes())
                )
            # Send frontier response:
            chain = self.persistence.get_chain(subcom_id)
            if chain and should_respond:
                self.send_packet(
                    peer, FrontierResponsePayload(subcom_id, chain.frontier.to_bytes())
                )

    def process_frontier_payload(
        self,
        peer: Peer,
        payload: Union[FrontierPayload, FrontierResponsePayload],
        should_respond: bool,
    ) -> None:
        frontier = Frontier.from_bytes(payload.frontier)
        chain_id = payload.chain_id
        # Process frontier
        if self.incoming_frontier_queue(chain_id):
            self.incoming_frontier_queue(chain_id).put_nowait(
                (peer, frontier, should_respond)
            )
        else:
            self.logger.error("Received unexpected frontier %s", hexlify(chain_id).decode())

    @lazy_wrapper(FrontierPayload)
    def received_frontier(self, peer: Peer, payload: FrontierPayload) -> None:
        self.process_frontier_payload(peer, payload, should_respond=True)

    @lazy_wrapper(FrontierResponsePayload)
    def received_frontier_response(
        self, peer: Peer, payload: FrontierResponsePayload
    ) -> None:
        self.process_frontier_payload(peer, payload, should_respond=False)

    @lazy_wrapper(BlocksRequestPayload)
    def received_blocks_request(
        self, peer: Peer, payload: BlocksRequestPayload
    ) -> None:
        f_diff = FrontierDiff.from_bytes(payload.frontier_diff)
        chain_id = payload.subcom_id
        vals_to_request = set()
        self.logger.debug(
            "Received block request %s from peer %s",
            f_diff,
            peer,
        )
        blocks_blob = self.persistence.get_block_blobs_by_frontier_diff(
            chain_id, f_diff, vals_to_request
        )
        self.logger.debug(
            "Sending %s blocks to peer %s",
            len(blocks_blob),
            peer
        )
        for block_blob in blocks_blob:
            block = PlexusBlock.unpack(block_blob)
            self.send_packet(peer, block.to_block_payload())

    def setup_messages(self) -> None:
        self.add_message_handler(FrontierPayload, self.received_frontier)
        self.add_message_handler(
            FrontierResponsePayload, self.received_frontier_response
        )
        self.add_message_handler(BlocksRequestPayload, self.received_blocks_request)


class SubComGossipMixin(
    GossipFrontiersMixin, SmartPeerSelectionStrategy, metaclass=ABCMeta
):
    @property
    def gossip_strategy(self) -> NextPeerSelectionStrategy:
        return self
