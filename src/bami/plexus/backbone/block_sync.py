from abc import ABCMeta, abstractmethod
from typing import Union, Iterable, Dict

from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer
from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.plexus.backbone.exceptions import InvalidBlockException
from bami.plexus.backbone.payload import (
    BlockBroadcastPayload,
    BlockPayload,
)


class BlockSyncMixin(MessageStateMachine, CommunityRoutines, metaclass=ABCMeta):
    def setup_messages(self) -> None:
        self.add_message_handler(BlockPayload, self.received_block)
        self.add_message_handler(BlockBroadcastPayload, self.received_block_broadcast)

    def send_block(
        self, block: PlexusBlock, peers: Iterable[Peer], ttl: int = 1
    ) -> None:
        """
        Send a block to the set of peers. If ttl is higher than 1: will gossip the message further.
        Args:
            block: block to send
            peers: set of peers
            ttl: Time to live for the message. If > 1 - this is a multi-hop message
        """
        if ttl > 1:
            # This is a block for gossip
            payload = BlockBroadcastPayload(*block.block_args(), ttl)
        else:
            payload = block.to_block_payload()
        for p in peers:
            self.send_packet(p, payload)

    @lazy_wrapper(BlockPayload)
    def received_block(self, peer: Peer, payload: BlockPayload):
        block = PlexusBlock.from_payload(payload, self.serializer)
        self.logger.debug(
            "Received block %s from peer %s", block, peer
        )
        self.validate_persist_block(block, peer)

    @lazy_wrapper(BlockBroadcastPayload)
    def received_block_broadcast(self, peer: Peer, payload: BlockBroadcastPayload):
        block = PlexusBlock.from_payload(payload, self.serializer)
        self.validate_persist_block(block, peer)
        self.process_broadcast_block(block, payload.ttl)

    def process_broadcast_block(self, block: PlexusBlock, ttl: int):
        """Process broadcast block and relay further"""
        if block.hash not in self.relayed_broadcasts and ttl > 1:
            # self.send_block(block, ttl=ttl - 1)
            pass

    def process_block(self, blk: PlexusBlock, peer: Peer) -> None:
        """
        Process a received half block immediately when received. Does not guarantee order on the block.
        """
        pass

    def process_block_ordered(self, block: PlexusBlock) -> None:
        """
        Process a block that we have received.

        Args:
            block: The received block.

        """
        pass

    def validate_persist_block(self, block: PlexusBlock, peer: Peer = None) -> None:
        """
        Validate a block and if it's valid, persist it.
        Raises:
            InvalidBlockException - if block is not valid
        """
        block_blob = block.pack()

        if not block.block_invariants_valid():
            raise InvalidBlockException("Block invalid", str(block), peer)
        else:
            if not self.persistence.has_block(block.hash):
                self.process_block(block, peer)
                if (
                    self.persistence.get_chain(block.community_id)
                    and self.persistence.get_chain(block.community_id).versions.get(
                        block.community_sequence_number
                    )
                    and block.short_hash
                    in self.persistence.get_chain(block.community_id).versions[
                        block.community_sequence_number
                    ]
                ):
                    raise Exception(
                        "Inconsisistency between block store and chain store",
                        self.persistence.get_chain(block.community_id).versions,
                        block.com_dot,
                    )
                self.persistence.add_block(block_blob, block)

    def create_signed_block(
        self,
        block_type: bytes = b"unknown",
        transaction: Dict = None,
        community_id: bytes = None,
    ) -> PlexusBlock:
        """
        This function will create, sign, persist a block with given parameters.
        Args:
            block_type: bytes of the block
            transaction: bytes blob of the transaction, or None to indicate an empty transaction payload
            community_id: sub-community id if applicable
        Returns:
            signed block
        """
        if not transaction:
            transaction = {}

        block = PlexusBlock.create(
            block_type,
            transaction,
            self.persistence,
            self.my_pub_key_bin,
            community_id=community_id,
        )
        block.sign(self.my_peer.key)
        self.validate_persist_block(block, self.my_peer)
        return block
