from __future__ import annotations

from binascii import hexlify
from collections import namedtuple
from hashlib import sha256
import logging
import time
from typing import Any, List, Dict

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.messaging.serialization import default_serializer, PackError

from bami.plexus.backbone.utils import (
    BytesLinks,
    decode_links,
    Dot,
    EMPTY_PK,
    EMPTY_SIG,
    encode_links,
    GENESIS_DOT,
    GENESIS_LINK,
    GENESIS_SEQ,
    Links,
    shorten,
    UNKNOWN_SEQ, encode_raw, NO_COMMUNITY, decode_raw,
)
from bami.plexus.backbone.payload import BlockPayload

SKIP_ATTRIBUTES = {
    "key",
    "serializer",
    "crypto",
    "_logger",
    "_previous",
    "_links",
}


class PlexusBlock(object):
    """
    Container for Bami block information
    """

    Data = namedtuple(
        "Data",
        [
            "type",
            "transaction",
            "public_key",
            "sequence_number",
            "previous",
            "community_links",
            "community_id",
            "community_sequence_number",
            "timestamp",
            "insert_time",
            "signature",
        ],
    )

    def __init__(self, data: List = None, serializer=default_serializer) -> None:
        """
        Create a new PlexusBlock or load from an existing database entry.

        :param data: Optional data to initialize this block with.
        :type data: Block.Data or list
        :param serializer: An optional custom serializer to use for this block.
        :type serializer: Serializer
        """
        super(PlexusBlock, self).__init__()
        self.serializer = serializer
        if data is None:
            # data
            self.type = b"unknown"
            self.transaction = {}
            self._transaction = encode_raw(self.transaction)
            # block identity
            self.public_key = EMPTY_PK
            self.sequence_number = GENESIS_SEQ

            # previous hash in the personal chain
            self.previous = GENESIS_LINK
            self._previous = encode_links(self.previous)

            # Linked blocks => links to the block in other chains
            self.community_links = GENESIS_LINK
            self._community_links = encode_links(self.community_links)

            # Metadata for community identifiers
            self.community_id = NO_COMMUNITY
            self.community_sequence_number: int = UNKNOWN_SEQ

            # Creation timestamp
            self.timestamp = int(time.time() * 1000)
            # Signature for the block
            self.signature = EMPTY_SIG
            # debug stuff
            self.insert_time = None
        else:
            self._transaction = data[1] if isinstance(data[1], bytes) else bytes(data[1])
            self.transaction = decode_raw(self._transaction)
            self._previous = (
                BytesLinks(data[4]) if isinstance(data[4], bytes) else bytes(data[4])
            )
            self._community_links = (
                BytesLinks(data[5]) if isinstance(data[5], bytes) else bytes(data[5])
            )

            self.previous = decode_links(self._previous)
            self.community_links = decode_links(self._community_links)

            self.type, self.public_key, self.sequence_number = data[0], data[2], data[3]
            self.community_id, self.community_sequence_number = (
                data[6],
                int(data[7]),
            )
            self.signature, self.timestamp, self.insert_time = (
                data[8],
                data[9],
                data[10],
            )

            # Convert the type to bytes if it is provided in string format
            self.type = (
                self.type
                if isinstance(self.type, bytes)
                else str(self.type).encode("utf-8")
            )

        self.hash = self.calculate_hash()
        self.crypto = default_eccrypto
        self._logger = logging.getLogger(self.__class__.__name__)

    def __str__(self):
        # This makes debugging and logging easier
        return "Block {0} from ...{1}:{2} links {3} for {4} type {5} cseq {6} cid {7}".format(
            self.readable_short_hash,
            shorten(self.public_key),
            self.sequence_number,
            self.community_links,
            self.transaction,
            self.type,
            self.community_sequence_number,
            self.community_id,
        )

    @property
    def short_hash(self):
        return shorten(self.hash)

    @property
    def readable_short_hash(self) -> str:
        return hexlify(self.short_hash).decode()

    def __hash__(self):
        return self.hash_number

    @property
    def pers_dot(self) -> Dot:
        return Dot((self.sequence_number, self.short_hash))

    @property
    def com_dot(self) -> Dot:
        return Dot((self.community_sequence_number, self.short_hash))

    @property
    def hash_number(self):
        """
        Return the hash of this block as a number (used as crawl ID).
        """
        return int(hexlify(self.hash), 16) % 100000000

    def calculate_hash(self) -> bytes:
        return sha256(self.pack()).digest()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PlexusBlock):
            return False
        return self.pack() == other.pack()

    @property
    def is_peer_genesis(self) -> bool:
        return self.sequence_number == GENESIS_SEQ and self.previous == GENESIS_LINK

    def block_args(self, signature: bool = True) -> List[Any]:
        args = [
            self.type,
            self._transaction,
            self.public_key,
            self.sequence_number,
            self._previous,
            self._community_links,
            self.community_id,
            self.community_sequence_number,
            self.signature if signature else EMPTY_SIG,
            self.timestamp,
        ]
        return args

    def to_block_payload(self, signature: bool = True) -> BlockPayload:
        return BlockPayload(*self.block_args(signature))

    def pack(self, signature: bool = True) -> bytes:
        """
        Encode the block
        Args:
            signature: False to pack EMPTY_SIG in the signature location, true to pack the signature field
        Returns:
            Block bytes
        """
        return self.serializer.pack_serializable(self.to_block_payload(signature))

    @classmethod
    def unpack(
        cls, block_blob: bytes, serializer: Any = default_serializer
    ) -> PlexusBlock:
        payload = serializer.unpack_serializable(BlockPayload, block_blob)
        return PlexusBlock.from_payload(payload[0])

    @classmethod
    def from_payload(
        cls, payload: BlockPayload, serializer=default_serializer
    ) -> PlexusBlock:
        """
        Create a block according to a given payload and serializer.
        This method can be used when receiving a block from the network.
        """
        return cls(
            [
                payload.type,
                payload.transaction,
                payload.public_key,
                payload.sequence_number,
                payload.previous,
                payload.community_links,
                payload.community_id,
                payload.community_sequence_number,
                payload.signature,
                payload.timestamp,
                time.time(),
            ],
            serializer,
        )

    def sign(self, key):
        """
        Signs this block with the given key
        :param key: the key to sign this block with
        """
        self.signature = self.crypto.create_signature(key, self.pack(signature=False))
        self.hash = self.calculate_hash()

    @classmethod
    def create(
        cls,
        block_type: bytes,
        transaction: Dict,
        database: "BaseDB",
        public_key: bytes,
        community_id: bytes = None,
    ) -> PlexusBlock:
        """
        Create PlexusBlock wrt local database knowledge.

        Args:
            block_type: type of the block in bytes
            transaction: transaction blob bytes
            database: local database with chains
            public_key: public key of the block creator
            community_id: id of the community which block is part of [optional]

        Returns:
            PlexusBlock

        """
        personal_chain = database.get_chain(public_key)

        # Determine the pointer to the previous block in the personal chain
        last_link = Links((GENESIS_DOT,)) if not personal_chain else personal_chain.consistent_terminal

        block = cls()
        block.type = block_type
        block.transaction = transaction
        block.sequence_number = max(last_link)[0] + 1
        block.previous = last_link

        # --- Community related logic ---
        if community_id:
            # There is community specified => will base block on the latest known information + filters
            block.community_id = community_id
            community_chain = database.get_chain(community_id)
            if not community_chain:
                block.community_links = GENESIS_LINK
            else:
                block.community_links = community_chain.consistent_terminal

            block.community_sequence_number = max(block.community_links)[0] + 1

        block._community_links = encode_links(block.community_links)
        block._previous = encode_links(block.previous)

        block.public_key = public_key
        block.signature = EMPTY_SIG
        block.hash = block.calculate_hash()
        return block

    def block_invariants_valid(self) -> bool:
        """Verify that block is valid wrt block invariants"""
        # 1. Sequence number should not be prior to genesis
        if self.sequence_number < GENESIS_SEQ and self.community_sequence_number < GENESIS_SEQ:
            self._logger.error("Sequence number wrong", self.sequence_number)
            return False
        # 2. Timestamp should be non negative
        if self.timestamp < 0:
            self._logger.error("Timestamp negative")
            return False
        # 3. Public key and signature should be valid
        if not self.crypto.is_valid_public_bin(self.public_key):
            self._logger.error("Public key is not valid")
            return False
        else:
            try:
                pck = self.pack(signature=False)
            except PackError:
                pck = None
            if pck is None or not self.crypto.is_valid_signature(
                self.crypto.key_from_public_bin(self.public_key), pck, self.signature
            ):
                self._logger.error("Cannot pack the block, or signature is not valid")
                return False
        return True
