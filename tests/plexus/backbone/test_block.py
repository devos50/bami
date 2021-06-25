from ipv8.keyvault.crypto import default_eccrypto
from bami.plexus.backbone.block import (
    EMPTY_PK,
    EMPTY_SIG,
    GENESIS_SEQ,
    PlexusBlock,
    UNKNOWN_SEQ,
)
from bami.plexus.backbone.utils import (
    encode_raw,
    GENESIS_DOT,
    Links,
    shorten,
    ShortKey, NO_COMMUNITY,
)
from bami.plexus.backbone.payload import BlockPayload

from tests.plexus.conftest import FakeBlock
from tests.plexus.mocking.mock_db import MockChain, MockDBManager


class TestChainBlock:
    """
    This class contains tests for a TrustChain block.
    """

    def test_sign(self):
        """
        Test signing a block and whether the signature is valid
        """
        crypto = default_eccrypto
        block = FakeBlock()
        assert crypto.is_valid_signature(
            block.key, block.pack(signature=False), block.signature
        )

    def test_short_hash(self):
        block = FakeBlock()
        assert shorten(block.hash) == block.short_hash

    def test_readable_short_hash(self):
        block = FakeBlock()
        assert len(block.readable_short_hash) == 16

    def test_create_genesis(self):
        """
        Test creating a genesis block
        """
        key = default_eccrypto.generate_key("curve25519")
        db = MockDBManager()
        block = PlexusBlock.create(
            b"test", {b"id": 43}, db, key.pub().key_to_bin()
        )

        assert block.previous == Links((GENESIS_DOT,))
        assert block.sequence_number == GENESIS_SEQ
        assert block.public_key == key.pub().key_to_bin()
        assert block.signature == EMPTY_SIG
        assert block.type == b"test"
        assert block.transaction == {b"id": 43}
        assert block.community_id == NO_COMMUNITY
        assert block.community_sequence_number == UNKNOWN_SEQ

    def test_create_next_pers(self, monkeypatch):
        """
        Test creating a block that points towards a previous block
        """
        db = MockDBManager()
        prev = FakeBlock()

        monkeypatch.setattr(
            MockDBManager, "get_chain", lambda _, chain_id: MockChain(),
        )
        monkeypatch.setattr(
            MockChain, "consistent_terminal", Links((prev.pers_dot,)),
        )

        block = PlexusBlock.create(
            b"test", {b"id": 42}, db, prev.public_key
        )

        assert block.previous == Links((prev.pers_dot,))
        assert block.sequence_number == prev.sequence_number + 1
        assert block.public_key == prev.public_key
        assert block.signature == EMPTY_SIG
        assert block.type == b"test"
        assert block.transaction == {b"id": 42}
        assert block.community_id == NO_COMMUNITY
        assert block.community_sequence_number == UNKNOWN_SEQ

    def test_create_link_to_pers(self, monkeypatch):
        """
        Test creating a Plexus block that links to the previous block in the peers' personal chain
        """
        key = default_eccrypto.generate_key("curve25519")
        db = MockDBManager()
        link = FakeBlock()

        monkeypatch.setattr(
            MockDBManager,
            "get_chain",
            lambda _, chain_id: MockChain() if chain_id == link.public_key else None,
        )
        monkeypatch.setattr(
            MockChain, "consistent_terminal", Links((link.pers_dot,)),
        )
        block = PlexusBlock.create(
            b"test", {b"id": 42}, db, key.pub().key_to_bin(), community_id=NO_COMMUNITY,
        )

        assert block.previous == Links((GENESIS_DOT,))
        assert block.sequence_number == GENESIS_SEQ
        assert block.community_sequence_number == 1
        assert block.public_key == key.pub().key_to_bin()
        assert block.signature == EMPTY_SIG
        assert block.type == b"test"
        assert block.transaction == {b"id": 42}
        assert block.community_id == NO_COMMUNITY

    def test_create_link_to_com_chain(self, monkeypatch):
        """
        Test creating a linked half that points back towards a previous block
        """
        key = default_eccrypto.generate_key(u"curve25519")
        community_id = b"a" * 20
        db = MockDBManager()
        com_link = Links(((1, ShortKey(b"aaaaaaaa")),))
        link = FakeBlock(community_id=community_id, community_links=com_link)

        monkeypatch.setattr(
            MockDBManager,
            "get_chain",
            lambda _, chain_id: MockChain() if chain_id == community_id else None,
        )
        monkeypatch.setattr(
            MockChain, "consistent_terminal", Links((link.com_dot,)),
        )
        block = PlexusBlock.create(
            b"test", {b"id": 42}, db, key.pub().key_to_bin(), community_id=community_id
        )

        assert block.community_links == Links((link.com_dot,))
        assert block.previous == Links((GENESIS_DOT,))
        assert block.sequence_number == GENESIS_SEQ
        assert block.community_sequence_number == link.community_sequence_number + 1
        assert block.public_key == key.pub().key_to_bin()
        assert block.signature == EMPTY_SIG
        assert block.type == b"test"
        assert block.transaction == {b"id": 42}
        assert block.community_id == community_id

    def test_invariant_negative_timestamp(self):
        """
        Test if negative sequence number blocks are not valid.
        """
        block = FakeBlock()
        block.timestamp = -1.0
        assert not block.block_invariants_valid()

    def test_invariant_invalid_key(self):
        """
        Test if illegal key blocks are not valid.
        """
        block = FakeBlock()
        block.public_key = b"definitelynotakey"
        assert not block.block_invariants_valid()

    def test_invariant_invalid_seq_num(self):
        """
        Test if illegal key blocks are not valid.
        """
        block = FakeBlock()
        block.sequence_number = -1
        assert not block.block_invariants_valid()

    def test_invariant_invalid_com_seq_num(self):
        """
        Test if illegal key blocks are not valid.
        """
        block = FakeBlock()
        block.community_sequence_number = -1
        assert not block.block_invariants_valid()

    def test_invalid_sign(self):
        key = default_eccrypto.generate_key(u"curve25519")

        blk = FakeBlock()
        blk.sign(key)

        assert not blk.block_invariants_valid()

    def test_block_valid(self):
        blk = FakeBlock()
        assert blk.block_invariants_valid()

    def test_block_payload(self):
        blk = FakeBlock()
        blk_bytes = blk.pack()
        payload = blk.serializer.unpack_serializable(BlockPayload, blk_bytes)
        blk2 = PlexusBlock.from_payload(payload[0])
        assert blk2 == blk

    def test_pack_unpack(self):
        blk = FakeBlock()
        blk_bytes = blk.pack()
        blk2 = PlexusBlock.unpack(blk_bytes, blk.serializer)
        assert blk == blk2

    def test_hash_function(self):
        """
        Check if the hash() function returns the Block hash.
        """
        block = FakeBlock()

        assert block.__hash__(), block.hash_number
