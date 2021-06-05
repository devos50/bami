# tests/conftest.py
import collections
from typing import List, Union, Dict

import pytest
from _pytest.config import Config

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.keyvault.private.libnaclkey import LibNaCLSK

from bami.plexus.backbone.block import EMPTY_SIG, PlexusBlock
from bami.plexus.backbone.datastore.chain_store import BaseChain, Chain
from bami.plexus.backbone.datastore.database import BaseDB
from bami.plexus.backbone.utils import (
    encode_links,
    encode_raw,
    GENESIS_LINK,
    Links, NO_COMMUNITY,
)
from tests.plexus.mocking.base import (
    create_and_connect_nodes,
    SetupValues,
    unload_nodes,
)


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "e2e: mark as end-to-end test.")


# Fixtures
class FakeBlock(PlexusBlock):
    """
    Test Block that simulates a block used in TrustChain.
    Also used in other test files for TrustChain.
    """

    def __init__(
        self,
        transaction: Dict = None,
        previous: Links = None,
        key: LibNaCLSK = None,
        community_links: Links = None,
        community_id: bytes = None,
        block_type: bytes = b"test",
    ):
        if not community_links:
            community_links = GENESIS_LINK
            community_sequence_number = 1
        else:
            community_sequence_number = max(community_links)[0] + 1

        if not previous:
            previous = GENESIS_LINK
        sequence_number = max(previous)[0] + 1

        if not community_id:
            community_id = NO_COMMUNITY

        self.key = key or default_eccrypto.generate_key("curve25519")
        transaction = transaction or {"id": 42}

        PlexusBlock.__init__(
            self,
            (
                block_type,
                encode_raw(transaction),
                self.key.pub().key_to_bin(),
                sequence_number,
                encode_links(previous),
                encode_links(community_links),
                community_id,
                community_sequence_number,
                EMPTY_SIG,
                0,
                0,
            ),
        )
        self.sign(self.key)


def create_block_batch(com_id, num_blocks=100):
    """
    Create a given amount of blocks. Each block points to the previous one.

    Args:
        com_id: The community identifier of the block batch.
        num_blocks: The number of blocks to create.

    Returns: A list with blocks.

    """
    blocks = []
    last_block_point = GENESIS_LINK
    for k in range(num_blocks):
        blk = FakeBlock(community_id=com_id, community_links=last_block_point)
        blocks.append(blk)
        last_block_point = Links(((blk.community_sequence_number, blk.short_hash),))
    return blocks


@pytest.fixture
def create_batches():
    def _create_batches(num_batches=2, num_blocks=100):
        """
        Creates batches of blocks within a random community.

        Args:
            num_batches: The number of batches to consider.
            num_blocks: The number of blocks in each batch.

        Returns: A list of batches where each batch represents a chain of blocks.

        """
        key = default_eccrypto.generate_key("curve25519")
        com_id = key.pub().key_to_hash()
        return [create_block_batch(com_id, num_blocks) for _ in range(num_batches)]

    return _create_batches


def insert_to_chain(
    chain_obj: BaseChain, blk: PlexusBlock, personal_chain: bool = True
):
    block_links = blk.community_links if not personal_chain else blk.previous
    block_seq_num = blk.community_sequence_number if not personal_chain else blk.sequence_number
    yield chain_obj.add_block(block_links, block_seq_num, blk.hash)


def insert_to_chain_or_blk_store(
    chain_obj: Union[BaseChain, BaseDB], blk: PlexusBlock, personal_chain: bool = True,
):
    if isinstance(chain_obj, BaseChain):
        yield from insert_to_chain(chain_obj, blk, personal_chain)
    else:
        yield chain_obj.add_block(blk.pack(), blk)


def insert_batch_seq(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[PlexusBlock],
    personal_chain: bool = False,
) -> None:
    for blk in batch:
        yield from insert_to_chain_or_blk_store(chain_obj, blk, personal_chain)


def insert_batch_reversed(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[PlexusBlock],
    personal_chain: bool = False,
) -> None:
    for blk in reversed(batch):
        yield from insert_to_chain_or_blk_store(chain_obj, blk, personal_chain)


def insert_batch_random(
    chain_obj: Union[BaseChain, BaseDB],
    batch: List[PlexusBlock],
    personal_chain: bool = False,
) -> None:
    from random import shuffle

    shuffle(batch)
    for blk in batch:
        yield from insert_to_chain_or_blk_store(chain_obj, blk, personal_chain)


batch_insert_functions = [insert_batch_seq, insert_batch_random, insert_batch_reversed]


@pytest.fixture(params=batch_insert_functions)
def insert_function(request):
    param = request.param
    return param


@pytest.fixture
def chain():
    return Chain()


insert_function_copy = insert_function

_DirsNodes = collections.namedtuple("DirNodes", ("dirs", "nodes"))


def _set_vals_init(tmpdir_factory, overlay_class, num_nodes) -> _DirsNodes:
    dirs = [
        tmpdir_factory.mktemp(str(overlay_class.__name__), numbered=True)
        for _ in range(num_nodes)
    ]
    nodes = create_and_connect_nodes(num_nodes, work_dirs=dirs, ov_class=overlay_class)

    return _DirsNodes(dirs, nodes)


def _set_vals_teardown(dirs) -> None:
    for k in dirs:
        k.remove(ignore_errors=True)


def _init_nodes(nodes, community_id) -> None:
    for node in nodes:
        node.overlay.subscribe_to_subcom(community_id)


@pytest.fixture
async def set_vals_by_key(
    tmpdir_factory, overlay_class, num_nodes: int, init_nodes: bool
):
    dirs, nodes = _set_vals_init(tmpdir_factory, overlay_class, num_nodes)
    # Make sure every node has a community to listen to
    community_key = default_eccrypto.generate_key("curve25519").pub()
    community_id = community_key.key_to_hash()
    if init_nodes:
        _init_nodes(nodes, community_id)
    yield SetupValues(nodes=nodes, community_id=community_id)
    await unload_nodes(nodes)
    _set_vals_teardown(dirs)


@pytest.fixture
async def set_vals_by_nodes(
    tmpdir_factory, overlay_class, num_nodes: int, init_nodes: bool
):
    dirs, nodes = _set_vals_init(tmpdir_factory, overlay_class, num_nodes)
    # Make sure every node has a community to listen to
    community_id = nodes[0].overlay.my_peer.key.key_to_hash()
    context = nodes[0].overlay.state_db.context
    if init_nodes:
        _init_nodes(nodes, community_id)
    yield SetupValues(nodes=nodes, community_id=community_id, context=context)
    await unload_nodes(nodes)
    _set_vals_teardown(dirs)
