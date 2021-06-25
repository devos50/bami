from typing import Dict

import pytest

from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.blockresponse import BlockResponseMixin, BlockResponse
from bami.plexus.backbone.community import PlexusCommunity
from bami.plexus.backbone.utils import encode_raw
from tests.plexus.mocking.base import deliver_messages


class BlockResponseCommunity(BlockResponseMixin, PlexusCommunity):
    """
    Basic community with block response functionality enabled.
    """

    def process_block_ordered(self, block: PlexusBlock) -> None:
        if block.transaction.get(b"to_peer", None) == self.my_peer.public_key.key_to_bin():
            self.add_block_to_response_processing(block)

    def block_response(
        self, block: PlexusBlock, wait_time: float = None, wait_blocks: int = None
    ) -> BlockResponse:
        if block.type == b"good":
            return BlockResponse.CONFIRM
        elif block.type == b"bad":
            return BlockResponse.REJECT
        return BlockResponse.IGNORE


@pytest.fixture
def init_nodes():
    return True


@pytest.mark.asyncio
@pytest.mark.parametrize("overlay_class", [PlexusCommunity])
@pytest.mark.parametrize("num_nodes", [2])
async def test_simple_frontier_reconciliation_after_partition(set_vals_by_key):
    """
    Test whether missing blocks are synchronized after a network partition.
    """
    for _ in range(3):
        # Note that we do not broadcast the block to the other node
        set_vals_by_key.nodes[0].overlay.create_signed_block(
            community_id=set_vals_by_key.community_id
        )

    # Force frontier exchange
    set_vals_by_key.nodes[0].overlay.frontier_gossip_sync_task(
        set_vals_by_key.community_id
    )

    await deliver_messages()

    frontier1 = (
        set_vals_by_key.nodes[0]
        .overlay.persistence.get_chain(set_vals_by_key.community_id)
        .frontier
    )
    frontier2 = (
        set_vals_by_key.nodes[1]
        .overlay.persistence.get_chain(set_vals_by_key.community_id)
        .frontier
    )
    assert len(frontier2.terminal) == 1
    assert frontier2.terminal[0][0] == 3
    assert frontier1 == frontier2


@pytest.mark.asyncio
@pytest.mark.parametrize("overlay_class", [BlockResponseCommunity])
@pytest.mark.parametrize("num_nodes", [2])
async def test_block_confirm(set_vals_by_key):
    """
    Test whether blocks are confirmed correctly.
    """
    block = set_vals_by_key.nodes[0].overlay.create_signed_block(
        community_id=set_vals_by_key.community_id,
        transaction={b"to_peer": 3},
        block_type=b"good",
    )
    set_vals_by_key.nodes[0].overlay.share_in_community(
        block, set_vals_by_key.community_id
    )
    await deliver_messages()
