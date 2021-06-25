from asyncio import sleep
from random import randint

import pytest

from bami.plexus.backbone.community import PlexusCommunity


class KeyValueCommunity(PlexusCommunity):
    """
    Very basic community that allows peers to write a value.
    """
    pass

    def write_random_value(self, community_id):
        value = randint(1, 1000)
        block = self.create_signed_block(
            community_id=community_id,
            transaction={b"value": value},
            block_type=b"test",
        )
        self.share_in_community(block, community_id, fanout=0)  # Disable block push gossip

    def get_current_value(self, community_id) -> int:
        frontier = self.persistence.get_chain(community_id).frontier
        max_value = 0
        for dot in frontier.terminal:
            block = self.get_block_by_dot(community_id, dot)
            if block.transaction[b"value"] > max_value:
                max_value = block.transaction[b"value"]
        return max_value


@pytest.fixture
def init_nodes():
    return True


@pytest.mark.asyncio
@pytest.mark.parametrize("overlay_class", [KeyValueCommunity])
@pytest.mark.parametrize("num_nodes", [5])
async def test_key_value_write(set_vals_by_key):
    def assert_peer_values():
        # Check if all peers end up with the same value
        values = []
        for node in set_vals_by_key.nodes:
            values.append(node.overlay.get_current_value(set_vals_by_key.community_id))

        assert all(value == values[0] for value in values)

    for node in set_vals_by_key.nodes:
        node.overlay.write_random_value(set_vals_by_key.community_id)
    await sleep(1)

    assert_peer_values()

    set_vals_by_key.nodes[0].overlay.write_random_value(set_vals_by_key.community_id)
    await sleep(1)

    assert_peer_values()
