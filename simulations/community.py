from random import randint

from bami.plexus.backbone.community import PlexusCommunity


class KeyValueCommunity(PlexusCommunity):
    """
    Very basic community that allows peers to write a value.
    """
    def on_community_started(self):
        self.subscribe_to_subcom(b"a" * 20)

    def write_random_value(self):
        value = randint(1, 1000)
        block = self.create_signed_block(
            community_id=b"a" * 20,
            transaction={b"value": value},
            block_type=b"test",
        )
        self.share_in_community(block, b"a" * 20, fanout=0)  # Disable block push gossip

    def get_current_value(self, community_id) -> int:
        frontier = self.persistence.get_chain(community_id).frontier
        max_value = 0
        for dot in frontier.terminal:
            block = self.get_block_by_dot(community_id, dot)
            if block.transaction[b"value"] > max_value:
                max_value = block.transaction[b"value"]
        return max_value
