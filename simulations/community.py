from bami.backbone.block import BamiBlock
from bami.backbone.community import BamiCommunity
from bami.backbone.utils import encode_raw


class TestCommunity(BamiCommunity):

    def create_new_tx(self):
        block = self.create_signed_block(
            block_type=b"test", transaction=encode_raw({}), com_id=b"a" * 20
        )
        self.share_in_community(block, b"a" * 20)

    def on_community_started(self):
        self.subscribe_to_subcom(b"a" * 20)

    def received_block_in_order(self, block: BamiBlock) -> None:
        pass
