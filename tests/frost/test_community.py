from bami.frost.community import FrostCommunity
from ipv8.test.base import TestBase


class TestFrostCommunity(TestBase):
    def setUp(self):
        super(TestFrostCommunity, self).setUp()
        self.initialize(FrostCommunity, 2, num_participants=2, threshold=2)

        # Assign IDs
        for ind, node in enumerate(self.nodes):
            node.overlay.index = ind

    async def test_bla(self):
        await self.introduce_nodes()

        self.nodes[0].overlay.share_public_key()

        await self.deliver_messages()
