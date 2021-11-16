from binascii import hexlify

from bami.frost import MSG
from bami.frost.community import FrostCommunity
from ipv8.test.base import TestBase


class TestFrostCommunity(TestBase):
    def setUp(self):
        super(TestFrostCommunity, self).setUp()
        self.initialize(FrostCommunity, 2)

        # Assign IDs and share knowledge
        managing_peers = {}
        for ind, node in enumerate(self.nodes):
            managing_peers[node.my_peer.public_key.key_to_bin()] = ind

        for ind, node in enumerate(self.nodes):
            node.overlay.initialize(ind, 2, 2, managing_peers)

    async def test_two_nodes_sign(self):
        await self.introduce_nodes()

        for node in self.nodes:
            node.overlay.share_public_key()

        await self.deliver_messages()

        for node in self.nodes:
            node.overlay.share_shares()

        await self.deliver_messages()

        sig = self.nodes[0].overlay.aggregator.signature
        sig_hex = hexlify(bytes(sig)).decode()
        print("Resulting Schnorr signature: %s" % sig_hex)
        self.nodes[0].overlay.participant.verify_signature(sig, MSG)
