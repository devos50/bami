from bami.skipgraph import LEFT
from bami.skipgraph.community import SkipGraphCommunity
from bami.skipgraph.membership_vector import MembershipVector
from bami.skipgraph.node import SGNode
from ipv8.messaging.interfaces.udp.endpoint import UDPv4Address

from ipv8.test.base import TestBase
from ipv8.test.mocking.ipv8 import MockIPv8


class TestSkipGraphCommunityBase(TestBase):
    NUM_NODES = 2

    def initialize_routing_tables(self, nodes_info):
        for ind, node_info in enumerate(nodes_info):
            # Pad the list until we have sufficient bits
            bin_list = node_info[1]
            while len(bin_list) != MembershipVector.LENGTH:
                bin_list += [0]
            self.nodes[ind].overlay.initialize_routing_table(node_info[0], mv=MembershipVector(bin_list))

    def setUp(self):
        super(TestSkipGraphCommunityBase, self).setUp()
        self.initialize(SkipGraphCommunity, self.NUM_NODES)

        MembershipVector.LENGTH = 2
        nodes_info = [(0, [0, 0]), (1, [0, 1])]

        self.initialize_routing_tables(nodes_info)

    def create_node(self):
        return MockIPv8("curve25519", SkipGraphCommunity)


class TestSkipGraphCommunity(TestSkipGraphCommunityBase):

    async def test_introductions(self):
        await self.introduce_nodes()
        for node in self.nodes:
            assert node.overlay.peers_info

    async def test_get_max_level(self):
        max_level = await self.nodes[0].overlay.get_max_level(self.nodes[1].overlay.my_peer)
        assert max_level == 0

        self.nodes[1].overlay.routing_table.extend(2)
        max_level = await self.nodes[0].overlay.get_max_level(self.nodes[1].overlay.my_peer)
        assert max_level == 3

    async def test_get_neighbour(self):
        found, node = await self.nodes[0].overlay.get_neighbour(self.nodes[1].overlay.my_peer, LEFT, 0)
        assert not found

        # Give node 1 a left neighbour on level 0
        self.nodes[1].overlay.routing_table.extend(0)
        self.nodes[1].overlay.routing_table.levels[0].neighbors[LEFT] = \
            SGNode(UDPv4Address("1.1.1.1", 1234), b"1234", 42, MembershipVector.from_bytes(b""))

        found, node = await self.nodes[0].overlay.get_neighbour(self.nodes[1].overlay.my_peer, LEFT, 0)
        assert found
        assert node.address == UDPv4Address("1.1.1.1", 1234)
        assert node.public_key == b"1234"
        assert node.key == 42

    async def test_join(self):
        await self.introduce_nodes()
        await self.nodes[0].overlay.join()


class TestSkipGraphCommunityLargeJoin(TestSkipGraphCommunityBase):
    NUM_NODES = 7

    def setUp(self):
        super(TestSkipGraphCommunityLargeJoin, self).setUp()
        self.initialize(SkipGraphCommunity, self.NUM_NODES)

        MembershipVector.LENGTH = 2
        nodes_info = [
            (13, [0, 0]),
            (21, [1, 0]),
            (33, [0, 1]),
            (36, [0, 1]),
            (48, [0, 0]),
            (75, [1, 1]),
            (99, [1, 1])]

        self.initialize_routing_tables(nodes_info)

    async def test_join(self):
        await self.introduce_nodes()
        await self.nodes[0].overlay.join(introducer_peer=self.nodes[4].overlay.my_peer)
        await self.nodes[1].overlay.join(introducer_peer=self.nodes[4].overlay.my_peer)
        await self.nodes[2].overlay.join(introducer_peer=self.nodes[4].overlay.my_peer)
        await self.nodes[3].overlay.join(introducer_peer=self.nodes[4].overlay.my_peer)
        await self.nodes[4].overlay.join(introducer_peer=self.nodes[0].overlay.my_peer)
        # for node in self.nodes:
        #    await node.overlay.join()

        for ind, node in enumerate(self.nodes):
            node.overlay.logger.error("=== RT node %d (key: %d) ===\n%s", ind, node.overlay.routing_table.key, node.overlay.routing_table)
