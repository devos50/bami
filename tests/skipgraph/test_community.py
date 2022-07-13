from asyncio import ensure_future

from bami.skipgraph import LEFT, RIGHT
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

        self.nodes[1].overlay.routing_table.max_level = 3
        max_level = await self.nodes[0].overlay.get_max_level(self.nodes[1].overlay.my_peer)
        assert max_level == 3

    async def test_get_neighbour(self):
        found, node = await self.nodes[0].overlay.get_neighbour(self.nodes[1].overlay.my_peer, LEFT, 0)
        assert not found

        # Give node 1 a left neighbour on level 0
        self.nodes[1].overlay.routing_table.levels[0].neighbors[LEFT] = \
            SGNode(UDPv4Address("1.1.1.1", 1234), b"1234", 42, MembershipVector.from_bytes(b""))

        found, node = await self.nodes[0].overlay.get_neighbour(self.nodes[1].overlay.my_peer, LEFT, 0)
        assert found
        assert node.address == UDPv4Address("1.1.1.1", 1234)
        assert node.public_key == b"1234"
        assert node.key == 42

    async def test_join(self):
        await self.introduce_nodes()
        await self.nodes[0].overlay.join(introducer_peer=self.nodes[1].overlay.my_peer)
        assert self.nodes[0].overlay.routing_table.height() == 3
        assert self.nodes[1].overlay.routing_table.height() == 3

    async def test_search(self):
        await self.introduce_nodes()
        await self.nodes[0].overlay.join(introducer_peer=self.nodes[1].overlay.my_peer)
        result = await self.nodes[1].overlay.search(0)
        assert result.key == 0
        result = await self.nodes[1].overlay.search(1)
        assert result.key == 1


class TestSkipGraphCommunityLargeJoin(TestSkipGraphCommunityBase):
    NUM_NODES = 7

    def setUp(self):
        super(TestSkipGraphCommunityBase, self).setUp()
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

    def verify_skip_graph(self):
        # Level 0
        assert not self.nodes[0].overlay.routing_table.get(0, LEFT)
        assert self.nodes[0].overlay.routing_table.get(0, RIGHT).key == 21

        assert self.nodes[1].overlay.routing_table.get(0, LEFT).key == 13
        assert self.nodes[1].overlay.routing_table.get(0, RIGHT).key == 33

        assert self.nodes[2].overlay.routing_table.get(0, LEFT).key == 21
        assert self.nodes[2].overlay.routing_table.get(0, RIGHT).key == 36

        assert self.nodes[3].overlay.routing_table.get(0, LEFT).key == 33
        assert self.nodes[3].overlay.routing_table.get(0, RIGHT).key == 48

        assert self.nodes[4].overlay.routing_table.get(0, LEFT).key == 36
        assert self.nodes[4].overlay.routing_table.get(0, RIGHT).key == 75

        assert self.nodes[5].overlay.routing_table.get(0, LEFT).key == 48
        assert self.nodes[5].overlay.routing_table.get(0, RIGHT).key == 99

        assert self.nodes[6].overlay.routing_table.get(0, LEFT).key == 75
        assert not self.nodes[6].overlay.routing_table.get(0, RIGHT)

        # Level 1
        assert not self.nodes[0].overlay.routing_table.get(1, LEFT)
        assert self.nodes[0].overlay.routing_table.get(1, RIGHT).key == 33

        assert not self.nodes[1].overlay.routing_table.get(1, LEFT)
        assert self.nodes[1].overlay.routing_table.get(1, RIGHT).key == 75

        assert self.nodes[2].overlay.routing_table.get(1, LEFT).key == 13
        assert self.nodes[2].overlay.routing_table.get(1, RIGHT).key == 36

        assert self.nodes[3].overlay.routing_table.get(1, LEFT).key == 33
        assert self.nodes[3].overlay.routing_table.get(1, RIGHT).key == 48

        assert self.nodes[4].overlay.routing_table.get(1, LEFT).key == 36
        assert not self.nodes[4].overlay.routing_table.get(1, RIGHT)

        assert self.nodes[5].overlay.routing_table.get(1, LEFT).key == 21
        assert self.nodes[5].overlay.routing_table.get(1, RIGHT).key == 99

        assert self.nodes[6].overlay.routing_table.get(1, LEFT).key == 75
        assert not self.nodes[6].overlay.routing_table.get(1, RIGHT)

        # Level 2
        assert not self.nodes[0].overlay.routing_table.get(2, LEFT)
        assert self.nodes[0].overlay.routing_table.get(2, RIGHT).key == 48

        assert not self.nodes[1].overlay.routing_table.get(2, LEFT)
        assert not self.nodes[1].overlay.routing_table.get(2, RIGHT)

        assert not self.nodes[2].overlay.routing_table.get(2, LEFT)
        assert self.nodes[2].overlay.routing_table.get(2, RIGHT).key == 36

        assert self.nodes[3].overlay.routing_table.get(2, LEFT).key == 33
        assert not self.nodes[3].overlay.routing_table.get(2, RIGHT)

        assert self.nodes[4].overlay.routing_table.get(2, LEFT).key == 13
        assert not self.nodes[4].overlay.routing_table.get(2, RIGHT)

        assert not self.nodes[5].overlay.routing_table.get(2, LEFT)
        assert self.nodes[5].overlay.routing_table.get(2, RIGHT).key == 99

        assert self.nodes[6].overlay.routing_table.get(2, LEFT).key == 75
        assert not self.nodes[6].overlay.routing_table.get(2, RIGHT)

    def assert_not_self_in_rt(self):
        """
        Make sure that we don't add ourselves to the routing tables.
        """
        for node in self.nodes:
            for level in node.overlay.routing_table.levels:
                assert not level.neighbors[LEFT] or level.neighbors[LEFT].key != node.overlay.routing_table.key
                assert not level.neighbors[RIGHT] or level.neighbors[RIGHT].key != node.overlay.routing_table.key

    async def test_join(self):
        """
        Test the graph example from the original Skip Graphs paper (page 8):
        https://www.researchgate.net/profile/Gauri-Shah-4/publication/1956507_Skip_Graphs/links/576a91a208aef2a864d1dcd4/Skip-Graphs.pdf
        """
        await self.introduce_nodes()

        for node in self.nodes[1:]:
            await node.overlay.join(introducer_peer=self.nodes[0].my_peer)
            self.assert_not_self_in_rt()

        for ind, node in enumerate(self.nodes):
            node.overlay.logger.error("=== RT node %d (key: %d) ===\n%s", ind, node.overlay.routing_table.key, node.overlay.routing_table)

        # Verify the Skip Graph
        self.verify_skip_graph()

    async def test_concurrent_join(self):
        await self.introduce_nodes()
        for node in self.nodes[1:]:
            ensure_future(node.overlay.join(introducer_peer=self.nodes[0].my_peer))

        await self.deliver_messages()
        self.assert_not_self_in_rt()

        for ind, node in enumerate(self.nodes):
            node.overlay.logger.error("=== RT node %d (key: %d) ===\n%s", ind, node.overlay.routing_table.key, node.overlay.routing_table)

        self.verify_skip_graph()

    async def test_search(self):
        """
        Test searching for particular nodes
        """
        await self.introduce_nodes()

        for node in self.nodes[1:]:
            await node.overlay.join(introducer_peer=self.nodes[0].my_peer)

        # Do some searches
        for node in self.nodes:
            result = await node.overlay.search(20)
            assert result.key == 13  # Node 13 is the greatest number closest to 20, our search.

            result = await self.nodes[0].overlay.search(13)
            assert result.key == 13

            result = await self.nodes[0].overlay.search(22)
            assert result.key == 21

            result = await self.nodes[0].overlay.search(100)
            assert result.key == 99

            result = await self.nodes[1].overlay.search(40)
            assert result.key == 36
