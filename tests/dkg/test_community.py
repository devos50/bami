from typing import List

from bami.dkg.community import DKGCommunity
from bami.dkg.content import Content
from bami.dkg.db.triplet import Triplet
from bami.skipgraph.util import verify_skip_graph_integrity

from tests.skipgraph.test_community import TestSkipGraphCommunityBase, TestSkipGraphCommunityFourNodesBase


class CustomKeyContent(Content):

    def get_keys(self, num_keys: int = 1) -> List[int]:
        return [int.from_bytes(self.identifier, 'big') % (2 ** 32)]


class TestDKGCommunitySingleReplication(TestSkipGraphCommunityBase):
    NUM_NODES = 2
    COMMUNITY = DKGCommunity

    def setUp(self):
        super(TestDKGCommunitySingleReplication, self).setUp()

        for node in self.nodes:
            node.overlay.should_verify_key = False
            node.overlay.replication_factor = 1
            node.overlay.start_rule_execution_engine()

    async def test_store_graph_node(self):
        """
        Test generating, storing and retrieving a graph node in the network.
        """
        await self.introduce_nodes()
        await self.nodes[1].overlay.join(introducer_peer=self.nodes[0].overlay.my_peer)
        triplet = Triplet(b"abcdefg", b"b", b"c")

        # Test the situation where no edges are returned
        triplets = await self.nodes[0].overlay.search_edges(b"abcdefg")
        assert len(triplets) == 0

        await self.nodes[0].overlay.on_new_triplets_generated(Content(b"abcdefg", b""), [triplet])
        await self.deliver_messages()
        assert self.nodes[1].overlay.knowledge_graph.get_num_edges() == 1

        # Now we try to fetch all edges from node 1
        triplets = await self.nodes[0].overlay.search_edges(b"abcdefg")
        assert len(triplets) == 1

        # Test searching locally
        triplets = await self.nodes[1].overlay.search_edges(b"abcdefg")
        assert len(triplets) == 1

    async def test_storage_request(self):
        """
        Test sending storage requests.
        """
        await self.introduce_nodes()
        await self.nodes[1].overlay.join(introducer_peer=self.nodes[0].overlay.my_peer)
        assert verify_skip_graph_integrity(self.nodes)

        content1 = CustomKeyContent(b"", b"")
        content2 = CustomKeyContent(b"\x01", b"")

        target_node = self.nodes[0].overlay.get_my_node()
        assert await self.nodes[0].overlay.send_storage_request(target_node, content1.identifier, content1.get_keys(1)[0])
        assert not await self.nodes[1].overlay.send_storage_request(target_node, content2.identifier, content2.get_keys(1)[0])

        target_node = self.nodes[1].overlay.get_my_node()
        assert not await self.nodes[1].overlay.send_storage_request(target_node, content1.identifier, content1.get_keys(1)[0])
        assert await self.nodes[1].overlay.send_storage_request(target_node, content2.identifier, content2.get_keys(1)[0])


class TestDKGCommunityDoubleReplication(TestSkipGraphCommunityFourNodesBase):
    COMMUNITY = DKGCommunity

    def setUp(self):
        super(TestDKGCommunityDoubleReplication, self).setUp()

        for node in self.nodes:
            node.overlay.should_verify_key = False
            node.overlay.replication_factor = 2
            node.overlay.start_rule_execution_engine()

    async def test_store_graph_node(self):
        """
        Test generating, storing and retrieving a graph node in the network.
        """
        await self.introduce_nodes()
        for node in self.nodes[1:]:
            await node.overlay.join(introducer_peer=self.nodes[0].overlay.my_peer)

        Content.custom_keys = [20, 50]
        triplet = Triplet(b"abcdefg", b"b", b"c")

        await self.nodes[0].overlay.on_new_triplets_generated(Content(b"abcdefg", b""), [triplet])
        await self.deliver_messages()

        # At least two nodes should store this triplet
        cnt = 0
        for node in self.nodes:
            if node.overlay.knowledge_graph.get_num_edges() == 1:
                cnt += 1
        assert cnt == 2

        # Now we try to fetch all edges from node 1
        triplets = await self.nodes[0].overlay.search_edges(b"abcdefg")
        assert len(triplets) == 1
        Content.custom_keys = None
