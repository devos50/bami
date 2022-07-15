from bami.dkg.community import DKGCommunity
from bami.dkg.content import Content
from bami.dkg.db.triplet import Triplet

from tests.skipgraph.test_community import TestSkipGraphCommunityBase


class TestDKGCommunity(TestSkipGraphCommunityBase):
    NUM_NODES = 2
    COMMUNITY = DKGCommunity

    def setUp(self):
        super(TestDKGCommunity, self).setUp()

        for node in self.nodes:
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
