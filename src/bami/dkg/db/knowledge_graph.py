import random
from typing import Set, List

import networkx as nx

from bami.dkg.db.triplet import Triplet


class KnowledgeGraph:
    """
    Represents the local knowledge graph.
    """

    def __init__(self) -> None:
        self.graph = nx.DiGraph()

    def add_triplet(self, triplet: Triplet) -> None:
        if self.graph.has_edge(triplet.head, triplet.tail):
            edge = self.graph.edges[triplet.head, triplet.tail]
            if edge["attr"]["relation"] == triplet.relation:
                # Edge seems to exist - simply merge the signatures
                for sig in triplet.signatures:
                    if sig not in edge["attr"]["signatures"]:
                        edge["attr"]["signatures"].append(sig)
                return

        # Otherwise, add the adge as new
        self.graph.add_edge(triplet.head, triplet.tail,
                            attr={"relation": triplet.relation, "signatures": triplet.signatures})

    def get_triplets_of_node(self, content: bytes) -> List[Triplet]:
        """
        Fetch the triplets around a particular node.
        """
        triplets = []
        for edge in list(self.graph.in_edges(content)) + list(self.graph.out_edges(content)):
            relation = self.graph.edges[edge]["attr"]["relation"]
            triplet = Triplet(edge[0], relation, edge[1])
            triplets.append(triplet)
        return triplets

    def get_random_edges(self, limit: int = 10) -> Set[Triplet]:
        random_edges = random.sample(self.graph.edges, min(self.get_num_edges(), limit))
        # Get the relations
        triplets = set()
        for random_edge in random_edges:
            relation = self.graph.edges[random_edge]["attr"]["relation"]
            triplet = Triplet(random_edge[0], relation, random_edge[1])
            triplets.add(triplet)
        return triplets

    def get_num_edges(self) -> int:
        return len(self.graph.edges)