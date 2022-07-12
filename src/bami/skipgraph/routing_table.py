from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from bami.skipgraph import RIGHT, LEFT
from bami.skipgraph.membership_vector import MembershipVector

if TYPE_CHECKING:
    from bami.skipgraph.node import SGNode


class RoutingTable:

    def __init__(self, key: int, mv: MembershipVector):
        self.key: int = key
        self.mv: MembershipVector = mv
        self.max_level: int = 0
        self.levels: List[RoutingTableSingleLevel] = []

        # Initialize all levels
        for level in range(MembershipVector.LENGTH + 1):
            self.levels.append(RoutingTableSingleLevel(self.key, level))

    def height(self) -> int:
        """
        Return the height of the routing table, i.e., the number of levels.
        """
        return len(self.levels)

    def size_per_level(self) -> [int]:
        sizes = []
        for i in range(self.height()):
            s = 0
            if self.levels[i].neighbors[LEFT]:
                s += 1
            if self.levels[i].neighbors[RIGHT]:
                s += 1
            sizes.append(s)
        return sizes

    def num_unique_nodes(self) -> int:
        """
        Return the number of unique nodes in the total routing table, excluding yourself.
        """
        nodes = set()
        for t in self.levels:
            if t.neighbors[LEFT]:
                nodes |= t.neighbors[LEFT]
            if t.neighbors[RIGHT]:
                nodes |= t.neighbors[RIGHT]
        return len(nodes)

    def __str__(self) -> str:
        if not self.levels:
            return "<empty routing table>"

        buf = []
        for i in range(len(self.levels)):
            buf.append(str(self.levels[i]))
        return "\n".join(buf[::-1])


class RoutingTableSingleLevel:
    def __init__(self, own_key: int, level: int):
        self.own_key: int = own_key
        self.neighbors: list[Optional[SGNode]] = [None, None]
        self.level = level

    def __str__(self) -> str:
        ln = "-" if not self.neighbors[LEFT] else str(self.neighbors[LEFT])
        rn = "-" if not self.neighbors[RIGHT] else str(self.neighbors[RIGHT])
        return "Level %d: LEFT=%s, RIGHT=%s" % (self.level, ln, rn)
