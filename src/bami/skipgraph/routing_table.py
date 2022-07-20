from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

from bami.skipgraph import RIGHT, LEFT, Direction
from bami.skipgraph.membership_vector import MembershipVector

if TYPE_CHECKING:
    from bami.skipgraph.node import SGNode


class RoutingTable:

    def __init__(self, key: int, mv: MembershipVector):
        self.key: int = key
        self.mv: MembershipVector = mv
        self.max_level: int = 0
        self.levels: List[RoutingTableSingleLevel] = []
        self.logger = logging.getLogger(__name__)

        # Initialize all levels
        for level in range(MembershipVector.LENGTH + 1):
            self.levels.append(RoutingTableSingleLevel(self.key, level))

    def get(self, level: int, side: Direction) -> Optional[SGNode]:
        if level >= len(self.levels):
            return None

        return self.levels[level].neighbors[side]

    def set(self, level: int, side: Direction, node: Optional[SGNode]) -> None:
        side_str = "left" if side == LEFT else "right"
        self.logger.debug("Node with key %d setting %s neighbour to %s at level %d", self.key, side_str, node, level)
        self.levels[level].neighbors[side] = node

    def remove_node(self, key: int):
        """
        Remove the node with a particular key from the routing table, replacing it with None.
        """
        for lvl in range(self.height()):
            ln = self.get(lvl, LEFT)
            if ln and ln.key == key:
                self.set(lvl, LEFT, None)

            rn = self.get(lvl, RIGHT)
            if rn and rn.key == key:
                self.set(lvl, RIGHT, None)

    def height(self) -> int:
        """
        Return the height of the routing table, i.e., the number of levels.
        """
        return len(self.levels)

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
