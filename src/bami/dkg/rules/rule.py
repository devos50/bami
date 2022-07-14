from abc import ABC, abstractmethod
from typing import Set, Any

from bami.dkg.db.triplet import Triplet
from bami.dkg.content import Content


class Rule(ABC):
    RULE_NAME = None

    @abstractmethod
    def apply_rule(self, content: Content) -> Set[Triplet]:
        """
        Apply this rule to a piece of content. Returns a set of triplets in the knowledge graph.
        """
        pass

    @staticmethod
    def convert_to_bytes(label: Any):
        """
        Convert a knowledge graph node label to bytes.
        """
        if isinstance(label, int):
            label = b"%d" % label
        elif isinstance(label, str):
            label = label.encode()
        return label

    def get_name(self) -> str:
        return self.RULE_NAME
