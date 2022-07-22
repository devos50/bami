from typing import Set

from bami.dkg.db.triplet import Triplet
from bami.dkg.content import Content
from bami.dkg.rule_execution_engine import RuleExecutionEngine
from bami.dkg.rules.rule import Rule

import PTN


class PTNRule(Rule):
    RULE_NAME = b"PTN"

    def apply_rule(self, engine: RuleExecutionEngine, content: Content) -> Set[Triplet]:
        metadata = PTN.parse(content.data.decode())
        triplets = set()
        for relation, tail in metadata.items():
            if not tail:
                continue
            if relation == "excess":
                continue

            relation = Rule.convert_to_bytes(relation)

            # Some items can be a list and we have to add multiple triplets
            if isinstance(tail, list):
                for tail_item in tail:
                    triplets.add(Triplet(content.identifier, relation, Rule.convert_to_bytes(tail_item)))
            else:
                triplets.add(Triplet(content.identifier, relation, Rule.convert_to_bytes(tail)))
        return triplets
