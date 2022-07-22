from binascii import hexlify
from typing import Set

from bami.dkg.content import Content
from bami.dkg.rules.rule import Rule
from bami.dkg.db.triplet import Triplet


class DummyRule(Rule):
    """
    A dummy rule that generates fixed edges. Useful for testing.
    """
    RULE_NAME = b"DUMMY"

    def apply_rule(self, content: Content) -> Set[Triplet]:
        return {Triplet(hexlify(content.identifier), b"a", b"b")}
