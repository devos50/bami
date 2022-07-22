from typing import List, Tuple

from bami.dkg.payloads import TripletPayload, SignaturePayload, RulePayload


class Triplet:

    def __init__(self, head: bytes, relation: bytes, tail: bytes):
        self.head: bytes = head
        self.relation: bytes = relation
        self.tail: bytes = tail
        self.signatures: List[Tuple[bytes, bytes]] = []
        self.rules: List[bytes] = []  # The rules that generated the triplet

    def add_signature(self, public_key: bytes, signature: bytes) -> None:
        self.signatures.append((public_key, signature))

    def add_rule(self, rule: bytes) -> None:
        self.rules.append(rule)

    def to_payload(self) -> TripletPayload:
        signatures = [SignaturePayload(key, sig) for key, sig in self.signatures]
        rules = [RulePayload(rule_name.encode()) for rule_name in self.rules]
        payload = TripletPayload(self.head, self.relation, self.tail, signatures, rules)
        return payload

    @staticmethod
    def from_payload(payload: TripletPayload):
        triplet = Triplet(payload.head, payload.relation, payload.tail)
        triplet.signatures = payload.signatures
        triplet.rules = payload.rules
        return triplet

    def __str__(self) -> str:
        return "<%s, %s, %s>" % (self.head.decode(), self.relation.decode(), self.tail.decode())
