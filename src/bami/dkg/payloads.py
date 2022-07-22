from typing import List

from ipv8.messaging.payload_dataclass import dataclass


@dataclass
class SignaturePayload:
    public_key: bytes
    signature: bytes


@dataclass
class RulePayload:
    rule: bytes


@dataclass
class TripletPayload:
    head: bytes
    relation: bytes
    tail: bytes
    signatures: List[SignaturePayload]
    rules: List[RulePayload]


@dataclass(msg_id=20)
class TripletMessage:
    triplet: TripletPayload


@dataclass(msg_id=21)
class StorageRequestPayload:
    identifier: int
    content_identifier: bytes
    key: int


@dataclass(msg_id=22)
class StorageResponsePayload:
    identifier: int
    response: bool


@dataclass(msg_id=23)
class TripletsRequestPayload:
    identifier: int
    content: bytes


@dataclass(msg_id=24)
class TripletsResponsePayload:
    identifier: int
    content: bytes
    total: int
    triplet: TripletPayload


@dataclass(msg_id=25)
class TripletsEmptyResponsePayload:
    identifier: int
    content: bytes


@dataclass(msg_id=26)
class SearchFailurePayload:
    content: bytes
    key_index: int


@dataclass(msg_id=27)
class IsStoringQueryPayload:
    identifier: int
    content: bytes


@dataclass(msg_id=28)
class IsStoringResponsePayload:
    identifier: int
    content: bytes
    storing: bool
