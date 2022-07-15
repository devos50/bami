from typing import List, Tuple

from ipv8.messaging.payload_dataclass import dataclass


@dataclass
class TripletPayload:
    head: bytes
    relation: bytes
    tail: bytes
    signatures: List[Tuple[bytes, bytes]]


@dataclass(msg_id=11)
class TripletMessage:
    triplet: TripletPayload


@dataclass(msg_id=12)
class StorageRequestPayload:
    identifier: int
    key: int


@dataclass(msg_id=13)
class StorageResponsePayload:
    identifier: int
    response: bool


@dataclass(msg_id=14)
class TripletsRequestPayload:
    identifier: int
    content: bytes


@dataclass(msg_id=15)
class TripletsResponsePayload:
    identifier: int
    content: bytes
    total: int
    triplet: TripletPayload


@dataclass(msg_id=16)
class TripletsEmptyResponsePayload:
    identifier: int
    content: bytes
