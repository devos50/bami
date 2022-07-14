from typing import List, Tuple

from ipv8.messaging.payload_dataclass import dataclass


@dataclass(msg_id=11)
class TripletsMessage:

    @dataclass
    class Triplet:
        head: bytes
        relation: bytes
        tail: bytes
        signatures: List[Tuple[bytes, bytes]]

    triplets: [Triplet]


@dataclass(msg_id=12)
class StorageRequestPayload:
    identifier: int
    key: int


@dataclass(msg_id=13)
class StorageResponsePayload:
    identifier: int
    response: bool
