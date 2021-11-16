from dataclasses import dataclass

from ipv8.messaging.payload_dataclass import type_from_format, overwrite_dataclass

dataclass = overwrite_dataclass(dataclass)


@dataclass(msg_id=1)
class FrostPublicKeyMessage:
    pk: type_from_format('64s')


@dataclass(msg_id=2)
class FrostShareMessage:
    share: type_from_format('32s')


@dataclass(msg_id=3)
class FrostAggregatedShareMessage:
    share: type_from_format('32s')


@dataclass(msg_id=4)
class FrostGroupCommitmentMessage:
    commitment: type_from_format('varlenH')
    aggregated_pk: type_from_format('varlenH')
    parity: type_from_format('I')


@dataclass(msg_id=5)
class FrostPartialSignatureMessage:
    partial_signature: type_from_format('32s')
