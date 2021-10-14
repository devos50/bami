from dataclasses import dataclass

from ipv8.messaging.payload_dataclass import type_from_format, overwrite_dataclass

dataclass = overwrite_dataclass(dataclass)


@dataclass(msg_id=1)
class FrostPublicKeyMessage:
    pk: type_from_format('64s')
