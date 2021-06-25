from ipv8.messaging.lazy_payload import VariablePayload, vp_compile


class ComparablePayload(VariablePayload):
    def __eq__(self, o: VariablePayload) -> bool:
        return self.format_list == o.format_list and self.names == o.names


@vp_compile
class BlockPayload(ComparablePayload):
    msg_id = 2
    format_list = [
        "varlenI",
        "varlenI",
        "74s",
        "I",
        "varlenI",
        "varlenI",
        "20s",
        "I",
        "64s",
        "Q",
    ]
    names = [
        "type",
        "transaction",
        "public_key",
        "sequence_number",
        "previous",
        "community_links",
        "community_id",
        "community_sequence_number",
        "signature",
        "timestamp",
    ]


@vp_compile
class BlockBroadcastPayload(BlockPayload):
    """
    Payload for a message that contains a half block and a TTL field for broadcasts.
    """

    msg_id = 4
    format_list = BlockPayload.format_list + ["I"]
    names = BlockPayload.names + ["ttl"]


@vp_compile
class FrontierPayload(ComparablePayload):
    msg_id = 5
    format_list = ["varlenH", "varlenH"]
    names = ["chain_id", "frontier"]


@vp_compile
class SubscriptionsPayload(ComparablePayload):
    """
    Payload that contains a list of all communities that a specific peer is part of.
    """

    msg_id = 7
    format_list = ["74s", "varlenH"]
    names = ["public_key", "subcoms"]


@vp_compile
class BlocksRequestPayload(ComparablePayload):
    msg_id = 8
    format_list = ["varlenH", "varlenH"]
    names = ["subcom_id", "frontier_diff"]


@vp_compile
class FrontierResponsePayload(ComparablePayload):
    msg_id = 11
    format_list = ["varlenH", "varlenH"]
    names = ["chain_id", "frontier"]
