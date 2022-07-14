from ipv8.messaging.lazy_payload import vp_compile, VariablePayload


@vp_compile
class NodeInfoPayload(VariablePayload):
    msg_id = 1
    names = ['address', 'public_key', 'key', 'mv']
    format_list = ['ip_address', 'varlenH', 'I', 'varlenH']


@vp_compile
class SearchPayload(VariablePayload):
    msg_id = 2
    names = ['identifier', 'originator', 'search_key', 'level']
    format_list = ['I', NodeInfoPayload, 'I', 'I']


@vp_compile
class SearchResponsePayload(VariablePayload):
    msg_id = 3
    names = ['identifier', 'response']
    format_list = ['I', NodeInfoPayload]


@vp_compile
class NeighbourRequestPayload(VariablePayload):
    msg_id = 6
    names = ['identifier', 'side', 'level']
    format_list = ['I', '?', 'I']


@vp_compile
class NeighbourResponsePayload(VariablePayload):
    msg_id = 7
    names = ['identifier', 'found', 'neighbour']
    format_list = ['I', '?', NodeInfoPayload]


@vp_compile
class GetLinkPayload(VariablePayload):
    msg_id = 8
    names = ['identifier', 'originator', 'side', 'level']
    format_list = ['I', NodeInfoPayload, '?', 'I']


@vp_compile
class SetLinkPayload(VariablePayload):
    msg_id = 9
    names = ['identifier', 'new_neighbour', 'level']
    format_list = ['I', NodeInfoPayload, 'I']


@vp_compile
class BuddyPayload(VariablePayload):
    msg_id = 10
    names = ['identifier', 'originator', 'level', 'val', 'side']
    format_list = ['I', NodeInfoPayload, 'I', 'I', 'I']
