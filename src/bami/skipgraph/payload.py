from ipv8.messaging.lazy_payload import vp_compile, VariablePayload


@vp_compile
class NodeInfoPayload(VariablePayload):
    msg_id = 1
    names = ['address', 'public_key', 'key', 'mv']
    format_list = ['ip_address', 'varlenH', 'I', 'varlenH']


@vp_compile
class SearchNextNodesRequestPayload(VariablePayload):
    msg_id = 2
    names = ['identifier', 'search_key', 'level']
    format_list = ['I', 'I', 'I']


@vp_compile
class SearchNextNodesResponsePayload(VariablePayload):
    msg_id = 3
    names = ['identifier', 'response', 'level']
    format_list = ['I', NodeInfoPayload, 'I']


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


@vp_compile
class DeletePayload(VariablePayload):
    msg_id = 11
    names = ['identifier', 'originator', 'level']
    format_list = ['I', NodeInfoPayload, 'I']


@vp_compile
class NoNeighbourPayload(VariablePayload):
    msg_id = 12
    names = ['identifier', 'level']
    format_list = ['I', 'I']


@vp_compile
class FindNewNeighbourPayload(VariablePayload):
    msg_id = 13
    names = ['identifier', 'originator', 'level']
    format_list = ['I', NodeInfoPayload, 'I']


@vp_compile
class FoundNewNeighbourPayload(VariablePayload):
    msg_id = 14
    names = ['identifier', 'neighbour', 'level']
    format_list = ['I', NodeInfoPayload, 'I']


@vp_compile
class ConfirmDeletePayload(VariablePayload):
    msg_id = 15
    names = ['identifier', 'level']
    format_list = ['I', 'I']


@vp_compile
class SetNeighbourNilPayload(VariablePayload):
    msg_id = 16
    names = ['identifier', 'originator', 'level']
    format_list = ['I', NodeInfoPayload, 'I']
