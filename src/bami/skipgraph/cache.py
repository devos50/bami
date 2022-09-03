from asyncio import Future, get_event_loop
from typing import Set, Optional

from bami.skipgraph.node import SGNode
from ipv8.requestcache import RandomNumberCache


class NeighbourRequestCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "neighbour")
        self.future = Future()


class LinkRequestCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "link")
        self.future = Future()


class BuddyCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "buddy")
        self.future = Future()


class SearchCache(RandomNumberCache):
    """
    This cache keeps track of the status of the ongoing search.
    """

    def __init__(self, community, search_key: int, level: int):
        super().__init__(community.request_cache, "search")
        self.search_key: int = search_key
        self.level: int = level
        self.hops: int = 0
        self.next_nodes_future: Optional[Future] = None
        self.start_time = get_event_loop().time()
        self.nodes: Set[SGNode] = set()


class DeleteCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "delete")
        self.future = Future()


class SetNeighbourNilCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "set-neighbour-nil")
        self.future = Future()


class FindNewNeighbourCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "find-new-neighbour")
        self.future = Future()
