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

    @property
    def timeout_delay(self):
        return 3600.0


class SearchNextNodesRequestCache(RandomNumberCache):
    """
    This cache keeps track of a request to fetch subsequent nodes during a search.
    """
    TIMEOUT = 2.0

    def __init__(self, community, search_identifier: int):
        super().__init__(community.request_cache, "search-next-nodes")
        self.search_identifier: int = search_identifier  # The identifier of the overarching search
        self.future: Future = Future()

    @property
    def timeout_delay(self) -> float:
        return SearchNextNodesRequestCache.TIMEOUT

    def on_timeout(self):
        self.future.set_result(None)


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
