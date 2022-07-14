from asyncio import Future

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


class SearchRequestCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "search")
        self.future = Future()
