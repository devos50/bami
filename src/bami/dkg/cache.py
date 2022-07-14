from asyncio import Future

from ipv8.requestcache import RandomNumberCache


class StorageRequestCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "store")
        self.future = Future()
