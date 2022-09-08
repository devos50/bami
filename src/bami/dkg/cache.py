from asyncio import Future

from ipv8.requestcache import RandomNumberCache


class StorageRequestCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "store")
        self.future = Future()


class IsStoringQueryCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "is-storing")
        self.future = Future()


class TripletsRequestCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "triplets")
        self.future = Future()

    @property
    def timeout_delay(self):
        return 5.0

    def on_timeout(self):
        self.future.set_result(None)
