from asyncio import Future
from typing import List

from bami.dkg.db.triplet import Triplet
from ipv8.requestcache import RandomNumberCache


class StorageRequestCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "store")
        self.future = Future()


class IsStoringQueryCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "is-storing")
        self.future = Future()


# TODO no timeouts in this cache yet!
class TripletsRequestCache(RandomNumberCache):

    def __init__(self, community):
        super().__init__(community.request_cache, "triplets")
        self.future = Future()
