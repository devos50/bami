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
        self.request_cache = community.request_cache
        self.expected_triplets = 0
        self.triplets: List[Triplet] = []
        self.future = Future()

    def on_triplet_response(self, payload):
        if self.expected_triplets == 0:
            self.expected_triplets = payload.total

        self.triplets.append(payload.triplet)

        if len(self.triplets) == self.expected_triplets:
            self.future.set_result(self.triplets)
            self.request_cache.pop("triplets", self.number)
