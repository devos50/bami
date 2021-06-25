from asyncio.queues import Queue
from typing import Any, Dict, Iterable, Optional, Type, Union

from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.blockresponse import BlockResponseMixin, BlockResponse
from bami.plexus.backbone.community import PlexusCommunity
from bami.plexus.backbone.community_routines import (
    CommunityRoutines,
    MessageStateMachine,
)
from bami.plexus.backbone.datastore.database import BaseDB
from bami.plexus.backbone.settings import BamiSettings
from bami.plexus.backbone.sub_community import (
    BaseSubCommunity,
    SubCommunityRoutines,
)
from ipv8.community import Community
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.peer import Peer
from ipv8.requestcache import RequestCache

from tests.plexus.mocking.mock_db import MockDBManager


class FakeRoutines(CommunityRoutines):
    @property
    def request_cache(self) -> RequestCache:
        pass

    @property
    def ipv8(self) -> Optional[Any]:
        pass

    @property
    def settings(self) -> Any:
        return BamiSettings()

    def __init__(self):
        self.crypto = default_eccrypto
        self.key = self.crypto.generate_key(u"medium")

    @property
    def my_pub_key(self) -> bytes:
        return self.key.pub().key_to_bin()

    def send_packet(self, peer: Peer, packet: Any) -> None:
        pass

    @property
    def persistence(self) -> BaseDB:
        return MockDBManager()


class MockSubCommuntiy(BaseSubCommunity):
    async def unload(self):
        pass

    def add_peer(self, peer: Peer):
        pass

    @property
    def subcom_id(self) -> bytes:
        pass

    def get_known_peers(self) -> Iterable[Peer]:
        pass


class MockSubCommunityRoutines(SubCommunityRoutines):
    def discovered_peers_by_subcom(self, subcom_id) -> Iterable[Peer]:
        pass

    def get_subcom(self, sub_com: bytes) -> Optional[BaseSubCommunity]:
        pass

    @property
    def my_subcoms(self) -> Iterable[bytes]:
        pass

    def notify_peers_on_new_subcoms(self) -> None:
        pass

    def on_join_subcommunity(self, sub_com_id: bytes) -> None:
        pass


class MockSettings(object):
    @property
    def frontier_gossip_collect_time(self):
        return 0.2

    @property
    def frontier_gossip_fanout(self):
        return 5


class MockedCommunity(Community, CommunityRoutines):
    community_id = Peer(default_eccrypto.generate_key(u"very-low")).mid

    def __init__(self, *args, **kwargs):
        if kwargs.get("work_dir"):
            self.work_dir = kwargs.pop("work_dir")
        super().__init__(*args, **kwargs)
        self._req = RequestCache()

        for base in self.__class__.__bases__:
            if issubclass(base, MessageStateMachine):
                base.setup_messages(self)

    @property
    def persistence(self) -> BaseDB:
        return MockDBManager()

    @property
    def settings(self) -> Any:
        return MockSettings()

    def send_packet(self, *args, **kwargs) -> None:
        self.ez_send(*args, **kwargs)

    @property
    def request_cache(self) -> RequestCache:
        return self._req

    async def unload(self):
        await self._req.shutdown()
        return await super().unload()


class FakeBackCommunity(PlexusCommunity, BlockResponseMixin):
    community_id = b"\x00" * 20


