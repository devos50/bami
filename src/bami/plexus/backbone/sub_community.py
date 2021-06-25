from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, Optional

from ipv8.community import Community
from ipv8.peer import Peer

from bami.plexus.backbone.community_routines import CommunityRoutines
from bami.plexus.backbone.exceptions import InvalidSubCommunityIdException


class BaseSubCommunity(ABC):
    @property
    @abstractmethod
    def subcom_id(self) -> bytes:
        pass

    @abstractmethod
    def get_known_peers(self) -> Iterable[Peer]:
        """
        Get all peers known to be in this sub-community
        Returns: list of known peers in the sub-community
        """
        pass

    @abstractmethod
    def add_peer(self, peer: Peer):
        pass

    @abstractmethod
    async def unload(self):
        pass


class IPv8SubCommunity(Community, BaseSubCommunity):
    def get_known_peers(self) -> Iterable[Peer]:
        return self.get_peers()

    @property
    def subcom_id(self) -> bytes:
        return self._subcom_id

    def __init__(self, *args, **kwargs):
        self._subcom_id = kwargs.pop("subcom_id")

        if len(self._subcom_id) != 20:
            raise InvalidSubCommunityIdException("Subcommunity ID should be 20 bytes and not %d bytes" %
                                                 len(self._subcom_id))

        self.community_id = self._subcom_id
        super().__init__(*args, **kwargs)

    def add_peer(self, peer: Peer):
        self.network.add_verified_peer(peer)
        self.network.discover_services(peer, [self.community_id])


class SubCommunityRoutines(ABC):
    @property
    @abstractmethod
    def my_subcoms(self) -> Iterable[bytes]:
        """
        All sub-communities that my peer is part of
        Returns: list with sub-community ids
        """
        pass

    @abstractmethod
    def get_subcom(self, sub_com: bytes) -> Optional[BaseSubCommunity]:
        pass

    @abstractmethod
    def notify_peers_on_new_subcoms(self) -> None:
        """Notify other peers on updates of the sub-communities"""
        pass

    @abstractmethod
    def on_join_subcommunity(self, sub_com_id: bytes) -> None:
        """
        Join to the gossip process for the sub-community
        Args:
            sub_com_id: sub-community identifier
        """
        pass
