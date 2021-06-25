from abc import ABC, abstractmethod, ABCMeta
from random import sample, shuffle
from typing import Iterable

from bami.plexus.backbone.community_routines import CommunityRoutines
from bami.plexus.backbone.datastore.frontiers import Frontier
from bami.plexus.backbone.sub_community import SubCommunityRoutines
from ipv8.peer import Peer


class NextPeerSelectionStrategy(ABC):
    @abstractmethod
    def get_next_gossip_peers(
        self, subcom_id: bytes, chain_id: bytes, my_frontier: Frontier, number: int
    ) -> Iterable[Peer]:
        """
        Get peers for the next gossip round.
        Args:
            subcom_id: identifier for the sub-community
            chain_id: identifier for the chain to gossip
            my_frontier: Current local frontier to share
            number: Number of peers to request

        Returns:
            Iterable with peers for the next gossip round
        """
        pass


class RandomPeerSelectionStrategy(
    NextPeerSelectionStrategy,
    SubCommunityRoutines,
    CommunityRoutines,
    metaclass=ABCMeta,
):
    def get_next_gossip_peers(
        self,
        subcom_id: bytes,
        chain_id: bytes,
        my_frontier: Frontier,
        number_peers: int,
    ) -> Iterable[Peer]:
        subcom = self.get_subcom(subcom_id)
        peer_set = subcom.get_known_peers() if subcom else []
        f = min(len(peer_set), number_peers)
        return sample(peer_set, f)


class SmartPeerSelectionStrategy(
    NextPeerSelectionStrategy,
    SubCommunityRoutines,
    CommunityRoutines,
    metaclass=ABCMeta,
):
    """
    This peer selection strategy picks the next peer based on the age of the frontier it has received from other peers.
    Specifically, it will prefer to ask peers from which it has received an older frontier earlier.
    """

    def get_next_gossip_peers(
        self, subcom_id: bytes, chain_id: bytes, my_frontier: Frontier, number: int
    ) -> Iterable[Peer]:
        subcom = self.get_subcom(subcom_id)
        peer_set = subcom.get_known_peers() if subcom else []

        selected_peers = []
        for p in peer_set:
            known_frontier = self.persistence.get_last_frontier(
                chain_id, p.public_key.key_to_bin()
            )
            if my_frontier > known_frontier:
                selected_peers.append(p)
        shuffle(selected_peers)
        return (
            selected_peers[:number] if len(selected_peers) > number else selected_peers
        )
