"""
The Plexus backbone
"""
from abc import ABCMeta
from asyncio import (
    ensure_future,
    Queue,
)
from binascii import hexlify, unhexlify
import random
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.block_sync import BlockSyncMixin
from bami.plexus.backbone.community_routines import MessageStateMachine
from bami.plexus.backbone.datastore.block_store import LMDBLockStore
from bami.plexus.backbone.datastore.chain_store import ChainFactory
from bami.plexus.backbone.datastore.database import BaseDB, ChainTopic, DBManager
from bami.plexus.backbone.datastore.frontiers import Frontier
from bami.plexus.backbone.exceptions import (
    DatabaseDesynchronizedException,
    SubCommunityEmptyException,
    UnknownChainException,
)
from bami.plexus.backbone.gossip import SubComGossipMixin
from bami.plexus.backbone.payload import SubscriptionsPayload
from bami.plexus.backbone.settings import BamiSettings
from bami.plexus.backbone.sub_community import (
    BaseSubCommunity,
    IPv8SubCommunity,
)
from bami.plexus.backbone.utils import (
    decode_raw,
    Dot,
    encode_raw,
    Links,
    Notifier,
)
from ipv8.community import Community
from ipv8.lazy_community import lazy_wrapper
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from ipv8_service import IPv8


class PlexusCommunity(
    Community,
    BlockSyncMixin,
    SubComGossipMixin,
):
    """
    Community for secure backbone.
    """

    community_id = unhexlify("5789fd5555d60c425694ac139d1d8d7ea37d009e")

    def __init__(
        self,
        my_peer: Peer,
        endpoint: Any,
        network: Network,
        ipv8: Optional[IPv8] = None,
        max_peers: int = None,
        anonymize: bool = False,
        db: BaseDB = None,
        work_dir: str = None,
        settings: BamiSettings = None,
        **kwargs
    ):
        """

        Args:
            my_peer:
            endpoint:
            network:
            max_peers:
            anonymize:
            db:
        """
        if not settings:
            self._settings = BamiSettings()
        else:
            self._settings = settings

        if not work_dir:
            work_dir = self.settings.work_directory
        if not db:
            self._persistence = DBManager(ChainFactory(), LMDBLockStore(work_dir))
        else:
            self._persistence = db
        if not max_peers:
            max_peers = self.settings.main_max_peers
        self._ipv8 = ipv8

        super(PlexusCommunity, self).__init__(
            my_peer, endpoint, network, max_peers, anonymize=anonymize
        )

        # Create DB Manager

        self.logger.debug(
            "The Plexus community started with Public Key: %s",
            hexlify(self.my_peer.public_key.key_to_bin()).decode(),
        )
        self.relayed_broadcasts = set()

        # Sub-Communities logic
        self.my_subscriptions = dict()

        self.frontier_sync_tasks = {}

        self.incoming_frontier_queues = {}
        self.processing_queue_tasks = {}

        self.ordered_notifier = Notifier()
        self.unordered_notifier = Notifier()

        # Setup and add message handlers
        for base in PlexusCommunity.__bases__:
            if issubclass(base, MessageStateMachine):
                base.setup_messages(self)

        for base in self.__class__.__mro__:
            if "setup_mixin" in base.__dict__.keys():
                base.setup_mixin(self)

        self.add_message_handler(SubscriptionsPayload, self.received_peer_subs)

    def join_subcom(self, subcom_id: bytes) -> None:
        if subcom_id not in self.my_subcoms:
            # This sub-community is still not known
            # Set max peers setting
            subcom = self.create_subcom(
                subcom_id=subcom_id, max_peers=self.settings.subcom_max_peers
            )
            # Add the sub-community to the main overlay
            self.my_subscriptions[subcom_id] = subcom

    def is_subscribed(self, community_id: bytes) -> bool:
        return community_id in self.my_subcoms

    def subscribe_to_subcom(self, subcom_id: bytes) -> None:
        """
        Subscribe to the SubCommunity with the public key master peer.
        Community is identified with a community_id.

        Args:
            subcom_id: bytes identifier of the community
        """
        if subcom_id not in self.my_subcoms:
            self.join_subcom(subcom_id)

            # Join the protocol audits/ updates
            self.on_join_subcommunity(subcom_id)

            # Notify other peers that you are part of the new community
            self.notify_peers_on_new_subcoms()

    def create_subcom(self, *args, **kwargs) -> BaseSubCommunity:
        """
        By default, the BackboneCommunity will start additional IPv8 sub-communities.
        This behaviour can be changed by subclasses.
        """
        return IPv8SubCommunity(self.my_peer, self.endpoint, self.network, *args, **kwargs)

    def on_join_subcommunity(self, sub_com_id: bytes) -> None:
        """
        This method is called when BAMI joins a new sub-community.
        We enable some functionality by default, but custom behaviour can be added by overriding this method.

        Args:
            sub_com_id: The ID of the sub-community we just joined.

        """
        if self.settings.frontier_gossip_enabled:
            # Start exchanging frontiers
            self.start_frontier_gossip_sync(sub_com_id)

        # By default, add a handler that is called with subsequent blocks
        self.subscribe_in_order_block(sub_com_id, self.process_block_ordered)

    # ----- Update notifiers for new blocks ------------

    def get_block_and_blob_by_dot(
        self, chain_id: bytes, dot: Dot
    ) -> Tuple[bytes, PlexusBlock]:
        """Get blob and serialized block and by the chain_id and dot.
        Can raise DatabaseDesynchronizedException if no block found."""
        blk_blob = self.persistence.get_block_blob_by_dot(chain_id, dot)
        if not blk_blob:
            raise DatabaseDesynchronizedException(
                "Block is not found in db: {chain_id}, {dot}".format(
                    chain_id=chain_id, dot=dot
                )
            )
        block = PlexusBlock.unpack(blk_blob, self.serializer)
        return blk_blob, block

    def get_block_by_dot(self, chain_id: bytes, dot: Dot) -> PlexusBlock:
        """Get block by the chain_id and dot. Can raise DatabaseDesynchronizedException"""
        return self.get_block_and_blob_by_dot(chain_id, dot)[1]

    def block_notify(self, chain_id: bytes, dots: List[Dot]):
        for dot in dots:
            block = self.get_block_by_dot(chain_id, dot)
            self.ordered_notifier.notify(chain_id, block)

    def subscribe_in_order_block(
        self, topic: Union[bytes, ChainTopic], callback: Callable[[PlexusBlock], None]
    ):
        """Subscribe on block updates received in-order. Callable will receive the block."""
        self._persistence.add_unique_observer(topic, self.block_notify)
        self.ordered_notifier.add_observer(topic, callback)

    def subscribe_out_order_block(
        self, topic: Union[bytes, ChainTopic], callback: Callable[[PlexusBlock], None]
    ):
        """Subscribe on block updates received in-order. Callable will receive the block."""
        self.unordered_notifier.add_observer(topic, callback)

    def process_block(self, blk: PlexusBlock, peer: Peer) -> None:
        self.unordered_notifier.notify(blk.community_id, blk)
        if peer != self.my_peer:
            frontier = Frontier(
                terminal=Links((blk.com_dot,)), holes=(), inconsistencies=()
            )
            processing_queue = self.incoming_frontier_queue(blk.community_id)
            if not processing_queue:
                raise UnknownChainException(
                    "Cannot process block received block with unknown chain. {subcom_id}".format(
                        subcom_id=blk.community_id
                    )
                )
            processing_queue.put_nowait((peer, frontier, True))

    # ---- Introduction handshakes => Exchange your subscriptions ----------------
    def create_introduction_request(
        self, socket_address: Any, extra_bytes: bytes = b"", new_style: bool = False
    ):
        extra_bytes = encode_raw(self.my_subcoms)
        return super().create_introduction_request(
            socket_address, extra_bytes, new_style
        )

    def create_introduction_response(
        self,
        lan_socket_address,
        socket_address,
        identifier,
        introduction=None,
        extra_bytes=b"",
        prefix=None,
        new_style: bool = False,
    ):
        extra_bytes = encode_raw(self.my_subcoms)
        return super().create_introduction_response(
            lan_socket_address,
            socket_address,
            identifier,
            introduction,
            extra_bytes,
            prefix,
            new_style,
        )

    def introduction_response_callback(self, peer, dist, payload):
        subcoms = decode_raw(payload.extra_bytes)
        self.process_peer_subscriptions(peer, subcoms)
        if self.settings.track_neighbours_chains:
            self.subscribe_to_subcom(peer.public_key.key_to_bin())

    def introduction_request_callback(self, peer, dist, payload):
        subcoms = decode_raw(payload.extra_bytes)
        self.process_peer_subscriptions(peer, subcoms)
        if self.settings.track_neighbours_chains:
            self.subscribe_to_subcom(peer.public_key.key_to_bin())

    # ----- Community routines ------

    async def unload(self):
        self.logger.debug("Unloading the Plexus Community.")

        for base in self.__class__.__mro__:
            if "unload_mixin" in base.__dict__.keys():
                base.unload_mixin(self)

        for mid in self.processing_queue_tasks:
            if not self.processing_queue_tasks[mid].done():
                self.processing_queue_tasks[mid].cancel()
        for subcom_id in self.my_subscriptions:
            await self.my_subscriptions[subcom_id].unload()
        await super(PlexusCommunity, self).unload()

        # Close the persistence layer
        self.persistence.close()

    @property
    def settings(self) -> BamiSettings:
        return self._settings

    @property
    def persistence(self) -> BaseDB:
        return self._persistence

    @property
    def my_pub_key_bin(self) -> bytes:
        return self.my_peer.public_key.key_to_bin()

    def send_packet(self, peer: Peer, packet: Any, sig: bool = True) -> None:
        self.ez_send(peer, packet, sig=sig)

    # ----- SubCommunity routines ------

    @property
    def my_subcoms(self) -> Iterable[bytes]:
        return list(self.my_subscriptions.keys())

    def get_subcom(self, subcom_id: bytes) -> Optional[BaseSubCommunity]:
        return self.my_subscriptions.get(subcom_id)

    def process_peer_subscriptions(self, peer: Peer, subcoms: List[bytes]) -> None:
        for c in subcoms:
            # For each sub-community that is also known to me - introduce peer.
            if c in self.my_subscriptions:
                self.my_subscriptions[c].add_peer(peer)

    @lazy_wrapper(SubscriptionsPayload)
    def received_peer_subs(self, peer: Peer, payload: SubscriptionsPayload) -> None:
        subcoms = decode_raw(payload.subcoms)
        self.process_peer_subscriptions(peer, subcoms)

    def notify_peers_on_new_subcoms(self) -> None:
        for peer in self.get_peers():
            self.send_packet(
                peer,
                SubscriptionsPayload(self.my_pub_key_bin, encode_raw(self.my_subcoms)),
            )

    # -------- Community block and frontier exchange  -------------

    def start_frontier_gossip_sync(
        self,
        community_id: bytes,
        delay: Callable[[], float] = None,
        interval: Callable[[], float] = None,
    ) -> None:
        self.logger.info("Starting gossip with frontiers on chain %s", hexlify(community_id).decode())
        self.frontier_sync_tasks[community_id] = self.register_task(
            "gossip_sync_" + hexlify(community_id).decode(),
            self.frontier_gossip_sync_task,
            community_id,
            delay=delay if delay else self._settings.frontier_gossip_sync_max_delay,
            interval=interval if interval else self._settings.frontier_gossip_interval,
        )
        self.incoming_frontier_queues[community_id] = Queue()
        self.processing_queue_tasks[community_id] = ensure_future(
            self.process_frontier_queue(community_id)
        )

    def incoming_frontier_queue(self, subcom_id: bytes) -> Optional[Queue]:
        return self.incoming_frontier_queues.get(subcom_id)

    def choose_community_peers(
        self, com_peers: List[Peer], current_seed: Any, commitee_size: int
    ) -> List[Peer]:
        rand = random.Random(current_seed)
        return rand.sample(com_peers, min(commitee_size, len(com_peers)))

    def share_in_community(
        self,
        block: Union[PlexusBlock, bytes],
        subcom_id: bytes = None,
        ttl: int = None,
        fanout: int = None,
        seed: Any = None,
    ) -> None:
        """
        Share a block with peers in a sub-community via push-based gossip.
        Args:
            block: the PlexusBlock to share, either as PlexusBlock instance or in serialized form
            subcom_id: identity of the sub-community, if not specified the main community connections will be used.
            ttl: ttl of the gossip, if not specified the default settings will be used
            fanout: of the gossip, if not specified the default settings will be used
            seed: seed for the peers selection, if not specified a random value will be used
        """
        if not subcom_id or not self.get_subcom(subcom_id):
            subcom_peers = self.get_peers()
        else:
            subcom_peers = self.get_subcom(subcom_id).get_known_peers()
        if not seed:
            seed = random.random()
        if not fanout:
            fanout = self.settings.push_gossip_fanout
        if not ttl:
            ttl = self.settings.push_gossip_ttl
        if subcom_peers:
            selected_peers = self.choose_community_peers(subcom_peers, seed, fanout)
            self.send_block(block, selected_peers, ttl)


class PlexusTestnetCommunity(PlexusCommunity, metaclass=ABCMeta):
    """
    This community defines the testnet for Plexus
    """

    DB_NAME = "plexus_testnet"
    community_id = unhexlify("4dcfcf5bacc89aa5af93ef8a695580c9ddf1f1a0")
