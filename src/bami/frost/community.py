from binascii import unhexlify, hexlify

from bami.frost.aggregator import Aggregator
from bami.frost.participant import Participant
from bami.frost.payload import FrostPublicKeyMessage, FrostShareMessage, FrostAggregatedShareMessage, \
    FrostGroupCommitmentMessage, FrostPartialSignatureMessage

from coincurve._libsecp256k1 import ffi, lib

from ipv8.community import Community
from ipv8.lazy_community import lazy_wrapper


class FrostCommunity(Community):
    community_id = unhexlify("821b39651663b4fddc51fa0d313673ff202f92d6")

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the FROST community and required variables.
        """
        self.participant = Participant()
        self.aggregator = None

        super(FrostCommunity, self).__init__(*args, **kwargs)

        self.add_message_handler(FrostPublicKeyMessage, self.on_frost_public_key)
        self.add_message_handler(FrostShareMessage, self.on_frost_share)
        self.add_message_handler(FrostAggregatedShareMessage, self.on_frost_aggregated_share)
        self.add_message_handler(FrostGroupCommitmentMessage, self.on_group_commitment)
        self.add_message_handler(FrostPartialSignatureMessage, self.on_partial_signature)

    def is_aggregator(self) -> bool:
        return self.participant.index == 0

    def initialize(self, index, num_participants, threshold, managing_peers) -> None:
        self.participant.initialize(index, num_participants, threshold, managing_peers)
        if self.is_aggregator():
            self.aggregator = Aggregator(self.participant, 2)  # TODO hard-coded!!!!

    def share_public_key(self) -> None:
        """
        Share your public key with other peers.
        """
        msg = FrostPublicKeyMessage(pk=bytes(self.participant.session.coeff_pk.data))
        for peer in self.get_peers():
            self.ez_send(peer, msg)

    @lazy_wrapper(FrostPublicKeyMessage)
    def on_frost_public_key(self, peer, payload) -> None:
        if peer.public_key.key_to_bin() not in self.participant.managing_peers:
            self.logger.warning("Peer %s is not a managing peer!" % peer)
            return

        self.participant.received_public_key(peer, payload)

    def share_shares(self) -> None:
        """
        Share your shares with other peers.
        """
        for peer_pk, peer_ind in self.participant.managing_peers.items():
            if peer_pk == self.my_peer.public_key.key_to_bin():
                continue

            peer = self.network.get_verified_by_public_key_bin(peer_pk)
            if not peer:
                self.logger.warning("Peer with public key %s not found in community", hexlify(peer_pk))
                continue

            msg = FrostShareMessage(share=bytes(self.participant.secret_shares[peer_ind].data))
            self.ez_send(peer, msg)

    @lazy_wrapper(FrostShareMessage)
    def on_frost_share(self, peer, payload) -> None:
        if peer.public_key.key_to_bin() not in self.participant.managing_peers:
            self.logger.warning("Peer %s is not a managing peer!" % peer)
            return

        self.participant.received_share(payload.share)
        if self.participant.num_received_shares == self.participant.num_participants:
            self.share_aggregated_share()

    def share_aggregated_share(self):
        if self.is_aggregator():
            self.aggregator.received_aggregated_share(self.participant.index, self.participant.aggregated_share)
        else:
            # Send the aggregated share
            aggregator_peer = self.network.get_verified_by_public_key_bin(self.participant.get_aggregator_pk())
            msg = FrostAggregatedShareMessage(share=bytes(self.participant.aggregated_share.data))
            self.ez_send(aggregator_peer, msg)
            self.participant.generate_nonced_pubkey()

    @lazy_wrapper(FrostAggregatedShareMessage)
    def on_frost_aggregated_share(self, peer, payload) -> None:
        if not self.is_aggregator():
            self.logger.info("Ignoring incoming aggregated share since we are not the aggregator")
            return

        if peer.public_key.key_to_bin() not in self.participant.managing_peers:
            self.logger.warning("Peer %s is not a managing peer!" % peer)
            return

        peer_ind = self.participant.managing_peers[peer.public_key.key_to_bin()]
        aggregated_share = ffi.new("secp256k1_frost_share *")
        aggregated_share.data = payload.share
        self.aggregator.received_aggregated_share(peer_ind, aggregated_share)
        if self.aggregator.has_group_commitment:
            self.share_group_commitment()

            # Compute the partial signature
            self.participant.compute_partial_signature([1, 2])
            self.aggregator.received_partial_signatures.append(self.participant.partial_signature)

    def share_group_commitment(self):
        msg = FrostGroupCommitmentMessage(commitment=bytes(self.aggregator.group_commitment),
                                          aggregated_pk=bytes(self.aggregator.aggregated_pk),
                                          parity=self.participant.session.pk_parity)
        for peer_pk in self.participant.managing_peers.keys():
            if peer_pk == self.my_peer.public_key.key_to_bin():
                continue
            peer = self.network.get_verified_by_public_key_bin(peer_pk)
            self.ez_send(peer, msg)

    @lazy_wrapper(FrostGroupCommitmentMessage)
    def on_group_commitment(self, peer, payload) -> None:
        if peer.public_key.key_to_bin() not in self.participant.managing_peers:
            self.logger.warning("Peer %s is not a managing peer!" % peer)
            return

        self.participant.group_commitment = ffi.new("unsigned char[33]", payload.commitment)
        self.participant.aggregated_pk = ffi.new("unsigned char[33]", payload.aggregated_pk)
        self.participant.aggregated_pk_parity = payload.parity

        self.participant.compute_partial_signature([1, 2])
        self.share_partial_signature()

    def share_partial_signature(self):
        aggregator_peer = self.network.get_verified_by_public_key_bin(self.participant.get_aggregator_pk())
        msg = FrostPartialSignatureMessage(partial_signature=bytes(self.participant.partial_signature))
        self.ez_send(aggregator_peer, msg)

    @lazy_wrapper(FrostPartialSignatureMessage)
    def on_partial_signature(self, peer, payload) -> None:
        if peer.public_key.key_to_bin() not in self.participant.managing_peers:
            self.logger.warning("Peer %s is not a managing peer!" % peer)
            return

        partial_signature = ffi.new("unsigned char[32]", payload.partial_signature)
        self.aggregator.received_partial_signatures.append(partial_signature)

        if len(self.aggregator.received_partial_signatures) == self.participant.num_participants:
            self.aggregator.compute_signature()
