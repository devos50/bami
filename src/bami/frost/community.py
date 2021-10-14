import os
from binascii import unhexlify

from bami.frost.payload import FrostPublicKeyMessage
from ipv8.community import Community

from coincurve._libsecp256k1 import ffi, lib
from coincurve import GLOBAL_CONTEXT

from ipv8.lazy_community import lazy_wrapper


class FrostCommunity(Community):
    community_id = unhexlify("821b39651663b4fddc51fa0d313673ff202f92d6")

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the FROST community and required variables.
        """
        self.context = GLOBAL_CONTEXT
        self.index = -1
        self.num_participants = kwargs.pop("num_participants")
        self.threshold = kwargs.pop("threshold")

        super(FrostCommunity, self).__init__(*args, **kwargs)

        self.session = ffi.new("secp256k1_frost_keygen_session *")
        self.secret_key = os.urandom(32)
        self.private_coefficients = ffi.new("secp256k1_scalar[%d]" % self.threshold)
        self.public_coefficients = ffi.new("secp256k1_pubkey[%d]" % self.threshold)
        self.secret_shares = ffi.new("secp256k1_frost_share[%d]" % self.num_participants)
        self.aggregated_share = ffi.new("secp256k1_frost_share *")
        self.partial_signature = ffi.new("unsigned char[32]")
        self.nonce = ffi.new("secp256k1_frost_secnonce *")

        self.frost_public_keys = {}

        self.add_message_handler(FrostPublicKeyMessage, self.on_frost_public_key)

        res = lib.secp256k1_frost_keygen_init(self.context.ctx,
                                              self.session,
                                              self.private_coefficients,
                                              self.public_coefficients,
                                              self.threshold,
                                              self.num_participants,
                                              self.index + 1,
                                              self.secret_key)

        if res == 0:
            raise Exception("Keygen initialization failed for participant %d" % (self.index + 1))

    def share_public_key(self) -> None:
        """
        Share your public key with other peers.
        """
        msg = FrostPublicKeyMessage(pk=bytes(self.session.coeff_pk.data))
        for peer in self.get_peers():
            self.ez_send(peer, msg)

    @lazy_wrapper(FrostPublicKeyMessage)
    def on_frost_public_key(self, peer, payload) -> None:
        pk = ffi.new("secp256k1_pubkey *")
        pk.data = payload.pk
        self.frost_public_keys[peer] = pk
