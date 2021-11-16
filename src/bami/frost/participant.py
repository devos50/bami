import logging
import os

from binascii import hexlify
from coincurve._libsecp256k1 import ffi, lib
from coincurve import GLOBAL_CONTEXT

from bami.frost import EMPTY_PK, MSG


class Participant:
    def __init__(self):
        self.context = GLOBAL_CONTEXT
        self.index = -1
        self.num_participants = -1
        self.threshold = -1
        self.managing_peers = None

        self.session = ffi.new("secp256k1_frost_keygen_session *")
        self.session_id = b'\x00' * 32 # os.urandom(32)
        self.secret_key = None
        self.private_coefficients = None
        self.public_coefficients = None
        self.secret_shares = None
        self.aggregated_share = ffi.new("secp256k1_frost_share *")
        self.partial_signature = ffi.new("unsigned char[32]")
        self.nonce = ffi.new("secp256k1_frost_secnonce *")
        self.group_commitment = None
        self.aggregated_pk = None
        self.aggregated_pk_parity = None

        self.frost_public_keys = None
        self.public_keys_received = 0

        self.received_shares = None
        self.num_received_shares = 0

        self.logger = logging.getLogger(self.__class__.__name__)

    def initialize(self, index, num_participants, threshold, managing_peers):
        self.index = index
        self.num_participants = num_participants
        self.threshold = threshold
        self.managing_peers = managing_peers

        self.secret_key = chr(index).encode() * 32  # os.urandom(32)
        self.private_coefficients = ffi.new("secp256k1_scalar[%d]" % self.threshold)
        self.public_coefficients = ffi.new("secp256k1_pubkey[%d]" % self.threshold)
        self.secret_shares = ffi.new("secp256k1_frost_share[%d]" % self.num_participants)

        self.frost_public_keys = ffi.new("secp256k1_pubkey[%d]" % self.num_participants)
        self.public_keys_received = 0

        self.received_shares = ffi.new("secp256k1_frost_share[%d]" % self.num_participants)
        self.num_received_shares = 0

        self.init_key()

        self.frost_public_keys[index].data = self.session.coeff_pk.data

    def init_key(self):
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

    def get_aggregator_pk(self):
        for peer_pk, peer_ind in self.managing_peers.items():
            if peer_ind == 0:
                return peer_pk
        return None

    def combine_pubkeys(self):
        res = lib.secp256k1_frost_pubkey_combine(self.context.ctx,
                                                 ffi.NULL,
                                                 self.session,
                                                 self.frost_public_keys)
        print("Group public key: %s" % hexlify(bytes(self.session.combined_pk.data)).decode())
        self.logger.info("Derived group public key: %s" % hexlify(bytes(self.session.combined_pk.data)).decode())
        if res == 0:
            raise Exception("Combining public keys failed for participant %d" % (self.index + 1))

    def generate_shares(self):
        lib.secp256k1_frost_generate_shares(self.secret_shares,
                                            self.private_coefficients,
                                            self.session)
        self.received_shares[self.num_received_shares] = self.secret_shares[self.index]
        self.num_received_shares = 1

    def aggregate_shares(self):
        lib.secp256k1_frost_aggregate_shares(self.aggregated_share,
                                             self.received_shares,
                                             self.session)

    def received_public_key(self, peer, payload):
        peer_ind = self.managing_peers[peer.public_key.key_to_bin()]
        if bytes(self.frost_public_keys[peer_ind].data) == EMPTY_PK:
            self.frost_public_keys[peer_ind].data = payload.pk
            self.public_keys_received += 1
            if self.public_keys_received == self.num_participants - 1:
                # We received all keys - compute the group key and generate secret shares
                self.combine_pubkeys()
                self.generate_shares()

    def received_share(self, share):
        self.received_shares[self.num_received_shares].data = share
        self.num_received_shares += 1

        if self.num_received_shares == self.num_participants:
            self.logger.info("Received sufficient shares - aggregating them")
            self.aggregate_shares()

            aggregated_share = hexlify(bytes(self.aggregated_share.data)).decode()
            print("Aggregated share of participant %d: %s" % (self.index, aggregated_share))

    def generate_nonced_pubkey(self):
        serialized_group_key = ffi.new("unsigned char[33]")
        lib.secp256k1_xonly_pubkey_serialize(self.context.ctx, serialized_group_key, ffi.addressof(self.session, "combined_pk"))
        lib.secp256k1_nonce_function_frost(self.nonce,
                                           self.session_id,
                                           self.aggregated_share.data,
                                           MSG,
                                           serialized_group_key,
                                           b"FROST/non",
                                           9,
                                           ffi.NULL)

        s = ffi.new("secp256k1_scalar *")
        rj = ffi.new("secp256k1_gej *")
        rp = ffi.new("secp256k1_ge *")

        lib.secp256k1_scalar_set_b32(s, self.nonce.data, ffi.NULL)
        lib.secp256k1_ecmult_gen_with_ctx(self.context.ctx, rj, s)
        lib.secp256k1_ge_set_gej(rp, rj)
        lib.secp256k1_pubkey_save(ffi.addressof(self.frost_public_keys, self.session.my_index - 1), rp)  # Save the nonced pubkey in pubkeys

    def compute_partial_signature(self, active_participants):
        # Step 4+5 of the sign protocol
        scalar1 = ffi.new("secp256k1_scalar *")
        scalar2 = ffi.new("secp256k1_scalar *")
        c = ffi.new("secp256k1_scalar *")
        l = ffi.new("secp256k1_scalar *")

        # scalar2 is the group commitment
        lib.secp256k1_schnorrsig_challenge(c, self.group_commitment, MSG, ffi.addressof(self.aggregated_pk, 1))  # Challenge stored in c
        lib.secp256k1_scalar_set_b32(scalar1, self.aggregated_share.data, ffi.NULL)
        lib.secp256k1_frost_lagrange_coefficient(l, active_participants, len(active_participants), self.session.my_index)
        lib.secp256k1_scalar_mul(scalar1, scalar1, l)
        lib.secp256k1_scalar_mul(scalar2, c, scalar1)
        lib.secp256k1_nonce_function_frost(self.nonce, self.session_id, self.aggregated_share.data, MSG,
                                           ffi.addressof(self.aggregated_pk, 1), b"FROST/non", 9,
                                           ffi.NULL)
        lib.secp256k1_scalar_set_b32(scalar1, self.nonce.data, ffi.NULL)
        if self.aggregated_pk_parity:
            lib.secp256k1_scalar_negate(scalar1, scalar1)
        lib.secp256k1_scalar_add(scalar2, scalar2, scalar1)
        lib.secp256k1_scalar_get_b32(self.partial_signature, scalar2)

    def verify_signature(self, signature, msg):
        assert lib.secp256k1_schnorrsig_verify(self.context.ctx, signature, msg, ffi.addressof(self.session, "combined_pk"))
