import logging

from binascii import hexlify
from coincurve._libsecp256k1 import ffi, lib


class Aggregator:
    def __init__(self, participant, n_signers):
        self.participant = participant
        self.context = self.participant.context
        self.participant.session.n_signers = n_signers
        self.signature = ffi.new("unsigned char[64]")
        self.aggregated_pk = ffi.new("unsigned char[33]")
        self.has_group_commitment = False
        self.group_commitment = ffi.new("unsigned char[33]")

        self.received_aggregated_shares = []
        self.received_partial_signatures = []

        self.logger = logging.getLogger(self.__class__.__name__)

    def received_aggregated_share(self, peer_ind, aggregate_share):
        self.received_aggregated_shares.append((peer_ind, aggregate_share))

        if len(self.received_aggregated_shares) == self.participant.num_participants:
            self.aggregate_shares([ind + 1 for ind in range(self.participant.num_participants)])

            self.participant.generate_nonced_pubkey()
            self.compute_group_commitment()

    def aggregate_shares(self, active_participants):
        self.logger.info("Aggregating aggregated shares")
        scalar1 = ffi.new("secp256k1_scalar *")
        scalar2 = ffi.new("secp256k1_scalar *")
        rj = ffi.new("secp256k1_gej *")
        rp = ffi.new("secp256k1_ge *")
        size = ffi.new("size_t *")
        size[0] = 33

        for participant_index, aggregated_share in self.received_aggregated_shares:
            participant_index += 1
            l = ffi.new("secp256k1_scalar *")
            lib.secp256k1_frost_lagrange_coefficient(l, active_participants, len(active_participants), participant_index)
            lib.secp256k1_scalar_set_b32(scalar1, aggregated_share.data, ffi.NULL)
            lib.secp256k1_scalar_mul(scalar1, scalar1, l)
            lib.secp256k1_scalar_add(scalar2, scalar2, scalar1)

        lib.secp256k1_ecmult_gen_with_ctx(self.context.ctx, rj, scalar2)
        lib.secp256k1_ge_set_gej(rp, rj)
        lib.secp256k1_pubkey_save(ffi.addressof(self.participant.frost_public_keys, self.participant.session.my_index - 1), rp)

        assert lib.secp256k1_ec_pubkey_serialize(self.context.ctx, self.aggregated_pk, size, ffi.addressof(self.participant.frost_public_keys, self.participant.session.my_index - 1), lib.SECP256K1_EC_COMPRESSED)

        aggregated_pk_hex = hexlify(bytes(self.aggregated_pk)).decode()
        print("Aggregated pk: %s" % aggregated_pk_hex)

    def compute_signature(self):
        z = ffi.new("secp256k1_scalar *")
        zi = ffi.new("secp256k1_scalar *")
        for partial_signature in self.received_partial_signatures:
            lib.secp256k1_scalar_set_b32(zi, partial_signature, ffi.NULL)
            lib.secp256k1_scalar_add(z, z, zi)

        ffi.memmove(ffi.addressof(self.signature, 0), self.group_commitment, 32)
        lib.secp256k1_scalar_get_b32(ffi.addressof(self.signature, 32), z)

    def compute_group_commitment(self):
        assert lib.secp256k1_frost_pubkey_combine(self.context.ctx, ffi.NULL, self.participant.session,
                                                  self.participant.frost_public_keys)  # Override the combined_key in the session of the aggregator!
        print("Combined key: %s" % hexlify(bytes(self.participant.session.combined_pk.data)).decode())
        assert lib.secp256k1_xonly_pubkey_serialize(self.context.ctx, self.group_commitment,
                                                    ffi.addressof(self.participant.session,
                                                                  "combined_pk"))  # group_commitment <- serialized(combined_pk)
        self.participant.group_commitment = self.group_commitment
        self.participant.aggregated_pk = self.aggregated_pk
        self.participant.aggregated_pk_parity = self.participant.session.pk_parity
        self.has_group_commitment = True
