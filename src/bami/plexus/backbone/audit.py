from abc import abstractmethod, ABCMeta
from logging import Logger
from typing import Any, Dict

from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.block_sync import BlockSyncMixin
from bami.plexus.backbone.exceptions import InvalidTransactionFormatException
from bami.plexus.backbone.utils import decode_raw, shorten, AUDIT_TYPE


class AuditMixin(BlockSyncMixin, metaclass=ABCMeta):
    logger: Logger

    def audit_tx_well_formatted(self, audit_tx: Dict) -> bool:
        """
        Returns:
            False if bad format
        """
        return True

    @abstractmethod
    def build_audit_tx(self, chain_id: bytes, seq_num: int) -> Dict:
        """
        Args:
            chain_id: bytes identifier of the chain
            seq_num: of the chain to audit to
        Returns:
            audit blob (bytes) if possible, None otherwise
        """
        pass

    @abstractmethod
    def apply_audit_tx(self, block: PlexusBlock, audit_tx: Any) -> None:
        pass

    def verify_audit_transaction(self, chain_id: bytes, audit_tx: Any) -> None:
        """
        Verify the audit transaction
        Raises:
             InvalidFormatTransaction
        """
        # 1. Audit transaction ill-formatted
        if not self.audit_tx_well_formatted(audit_tx):
            raise InvalidTransactionFormatException(
                "Invalid audit transaction", chain_id, audit_tx
            )

    def audit(self, chain_id: bytes, seq_num: int) -> None:
        """
        Audit the chain up to a sequence number.
        If chain is known and data exists:
         - Will create a audit block, link to latest known blocks and share in the community.
        Otherwise:
         - Do nothing
        Args:
            chain_id: id of the chain
            seq_num: sequence number of the block:
        """
        chain = self.persistence.get_chain(chain_id)
        if chain:
            audit_tx = self.build_audit_tx(chain_id, seq_num)
            blk = self.create_signed_block(
                block_type=AUDIT_TYPE,
                transaction=audit_tx,
            )
            self.logger.debug(
                "Creating audit block on chain %s: %s, com_dot %s, pers_dot %s",
                shorten(blk.community_id),
                seq_num,
                blk.com_dot,
                blk.pers_dot,
            )
            self.share_in_community(blk, chain_id)

    def process_audit(self, block: PlexusBlock) -> None:
        """Process received audit transaction"""
        audit_tx = decode_raw(block.transaction)
        chain_id = block.community_id
        self.verify_audit_transaction(chain_id, audit_tx)
        # Apply to db
        self.apply_audit_tx(block, audit_tx)
