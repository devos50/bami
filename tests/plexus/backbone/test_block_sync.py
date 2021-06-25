from unittest.mock import ANY

from bami.plexus.backbone.block_sync import BlockSyncMixin
from bami.plexus.backbone.payload import (
    BlockBroadcastPayload,
)
import pytest

from tests.plexus.conftest import FakeBlock
from tests.plexus.mocking.base import deliver_messages
from tests.plexus.mocking.community import MockedCommunity
from tests.plexus.mocking.mock_db import MockDBManager


class BlockSyncCommunity(MockedCommunity, BlockSyncMixin):
    pass


@pytest.fixture()
def overlay_class():
    return BlockSyncCommunity


@pytest.fixture()
def init_nodes():
    return False


@pytest.fixture()
def num_nodes():
    return 2


def test_init_setup(set_vals_by_key):
    assert set_vals_by_key.nodes[0].overlay.decode_map[BlockBroadcastPayload.msg_id]


@pytest.mark.asyncio
async def test_send_receive_block(monkeypatch, mocker, set_vals_by_key):
    blk = FakeBlock(transaction={})
    set_vals_by_key.nodes[0].overlay.send_block(
        blk, [set_vals_by_key.nodes[1].overlay.my_peer]
    )
    monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___: None)
    monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)

    spy = mocker.spy(MockDBManager, "has_block")

    await deliver_messages()
    spy.assert_called_with(ANY, blk.hash)


def test_create_block(monkeypatch, mocker, set_vals_by_key):
    monkeypatch.setattr(MockDBManager, "add_block", lambda _, __, ___: None)
    monkeypatch.setattr(MockDBManager, "has_block", lambda _, __: False)
    spy = mocker.spy(MockDBManager, "has_block")
    blk = set_vals_by_key.nodes[0].overlay.create_signed_block()
    spy.assert_called_with(ANY, blk.hash)
