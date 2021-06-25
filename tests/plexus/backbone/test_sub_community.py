from ipv8.keyvault.crypto import default_eccrypto
import pytest
from bami.plexus.backbone.sub_community import IPv8SubCommunity
from ipv8.test.mocking.ipv8 import MockIPv8

from tests.plexus.mocking.community import (
    FakeRoutines,
    MockSubCommunityRoutines,
    MockSubCommunityDiscoveryStrategy,
)


class FakeSubCommunity(MockSubCommunityRoutines, FakeRoutines):
    pass


def test_is_sub(monkeypatch):
    monkeypatch.setattr(MockSubCommunityRoutines, "my_subcoms", [b"test1"])
    f = FakeSubCommunity()
    assert f.is_subscribed(b"test1")


# Test sub to multiple comms


class TestSub:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.f = FakeSubCommunity()

    def test_one_sub_when_subscribed(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_subcoms", [b"test1"])
        self.f.subscribe_to_subcom(b"test1")
        assert self.f.is_subscribed(b"test1")

    def test_no_ipv8(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_subcoms", [])
        monkeypatch.setattr(
            MockSubCommunityRoutines, "discovered_peers_by_subcom", lambda _, __: []
        )
        f = FakeSubCommunity()
        f.discovery_strategy = MockSubCommunityDiscoveryStrategy(None)
        f.subscribe_to_subcom(b"test1")

    def test_one_sub(self, monkeypatch):
        monkeypatch.setattr(MockSubCommunityRoutines, "my_subcoms", [])
        monkeypatch.setattr(
            MockSubCommunityRoutines, "discovered_peers_by_subcom", lambda _, __: []
        )
        key = default_eccrypto.generate_key(u"medium").pub()
        monkeypatch.setattr(
            FakeRoutines,
            "ipv8",
            MockIPv8(u"curve25519", IPv8SubCommunity, subcom_id=key.key_to_hash()),
        )
        f = FakeSubCommunity()
        f.discovery_strategy = MockSubCommunityDiscoveryStrategy(None)
        f.subscribe_to_subcom(b"test1")
