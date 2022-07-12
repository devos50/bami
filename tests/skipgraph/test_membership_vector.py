import pytest

from bami.skipgraph.membership_vector import MembershipVector, MEMBERSHIP_VECTOR_DIGITS


@pytest.fixture
def membership_vector() -> MembershipVector:
    return MembershipVector()


def test_init(membership_vector):
    assert membership_vector.val
    assert len(membership_vector.val) == MEMBERSHIP_VECTOR_DIGITS

    # Custom initialization
    vector = MembershipVector(32)
    assert vector.val[5] == 1  # This bit should be set


def test_common_prefix_length():
    vector1 = MembershipVector(32)
    vector2 = MembershipVector(32)
    assert vector1.common_prefix_length(vector2) == MEMBERSHIP_VECTOR_DIGITS

    vector3 = MembershipVector(16)
    assert vector1.common_prefix_length(vector3) == 4

    vector4 = MembershipVector(11)
    vector5 = MembershipVector(1)
    assert vector4.common_prefix_length(vector5) == 1
