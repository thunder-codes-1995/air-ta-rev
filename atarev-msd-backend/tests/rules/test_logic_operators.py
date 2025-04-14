import pytest

from rules.core.operators import And, Or


@pytest.fixture
def block1():
    return [lambda: 5 > 7, lambda: "h" == "h"]


@pytest.fixture
def block2():
    return [lambda: 5 > 4, lambda: "h" == "h"]


@pytest.fixture
def block3():
    return [lambda: 5 > 7, lambda: "h" == "a"]


def test_and_operator_is_false(block1):
    assert And(block1)() is False


def test_and_operator_is_true(block2):
    assert And(block2)() is True


def test_or_operator_is_true(block2):
    assert Or(block2)() is True


def test_or_operator_is_false(block3):
    assert Or(block3)() is False


def test_nested_is_true(block1, block2):
    assert And([Or(block1), And(block2)])() is True


def test_nested_is_false(block1, block2):
    assert And([And(block1), And(block2)])() is False
