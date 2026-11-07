import random

import pytest

from olist.simulator import STATUS_FLOW, TERMINAL, next_status, pick_event


@pytest.mark.parametrize("status", sorted(TERMINAL))
def test_terminal_status_never_moves(status):
    assert next_status(status, random.Random(0)) is None


def test_unknown_status_never_moves():
    assert next_status("unknown_status", random.Random(0)) is None


def test_status_only_moves_forward_or_cancels():
    rng = random.Random(42)
    for current in STATUS_FLOW[:-1]:
        for _ in range(200):
            target = next_status(current, rng)
            assert target == "canceled" or STATUS_FLOW.index(target) == STATUS_FLOW.index(current) + 1


def test_shipped_order_is_never_canceled():
    rng = random.Random(1)
    assert {next_status("shipped", rng, cancel_rate=1.0) for _ in range(50)} == {"delivered"}


def test_pick_event_returns_known_events():
    rng = random.Random(7)
    events = {pick_event(rng) for _ in range(500)}
    assert events == {"new_order", "advance_order", "add_review", "delete_review"}
