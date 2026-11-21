"""Unit tests for ml/promotion.py"""

from ml.promotion import decide, pick_candidate

ORDER = ["price", "price_structure", "price_structure_sentiment"]


def test_a_group_enters_only_by_lowering_log_loss():
    assert pick_candidate({"price": 0.690, "price_structure": 0.689, "price_structure_sentiment": 0.688}, ORDER) \
        == "price_structure_sentiment"
    # A tie is not a win
    assert pick_candidate({"price": 0.690, "price_structure": 0.689, "price_structure_sentiment": 0.689}, ORDER) \
        == "price_structure"


def test_search_stops_at_the_first_group_that_does_not_help():
    # Sentiment looks better but its variant carries the structure group that just failed
    assert pick_candidate({"price": 0.690, "price_structure": 0.691, "price_structure_sentiment": 0.680}, ORDER) \
        == "price"


def test_promotes_only_when_it_beats_both():
    assert decide(0.680, always_up=0.690, champion=0.685).promote
    assert decide(0.680, always_up=0.690, champion=None).promote


def test_never_promotes_a_model_worse_than_or_equal_to_always_up():
    assert not decide(0.6925, always_up=0.6920, champion=None).promote
    assert not decide(0.6920, always_up=0.6920, champion=None).promote


def test_never_replaces_a_better_champion():
    d = decide(0.686, always_up=0.690, champion=0.684)
    assert not d.promote and "champion" in d.reason