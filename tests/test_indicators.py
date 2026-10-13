"""Unit tests for spark/indicators/ta_indicators.py"""

import numpy as np
import pandas as pd
import pytest

from indicators.ta_indicators import bollinger_bands, indicator_frame, macd, rsi, sma


def random_walk(n=300, seed=7):
    rng = np.random.default_rng(seed)
    return pd.Series(100 + rng.normal(0, 1, n).cumsum())


# SMA test
def test_sma_hand_computed():
    out = sma(pd.Series([1.0, 2, 3, 4, 5]), 3)
    assert out.isna().tolist() == [True, True, False, False, False]
    assert out.iloc[2:].tolist() == [2.0, 3.0, 4.0]


# RSI tests

def test_rsi_all_gains_is_100():
    assert (rsi(pd.Series(np.arange(1.0, 40.0)), 14).dropna() == 100).all()

def test_rsi_all_losses_is_0():
    assert (rsi(pd.Series(np.arange(40.0, 1.0, -1)), 14).dropna() == 0).all()

def test_rsi_bounded_on_random_walk():
    out = rsi(random_walk(), 14).dropna()
    assert len(out) > 0
    assert out.between(0, 100).all()

def test_rsi_flat_series_is_nan():
    # No gains and no losses is 0/0 stored as NaN 
    # dbt turns it to null
    assert rsi(pd.Series([50.0] * 30), 14).isna().all()


# Bollinger Bands tests
def test_bollinger_ordering():
    upper, middle, lower = bollinger_bands(random_walk(), 20, 2)
    valid = middle.notna()
    assert valid.sum() > 0
    assert (lower[valid] <= middle[valid]).all()
    assert (middle[valid] <= upper[valid]).all()


def test_bollinger_middle_is_sma():
    s = random_walk()
    _, middle, _ = bollinger_bands(s, 20, 2)
    pd.testing.assert_series_equal(middle, sma(s, 20), check_names=False)


def test_bollinger_uses_population_std():
    # [1, 2, 3, 4]: mean 2.5, population variance 5/4
    upper, middle, lower = bollinger_bands(pd.Series([1.0, 2, 3, 4]), 4, 2)
    assert middle.iloc[-1] == pytest.approx(2.5)
    assert upper.iloc[-1] == pytest.approx(2.5 + 2 * 1.25 ** 0.5)
    assert lower.iloc[-1] == pytest.approx(2.5 - 2 * 1.25 ** 0.5)


# MACD tests

def test_macd_hand_computed():
    line, signal, hist = macd(pd.Series([1.0, 2, 3, 4]), fast=2, slow=3, signal=2)
    assert line.isna().tolist() == [True, True, False, False]
    assert line.iloc[2] == pytest.approx(11 / 36)
    assert line.iloc[3] == pytest.approx(85 / 216)
    assert signal.isna().tolist() == [True, True, True, False]
    assert signal.iloc[3] == pytest.approx(59 / 162)
    assert hist.iloc[3] == pytest.approx(19 / 648)

@pytest.mark.parametrize("name, fn", [
    ("sma_20", lambda s: sma(s, 20)),
    ("rsi_14", lambda s: rsi(s, 14)),
    ("bb_upper", lambda s: bollinger_bands(s, 20, 2)[0]),
    ("macd_line", lambda s: macd(s)[0]),
    ("macd_signal", lambda s: macd(s)[1]),
])
def test_short_input_is_all_nan(name, fn):
    s = pd.Series(np.linspace(100, 110, 10))
    out = fn(s)
    assert len(out) == len(s)
    assert out.isna().all(), f"{name} produced values before warm-up"


# indicator_frame tests

def test_indicator_frame_columns_and_index():
    s = random_walk(80)
    s.index = range(100, 180)  # non-default index must be preserved
    out = indicator_frame(s)
    assert list(out.columns) == [
        "sma_20", "sma_50", "rsi_14", "bb_upper", "bb_middle", "bb_lower", "macd", "macd_signal", "macd_hist",
    ]
    assert out.index.equals(s.index)