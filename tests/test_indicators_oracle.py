"""
pandas-ta as a reference oracle
Compared on the tail of a long series with different initial conditions
After the indicators warm-up the values must agree
"""

import numpy as np
import pandas as pd
import pandas_ta as ta
import pytest

from indicators.ta_indicators import bollinger_bands, macd, rsi, sma

N, TAIL = 500, 200


@pytest.fixture
def close():
    rng = np.random.default_rng(42)
    return pd.Series(100 + rng.normal(0, 1, N).cumsum())


def tail(s):
    return s.iloc[-TAIL:].to_numpy()


def test_sma_matches_pandas_ta(close):
    np.testing.assert_allclose(tail(sma(close, 20)), tail(ta.sma(close, length=20)), rtol=1e-9)


def test_rsi_matches_pandas_ta(close):
    np.testing.assert_allclose(tail(rsi(close, 14)), tail(ta.rsi(close, length=14)), atol=1e-6)


def test_bollinger_matches_pandas_ta(close):
    upper, middle, lower = bollinger_bands(close, 20, 2)
    ref = ta.bbands(close, length=20, std=2, ddof=0)  # columns: lower, mid, upper, bandwidth, percent
    np.testing.assert_allclose(tail(lower), tail(ref.iloc[:, 0]), rtol=1e-9)
    np.testing.assert_allclose(tail(middle), tail(ref.iloc[:, 1]), rtol=1e-9)
    np.testing.assert_allclose(tail(upper), tail(ref.iloc[:, 2]), rtol=1e-9)


def test_macd_matches_pandas_ta(close):
    line, signal, hist = macd(close, 12, 26, 9)
    ref = ta.macd(close, fast=12, slow=26, signal=9)  # columns: macd, histogram, signal
    np.testing.assert_allclose(tail(line), tail(ref.iloc[:, 0]), atol=1e-6)
    np.testing.assert_allclose(tail(hist), tail(ref.iloc[:, 1]), atol=1e-6)
    np.testing.assert_allclose(tail(signal), tail(ref.iloc[:, 2]), atol=1e-6)