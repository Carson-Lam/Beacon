"""
Ad-hoc verification: compares spark/indicators/ta_indicators.py against
pandas-ta on real AAPL data. Not part of the pipeline — run manually:

  pip install yfinance pandas-ta pandas
  python scripts/verify_indicators.py
"""

import sys
sys.path.append("spark")

import yfinance as yf
import pandas_ta as pta
import pandas as pd
from indicators.ta_indicators import sma, rsi, bollinger_bands, macd

df = yf.download("AAPL", period="7d", interval="1m", progress=False)
df = df.dropna()

if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)

close = df["Close"]
if isinstance(close, pd.DataFrame):
    close = close.iloc[:, 0]
close = close.astype(float)

# implementations 
our_sma20 = sma(close, 20)
our_rsi14 = rsi(close, 14)
our_bb_upper, our_bb_mid, our_bb_lower = bollinger_bands(close, 20, 2)
our_macd, our_signal, our_hist = macd(close, 12, 26, 9)

# pandas-ta reference 
ref_sma20 = pta.sma(close, length=20)
ref_rsi14 = pta.rsi(close, length=14)
ref_bb = pta.bbands(close, length=20, std=2)
ref_macd = pta.macd(close, fast=12, slow=26, signal=9)

def compare(name, ours, ref):
    diff = (ours - ref).abs()
    print(f"{name:12s}  max_abs_diff={diff.max():.6f}  mean_abs_diff={diff.mean():.6f}")

print(f"AAPL, {len(df)} 1-min bars over the last 7 days\n")
compare("SMA-20", our_sma20, ref_sma20)
compare("RSI-14", our_rsi14, ref_rsi14)
compare("BB upper", our_bb_upper, ref_bb["BBU_20_2.0"])
compare("BB lower", our_bb_lower, ref_bb["BBL_20_2.0"])
compare("MACD", our_macd, ref_macd["MACD_12_26_9"])
compare("MACD signal", our_signal, ref_macd["MACDs_12_26_9"])