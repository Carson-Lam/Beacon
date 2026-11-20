"""
Walk-forward folds for beacon-direction with an expanding window and one calendar year per test fold.

- Fold for year k trains on every year before k and tests on year k. 
- Labels span open t+1 to open t+6
"""

from dataclasses import dataclass

import pandas as pd

PURGE_SESSIONS = 6
MIN_TRAIN_YEARS = 3  # the first year is partial for indicator warm-up


@dataclass(frozen=True)
class Fold:
    test_year: int
    train_end: pd.Timestamp   # last training session, inclusive
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def walk_forward_folds(trade_dates, min_train_years=MIN_TRAIN_YEARS, purge=PURGE_SESSIONS) -> list[Fold]:
    """Folds over the session calendar formed by every distinct date in trade_dates."""
    sessions = pd.DatetimeIndex(pd.unique(pd.to_datetime(trade_dates))).sort_values()
    years = sorted(sessions.year.unique())
    folds = []
    for test_year in years[min_train_years:]:
        in_year = sessions[sessions.year == test_year]
        first = sessions.get_loc(in_year[0])
        folds.append(Fold(int(test_year), sessions[first - purge - 1], in_year[0], in_year[-1]))
    return folds


def split(df: pd.DataFrame, fold: Fold, date_col="trade_date"):
    dates = pd.to_datetime(df[date_col])
    train = df[dates <= fold.train_end]
    test = df[(dates >= fold.test_start) & (dates <= fold.test_end)]
    return train, test