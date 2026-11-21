"""Unit tests for ml/walk_forward.py"""

import pandas as pd

from ml.walk_forward import calibration_split, split, walk_forward_folds

SESSIONS = pd.bdate_range("2016-03-01", "2026-11-13")
LABEL_HORIZON = 6  # mart_model_labels t+1 to t+6

def test_one_fold_per_year_after_min_train_years():
    folds = walk_forward_folds(SESSIONS)
    assert [f.test_year for f in folds] == list(range(2019, 2027))
    for f in folds:
        assert f.test_start.year == f.test_end.year == f.test_year


def test_purge_leaves_label_windows_clear_of_the_test_fold():
    pos = {d: i for i, d in enumerate(SESSIONS)}
    for f in walk_forward_folds(SESSIONS):
        assert pos[f.train_end] + LABEL_HORIZON < pos[f.test_start]


def test_split_is_disjoint_and_expanding():
    df = pd.DataFrame({"trade_date": SESSIONS.repeat(2), "ticker": ["A", "B"] * len(SESSIONS)})
    previous_train = 0
    for f in walk_forward_folds(df.trade_date):
        train, test = split(df, f)
        assert train.trade_date.max() < test.trade_date.min()
        assert set(test.trade_date.dt.year) == {f.test_year}
        assert len(train) > previous_train
        previous_train = len(train)

def test_calibration_slice_is_the_last_year_with_a_purge_in_front():
    df = pd.DataFrame({"trade_date": SESSIONS.repeat(2), "ticker": ["A", "B"] * len(SESSIONS)})
    pos = {d: i for i, d in enumerate(SESSIONS)}
    for f in walk_forward_folds(df.trade_date):
        train, _ = split(df, f)
        classifier_rows, cal = calibration_split(train)
        assert cal.trade_date.max() == train.trade_date.max()
        assert cal.trade_date.nunique() == 252
        # No classifier label ends inside the calibration slice
        assert pos[classifier_rows.trade_date.max()] + LABEL_HORIZON < pos[cal.trade_date.min()]