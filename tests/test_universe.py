import pytest

from universe import COLUMNS, load_universe


def write_csv(tmp_path, *rows):
    path = tmp_path / "tickers.csv"
    path.write_text("\n".join([",".join(COLUMNS), *rows]) + "\n")
    return path


ROWS = (
    "AAPL,equity,AAPL,AAPL,AAPL,true",
    "BRK.B,equity,BRK.B,BRK-B,BRK.B,false",
    "BTC,crypto,BTC/USD,BTC-USD,BTC.X,true",
)


def test_live_filter_excludes_batch_only_rows(tmp_path):
    path = write_csv(tmp_path, *ROWS)
    assert [r["ticker"] for r in load_universe(asset_class="equity", live=True, path=path)] == ["AAPL"]


def test_batch_includes_live_and_batch_only_equities(tmp_path):
    path = write_csv(tmp_path, *ROWS)
    assert [r["ticker"] for r in load_universe(asset_class="equity", path=path)] == ["AAPL", "BRK.B"]


@pytest.mark.parametrize("flag", ["yes", "1", ""])
def test_ambiguous_live_flag_raises(tmp_path, flag):
    # A typo must not silently become a websocket subscription or a dropped symbol
    with pytest.raises(ValueError):
        load_universe(path=write_csv(tmp_path, f"AAPL,equity,AAPL,AAPL,AAPL,{flag}"))