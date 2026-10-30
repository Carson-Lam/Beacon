# Model Card: beacon-direction

## Known limitations

### Survivorship bias

The training and backtest universe (`config/tickers.csv`) is 86 US large caps chosen in
October 2026: companies that are large and listed **today** and were already listed on
October 2016. Point-in-time index constituent data, which is costly, is a possible avenue
for future development.

Effects:

- **Backtest returns are biased upward.** Total return, CAGR and max drawdown are
  measured on winners.
  
- **The comparison to buy-and-hold is the more reliable number.** Every strategy result
  is reported against buy-and-hold on the same symbols and dates, so the bias appears
  on both sides of the comparison.
  
- **The model learns from survivors.** Every deep drawdown in the training data was
  followed by a recovery, because the companies that did not recover are not in the
  data. This can bias the model toward reading sharp drops as bullish.

Other possible sources of bias:

- Names with corporate actions that leave unreliable adjusted history (GE and MMM
  spinoffs, 2023-24) were excluded.
- Marsh McLennan is held under its current ticker, MRSH. Mergers and renames over
  10 years mean "the same universe" is a modern reconstruction, not what an investor
  could have selected in 2016.
