{% docs ticker %}
Canonical ticker shared across market and sentiment data.
{% enddocs %}

{% docs asset_class %}
`equity` or `crypto`.
{% enddocs %}

{% docs sma_20 %}
20-period simple moving average of close. Null until 20 periods of history exist.
{% enddocs %}

{% docs sma_50 %}
50-period simple moving average of close. Null until 50 periods of history exist.
{% enddocs %}

{% docs rsi_14 %}
14-period Relative Strength Index w Wilder's smoothing, 0–100. Above 70 is conventionally overbought, below 30 oversold.
{% enddocs %}

{% docs bb_upper %}
Upper Bollinger Band: 20-period SMA + 2 population standard deviations.
{% enddocs %}

{% docs bb_lower %}
Lower Bollinger Band: 20-period SMA minus 2 population standard deviations.
{% enddocs %}

{% docs macd %}
MACD line: 12-period EMA minus 26-period EMA of close. Null until 26 periods of history exist.
{% enddocs %}

{% docs macd_signal %}
MACD signal line: 9-period EMA of the MACD line. Null until 34 periods of history exist.
{% enddocs %}

{% docs macd_hist %}
MACD histogram: MACD line minus signal lines
{% enddocs %}