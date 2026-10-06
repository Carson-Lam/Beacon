select
    ticker,
    cast(post_id as varchar)            as post_id,
    source,
    cast("timestamp" as timestamptz)    as posted_at,
    text                                as post_text,
    cast(finbert_positive as double)    as finbert_positive,
    cast(finbert_negative as double)    as finbert_negative,
    cast(finbert_neutral as double)     as finbert_neutral,
    finbert_label,
    lower(user_sentiment_label)         as user_sentiment_label  
from {{ source('silver', 'sentiment_scored') }}