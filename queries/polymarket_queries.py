merge_events = """MERGE polymarket.events AS target
USING (
  SELECT 
    event_id, ticker, slug, title, description, start_date, creation_date, end_date, 
    image_url, icon_url, active, closed, archived, featured, liquidity, volume, volume24hr, 
    open_interest, commentCount AS comment_count, tags,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY api_fetch_timestamp DESC) AS rn
  FROM polymarket.events_raw
) AS source
ON target.event_id = source.event_id AND target.is_current = TRUE
AND source.rn = 1

WHEN MATCHED AND (
  target.ticker != source.ticker OR
  target.slug != source.slug OR
  target.title != source.title OR
  target.description != source.description OR
  target.start_date != source.start_date OR
  target.creation_date != source.creation_date OR
  target.end_date != source.end_date OR
  target.image_url != source.image_url OR
  target.icon_url != source.icon_url OR
  target.active != source.active OR
  target.closed != source.closed OR
  target.archived != source.archived OR
  target.featured != source.featured OR
  target.liquidity != source.liquidity OR
  target.volume != source.volume OR
  target.volume24hr != source.volume24hr OR
  target.open_interest != source.open_interest OR
  target.comment_count != source.comment_count
)
THEN
  -- Mark old record as expired
  UPDATE SET target.valid_to = source.valid_from, target.is_current = FALSE

WHEN NOT MATCHED THEN
  -- Insert new record
  INSERT (
    event_id, ticker, slug, title, description, start_date, creation_date, end_date,
    image_url, icon_url, active, closed, archived, featured, liquidity, volume, volume24hr, 
    open_interest, comment_count, valid_from, valid_to, is_current, tags
  )
  VALUES (
    source.event_id, source.ticker, source.slug, source.title, source.description, 
    source.start_date, source.creation_date, source.end_date, source.image_url, 
    source.icon_url, source.active, source.closed, source.archived, source.featured, 
    source.liquidity, source.volume, source.volume24hr, source.open_interest, 
    source.comment_count, source.valid_from, NULL, TRUE, source.tags
  );
"""

update_event_status = """UPDATE polymarket.events AS target
SET valid_to = CURRENT_TIMESTAMP(), is_current = FALSE
WHERE is_current = TRUE
  AND NOT EXISTS (
    SELECT 1
    FROM polymarket.events_raw AS source
    WHERE source.api_fetch_timestamp = (
        SELECT MAX(api_fetch_timestamp) FROM polymarket.events_raw
    )
    AND target.event_id = source.event_id
  );
"""

merge_markets = """MERGE polymarket.markets AS target
USING (
  SELECT 
    market_id, event_id, condition_id, question, description, slug, image_url, icon_url,
    start_date, end_date, active, closed, liquidity, volume, volume24hr, competitive,
    submitted_by, resolved_by, spread, last_trade_price, best_bid, best_ask, 
    order_book_enabled, clobTokenIds, outcome_1, outcome_2, outcome_1_price, outcome_2_price,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY api_fetch_timestamp DESC) AS rn
  FROM polymarket.markets_raw
) AS source
ON target.market_id = source.market_id AND target.is_current = TRUE
AND source.rn = 1

WHEN MATCHED AND (
  target.condition_id != source.condition_id OR
  target.question != source.question OR
  target.description != source.description OR
  target.slug != source.slug OR
  target.image_url != source.image_url OR
  target.icon_url != source.icon_url OR
  target.start_date != source.start_date OR
  target.end_date != source.end_date OR
  target.active != source.active OR
  target.closed != source.closed OR
  target.liquidity != source.liquidity OR
  target.volume != source.volume OR
  target.volume24hr != source.volume24hr OR
  target.competitive != source.competitive OR
  target.submitted_by != source.submitted_by OR
  target.resolved_by != source.resolved_by OR
  target.spread != source.spread OR
  target.last_trade_price != source.last_trade_price OR
  target.best_bid != source.best_bid OR
  target.best_ask != source.best_ask OR
  target.order_book_enabled != source.order_book_enabled OR
  target.outcome_1_price != source.outcome_1_price OR
  target.outcome_2_price != source.outcome_2_price
)
THEN
  -- Mark old record as expired
  UPDATE SET target.valid_to = source.valid_from, target.is_current = FALSE

WHEN NOT MATCHED THEN
  -- Insert new record
  INSERT (
    market_id, event_id, condition_id, question, description, slug, image_url, icon_url,
    start_date, end_date, active, closed, liquidity, volume, volume24hr, competitive,
    submitted_by, resolved_by, spread, last_trade_price, best_bid, best_ask, 
    order_book_enabled, clobTokenIds, outcome_1, outcome_2, outcome_1_price, outcome_2_price,
    valid_from, valid_to, is_current
  )
  VALUES (
    source.market_id, source.event_id, source.condition_id, source.question, source.description, 
    source.slug, source.image_url, source.icon_url, source.start_date, source.end_date, 
    source.active, source.closed, source.liquidity, source.volume, source.volume24hr, 
    source.competitive, source.submitted_by, source.resolved_by, source.spread, 
    source.last_trade_price, source.best_bid, source.best_ask, source.order_book_enabled, 
    source.clobTokenIds, source.outcome_1, source.outcome_2, source.outcome_1_price, 
    source.outcome_2_price, source.valid_from, NULL, TRUE
  );
"""

update_markets_status = """UPDATE polymarket.markets AS target
SET valid_to = CURRENT_TIMESTAMP(), is_current = FALSE
WHERE is_current = TRUE
  AND NOT EXISTS (
    SELECT 1
    FROM polymarket.markets_raw AS source
    WHERE source.api_fetch_timestamp = (
        SELECT MAX(api_fetch_timestamp) FROM polymarket.markets_raw
    )
    AND target.market_id = source.market_id
  );
"""