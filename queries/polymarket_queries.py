merge_events = """BEGIN TRANSACTION;

-- 1. EXPIRE outdated or missing events
UPDATE polymarket.events AS target
SET 
  is_current = FALSE,
  valid_to = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
AND (
    -- CASE 1: Event fields have changed
    EXISTS (
      SELECT 1
      FROM (
          SELECT 
            event_id, ticker, slug, title, description, start_date, creation_date, end_date,
            image_url, icon_url, active, closed, archived, featured, liquidity, volume, volume24hr,
            open_interest, commentCount,
            ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY api_fetch_timestamp DESC) AS rn
          FROM polymarket.events_raw
      ) AS source
      WHERE source.rn = 1
        AND source.event_id = target.event_id
        AND (
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
          target.comment_count != source.commentCount
        )
    )
    
    OR
    
    -- CASE 2: Event missing completely
    NOT EXISTS (
      SELECT 1
      FROM (
          SELECT event_id,
          ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY api_fetch_timestamp DESC) AS rn
          FROM polymarket.events_raw
      ) AS source
      WHERE source.event_id = target.event_id and rn = 1
    )
);

-- 2. INSERT new or changed events
INSERT INTO polymarket.events (
    event_id, ticker, slug, title, description, start_date, creation_date, end_date,
    image_url, icon_url, active, closed, archived, featured, liquidity, volume, volume24hr,
    open_interest, comment_count, valid_from, valid_to, is_current, tags
)
SELECT 
    source.event_id, source.ticker, source.slug, source.title, source.description, 
    source.start_date, source.creation_date, source.end_date, source.image_url, 
    source.icon_url, source.active, source.closed, source.archived, source.featured, 
    source.liquidity, source.volume, source.volume24hr, source.open_interest, 
    source.commentCount,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    source.tags
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY api_fetch_timestamp DESC) AS rn
    FROM polymarket.events_raw
) AS source
LEFT JOIN polymarket.events AS target
  ON source.event_id = target.event_id
  AND target.is_current = TRUE
WHERE source.rn = 1
AND (
    target.event_id IS NULL  -- New event
    OR
    (
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
      target.comment_count != source.commentCount
    )
);

COMMIT TRANSACTION;
"""

merge_markets = """BEGIN TRANSACTION;

-- 1. EXPIRE outdated or missing markets
UPDATE polymarket.markets AS target
SET 
  is_current = FALSE,
  valid_to = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
AND (
    -- CASE 1: Market is outdated (field changes)
    EXISTS (
      SELECT 1
      FROM (
          SELECT market_id, event_id, condition_id, question, description, slug, image_url, icon_url,
            start_date, end_date, active, closed, liquidity, volume, volume24hr, competitive,
            submitted_by, resolved_by, spread, last_trade_price, best_bid, best_ask, 
            order_book_enabled, clobTokenIds, outcome_1, outcome_2, outcome_1_price, outcome_2_price,
            valid_from, valid_to, is_current,
            ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY api_fetch_timestamp DESC) AS rn
          FROM polymarket.markets_raw
      ) AS source
      WHERE source.rn = 1
        AND source.market_id = target.market_id
        AND (
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
    )
    
    OR
    
    -- CASE 2: Market missing entirely from latest snapshot
    NOT EXISTS (
      SELECT 1
      FROM (
          SELECT market_id, 
          ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY api_fetch_timestamp DESC) AS rn
          FROM polymarket.markets_raw
      ) AS source
      WHERE source.market_id = target.market_id and rn = 1
    )
);

-- 2. INSERT new records if needed
INSERT INTO polymarket.markets (
    market_id, event_id, condition_id, question, description, slug, image_url, icon_url,
    start_date, end_date, active, closed, liquidity, volume, volume24hr, competitive,
    submitted_by, resolved_by, spread, last_trade_price, best_bid, best_ask, 
    order_book_enabled, clobTokenIds, outcome_1, outcome_2, outcome_1_price, outcome_2_price,
    valid_from, valid_to, is_current
)
SELECT 
    source.market_id, source.event_id, source.condition_id, source.question, source.description, 
    source.slug, source.image_url, source.icon_url, source.start_date, source.end_date, 
    source.active, source.closed, source.liquidity, source.volume, source.volume24hr, 
    source.competitive, source.submitted_by, source.resolved_by, source.spread, 
    source.last_trade_price, source.best_bid, source.best_ask, source.order_book_enabled, 
    source.clobTokenIds, source.outcome_1, source.outcome_2, source.outcome_1_price, 
    source.outcome_2_price,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY api_fetch_timestamp DESC) AS rn
    FROM polymarket.markets_raw
) AS source
LEFT JOIN polymarket.markets AS target
  ON source.market_id = target.market_id
  AND target.is_current = TRUE
WHERE source.rn = 1
AND (
    target.market_id IS NULL  -- Brand new market
    OR
    (
      -- Changed fields
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
);

COMMIT TRANSACTION;
"""
