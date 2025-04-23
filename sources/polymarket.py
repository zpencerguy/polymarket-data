import ast
import json
import pandas as pd
from datetime import datetime
from requests_helper import get_paginated_data, make_get_request

from services.bigquery_service import get_bigquery_client
from queries.polymarket_queries import merge_markets, merge_events, update_event_status, update_markets_status
import settings

BASE_URL = 'https://gamma-api.polymarket.com'

ACTIVE_EVENT_PARAMS = {
    'active': True,
    'archived': False,
    'closed': False,
    'order': 'volume24hr',
    'ascending': False
}


def get_events_url():
    return f'{BASE_URL}/events/pagination'


def run_etl():
    data = get_active_events_data()
    print(f"Fetched {len(data)} from API")
    result = load_polymarket_data(data)
    print(f"Raw data load status: {result}")

    if result == "Success":
        client = get_bigquery_client()
        etl_queries = {
            'events': merge_events,
            'markets': merge_markets,
            'events_status': update_event_status,
            'markets_status': update_markets_status,
        }
        for name, query in etl_queries.items():
            print(f'Updating {name} table')
            client.query(query).result()
            print("ETL step completed.")


def load_polymarket_data(data) -> str:
    """Fetch API data and load into BigQuery staging tables for events and markets."""
    fetch_timestamp = datetime.utcnow().isoformat()

    events_rows = []
    markets_rows = []

    for event in data:
        # Extract event details
        event_id = event["id"]
        event_ticker = event["ticker"]
        event_slug = event["slug"]
        event_title = event["title"]
        event_desc = event.get("description")
        event_start = event.get("startDate")
        event_creation = event.get("creationDate")
        event_end = event.get("endDate")
        event_image = event.get("image")
        event_icon = event.get("icon")
        event_active = event["active"]
        event_closed = event["closed"]
        event_archived = event["archived"]
        event_featured = event["featured"]
        event_liquidity = event.get("liquidity")
        event_volume = event.get("volume")
        event_volume_24hr = event.get("volume24hr")
        event_comments = event.get("commentCount")
        event_open_interest = event.get("openInterest", 0)

        # Extract tags as nested array
        event_tags = [
            {"tag_id": tag["id"], "label": tag["label"], "slug": tag["slug"]}
            for tag in event.get("tags", [])
        ]

        # Add event row to list
        events_rows.append({
            "event_id": event_id,
            "ticker": event_ticker,
            "slug": event_slug,
            "title": event_title,
            "description": event_desc,
            "start_date": event_start,
            "creation_date": event_creation,
            "end_date": event_end,
            "image_url": event_image,
            "icon_url": event_icon,
            "active": event_active,
            "closed": event_closed,
            "archived": event_archived,
            "featured": event_featured,
            "volume": event_volume,
            "liquidity": event_liquidity,
            "open_interest": event_open_interest,
            "volume24hr": event_volume_24hr,
            "commentCount": event_comments,
            "tags": event_tags,
            "api_fetch_timestamp": fetch_timestamp,
        })

        # Process markets linked to this event
        for market in event.get("markets", []):
            # Convert clobTokenIds from string to list
            clob_token_ids = market.get("clobTokenIds", "[]")
            try:
                clob_token_ids = json.loads(clob_token_ids)
            except json.JSONDecodeError:
                clob_token_ids = []

            # Convert outcomes & prices from stringified lists
            outcomes = market.get("outcomes", "[]")
            outcome_prices = market.get("outcomePrices", "[]")

            try:
                outcomes = json.loads(outcomes)
                outcome_prices = [float(price) for price in json.loads(outcome_prices)]
            except json.JSONDecodeError:
                outcomes = ["Unknown", "Unknown"]
                outcome_prices = [None, None]

            # Ensure two outcomes are always present
            if len(outcomes) < 2:
                outcomes += ["Unknown"] * (2 - len(outcomes))
            if len(outcome_prices) < 2:
                outcome_prices += [None] * (2 - len(outcome_prices))

            # Add market row to list
            markets_rows.append({
                "market_id": market["id"],
                "event_id": event_id,  # Link to event
                "condition_id": market["conditionId"],
                "question": market["question"],
                "description": market["description"],
                "slug": market["slug"],
                "image_url": market.get("image"),
                "icon_url": market.get("icon"),
                "start_date": market.get("startDate"),
                "end_date": market.get("endDate"),
                "active": market["active"],
                "closed": market["closed"],
                "liquidity": market.get("liquidityNum"),
                "volume": market.get("volumeNum"),
                "volume24hr": market.get("volume24hr"),
                "competitive": market.get("competitive"),
                "submitted_by": market.get("submitted_by"),
                "resolved_by": market.get("resolvedBy"),
                "spread": market.get("spread"),
                "last_trade_price": market.get("lastTradePrice"),
                "best_bid": market.get("bestBid"),
                "best_ask": market.get("bestAsk"),
                "order_book_enabled": market.get("enableOrderBook"),
                "clobTokenIds": clob_token_ids,
                "outcome_1": outcomes[0],  # Store full outcome as string
                "outcome_2": outcomes[1],  # Store full outcome as string
                "outcome_1_price": outcome_prices[0],
                "outcome_2_price": outcome_prices[1],
                "api_fetch_timestamp": fetch_timestamp
            })

    print("Inserting event and market data")
    # Insert data into BigQuery
    client = get_bigquery_client()
    event_errors = client.insert_rows_json(f"{settings.GCP_PROJECT_ID}.polymarket.events_raw", events_rows)
    market_errors = client.insert_rows_json(f"{settings.GCP_PROJECT_ID}.polymarket.markets_raw", markets_rows)

    if event_errors or market_errors:
        print(f"Errors inserting into BigQuery: Events: {event_errors}, Markets: {market_errors}")
        return "Failed"

    return "Success"

def get_active_events_data():
    return get_paginated_data(
        url=get_events_url(),
        params=ACTIVE_EVENT_PARAMS,
        max_offset=10000
    )

def extract_markets_data(events_df: pd.DataFrame) -> pd.DataFrame:
    # Convert 'markets' column from string to actual dictionaries
    df = events_df.copy()
    df['markets'] = df['markets'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

    # Explode the markets column into separate rows while keeping event_id
    markets_df = df[['id', 'markets']].explode('markets').reset_index(drop=True)

    # Convert the dictionaries in 'markets' column to separate columns
    markets_expanded_df = pd.json_normalize(markets_df['markets'])

    # Add event_id for reference
    markets_expanded_df.insert(0, 'event_id', markets_df['id'])
    return markets_expanded_df
