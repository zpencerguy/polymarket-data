from sources.polymarket import run_etl


def run_etl_flow(event, context):
    """Run process to fetch latest Market & Event data from Polymarket API and update tables in BigQuery"""
    try:
        run_etl()
    except Exception as e:
        print(f"ETL failed: {e}")
        raise