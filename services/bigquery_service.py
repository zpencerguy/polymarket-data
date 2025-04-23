from google.cloud import bigquery
from settings import GCP_PROJECT_ID, BQ_DATASET


def get_bigquery_client():
    return bigquery.Client(project=GCP_PROJECT_ID)
