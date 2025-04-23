import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Google Cloud & BigQuery Settings
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")

# Google Application Credentials (for BigQuery)
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")