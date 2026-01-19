import pandas as pd
import os
import random
import logging
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from prefect import task

# ---------- CONFIGURATION ----------
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
RAW_DATA_DIR = "data_lake"
LOG_FILE = "pipeline_audit.log"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# ---------- THE LOGGING FIX (Task 3 Requirement) ----------
# We use a custom formatter and ensure 'delay=False' to open the file immediately
logger = logging.getLogger("recomart_audit")
logger.setLevel(logging.INFO)

# Create file handler which logs even debug messages
fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8', delay=False)
fh.setLevel(logging.INFO)

# Create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# Add the handlers to the logger
if not logger.handlers:
    logger.addHandler(fh)
    logger.addHandler(ch)

def generate_interactions(n=1000):
    """Simulates high-frequency user interactions (Source 1)"""
    data = [{
        "user_id": random.randint(1, 100),
        "item_id": random.randint(1, 50),
        "rating": random.randint(1, 5),
        "timestamp": datetime.now()
    } for _ in range(n)]
    return pd.DataFrame(data)

@task(retries=3, retry_delay_seconds=10)
def ingest_data():
    engine = create_engine(DB_URL)
    try:
        # Start Ingestion
        logger.info("--- STARTING INGESTION BATCH ---")
        
        interactions = generate_interactions(1000)
        
        # 1. Data Lake Save
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(RAW_DATA_DIR, f"batch_{timestamp}.csv")
        interactions.to_csv(csv_path, index=False)
        logger.info(f"AUDIT: Saved {len(interactions)} rows to Data Lake: {csv_path}")

        # 2. Database Save
        with engine.begin() as conn:
            interactions.to_sql("raw_interactions", conn, schema="recomart", if_exists="append", index=False)
        
        logger.info("AUDIT SUCCESS: Records committed to PostgreSQL warehouse.")
        # Force flush the log file to disk
        fh.flush() 
        
        return interactions

    except Exception as e:
        logger.error(f"AUDIT FAILURE: {str(e)}")
        fh.flush()
        raise e
    finally:
        engine.dispose()

if __name__ == "__main__":
    # Manual Test Run
    ingest_data.fn()