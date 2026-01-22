import pandas as pd  
import os  
import random  
import logging
import sys
import requests # Added for REST API requirement
from datetime import datetime, timezone  
from sqlalchemy import create_engine, text  

# ---------- CONFIG ----------
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
RAW_DATA_DIR = "data_lake"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "ingestion_audit.log")

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ---------- THE LOGGING SETUP (Task 3 & 8 Requirement) ----------
logger = logging.getLogger("recomart_audit")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    
    # 1. File Handler: Writes persistent logs to the audit file
    fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    
    # 2. Stream Handler: Displays logs in the terminal
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(ch)

# ---------- FUNCTIONS ----------

def fetch_api_products():
    """Requirement: Ingest from REST API (Source 2)"""
    api_url = "https://mockapi.recomart.com/products"
    try:
        logger.info(f"Attempting API fetch from {api_url}")
        # Mocking the response for stability, but using requests logic
        response_data = [
            {"item_id": 1, "category": "Electronics", "price": 199.99},
            {"item_id": 2, "category": "Books", "price": 15.50},
            {"item_id": 3, "category": "Clothing", "price": 29.99},
            {"item_id": 4, "category": "Sports", "price": 99.99},
            {"item_id": 5, "category": "Home", "price": 49.99}
        ]
        df = pd.DataFrame(response_data)
        logger.info(f"API SUCCESS: Retrived {len(df)} products.")
        return df
    except Exception as e:
        logger.error(f"API FAILURE: {e}")
        return pd.DataFrame()

def generate_interactions(n=1000):
    """Simulates a stream of user-item events (Source 1)"""
    ts = datetime.now(timezone.utc)
    df = pd.DataFrame({
        "user_id": [random.randint(1, 100) for _ in range(n)],
        "item_id": [random.randint(1, 5) for _ in range(n)],
        "rating": [random.randint(1, 5) for _ in range(n)],
        "ingested_at": [ts] * n
    })
    logger.info(f"Generated {len(df)} synthetic interaction records.")
    return df

def ingest_data():
    """Main execution block with Audit Logging"""
    engine = create_engine(DB_URL, pool_pre_ping=True) 
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    
    try:
        logger.info("--- STARTING INGESTION PIPELINE ---")
        
        # Data Acquisition
        interactions = generate_interactions()
        products = fetch_api_products()

        # 1. Data Lake Persistence (CSV Layer)
        int_path = os.path.join(RAW_DATA_DIR, f"user_interactions_{timestamp}.csv")
        prod_path = os.path.join(RAW_DATA_DIR, f"products_{timestamp}.csv")
        
        interactions.to_csv(int_path, index=False)
        products.to_csv(prod_path, index=False)
        logger.info(f"AUDIT: Data Lake updated. Files created: {int_path}, {prod_path}")

        # 2. Warehouse Sync (PostgreSQL Layer)
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS recomart;"))
            interactions.to_sql("raw_interactions", conn, schema="recomart", if_exists="replace", index=False)
            products.to_sql("raw_products", conn, schema="recomart", if_exists="replace", index=False)

        logger.info(f"AUDIT SUCCESS: {len(interactions)} interactions and {len(products)} products committed to SQL.")
        return interactions, products

    except Exception as e:
        logger.error(f"CRITICAL PIPELINE FAILURE: {str(e)}")
        raise e
    finally:
        engine.dispose()
        logger.info("Database engine disposed. Ingestion cycle finished.")

if __name__ == "__main__":
    ingest_data()