import pandas as pd  # Import pandas for data frame manipulation and CSV exporting
import os  # Import os for directory management and file path joins
import random  # Import random to generate synthetic user/item IDs and ratings
import logging # Import logging for the Task 8 audit trail requirement
import sys # Import sys to direct logs to the standard output (terminal)
import requests # Import requests to handle REST API data fetching (Source 2)
from datetime import datetime, timezone  # Import datetime for precise, time-zoned timestamps
from sqlalchemy import create_engine, text  # Import SQLAlchemy for PostgreSQL connectivity

# ---------- CONFIG ----------
# Database connection string for the PostgreSQL warehouse instance
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
# Local directory serving as the landing zone for the raw Data Lake
RAW_DATA_DIR = "data_lake"
# Directory for storing persistent audit logs
LOG_DIR = "logs"
# Full path to the specific audit log file for tracking lineage
LOG_FILE = os.path.join(LOG_DIR, "ingestion_audit.log")

# Ensure the raw data landing zone directory exists
os.makedirs(RAW_DATA_DIR, exist_ok=True)
# Ensure the logging directory exists to prevent I/O errors
os.makedirs(LOG_DIR, exist_ok=True)

# ---------- THE LOGGING SETUP (Task 3 & 8 Requirement) ----------
# Initialize a named logger for the RecoMart audit trail
logger = logging.getLogger("recomart_audit")
# Prevent adding duplicate handlers if the script is re-run in the same process
if not logger.handlers:
    # Set the global logging level to INFO to capture all significant events
    logger.setLevel(logging.INFO)
    
    # 1. File Handler: Configures persistent logging to the audit file (Append mode)
    fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    # Define a clear format including timestamp, log level, and the message
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    # Attach the file handler to the logger instance
    logger.addHandler(fh)
    
    # 2. Stream Handler: Configures real-time log display in the terminal/console
    ch = logging.StreamHandler(sys.stdout)
    # Use a simpler format for console output to improve readability
    ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    # Attach the console handler to the logger instance
    logger.addHandler(ch)

# ---------- FUNCTIONS ----------

def fetch_api_products():
    """Requirement Task 3: Ingest from REST API (Source 2)"""
    # Endpoint URL for the product metadata API
    api_url = "https://mockapi.recomart.com/products"
    try:
        # Log the start of the API communication attempt
        logger.info(f"Attempting API fetch from {api_url}")
        # Simulation of API response data (representing a JSON REST response)
        response_data = [
            {"item_id": 1, "category": "Electronics", "price": 199.99},
            {"item_id": 2, "category": "Books", "price": 15.50},
            {"item_id": 3, "category": "Clothing", "price": 29.99},
            {"item_id": 4, "category": "Sports", "price": 99.99},
            {"item_id": 5, "category": "Home", "price": 49.99}
        ]
        # Convert the received JSON-like list into a Pandas DataFrame
        df = pd.DataFrame(response_data)
        # Log the successful acquisition of product data
        logger.info(f"API SUCCESS: Retrived {len(df)} products.")
        # Return the populated DataFrame
        return df
    except Exception as e:
        # Log any network or parsing errors to the audit trail
        logger.error(f"API FAILURE: {e}")
        # Return an empty DataFrame to allow the pipeline to fail gracefully
        return pd.DataFrame()

def generate_interactions(n=1000):
    """Requirement Task 3: Simulates a stream of user-item events (Source 1)"""
    # Capture the current UTC time for data synchronization
    ts = datetime.now(timezone.utc)
    # Construct a DataFrame with synthetic interaction features
    df = pd.DataFrame({
        # Generate random user IDs between 1 and 100
        "user_id": [random.randint(1, 100) for _ in range(n)],
        # Generate random item IDs matching the product catalog range
        "item_id": [random.randint(1, 5) for _ in range(n)],
        # Generate random ratings from 1 to 5
        "rating": [random.randint(1, 5) for _ in range(n)],
        # Apply the same ingestion timestamp to all records in this batch
        "ingested_at": [ts] * n
    })
    # Log the successful generation of synthetic data
    logger.info(f"Generated {len(df)} synthetic interaction records.")
    # Return the interaction DataFrame
    return df

def ingest_data():
    """Requirement Task 10: Main execution block with Audit Logging and Error Handling"""
    # Initialize the database engine with pre-ping to ensure connection health
    engine = create_engine(DB_URL, pool_pre_ping=True) 
    # Create a unique timestamp string for naming files in the Data Lake
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    
    try:
        # Log the initiation of the full ingestion cycle
        logger.info("--- STARTING INGESTION PIPELINE ---")
        
        # Acquisition: Trigger both data source functions
        interactions = generate_interactions()
        products = fetch_api_products()

        # 1. Data Lake Persistence: Save DataFrames as CSVs (Task 3 Requirement)
        int_path = os.path.join(RAW_DATA_DIR, f"user_interactions_{timestamp}.csv")
        prod_path = os.path.join(RAW_DATA_DIR, f"products_{timestamp}.csv")
        
        # Write interactions to CSV in the data_lake folder
        interactions.to_csv(int_path, index=False)
        # Write products to CSV in the data_lake folder
        products.to_csv(prod_path, index=False)
        # Audit the successful creation of raw files for lineage tracking
        logger.info(f"AUDIT: Data Lake updated. Files created: {int_path}, {prod_path}")

        # 2. Warehouse Sync: Load data into PostgreSQL (Task 3 Requirement)
        # Open a transaction block to ensure atomic database writes
        with engine.begin() as conn:
            # Idempotency: Create the schema if it doesn't exist
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS recomart;"))
            # Sync interactions to the 'raw_interactions' table (Replace mode for sync)
            interactions.to_sql("raw_interactions", conn, schema="recomart", if_exists="replace", index=False)
            # Sync products to the 'raw_products' table (Replace mode for sync)
            products.to_sql("raw_products", conn, schema="recomart", if_exists="replace", index=False)

        # Log final success including the row counts for the audit trail
        logger.info(f"AUDIT SUCCESS: {len(interactions)} interactions and {len(products)} products committed to SQL.")
        # Return both DataFrames for potential downstream use in Prefect
        return interactions, products

    except Exception as e:
        # Log critical errors that stop the pipeline for monitoring and alerts
        logger.error(f"CRITICAL PIPELINE FAILURE: {str(e)}")
        # Re-raise the exception to inform the orchestrator (Prefect) of the failure
        raise e
    finally:
        # Clean up database resources by disposing of the connection pool
        engine.dispose()
        # Log the final cleanup step to the audit trail
        logger.info("Database engine disposed. Ingestion cycle finished.")

# Entry point protection: ensures script only runs when executed directly
if __name__ == "__main__":
    # Execute the ingestion pipeline
    ingest_data()