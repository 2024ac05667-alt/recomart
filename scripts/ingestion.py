import pandas as pd
import os
import random
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

# ---------- CONFIG ----------
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
RAW_DATA_DIR = "data_lake"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# ---------- FUNCTIONS ----------
def ensure_schema(engine):
    # 'with' ensures the connection is closed even if an error occurs
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS recomart;"))
        conn.commit() # Explicitly commit schema creation

def generate_interactions(n=100):
    ts = datetime.now(timezone.utc)
    return pd.DataFrame({
        "user_id": [random.randint(1, 20) for _ in range(n)],
        "item_id": [random.randint(1, 20) for _ in range(n)],
        "rating": [random.randint(1, 5) for _ in range(n)],
        "ingested_at": [ts] * n
    })

def generate_products():
    return pd.DataFrame([
        {"item_id": 1, "category": "Electronics", "price": 199.99},
        {"item_id": 2, "category": "Books", "price": 15.50},
        {"item_id": 3, "category": "Clothing", "price": 29.99},
        {"item_id": 4, "category": "Sports", "price": 99.99},
        {"item_id": 5, "category": "Home", "price": 49.99}
    ])

def ingest_data():
    # 1. Initialize Engine
    engine = create_engine(DB_URL, pool_pre_ping=True) 
    
    try:
        ensure_schema(engine)

        # 2. Generate data
        interactions = generate_interactions()
        products = generate_products()

        # 3. Save to local Data Lake (CSV)
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        interactions.to_csv(os.path.join(RAW_DATA_DIR, f"user_interactions_{timestamp}.csv"), index=False)
        products.to_csv(os.path.join(RAW_DATA_DIR, f"products_{timestamp}.csv"), index=False)

        # 4. Insert into PostgreSQL
        # We use a context manager for the connection to ensure it's returned to the pool
        with engine.begin() as conn:
            interactions.to_sql("raw_interactions", conn, schema="recomart", if_exists="replace", index=False)
            products.to_sql("raw_products", conn, schema="recomart", if_exists="replace", index=False)

        print(f"INGEST SUCCESS | Interactions: {len(interactions)}, Products: {len(products)}")
        return interactions, products

    finally:
        # 5. CRITICAL: Shut down the engine and close all connections
        engine.dispose()
        print("Database engine disposed.")

if __name__ == "__main__":
    ingest_data()