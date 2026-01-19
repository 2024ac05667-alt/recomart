import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime

# --- CONFIGURATION ---
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
REGISTRY_PATH = "feature_store/feature_registry.json"

def transform_features(interactions):
    """
    Task 6: Feature Engineering
    Task 7: Feature Store Persistence & Metadata Update
    """
    engine = create_engine(DB_URL)
    
    # --- 1. FEATURE ENGINEERING (Task 6) ---
    # User Activity Frequency
    user_activity = interactions.groupby("user_id")["item_id"].count().reset_index().rename(
        columns={"item_id": "user_activity_count"}
    )
    
    # Average Ratings (User & Item Bias)
    avg_user = interactions.groupby("user_id")["rating"].mean().reset_index().rename(
        columns={"rating": "avg_user_rating"}
    )
    avg_item = interactions.groupby("item_id")["rating"].mean().reset_index().rename(
        columns={"rating": "avg_item_rating"}
    )
    
    # Co-occurrence (Item Popularity)
    co_occurrence = interactions.groupby("item_id")["user_id"].nunique().reset_index().rename(
        columns={"user_id": "co_occurrence_count"}
    )
    
    # --- 2. MERGING & CLEANING ---
    features = interactions.merge(user_activity, on="user_id") \
                           .merge(avg_user, on="user_id") \
                           .merge(avg_item, on="item_id") \
                           .merge(co_occurrence, on="item_id")
    
    features = features[[
        "user_id", "item_id", "rating", "avg_user_rating", 
        "avg_item_rating", "user_activity_count", "co_occurrence_count"
    ]].drop_duplicates()
    
    # Add Versioning Timestamp (Task 7 requirement)
    features["last_updated"] = datetime.now()
    
    # --- 3. PERSISTENCE (Task 7 Store) ---
    with engine.begin() as conn:
        features.to_sql("feature_transform", conn, schema="recomart", if_exists="replace", index=False)
        # Create Index for Versioned Retrieval
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ft_user_time ON recomart.feature_transform (user_id, last_updated);"))

    # --- 4. DYNAMIC METADATA REGISTRY (Task 7 Deliverable) ---
    update_json_registry(len(features))
    
    print(f"SUCCESS: Feature Store updated and Registry Synced at {datetime.now()}")
    return features

def update_json_registry(row_count):
    """
    Updates the JSON Metadata Registry to demonstrate a managed Feature Store.
    """
    registry_data = {
        "registry_name": "RecoMart_Feature_Store",
        "last_sync_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "active_version": "v1.0.0",
        "target_table": "recomart.feature_transform",
        "record_count": row_count,
        "features_documented": [
            {"name": "avg_user_rating", "type": "float", "description": "User bias baseline"},
            {"name": "avg_item_rating", "type": "float", "description": "Item quality baseline"},
            {"name": "user_activity_count", "type": "int", "description": "Engagement frequency"},
            {"name": "co_occurrence_count", "type": "int", "description": "Collaborative reach"}
        ]
    }
    
    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry_data, f, indent=4)

if __name__ == "__main__":
    # Test block
    engine = create_engine(DB_URL)
    raw_data = pd.read_sql("SELECT * FROM recomart.raw_interactions LIMIT 1000", engine)
    transform_features(raw_data)