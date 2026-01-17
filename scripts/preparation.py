import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from datetime import datetime

# --- CONFIGURATION ---
EDA_DIR = "eda"
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"

def prepare_and_save_eda(interactions, products):
    """Cleans data and forces EDA export with debug prints."""
    # 1. Ensure directory exists
    if not os.path.exists(EDA_DIR):
        os.makedirs(EDA_DIR)
        print(f"Created directory: {EDA_DIR}")

    # 2. Check if data exists
    print(f"Input rows - Interactions: {len(interactions)}, Products: {len(products)}")

    # 3. Clean
    interactions = interactions.dropna(subset=["user_id", "item_id", "rating"]).copy()
    products = products.dropna(subset=["item_id", "category", "price"]).copy()
    
    if interactions.empty:
        print("!! STOPPING: Interactions dataframe is empty after dropna !!")
        return interactions, products

    # 4. Force Plotting
    try:
        # Plot 1: Ratings
        plt.figure(figsize=(8, 5))
        sns.countplot(x="rating", data=interactions)
        plt.title("Rating Distribution")
        path1 = os.path.join(EDA_DIR, "rating_distribution.png")
        plt.savefig(path1)
        plt.close()
        print(f"Successfully saved: {path1}")

        # Plot 2: Top Items
        plt.figure(figsize=(10, 6))
        item_counts = interactions["item_id"].value_counts().head(10)
        item_counts.plot(kind='bar')
        plt.title("Top 10 Popular Items")
        path2 = os.path.join(EDA_DIR, "item_popularity.png")
        plt.savefig(path2)
        plt.close()
        print(f"Successfully saved: {path2}")

    except Exception as e:
        print(f"Error during plotting: {e}")

    return interactions, products

def transform_and_upload(interactions):
    """Calculates features and pushes to PostgreSQL."""
    if interactions.empty:
        print("Skipping SQL upload: No data.")
        return
    
    engine = create_engine(DB_URL)
    
    # Aggregations
    user_feats = interactions.groupby("user_id").agg(
        user_activity_count=("item_id", "count"),
        avg_user_rating=("rating", "mean")
    ).reset_index()

    item_feats = interactions.groupby("item_id").agg(
        avg_item_rating=("rating", "mean"),
        item_popularity=("user_id", "nunique")
    ).reset_index()

    # Merge
    features = interactions[["user_id", "item_id"]].drop_duplicates()
    features = features.merge(user_feats, on="user_id").merge(item_feats, on="item_id")
    features["last_updated"] = datetime.now()

    print(f"Ready to upload {len(features)} rows to SQL.")

    # Transactional Upload
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS recomart;"))
            features.to_sql(
                "feature_store", 
                conn, 
                schema="recomart", 
                if_exists="replace", 
                index=False
            )
        print("Database upload successful.")
    except Exception as e:
        print(f"Database error: {e}")

# --- EXECUTION FLOW ---
# Example of how to call them:
# df_interactions = pd.read_csv("your_data.csv")
# df_products = pd.read_csv("your_products.csv")
# clean_int, clean_prod = prepare_and_save_eda(df_interactions, df_products)
# transform_and_upload(clean_int)