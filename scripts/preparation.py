import pandas as pd  # Data manipulation library used for cleaning and preparation
import os  # Standard library for directory and file path management
import matplotlib.pyplot as plt  # Core plotting engine for generating visual insights
import seaborn as sns  # Advanced visualization library for professional statistical charts
from sqlalchemy import create_engine, text  # Database engine and SQL text utilities for warehouse connectivity
from datetime import datetime  # Used for timestamping data updates for lineage tracking

# --- CONFIGURATION ---
# Target directory for persisting static EDA chart images
EDA_DIR = "eda"
# Database connection string pointing to the project's PostgreSQL instance
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"

def prepare_and_save_eda(interactions, products):
    """
    Cleans incoming raw data and generates a suite of EDA visualizations 
    to validate data distribution before modeling.
    """
    # 1. DIRECTORY MANAGEMENT: Ensures the output folder exists to prevent I/O errors
    if not os.path.exists(EDA_DIR):
        os.makedirs(EDA_DIR)
        print(f"Created directory: {EDA_DIR}")

    # 2. DATA AUDIT: Initial diagnostic print to verify the volume of data received
    print(f"Input rows - Interactions: {len(interactions)}, Products: {len(products)}")

    # 3. DATA CLEANING: Removes rows with missing critical identifiers or target values
    # The .copy() prevents 'SettingWithCopy' warnings in pandas
    interactions = interactions.dropna(subset=["user_id", "item_id", "rating"]).copy()
    products = products.dropna(subset=["item_id", "category", "price"]).copy()
    
    # DEFENSIVE CHECK: Halts the pipeline if cleaning results in an empty dataset
    if interactions.empty:
        print("!! STOPPING: Interactions dataframe is empty after dropna !!")
        return interactions, products

    # 4. VISUALIZATION SUITE: Generates charts for the project report
    try:
        # Plot 1: RATING DISTRIBUTION - Analyzes for data imbalance or skewness
        plt.figure(figsize=(8, 5))
        sns.countplot(x="rating", data=interactions)
        plt.title("Rating Distribution")
        path1 = os.path.join(EDA_DIR, "rating_distribution.png")
        plt.savefig(path1) # Persists the chart as a static PNG for the documentation
        plt.close() # Frees memory by closing the plot object
        print(f"Successfully saved: {path1}")

        # Plot 2: ITEM POPULARITY - Identifies the 'Long Tail' effect in interactions
        plt.figure(figsize=(10, 6))
        # Aggregates interaction counts for the top 10 most frequent items
        item_counts = interactions["item_id"].value_counts().head(10)
        item_counts.plot(kind='bar')
        plt.title("Top 10 Popular Items")
        path2 = os.path.join(EDA_DIR, "item_popularity.png")
        plt.savefig(path2)
        plt.close()
        print(f"Successfully saved: {path2}")

    except Exception as e:
        # Error handling to ensure a visualization failure doesn't crash the whole pipeline
        print(f"Error during plotting: {e}")

    # Returns the cleaned datasets to the main Prefect flow
    return interactions, products

def transform_and_upload(interactions):
    """
    Feature Engineering & Data Loading: Computes user/item aggregates 
    and persists them to the SQL Feature Store.
    """
    # Validation to ensure we don't attempt to upload an empty frame
    if interactions.empty:
        print("Skipping SQL upload: No data.")
        return
    
    # Initialize the database engine
    engine = create_engine(DB_URL)
    
    # 5. USER AGGREGATIONS: Captures historical activity and rating bias per user
    user_feats = interactions.groupby("user_id").agg(
        user_activity_count=("item_id", "count"),
        avg_user_rating=("rating", "mean")
    ).reset_index()

    # 6. ITEM AGGREGATIONS: Captures global item popularity and average sentiment
    item_feats = interactions.groupby("item_id").agg(
        avg_item_rating=("rating", "mean"),
        item_popularity=("user_id", "nunique")
    ).reset_index()

    # 7. FEATURE MERGE: Joins aggregates back to the primary interaction grain
    features = interactions[["user_id", "item_id"]].drop_duplicates()
    features = features.merge(user_feats, on="user_id").merge(item_feats, on="item_id")
    # Adds metadata column to track when these features were last refreshed
    features["last_updated"] = datetime.now()

    print(f"Ready to upload {len(features)} rows to SQL.")

    # 8. TRANSACTIONAL UPLOAD: Ensures atomicity when writing to the database
    try:
        # engine.begin() starts a transaction; rolls back automatically on error
        with engine.begin() as conn:
            # Idempotency check: ensures schema exists before table creation
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS recomart;"))
            # Writes to the feature_transform table, replacing old data with fresh signals
            features.to_sql(
                "feature_transform", 
                conn, 
                schema="recomart", 
                if_exists="replace", 
                index=False
            )
        print("Database upload successful.")
    except Exception as e:
        # Provides detailed logs if the database write fails
        print(f"Database error: {e}")

# Entry point protection for modular execution
if __name__ == "__main__":
    pass # Script intended to be called by main.py flow