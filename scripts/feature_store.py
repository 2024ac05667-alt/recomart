from prefect import task
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import logging

DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
logger = logging.getLogger(__name__)

@task
def store_features(interactions: pd.DataFrame, products: pd.DataFrame):
    """
    Compute features per user-item combination and store them in recomart.feature_store.
    """

    engine = create_engine(DB_URL)

    if interactions.empty or products.empty:
        logger.warning("No data to compute features")
        return "No features inserted"

    # 1️⃣ Average rating per item
    avg_item_rating = interactions.groupby("item_id")["rating"].mean().reset_index()
    avg_item_rating.rename(columns={"rating": "avg_item_rating"}, inplace=True)

    # 2️⃣ User activity count
    user_activity = interactions.groupby("user_id")["item_id"].count().reset_index()
    user_activity.rename(columns={"item_id": "user_activity_count"}, inplace=True)

    # 3️⃣ Merge with interactions
    features = interactions.merge(avg_item_rating, on="item_id", how="left")
    features = features.merge(user_activity, on="user_id", how="left")

    # 4️⃣ Encode product category
    products_subset = products[["item_id", "category"]].drop_duplicates()
    products_subset["category_encoded"] = pd.factorize(products_subset["category"])[0]
    features = features.merge(products_subset[["item_id", "category_encoded"]], on="item_id", how="left")

    # 5️⃣ Add last_updated column
    features["last_updated"] = datetime.now()

    # 6️⃣ Ensure all required columns exist before selecting
    required_columns = [
        "user_id",
        "item_id",
        "avg_item_rating",
        "user_activity_count",
        "category_encoded",
        "last_updated"
    ]

    for col in required_columns:
        if col not in features.columns:
            features[col] = None  # Fill missing columns with None

    # 7️⃣ Select and drop duplicates
    features_to_store = features[required_columns].drop_duplicates(subset=["user_id", "item_id"])

    print(f"Inserting {len(features_to_store)} rows into feature_store...")
    features_to_store.to_sql(
        "feature_store",
        engine,
        schema="recomart",
        if_exists="replace",  # Replace previous version
        index=False
    )
    print("Feature store insert complete.")

    return features_to_store
