import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"

def transform_features(interactions):
    engine = create_engine(DB_URL)
    # Features
    user_activity = interactions.groupby("user_id")["item_id"].count().reset_index().rename(columns={"item_id":"user_activity_count"})
    avg_user = interactions.groupby("user_id")["rating"].mean().reset_index().rename(columns={"rating":"avg_user_rating"})
    avg_item = interactions.groupby("item_id")["rating"].mean().reset_index().rename(columns={"rating":"avg_item_rating"})
    co_occurrence = interactions.groupby("item_id")["user_id"].nunique().reset_index().rename(columns={"user_id":"co_occurrence_count"})
    features = interactions.merge(user_activity,on="user_id").merge(avg_user,on="user_id").merge(avg_item,on="item_id").merge(co_occurrence,on="item_id")
    features = features[["user_id","item_id","avg_user_rating","avg_item_rating","user_activity_count","co_occurrence_count"]].drop_duplicates()
    features["last_updated"] = datetime.now()
    features.to_sql("feature_store", engine, schema="recomart", if_exists="replace", index=False)
    return features
