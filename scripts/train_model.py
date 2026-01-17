import pandas as pd
from sqlalchemy import create_engine
from sklearn.decomposition import TruncatedSVD
from datetime import datetime
import uuid
import json
import os

DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
REPORT_DIR="reports"
os.makedirs(REPORT_DIR,exist_ok=True)

def train_model():
    engine = create_engine(DB_URL)
    interactions = pd.read_sql("SELECT user_id,item_id,rating FROM recomart.raw_interactions",engine)
    if interactions.empty:
        print("No interactions, skipping model training")
        return None,None
    matrix = interactions.pivot_table(index="user_id",columns="item_id",values="rating",fill_value=0)
    n_comp = min(5,matrix.shape[1]-1)
    model = TruncatedSVD(n_components=n_comp,random_state=42)
    model.fit(matrix)
    metrics = {"explained_variance":float(model.explained_variance_ratio_.sum())}
    # Save JSON
    with open(os.path.join(REPORT_DIR,f"model_performance_{datetime.now().strftime('%Y%m%dT%H%M%S')}.json"),"w") as f:
        json.dump(metrics,f)
    # Store metadata
    run_id = str(uuid.uuid4())
    metadata = pd.DataFrame([{
        "run_id":run_id,
        "training_date":datetime.now(),
        "rmse":None,
        "n_features":matrix.shape[1],
        "model_type":"Collaborative SVD"
    }])
    metadata.to_sql("model_metadata",engine,schema="recomart",if_exists="append",index=False)
    return model,metrics
