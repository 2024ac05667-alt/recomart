import pandas as pd  # Data manipulation library for handling the interaction matrix
from sqlalchemy import create_engine  # Database engine for PostgreSQL connectivity
from sklearn.decomposition import TruncatedSVD  # Core ML algorithm for Collaborative Filtering
from datetime import datetime  # Used for timestamping model runs for versioning
import uuid  # Generates unique identifiers for each training session [cite: 13]
import json  # Used to export model metrics for external reporting
import os  # System utility to manage report directories

# --- CONFIGURATION ---
# Database URI pointing to the PostgreSQL warehouse [cite: 13]
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
# Target directory for performance JSON files [cite: 5]
REPORT_DIR = "reports"
# Safeguard to ensure the reporting directory exists before writing [cite: 5]
os.makedirs(REPORT_DIR, exist_ok=True)

def train_model():
    """Executes the training lifecycle from data extraction to metadata persistence [cite: 4, 9]"""
    # Initializes the database connection pool
    engine = create_engine(DB_URL)
    
    # 1. DATA EXTRACTION: Pulls raw user-item interactions from the warehouse [cite: 32]
    interactions = pd.read_sql("SELECT user_id, item_id, rating FROM recomart.raw_interactions", engine)
    
    # Validation check to prevent pipeline crashes on empty datasets 
    if interactions.empty:
        print("No interactions, skipping model training")
        return None, None

    # 2. FEATURE ENGINEERING: Reshapes data into a Sparse User-Item Matrix for SVD [cite: 89, 90]
    # fill_value=0 assumes no preference for items not yet interacted with
    matrix = interactions.pivot_table(index="user_id", columns="item_id", values="rating", fill_value=0)
    
    # 3. MODEL HYPERPARAMETERS: Dynamically adjusts components based on dataset size [cite: 83]
    n_comp = min(5, matrix.shape[1] - 1)
    
    # 4. TRAINING: Initializing TruncatedSVD with a fixed seed for reproducibility [cite: 89, 120]
    model = TruncatedSVD(n_components=n_comp, random_state=42)
    # Learns latent factors (user preferences and item characteristics)
    model.fit(matrix)
    
    # 5. METRICS: Calculates Explained Variance to measure information retention [cite: 91, 118]
    metrics = {"explained_variance": float(model.explained_variance_ratio_.sum())}
    
    # 6. PERSISTENCE: Exports metrics to a timestamped JSON for MLOps tracking [cite: 93, 94]
    with open(os.path.join(REPORT_DIR, f"model_performance_{datetime.now().strftime('%Y%m%dT%H%M%S')}.json"), "w") as f:
        json.dump(metrics, f)
    
    # 7. METADATA TRACKING: Creates a unique Run ID for model lineage [cite: 13, 105]
    run_id = str(uuid.uuid4())
    
    # Constructs the record for the 'model_metadata' audit table [cite: 92, 111]
    metadata = pd.DataFrame([{
        "run_id": run_id, # Unique identifier for this specific training run
        "training_date": datetime.now(), # Temporal marker for model promotion
        "rmse": None, # Placeholder for regression metrics (calculated during validation)
        "n_features": matrix.shape[1], # Tracks dimensionality of the feature space
        "model_type": "Collaborative SVD" # Documentation of the chosen algorithm
    }])
    
    # Appends this run's metadata to the database for historical comparison [cite: 111]
    metadata.to_sql("model_metadata", engine, schema="recomart", if_exists="append", index=False)
    
    # Console feedback for the pipeline logger [cite: 99]
    print(f"Model trained successfully. Run ID: {run_id}")
    return model, metrics

# Standard execution block
if __name__ == "__main__":
    train_model()