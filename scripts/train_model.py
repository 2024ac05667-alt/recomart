import pandas as pd  # Data manipulation library for handling interaction matrices
import numpy as np  # Numerical library for matrix reconstruction and math operations
from sqlalchemy import create_engine  # Database engine for PostgreSQL warehouse connectivity
from sklearn.decomposition import TruncatedSVD  # Collaborative Filtering algorithm (Matrix Factorization)
from sklearn.model_selection import train_test_split  # Utility to split data for evaluation
from sklearn.metrics import mean_squared_error  # Metric to calculate prediction error
from datetime import datetime  # Used for timestamping model metadata
import uuid  # Generates unique identifiers (Run IDs) for experiment tracking
import json  # Exports performance metrics to standard JSON format
import os  # System utility to manage folder paths and directories
import logging  # Standard logging to maintain the project audit trail

# --- CONFIGURATION ---
# Database URI for the PostgreSQL instance where interactions are stored
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
# Folder name for storing generated performance reports
REPORT_DIR = "reports"
# Path to the central audit log file used across the pipeline
LOG_FILE = "logs/ingestion_audit.log"
# Ensure the reports directory exists to prevent file writing errors
os.makedirs(REPORT_DIR, exist_ok=True)

# Initialize the audit logger for training events
logger = logging.getLogger("recomart_audit")

def train_model():
    """Requirement Task 9: Training, Evaluation, and Metadata Persistence"""
    # Create the database connection engine
    engine = create_engine(DB_URL)
    
    # 1. DATA EXTRACTION
    try:
        # Define the SQL query to pull the specific columns needed for training
        query = "SELECT user_id, item_id, rating FROM recomart.raw_interactions"
        # Load interaction data directly into a Pandas DataFrame
        interactions = pd.read_sql(query, engine)
    except Exception as e:
        # Log failure if the table doesn't exist or connection fails
        logger.error(f"Data extraction failed: {e}")
        return None

    # Safeguard: Exit if there is no data to train on
    if interactions.empty:
        logger.warning("No data found in raw_interactions. Skipping training.")
        return None

    # 2. EVALUATION SPLIT (Requirement: Evaluation using metrics)
    # Split 80% for learning and 20% to test how well the model predicts ratings
    train_data, test_data = train_test_split(interactions, test_size=0.2, random_state=42)

    # 3. MATRIX PREPARATION
    # Pivot the training data into a Sparse User-Item Matrix (Users as rows, Items as columns)
    # fill_value=0 assumes no preference for items a user hasn't seen yet
    train_matrix = train_data.pivot_table(index="user_id", columns="item_id", values="rating", fill_value=0)
    
    # 4. TRAINING (Collaborative Filtering / SVD)
    # Define number of latent factors; min(5, total items - 1) to avoid dimensionality errors
    n_comp = min(5, train_matrix.shape[1] - 1)
    # Initialize TruncatedSVD (Matrix Factorization) with a fixed seed for reproducibility
    svd = TruncatedSVD(n_components=n_comp, random_state=42)
    # Decompose the matrix into User Factors and Item Factors
    user_factors = svd.fit_transform(train_matrix)
    item_factors = svd.components_
    
    # Reconstruct the matrix by multiplying factors back together to get 'predicted' ratings
    reconstructed_matrix = np.dot(user_factors, item_factors)
    # Convert the reconstructed array back to a DataFrame for easy lookup
    preds_df = pd.DataFrame(reconstructed_matrix, index=train_matrix.index, columns=train_matrix.columns)

    # 5. EVALUATION METRICS (RMSE)
    # Lists to store the actual ratings and the model's predicted ratings
    y_true = []
    y_pred = []
    # Loop through the test set to compare real ratings against predictions
    for _, row in test_data.iterrows():
        # Check if the user and item exist in our training matrix
        if row['user_id'] in preds_df.index and row['item_id'] in preds_df.columns:
            y_true.append(row['rating'])
            y_pred.append(preds_df.loc[row['user_id'], row['item_id']])
    
    # Calculate Root Mean Squared Error (Standard recommendation accuracy metric)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred)) if y_true else 0.0
    # Calculate how much information was kept after compression (Explained Variance)
    explained_var = float(svd.explained_variance_ratio_.sum())

    # 6. METADATA TRACKING (Requirement: Store Run IDs, Parameters, Metrics)
    # Generate a unique session ID for this training run
    run_id = str(uuid.uuid4())
    # Aggregate all parameters and results into a dictionary
    metrics = {
        "run_id": run_id,
        "rmse": round(rmse, 4),
        "explained_variance": round(explained_var, 4),
        "n_components": n_comp,
        "timestamp": datetime.now().isoformat()
    }

    # Save Performance Report: Exports the dictionary to a JSON file (Task 9 Deliverable)
    report_path = os.path.join(REPORT_DIR, f"report_{run_id[:8]}.json")
    with open(report_path, "w") as f:
        json.dump(metrics, f, indent=4)

    # Save to Database: Persistent storage of model metadata for lineage tracking
    metadata_df = pd.DataFrame([metrics])
    # Append the results to the 'model_metadata' table in the 'recomart' schema
    metadata_df.to_sql("model_metadata", engine, schema="recomart", if_exists="append", index=False)

    # Log successful completion to the audit trail
    logger.info(f"MODEL SUCCESS: RunID {run_id} | RMSE: {rmse}")
    # Print feedback to the terminal
    print(f"Training Complete. Metrics saved to {report_path}")
    
    # Return the trained model object and the metrics dictionary
    return svd, metrics

# Protection to ensure the script only runs when executed directly
if __name__ == "__main__":
    # Call the training function
    train_model()