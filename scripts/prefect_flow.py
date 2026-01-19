# Imports the core Prefect components: flow for orchestration, task for modular units, and logger for observability [cite: 5, 14]
from prefect import flow, task, get_run_logger
# Imports timedelta to handle time-based calculations for scheduling or retries [cite: 14]
from datetime import timedelta

# --- Import custom project modules representing the distinct stages of the ML lifecycle [cite: 4, 26] ---
from ingestion import ingest_data  # Handles data acquisition from source systems [cite: 33]
from validation import validate_data  # Ensures data integrity and schema adherence [cite: 50]
from preparation import prepare_and_save_eda  # Conducts cleaning and Exploratory Data Analysis [cite: 63]
from transformation import transform_features  # Performs feature engineering for the model [cite: 72]
from train_model import train_model  # Executes the ML training and evaluation logic [cite: 85]

@task
def task_ingest():
    """Wraps the ingestion logic into a Prefect task for tracking and retries [cite: 99]"""
    # Executes the raw data generation and loading process [cite: 38]
    return ingest_data()

@task
def task_validate():
    """Validates the raw data to prevent 'garbage-in, garbage-out' scenarios [cite: 52]"""
    # Triggers the validation suite which generates a quality report [cite: 57]
    return validate_data()

@task
def task_prepare(interactions, products):
    """Modular task to generate visual insights from cleaned data [cite: 65]"""
    # Passes the dataframes to the EDA script for plot generation [cite: 70]
    return prepare_and_save_eda(interactions, products)

@task
def task_transform(interactions):
    """Transforms raw interactions into model-ready features in the feature store [cite: 75, 80]"""
    # Calculates user and item-level features like co-occurrence [cite: 77, 84]
    return transform_features(interactions)

@task
def task_train():
    """Final stage: Trains the Collaborative Filtering model [cite: 89]"""
    # Learns latent factors using TruncatedSVD and saves performance metrics [cite: 91, 92]
    return train_model()

# Defines the entry point for the pipeline with logging enabled for transparency [cite: 104]
@flow(name="RecoMart Pipeline", log_prints=True)
def full_pipeline():
    """Orchestrates the execution order and data flow between tasks [cite: 100]"""
    # Initializes the logger to record system health and metadata [cite: 45]
    logger = get_run_logger()
    
    # 1. Execution of the Ingestion stage [cite: 33]
    task_ingest()
    
    # 2. Execution of Validation; capturing outputs for downstream tasks [cite: 50]
    interactions, products = task_validate()
    # Logs a milestone marker in the Prefect dashboard [cite: 104]
    logger.info("Validation complete.")

    # 3. Executes Data Preparation and EDA generation [cite: 63]
    task_prepare(interactions, products)

    # 4. Executes Feature Transformation to update the feature store [cite: 72, 80]
    features = task_transform(interactions)

    # 5. Executes Training and captures the model and its RMSE/metrics [cite: 85, 92]
    model, metrics = task_train()
    
    # Records final pipeline performance for MLOps monitoring [cite: 111, 118]
    logger.info(f"Pipeline finished. Metrics: {metrics}")
    # Returns final metrics for potential CI/CD evaluation [cite: 4]
    return metrics

# Entry point for serving the pipeline as a persistent background worker [cite: 14]
if __name__ == "__main__":
    # Configures the pipeline to run automatically on a fixed schedule [cite: 101]
    full_pipeline.serve(
        # Assigns a unique name to this deployment for easy identification [cite: 121]
        name="recomart-production-run",
        # Sets a 3-minute interval (180 seconds) for continuous data syncing [cite: 101]
        interval=180,  
        # Provides a business-friendly description for the UI [cite: 21]
        description="RecoMart pipeline syncing every 3 minutes."
    )