import logging
from prefect import flow, task, get_run_logger
from prefect.tasks import exponential_backoff
from datetime import timedelta

# --- Import custom project modules ---
from ingestion import ingest_data 
from validation import validate_data 
from preparation import prepare_and_save_eda 
from transformation import transform_features 
from train_model import train_model

# --- Helper: Dual Logging for Task 8 Audit Requirements ---
def get_audit_logger():
    """Writes to both Prefect UI and a local persistent audit file."""
    logger = logging.getLogger("recomart_audit")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        # Persistent Audit File
        fh = logging.FileHandler("pipeline_audit.log", mode='a', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
    return logger

# --- Orchestration Tasks ---

@task(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
def task_ingest():
    """Task 10: Ingestion with exponential backoff to handle DB locks."""
    audit_log = get_audit_logger()
    audit_log.info("AUDIT: Ingestion stage started.")
    return ingest_data()

@task(retries=2)
def task_validate():
    """Task 5 & 10: Data validation and quality reporting."""
    audit_log = get_audit_logger()
    result = validate_data()
    audit_log.info("AUDIT: Validation stage completed successfully.")
    return result

@task
def task_prepare(interactions, products):
    """Task 5: EDA and Data Cleaning."""
    return prepare_and_save_eda(interactions, products)

@task
def task_transform(interactions):
    """Task 7: Feature Store Synchronization."""
    audit_log = get_audit_logger()
    result = transform_features(interactions)
    audit_log.info(f"AUDIT: Feature Store updated with {len(result)} records.")
    return result

@task
def task_train():
    """Task 10: Model training and performance tracking."""
    audit_log = get_audit_logger()
    model, metrics = train_model()
    audit_log.info(f"AUDIT: Model training finished. Metrics: {metrics}")
    return model, metrics

# --- Main Flow ---

@flow(name="RecoMart-Continuous-Retraining", log_prints=True)
def full_pipeline():
    """
    Main DAG: Ingestion -> Validation -> Preparation -> Transformation -> Training
    """
    logger = get_run_logger()
    
    # Sequential Execution Logic
    task_ingest()
    interactions, products = task_validate()
    
    logger.info("Proceeding to Preparation and Transformation...")
    task_prepare(interactions, products)
    features = task_transform(interactions)
    
    model, metrics = task_train()
    
    logger.info(f"🚀 Pipeline Run Successful. Latest RMSE: {metrics.get('rmse')}")

if __name__ == "__main__":
    full_pipeline.serve(
        name="recomart-production-deployment",
        interval=300,
        # CRITICAL FIX: limit=1 prevents concurrent runs from locking the DB
        limit=1, 
        description="Deployment with concurrency protection to prevent SQLite locks."
    )