from prefect import flow, task, get_run_logger
from datetime import timedelta

# Import your custom modules
from ingestion import ingest_data
from validation import validate_data
from preparation import prepare_and_save_eda
from transformation import transform_features
from train_model import train_model

@task
def task_ingest():
    return ingest_data()

@task
def task_validate():
    # Matches your existing script (0 arguments)
    return validate_data()

@task
def task_prepare(interactions, products):
    # Matches your script (takes the two dataframes)
    return prepare_and_save_eda(interactions, products)

@task
def task_transform(interactions):
    # Matches your script (takes interactions)
    return transform_features(interactions)

@task
def task_train():
    # FIXED: Matches your script (takes 0 arguments)
    return train_model()

@flow(name="RecoMart Pipeline", log_prints=True)
def full_pipeline():
    logger = get_run_logger()
    
    # 1. Ingest
    task_ingest()
    
    # 2. Validate (Self-contained, no arguments)
    interactions, products = task_validate()
    logger.info("Validation complete.")

    # 3. Preparation
    task_prepare(interactions, products)

    # 4. Transform
    features = task_transform(interactions)

    # 5. Train (Self-contained, no arguments)
    # This prevents the TypeError you just saw
    model, metrics = task_train()
    
    logger.info(f"Pipeline finished. Metrics: {metrics}")
    return metrics

if __name__ == "__main__":
    # SERVE with a 1-minute interval
    full_pipeline.serve(
        name="recomart-production-run",
        interval=180,  # 60 seconds = 1 minute
        description="RecoMart pipeline syncing every minute."
    )