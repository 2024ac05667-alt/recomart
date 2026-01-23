@echo off
SETLOCAL EnableDelayedExpansion

echo ====================================================
echo   RecoMart MLOps Pipeline: Windows Runner
echo ====================================================

:: 1. Virtual Environment Setup
if not exist "venv" (
    echo [INFO] Creating Virtual Environment...
    python -m venv venv
)
echo [INFO] Activating Virtual Environment...
call venv\Scripts\activate

:: 2. Install/Update Dependencies
echo [INFO] Syncing libraries...
pip install -r requirements.txt

:: 3. PREFECT CONFIGURATION (The Lock Fix)
:: This tells Prefect to use PostgreSQL instead of the locked SQLite file
echo [INFO] Configuring Prefect for PostgreSQL orchestration...
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://postgres:admin@localhost:5432/postgres"

:: 4. Directory Check
for %%d in (data_lake logs reports eda feature_store scripts) do (
    if not exist "%%d" mkdir "%%d"
)

:: 5. Launch Orchestration (Task 10)
echo ====================================================
echo   LAUNCHING ORCHESTRATION SERVER
echo ====================================================
echo [STEP 1] Starting Prefect Server in a new window...
start "Prefect Server" cmd /c "call venv\Scripts\activate && prefect server start"

echo [WAIT] Waiting 15 seconds for Server to initialize...
timeout /t 15 /nobreak >nul

echo [STEP 2] Triggering the RecoMart Flow...
:: Direct execution for the demo
python scripts/prefect_flow.py

echo ====================================================
echo   PIPELINE COMPLETE - CHECK 'reports' FOLDER
echo ====================================================
pause