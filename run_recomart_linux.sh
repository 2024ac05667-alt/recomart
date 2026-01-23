#!/bin/bash
echo "[INFO] Starting Linux/Mac Setup..."
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start Prefect in the background
prefect server start & 
sleep 10

python scripts/prefect_flow.py