# RecoMart: End-to-End MLOps Recommendation Pipeline
**Automated Data Management & Model Training System**

## 🚀 Project Overview
This project implements a modular pipeline for **RecoMart**, handling everything from REST API ingestion to SVD Model Training. It satisfies all requirements for the DM4ML Assignment I.

## 🛠️ System Requirements
- **Python:** 3.9+
- **Database:** PostgreSQL (User: `postgres`, Pass: `admin`, DB: `postgres`)
- **Orchestration:** Prefect 2.x

## ⚙️ Setup & Execution

### 1. Environment Initialization
Install the professional dependency stack:
```bash
pip install -r requirements.txt

###Start the Orchestration Server In a dedicated terminal, launch the dashboard .Access the UI at: http://localhost:4200
prefect server start

###Execute the Flow In a second terminal, trigger the automated DAG:
python scripts/prefect_flow.py

##Git Link (All codes)
Git Hub (Source Code):https://github.com/2024ac05667-alt/recomart

##Video Link
Video:https://drive.google.com/drive/folders/1Q4sj1WfAgGXtWLeeF34jqWaKP5w8p2lN

##Documentation .pdf and reports in taxilla portal

##To run project in windows
run_recomart_win.bat

##To run project in linux
run_recomart_linux.sh
