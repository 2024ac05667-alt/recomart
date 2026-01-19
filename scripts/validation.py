import pandas as pd  # Library for tabular data manipulation and quality checking
from sqlalchemy import create_engine  # Database engine for retrieving raw data
import os  # Standard library for managing file paths and directories
from reportlab.pdfgen import canvas  # Tool for generating professional PDF audit reports
from datetime import datetime  # Used for timestamping reports to maintain an audit trail

# --- CONFIGURATION ---
# Database URI defining the connection to the PostgreSQL data warehouse
DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
# Target directory where data quality certificates (PDFs) will be stored
REPORT_DIR = "reports"
# Ensures the reporting directory exists; vital for automated pipeline resilience
os.makedirs(REPORT_DIR, exist_ok=True)

def validate_data():
    """
    Data Quality Stage: Performs integrity checks and generates a 
    non-mutable PDF report for compliance and auditing.
    """
    # Initializes the connection to the PostgreSQL instance
    engine = create_engine(DB_URL)
    
    # DATA EXTRACTION: Pulls the latest raw records for validation [cite: 5, 33]
    interactions = pd.read_sql("SELECT * FROM recomart.raw_interactions", engine)
    products = pd.read_sql("SELECT * FROM recomart.raw_products", engine)

    # 1. INITIALIZING THE VALIDATION SUITE
    report = {}
    
    # 2. INTERACTION CHECKS: Identifying anomalies in user behavior data 
    report["interaction_rows"] = len(interactions) # Baseline volume check
    report["interaction_duplicates"] = interactions.duplicated().sum() # Checking for redundancy
    report["missing_rating"] = interactions['rating'].isnull().sum() # Detecting null values
    # Business Logic Check: Ratings must fall within the defined 1-5 scale [cite: 111]
    report["invalid_rating"] = (~interactions['rating'].between(1, 5)).sum()
    
    # 3. PRODUCT CHECKS: Ensuring consistency in metadata [cite: 56]
    report["product_rows"] = len(products)
    report["missing_price"] = products['price'].isnull().sum() # Ensuring every item has a cost
    # Integrity Check: Prices cannot be negative in an e-commerce context
    report["invalid_price"] = (products['price'] < 0).sum()

    # 4. AUDIT REPORT GENERATION: Creating a timestamped PDF [cite: 57, 61]
    pdf_path = os.path.join(REPORT_DIR, f"data_quality_{datetime.now().strftime('%Y%m%dT%H%M%S')}.pdf")
    # Initializing the PDF document object
    c = canvas.Canvas(pdf_path)
    # Adding a professional header to the document
    c.drawString(50, 800, "RecoMart Data Quality Report")
    
    # Iterative block to write all validation metrics to the PDF
    y = 780 # Starting vertical position on the page
    for k, v in report.items():
        c.drawString(50, y, f"{k}: {v}")
        y -= 20 # Moves cursor down for the next metric line
        
    # Finalizing and closing the PDF file to persist it to disk [cite: 61]
    c.save()
    
    # Console feedback for the pipeline logs
    print(f"VALIDATION SUCCESS | Report saved: {pdf_path}")
    # Returning DataFrames to the Prefect flow for downstream processing [cite: 60]
    return interactions, products