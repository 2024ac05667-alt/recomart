import pandas as pd
from sqlalchemy import create_engine
import os
from reportlab.pdfgen import canvas
from datetime import datetime

DB_URL = "postgresql://postgres:admin@localhost:5432/postgres"
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

def validate_data():
    engine = create_engine(DB_URL)
    interactions = pd.read_sql("SELECT * FROM recomart.raw_interactions", engine)
    products = pd.read_sql("SELECT * FROM recomart.raw_products", engine)

    report = {}
    # Interactions
    report["interaction_rows"] = len(interactions)
    report["interaction_duplicates"] = interactions.duplicated().sum()
    report["missing_rating"] = interactions['rating'].isnull().sum()
    report["invalid_rating"] = (~interactions['rating'].between(1,5)).sum()
    # Products
    report["product_rows"] = len(products)
    report["missing_price"] = products['price'].isnull().sum()
    report["invalid_price"] = (products['price']<0).sum()

    # Generate PDF
    pdf_path = os.path.join(REPORT_DIR, f"data_quality_{datetime.now().strftime('%Y%m%dT%H%M%S')}.pdf")
    c = canvas.Canvas(pdf_path)
    c.drawString(50, 800, "Data Quality Report")
    y = 780
    for k,v in report.items():
        c.drawString(50, y, f"{k}: {v}")
        y -= 20
    c.save()
    return interactions, products
