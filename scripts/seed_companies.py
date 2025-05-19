"""
Script to seed the database with S&P 500 companies.
"""
import sys
import os
import csv
import logging
from pathlib import Path

# Add the parent directory to the path so we can import from app
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.exc import IntegrityError

from app.database import SessionLocal, init_db
from app.models import Company

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Sample S&P 500 data - just a few companies for demonstration
# In a real implementation, you would download the full list
SAMPLE_DATA = [
    {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Information Technology", "industry": "Technology Hardware, Storage & Peripherals"},
    {"symbol": "MSFT", "name": "Microsoft Corp", "sector": "Information Technology", "industry": "Software"},
    {"symbol": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary", "industry": "Internet & Direct Marketing Retail"},
    {"symbol": "NVDA", "name": "NVIDIA Corp", "sector": "Information Technology", "industry": "Semiconductors & Semiconductor Equipment"},
    {"symbol": "GOOGL", "name": "Alphabet Inc. Class A", "sector": "Communication Services", "industry": "Interactive Media & Services"},
    {"symbol": "META", "name": "Meta Platforms Inc. Class A", "sector": "Communication Services", "industry": "Interactive Media & Services"},
    {"symbol": "BRK.B", "name": "Berkshire Hathaway Inc. Class B", "sector": "Financials", "industry": "Diversified Financial Services"},
    {"symbol": "TSLA", "name": "Tesla Inc", "sector": "Consumer Discretionary", "industry": "Automobiles"},
    {"symbol": "UNH", "name": "UnitedHealth Group Inc", "sector": "Health Care", "industry": "Health Care Providers & Services"},
    {"symbol": "JNJ", "name": "Johnson & Johnson", "sector": "Health Care", "industry": "Pharmaceuticals"},
]

def seed_companies(csv_path=None):
    """Seed the database with S&P 500 companies from a CSV file or sample data."""
    # Initialize the database if needed
    init_db()
    
    db = SessionLocal()
    try:
        companies = []
        
        if csv_path and os.path.exists(csv_path):
            # Read from CSV file if provided
            logger.info(f"Reading companies from {csv_path}")
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    companies.append({
                        "symbol": row.get("Symbol", "").strip(),
                        "name": row.get("Name", "").strip(),
                        "sector": row.get("Sector", "").strip(),
                        "industry": row.get("Industry", "").strip()
                    })
        else:
            # Use sample data
            logger.info("Using sample data (10 companies)")
            companies = SAMPLE_DATA
        
        # Add companies to database
        for company_data in companies:
            # Skip empty symbols
            if not company_data.get("symbol"):
                continue
                
            company = Company(
                symbol=company_data["symbol"],
                name=company_data["name"],
                sector=company_data["sector"],
                industry=company_data["industry"]
            )
            
            try:
                db.add(company)
                db.commit()
            except IntegrityError:
                # Skip duplicates
                db.rollback()
                logger.warning(f"Company {company_data['symbol']} already exists, skipping")
        
        logger.info(f"Added {len(companies)} companies to the database")
        
    except Exception as e:
        logger.error(f"Error seeding companies: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    # Accept CSV path as command line argument
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    seed_companies(csv_path)