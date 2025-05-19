"""
Script to seed the database with S&P 500 companies.
"""
import sys
import logging
from pathlib import Path

# Add the parent directory to the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal, engine, Base
from app.models import Company

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sample S&P 500 data
SAMPLE_DATA = [
    {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Information Technology", "industry": "Technology Hardware"},
    {"symbol": "MSFT", "name": "Microsoft Corp", "sector": "Information Technology", "industry": "Software"},
    {"symbol": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary", "industry": "Internet Retail"},
    {"symbol": "NVDA", "name": "NVIDIA Corp", "sector": "Information Technology", "industry": "Semiconductors"},
    {"symbol": "GOOGL", "name": "Alphabet Inc. Class A", "sector": "Communication Services", "industry": "Internet"},
]

def create_tables():
    """Create all tables in the database."""
    # Import only what's needed for table creation
    from app.models import Company, NewsItem, NewsSentiment
    Base.metadata.create_all(bind=engine)

def seed_companies():
    """Seed companies into the database."""
    db = SessionLocal()
    try:
        # Check if companies already exist
        existing_count = db.query(Company).count()
        if existing_count > 0:
            logger.info(f"Database already has {existing_count} companies, skipping seeding")
            return
        
        # Add companies
        for data in SAMPLE_DATA:
            company = Company(
                symbol=data["symbol"],
                name=data["name"],
                sector=data["sector"],
                industry=data["industry"]
            )
            db.add(company)
            logger.info(f"Added company: {data['symbol']}")
        
        db.commit()
        logger.info(f"Successfully added {len(SAMPLE_DATA)} companies")
    except Exception as e:
        db.rollback()
        logger.error(f"Error seeding companies: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    logger.info("Creating database tables...")
    create_tables()
    
    logger.info("Seeding companies...")
    seed_companies()
    
    logger.info("Done!")