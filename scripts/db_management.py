#!/usr/bin/env python3
"""
Database management scripts for the news API
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Add the parent directory to the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal, engine, Base
from app.models import Company, NewsItem
from app.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tables():
    """Create all tables in the database."""
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")

def check_database_connection():
    """Check if database connection is working."""
    try:
        db = SessionLocal()
        # Try a simple query
        result = db.execute("SELECT 1").fetchone()
        db.close()
        logger.info("✅ Database connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False

def get_database_stats():
    """Get statistics about the database contents."""
    db = SessionLocal()
    try:
        company_count = db.query(Company).count()
        news_count = db.query(NewsItem).count()
        
        # Get date range of news
        oldest_news = db.query(NewsItem).order_by(NewsItem.published_at.asc()).first()
        newest_news = db.query(NewsItem).order_by(NewsItem.published_at.desc()).first()
        
        logger.info("📊 Database Statistics:")
        logger.info(f"   Companies: {company_count}")
        logger.info(f"   News Articles: {news_count}")
        
        if oldest_news and newest_news:
            logger.info(f"   Date Range: {oldest_news.published_at.date()} to {newest_news.published_at.date()}")
            days_covered = (newest_news.published_at - oldest_news.published_at).days
            logger.info(f"   Days Covered: {days_covered}")
        
        # Get news by company
        from sqlalchemy import func
        company_news_stats = db.query(
            Company.symbol,
            func.count(NewsItem.id).label('news_count')
        ).join(
            Company.news_items
        ).group_by(Company.symbol).order_by(func.count(NewsItem.id).desc()).limit(5).all()
        
        if company_news_stats:
            logger.info("   Top Companies by News Count:")
            for symbol, count in company_news_stats:
                logger.info(f"     {symbol}: {count} articles")
                
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
    finally:
        db.close()

def cleanup_old_news(days_to_keep=90):
    """Remove news older than specified days."""
    db = SessionLocal()
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        # Count articles to be deleted
        old_count = db.query(NewsItem).filter(NewsItem.published_at < cutoff_date).count()
        
        if old_count == 0:
            logger.info(f"No articles older than {days_to_keep} days found")
            return
        
        # Delete old news items (cascades to related records)
        deleted = db.query(NewsItem).filter(NewsItem.published_at < cutoff_date).delete()
        db.commit()
        
        logger.info(f"🗑️ Cleaned up {deleted} articles older than {days_to_keep} days")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error cleaning up old news: {e}")
    finally:
        db.close()

def main():
    """Main function to run database management tasks."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Database management for News API")
    parser.add_argument("--check", action="store_true", help="Check database connection")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--create-tables", action="store_true", help="Create database tables")
    parser.add_argument("--cleanup", type=int, metavar="DAYS", help="Clean up news older than DAYS")
    
    args = parser.parse_args()
    
    if args.check:
        check_database_connection()
    
    if args.create_tables:
        create_tables()
    
    if args.stats:
        get_database_stats()
    
    if args.cleanup:
        cleanup_old_news(args.cleanup)
    
    if not any(vars(args).values()):
        # No arguments provided, run basic checks
        logger.info("🔧 News API Database Management")
        check_database_connection()
        get_database_stats()

if __name__ == "__main__":
    main()