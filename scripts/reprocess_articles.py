#!/usr/bin/env python3
"""
SAFE article reprocessing script - preserves all news articles
Only updates company-news associations using hybrid ML
Creates backup before making changes
"""

import sys
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import json

# Add the parent directory to the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal
from app.models import Company, NewsItem, company_news_association
from app.processor import NewsProcessor
from sqlalchemy import delete, text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_backup():
    """Create backup of current company-news associations"""
    db = SessionLocal()
    backup_data = []
    
    try:
        # Create backups directory if it doesn't exist
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)
        
        # Get all current associations
        result = db.execute(text("SELECT company_id, news_id FROM company_news_association"))
        associations = result.fetchall()
        
        for company_id, news_id in associations:
            backup_data.append({"company_id": company_id, "news_id": news_id})
        
        # Save backup to file in backups directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"backup_associations_{timestamp}.json"
        
        with open(backup_file, 'w') as f:
            json.dump(backup_data, f, indent=2)
        
        logger.info(f"💾 Backup created: {backup_file} ({len(backup_data)} associations)")
        return str(backup_file)
        
    finally:
        db.close()

def restore_from_backup(backup_file: str):
    """Restore associations from backup file"""
    db = SessionLocal()
    
    try:
        # Clear current associations
        logger.info("🗑️ Clearing current associations for restore...")
        db.execute(delete(company_news_association))
        
        # Handle both absolute and relative paths
        backup_path = Path(backup_file)
        if not backup_path.exists():
            # Try in backups directory
            backup_path = Path("backups") / backup_file
        
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_file}")
        
        # Load backup data
        with open(backup_path, 'r') as f:
            backup_data = json.load(f)
        
        # Restore associations
        for item in backup_data:
            db.execute(
                company_news_association.insert().values(
                    company_id=item['company_id'],
                    news_id=item['news_id']
                )
            )
        
        db.commit()
        logger.info(f"✅ Restored {len(backup_data)} associations from {backup_path}")
        
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Restore failed: {e}")
        raise
    finally:
        db.close()

def analyze_current_state():
    """Analyze current article distribution"""
    db = SessionLocal()
    
    try:
        # Get current stats
        from sqlalchemy import func
        
        # Total articles
        total_articles = db.query(NewsItem).count()
        
        # Articles with company associations
        articles_with_companies = db.query(NewsItem.id).join(
            company_news_association,
            NewsItem.id == company_news_association.c.news_id
        ).distinct().count()
        
        # Company stats
        company_stats = db.query(
            Company.symbol,
            func.count(NewsItem.id).label('article_count')
        ).join(
            company_news_association,
            Company.id == company_news_association.c.company_id
        ).join(
            NewsItem,
            NewsItem.id == company_news_association.c.news_id
        ).group_by(Company.symbol).all()
        
        logger.info("📊 Current Database State:")
        logger.info(f"   Total articles: {total_articles}")
        logger.info(f"   Articles with companies: {articles_with_companies}")
        logger.info(f"   Articles without companies: {total_articles - articles_with_companies}")
        
        logger.info("📈 Current articles per company:")
        total_associations = 0
        for symbol, count in sorted(company_stats):
            logger.info(f"   {symbol}: {count} articles")
            total_associations += count
        
        logger.info(f"   Total associations: {total_associations}")
        
        return {
            'total_articles': total_articles,
            'articles_with_companies': articles_with_companies,
            'company_stats': dict(company_stats),
            'total_associations': total_associations
        }
        
    finally:
        db.close()

def safe_reprocess_articles(batch_size: int = 100):
    """Safely reprocess all articles with hybrid ML - preserves all news articles"""
    
    db = SessionLocal()
    processor = NewsProcessor()
    
    try:
        # Get all companies
        companies = db.query(Company).all()
        company_data = [{"id": c.id, "symbol": c.symbol, "name": c.name} for c in companies]
        
        logger.info(f"🔄 Found {len(companies)} companies")
        
        # Update processor with company knowledge
        processor.update_company_knowledge(companies=company_data)
        
        # Get all news items (preserving all articles)
        total_articles = db.query(NewsItem).count()
        logger.info(f"📰 Found {total_articles} articles to reprocess")
        
        # IMPORTANT: Only clear associations, not the articles themselves
        logger.info("🗑️ Clearing old company-news associations (keeping all articles)...")
        db.execute(delete(company_news_association))
        db.commit()
        
        # Process articles in batches for memory efficiency
        articles_processed = 0
        articles_with_companies = 0
        company_article_counts = {}
        
        offset = 0
        while offset < total_articles:
            # Get batch of articles
            batch = db.query(NewsItem).offset(offset).limit(batch_size).all()
            
            if not batch:
                break
                
            logger.info(f"🔄 Processing batch {offset//batch_size + 1} ({offset + 1}-{min(offset + len(batch), total_articles)} of {total_articles})")
            
            for news_item in batch:
                try:
                    # Use hybrid ML to identify relevant companies
                    relevant_companies = processor.identify_relevant_companies(
                        news_item.title,
                        news_item.content_snippet or ""
                    )
                    
                    if relevant_companies:
                        # Add new associations
                        for company_info in relevant_companies:
                            company_id = company_info['company_id']
                            
                            # Insert into association table
                            db.execute(
                                company_news_association.insert().values(
                                    company_id=company_id,
                                    news_id=news_item.id
                                )
                            )
                            
                            # Count for statistics
                            symbol = company_info['company_symbol']
                            company_article_counts[symbol] = company_article_counts.get(symbol, 0) + 1
                        
                        articles_with_companies += 1
                        
                        # Log interesting high-confidence classifications
                        high_conf_companies = [rc for rc in relevant_companies if rc['relevance_score'] >= 0.8]
                        if high_conf_companies:
                            scores = [f"{rc['company_symbol']}:{rc['relevance_score']:.2f}" for rc in high_conf_companies]
                            logger.info(f"📰 '{news_item.title[:60]}...' → {', '.join(scores)}")
                    
                    articles_processed += 1
                    
                except Exception as e:
                    logger.error(f"❌ Error processing article ID {news_item.id}: {e}")
                    continue
            
            # Commit batch
            db.commit()
            logger.info(f"✅ Batch committed - {articles_processed} articles processed so far")
            
            offset += batch_size
        
        # Final summary
        logger.info("🎉 Reprocessing complete!")
        logger.info(f"📊 Summary:")
        logger.info(f"   Total articles: {total_articles} (all preserved)")
        logger.info(f"   Articles processed: {articles_processed}")
        logger.info(f"   Articles with companies: {articles_with_companies}")
        logger.info(f"   Classification rate: {articles_with_companies/articles_processed:.1%}")
        
        logger.info(f"📈 Articles per company (after hybrid ML):")
        for symbol, count in sorted(company_article_counts.items()):
            logger.info(f"   {symbol}: {count} articles")
        
        return {
            'articles_processed': articles_processed,
            'articles_with_companies': articles_with_companies,
            'company_counts': company_article_counts
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error during reprocessing: {e}")
        raise
    finally:
        db.close()

def main():
    """Main function with safety checks"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Safely reprocess articles with hybrid ML")
    parser.add_argument("--analyze", action="store_true", help="Analyze current state only")
    parser.add_argument("--reprocess", action="store_true", help="Reprocess all articles (with backup)")
    parser.add_argument("--restore", type=str, help="Restore from backup file")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    
    args = parser.parse_args()
    
    if args.restore:
        logger.info(f"🔄 Restoring from backup: {args.restore}")
        restore_from_backup(args.restore)
        logger.info("✅ Restore complete")
        return
    
    if args.analyze:
        logger.info("🔍 Analyzing current database state...")
        current_state = analyze_current_state()
        return
    
    if args.reprocess:
        logger.info("🔄 Starting SAFE article reprocessing with hybrid ML...")
        logger.info("📋 This process will:")
        logger.info("   ✅ Preserve ALL news articles")
        logger.info("   ✅ Create backup of current associations")
        logger.info("   🔄 Update company-article associations using hybrid ML")
        logger.info("   ❌ NOT delete any articles")
        
        if not args.force:
            response = input("\n⚠️  Continue with reprocessing? (y/N): ")
            if response.lower() != 'y':
                logger.info("❌ Cancelled")
                return
        
        # Analyze current state
        logger.info("\n📊 Analyzing current state...")
        current_state = analyze_current_state()
        
        # Create backup
        logger.info("\n💾 Creating backup...")
        backup_file = create_backup()
        
        try:
            # Reprocess articles
            logger.info(f"\n🔄 Starting reprocessing (batch size: {args.batch_size})...")
            results = safe_reprocess_articles(batch_size=args.batch_size)
            
            # Analyze new state
            logger.info("\n📊 Analyzing new state...")
            new_state = analyze_current_state()
            
            # Show comparison
            logger.info(f"\n📈 BEFORE vs AFTER comparison:")
            logger.info(f"   Total articles: {current_state['total_articles']} → {new_state['total_articles']} (preserved)")
            logger.info(f"   Articles with companies: {current_state['articles_with_companies']} → {new_state['articles_with_companies']}")
            logger.info(f"   Total associations: {current_state['total_associations']} → {new_state['total_associations']}")
            
            logger.info(f"\n📋 Company changes:")
            for symbol in sorted(set(list(current_state['company_stats'].keys()) + list(new_state['company_stats'].keys()))):
                old_count = current_state['company_stats'].get(symbol, 0)
                new_count = new_state['company_stats'].get(symbol, 0)
                change = new_count - old_count
                change_str = f"({change:+d})" if change != 0 else ""
                logger.info(f"   {symbol}: {old_count} → {new_count} {change_str}")
            
            logger.info(f"\n💾 Backup saved as: {backup_file}")
            logger.info("✅ Reprocessing completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Reprocessing failed: {e}")
            logger.info(f"🔄 You can restore from backup using:")
            logger.info(f"   python scripts/reprocess_articles.py --restore {backup_file}")
            raise
    
    else:
        # Show current stats and options
        logger.info("📊 Current database state:")
        analyze_current_state()
        
        logger.info("\n🔧 Options:")
        logger.info("  python scripts/reprocess_articles.py --analyze                    # Show current stats")
        logger.info("  python scripts/reprocess_articles.py --reprocess                  # Reprocess all articles (with backup)")
        logger.info("  python scripts/reprocess_articles.py --restore backups/FILE      # Restore from backup")
        logger.info("  ls backups/                                                      # List available backups")

if __name__ == "__main__":
    main()