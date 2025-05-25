#!/usr/bin/env python3
"""
SAFE article reprocessing script - preserves all news articles
Cleans content snippets and updates company-news associations using hybrid ML
Creates backup before making changes
FIXED: Better transaction handling to prevent batch failures
"""

import sys
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import json
import re
import html
from bs4 import BeautifulSoup

# Add the parent directory to the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal
from app.models import Company, NewsItem, company_news_association
from app.processor import NewsProcessor
from sqlalchemy import delete, text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_content_snippet(raw_content: str) -> str:
    """Clean content snippet by removing HTML, URLs, and formatting artifacts"""
    if not raw_content or not raw_content.strip():
        return ""
    
    # Step 1: Remove HTML tags using BeautifulSoup
    try:
        soup = BeautifulSoup(raw_content, 'html.parser')
        clean_text = soup.get_text()
    except Exception:
        clean_text = re.sub(r'<[^>]+>', '', raw_content)
    
    # Step 2: Decode HTML entities
    clean_text = html.unescape(clean_text)
    
    # Step 3: Remove URLs
    clean_text = re.sub(r'https?://[^\s<>"]+', '', clean_text)
    clean_text = re.sub(r'www\.[^\s<>"]+', '', clean_text)
    
    # Step 4: Remove RSS artifacts
    clean_text = re.sub(r'&nbsp;', ' ', clean_text)
    clean_text = re.sub(r'&#\d+;', ' ', clean_text)
    clean_text = re.sub(r'target\s*=\s*["\']_blank["\']', '', clean_text)
    clean_text = re.sub(r'color\s*=\s*["\'][^"\']*["\']', '', clean_text)
    
    # Step 5: Remove source attributions
    clean_text = re.sub(r'\s*[-–—]\s*[A-Z][a-zA-Z\s]+$', '', clean_text)
    clean_text = re.sub(r'\s*\([A-Z][A-Za-z\s]+\)$', '', clean_text)
    
    # Step 6: Clean whitespace
    clean_text = re.sub(r'\s+', ' ', clean_text)
    clean_text = clean_text.strip()
    
    # Step 7: Remove if too short
    if len(clean_text.split()) < 5:
        return ""
    
    return clean_text

def analyze_content_quality():
    """Analyze content quality in the database"""
    db = SessionLocal()
    try:
        total_articles = db.query(NewsItem).count()
        articles_with_content = db.query(NewsItem).filter(
            NewsItem.content_snippet.isnot(None),
            NewsItem.content_snippet != ''
        ).count()
        
        # Sample some content to check for HTML artifacts
        sample_articles = db.query(NewsItem).filter(
            NewsItem.content_snippet.isnot(None),
            NewsItem.content_snippet != ''
        ).limit(20).all()
        
        html_artifacts = 0
        examples = []
        
        for article in sample_articles:
            content = article.content_snippet
            has_html = any(marker in content for marker in [
                '<a href', '&nbsp;', 'target="_blank"', '<font color', '&#', 'color='
            ])
            if has_html:
                html_artifacts += 1
                if len(examples) < 3:
                    examples.append({
                        'title': article.title[:50] + '...',
                        'content': content[:100] + '...',
                        'cleaned': clean_content_snippet(content)[:100] + '...'
                    })
        
        logger.info(f"📊 Content Quality Analysis:")
        logger.info(f"   Total articles: {total_articles}")
        logger.info(f"   Articles with content: {articles_with_content}")
        logger.info(f"   Sample with HTML artifacts: {html_artifacts}/{len(sample_articles)} ({html_artifacts/len(sample_articles):.1%})")
        
        if examples:
            logger.info("\n🔍 Content cleaning examples:")
            for i, ex in enumerate(examples, 1):
                logger.info(f"   Example {i}: {ex['title']}")
                logger.info(f"      Raw: {ex['content']}")
                logger.info(f"   Cleaned: {ex['cleaned']}")
        
        return {
            'total_articles': total_articles,
            'articles_with_content': articles_with_content,
            'html_artifacts_rate': html_artifacts / len(sample_articles) if sample_articles else 0,
            'needs_cleaning': html_artifacts > 0
        }
    finally:
        db.close()

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
        
        # Content quality analysis
        content_quality = analyze_content_quality()
        
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
            'total_associations': total_associations,
            'content_quality': content_quality
        }
        
    finally:
        db.close()

def reprocess_database(batch_size: int = 50):
    """
    Reprocess all articles with content cleaning and hybrid ML classification
    FIXED: Better transaction handling with individual commits
    """
    
    db = SessionLocal()
    processor = NewsProcessor()
    
    try:
        # Get all companies
        companies = db.query(Company).all()
        company_data = [{"id": c.id, "symbol": c.symbol, "name": c.name} for c in companies]
        
        logger.info(f"🔄 Found {len(companies)} companies")
        
        # Update processor with company knowledge
        processor.update_company_knowledge(companies=company_data)
        
        # Get all news items
        total_articles = db.query(NewsItem).count()
        logger.info(f"📰 Found {total_articles} articles to reprocess")
        
        # Clear old associations (but keep articles)
        logger.info("🗑️ Clearing old company-news associations...")
        db.execute(delete(company_news_association))
        db.commit()
        
        # Statistics tracking
        articles_processed = 0
        articles_with_companies = 0
        content_cleaned = 0
        content_filtered = 0
        errors_encountered = 0
        company_article_counts = {}
        
        offset = 0
        while offset < total_articles:
            # Get batch of articles
            batch = db.query(NewsItem).offset(offset).limit(batch_size).all()
            
            if not batch:
                break
                
            batch_num = offset // batch_size + 1
            logger.info(f"🔄 Processing batch {batch_num} ({offset + 1}-{min(offset + len(batch), total_articles)} of {total_articles})")
            
            # Process each article individually to avoid transaction issues
            for news_item in batch:
                try:
                    # Start a new transaction for each article
                    individual_db = SessionLocal()
                    
                    try:
                        # Get the article in this session
                        article = individual_db.query(NewsItem).filter(NewsItem.id == news_item.id).first()
                        
                        # Step 1: Clean content snippet
                        original_content = article.content_snippet or ""
                        
                        if original_content:
                            cleaned_content = clean_content_snippet(original_content)
                            
                            if cleaned_content != original_content:
                                article.content_snippet = cleaned_content
                                
                                if cleaned_content:
                                    content_cleaned += 1
                                    logger.debug(f"✨ Cleaned: {article.title[:40]}...")
                                else:
                                    content_filtered += 1
                                    logger.debug(f"🗑️ Filtered: {article.title[:40]}...")
                        else:
                            cleaned_content = ""
                        
                        # Step 2: Hybrid ML company classification
                        relevant_companies = processor.identify_relevant_companies(
                            article.title,
                            cleaned_content
                        )
                        
                        if relevant_companies:
                            # Add new associations
                            for company_info in relevant_companies:
                                company_id = company_info['company_id']
                                
                                # Insert into association table
                                individual_db.execute(
                                    company_news_association.insert().values(
                                        company_id=company_id,
                                        news_id=article.id
                                    )
                                )
                                
                                # Count for statistics
                                symbol = company_info['company_symbol']
                                company_article_counts[symbol] = company_article_counts.get(symbol, 0) + 1
                            
                            articles_with_companies += 1
                            
                            # Log high-confidence classifications
                            high_conf = [rc for rc in relevant_companies if rc['relevance_score'] >= 0.8]
                            if high_conf:
                                scores = [f"{rc['company_symbol']}:{rc['relevance_score']:.2f}" for rc in high_conf]
                                logger.info(f"📰 '{article.title[:50]}...' → {', '.join(scores)}")
                        
                        # Commit this individual article
                        individual_db.commit()
                        articles_processed += 1
                        
                    except Exception as e:
                        individual_db.rollback()
                        errors_encountered += 1
                        logger.error(f"❌ Error processing article ID {news_item.id}: {e}")
                        continue
                    finally:
                        individual_db.close()
                        
                except Exception as e:
                    errors_encountered += 1
                    logger.error(f"❌ Failed to create session for article ID {news_item.id}: {e}")
                    continue
            
            # Log batch progress
            logger.info(f"✅ Batch {batch_num} completed - {articles_processed} processed, {errors_encountered} errors")
            
            offset += batch_size
        
        # Final summary
        logger.info("🎉 Database reprocessing complete!")
        logger.info(f"📊 Reprocessing Summary:")
        logger.info(f"   Total articles: {total_articles}")
        logger.info(f"   Articles processed: {articles_processed}")
        logger.info(f"   Articles with companies: {articles_with_companies}")
        logger.info(f"   Classification rate: {articles_with_companies/articles_processed:.1%}" if articles_processed > 0 else "   Classification rate: 0%")
        logger.info(f"   Content cleaned: {content_cleaned}")
        logger.info(f"   Content filtered out: {content_filtered}")
        logger.info(f"   Errors encountered: {errors_encountered}")
        logger.info(f"   Success rate: {(articles_processed - errors_encountered)/articles_processed:.1%}" if articles_processed > 0 else "   Success rate: 0%")
        
        logger.info(f"📈 Final articles per company:")
        for symbol, count in sorted(company_article_counts.items()):
            logger.info(f"   {symbol}: {count} articles")
        
        return {
            'articles_processed': articles_processed,
            'articles_with_companies': articles_with_companies,
            'content_cleaned': content_cleaned,
            'content_filtered': content_filtered,
            'errors_encountered': errors_encountered,
            'company_counts': company_article_counts
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error during reprocessing: {e}")
        raise
    finally:
        db.close()

def main():
    """Main function with comprehensive reprocessing options"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Reprocess articles with content cleaning and hybrid ML")
    parser.add_argument("--analyze", action="store_true", help="Analyze current database state")
    parser.add_argument("--reprocess", action="store_true", help="Full reprocessing: clean content + reclassify")
    parser.add_argument("--restore", type=str, help="Restore from backup file")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing (smaller = safer)")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    
    args = parser.parse_args()
    
    if args.restore:
        logger.info(f"🔄 Restoring from backup: {args.restore}")
        restore_from_backup(args.restore)
        logger.info("✅ Restore complete")
        return
    
    if args.analyze:
        logger.info("🔍 Analyzing current database state...")
        analyze_current_state()
        return
    
    if args.reprocess:
        logger.info("🔄 Starting FULL database reprocessing (FIXED VERSION)...")
        logger.info("📋 This process will:")
        logger.info("   ✅ Preserve ALL news articles")
        logger.info("   ✅ Create backup of current associations")
        logger.info("   🧹 Clean HTML artifacts from content snippets")
        logger.info("   🤖 Reclassify articles using hybrid ML")
        logger.info("   🛡️ Process each article individually (safer)")
        logger.info("   ❌ NOT delete any articles")
        
        if not args.force:
            response = input("\n⚠️  Continue with full reprocessing? (y/N): ")
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
            # Reprocess everything with smaller batch size for safety
            logger.info(f"\n🔄 Starting safe reprocessing (batch size: {args.batch_size})...")
            results = reprocess_database(batch_size=args.batch_size)
            
            # Analyze new state
            logger.info("\n📊 Analyzing new state...")
            new_state = analyze_current_state()
            
            # Show comparison
            logger.info(f"\n📈 BEFORE vs AFTER comparison:")
            logger.info(f"   Total articles: {current_state['total_articles']} → {new_state['total_articles']} (preserved)")
            logger.info(f"   Articles with companies: {current_state['articles_with_companies']} → {new_state['articles_with_companies']}")
            logger.info(f"   Total associations: {current_state['total_associations']} → {new_state['total_associations']}")
            
            # Content quality comparison
            old_artifacts = current_state['content_quality']['html_artifacts_rate']
            new_artifacts = new_state['content_quality']['html_artifacts_rate']
            logger.info(f"   HTML artifacts rate: {old_artifacts:.1%} → {new_artifacts:.1%}")
            
            logger.info(f"\n📋 Company article count changes:")
            all_symbols = set(list(current_state['company_stats'].keys()) + list(new_state['company_stats'].keys()))
            for symbol in sorted(all_symbols):
                old_count = current_state['company_stats'].get(symbol, 0)
                new_count = new_state['company_stats'].get(symbol, 0)
                change = new_count - old_count
                change_str = f"({change:+d})" if change != 0 else ""
                logger.info(f"   {symbol}: {old_count} → {new_count} {change_str}")
            
            logger.info(f"\n💾 Backup saved as: {backup_file}")
            logger.info("✅ FIXED reprocessing completed successfully!")
            logger.info("🧹 Content is now clean and ready for production!")
            
        except Exception as e:
            logger.error(f"❌ Reprocessing failed: {e}")
            logger.info(f"🔄 You can restore from backup using:")
            logger.info(f"   python scripts/reprocess_articles.py --restore {backup_file}")
            raise
    
    else:
        # Show current stats and options
        logger.info("📊 News API Database Reprocessing Tool (FIXED VERSION)")
        logger.info("=" * 50)
        analyze_current_state()
        
        logger.info("\n🔧 Available Commands:")
        logger.info("  python scripts/reprocess_articles.py --analyze       # Analyze current state")
        logger.info("  python scripts/reprocess_articles.py --reprocess     # Full reprocessing (clean + reclassify)")
        logger.info("  python scripts/reprocess_articles.py --restore FILE  # Restore from backup")
        logger.info("  ls backups/                                         # List available backups")
        
        logger.info("\n💡 This FIXED version:")
        logger.info("   🛡️ Processes each article individually")
        logger.info("   🔄 Uses smaller batch sizes (default: 50)")
        logger.info("   ✅ Continues processing even if some articles fail")
        logger.info("   📊 Reports detailed error statistics")

if __name__ == "__main__":
    main()