from google.cloud import bigquery
from google.auth import default
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any
import os
from app.models import NewsCategoryEnum, SentimentEnum

logger = logging.getLogger(__name__)

# Simple progress tracker
class BackfillProgress:
    def __init__(self):
        self.is_running = False
        self.start_time = None
        self.articles_added = 0
        self.current_date = None
        self.progress_percent = 0
        self.errors = []
        self.eta = None
    
    def to_dict(self):
        elapsed = str(datetime.now() - self.start_time).split('.')[0] if self.start_time else "0:00:00"
        return {
            "status": "running" if self.is_running else ("completed" if self.start_time else "not_started"),
            "progress_percent": self.progress_percent,
            "current_date": self.current_date,
            "articles_added": self.articles_added,
            "elapsed_time": elapsed,
            "estimated_completion": self.eta.strftime('%Y-%m-%d %H:%M:%S') if self.eta else None,
            "errors": self.errors[-3:] if self.errors else []
        }

# Global progress tracker
progress = BackfillProgress()

class SimpleGDELTCollector:
    """Simple GDELT collector - handles everything"""
    
    def __init__(self):
        self.client = None
        self.available = False
        self._setup_bigquery()
        
        # Your companies
        self.companies = {
            'AAPL': ['Apple Inc', 'Apple'],
            'MSFT': ['Microsoft Corp', 'Microsoft'],
            'AMZN': ['Amazon.com Inc', 'Amazon'],
            'GOOGL': ['Alphabet Inc', 'Google', 'Alphabet'],
            'NVDA': ['NVIDIA Corp', 'Nvidia']
        }
    
    def _setup_bigquery(self):
        """Setup BigQuery client - fail gracefully if not configured"""
        try:
            credentials, project = default()
            self.client = bigquery.Client(credentials=credentials, project=project)
            # Test connection
            self.client.query("SELECT 1 LIMIT 1").result()
            self.available = True
            logger.info("✅ GDELT BigQuery connected")
        except Exception as e:
            logger.warning(f"⚠️ GDELT not available: {e}")
            self.available = False
    
    def is_available(self):
        return self.available
    
    def get_historical_articles(self, start_date: str, end_date: str, limit: int = 1000):
        """Get historical articles from GDELT"""
        if not self.available:
            return []
        
        try:
            # Convert dates
            start_gdelt = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')
            end_gdelt = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d')
            
            # Build company filter
            company_conditions = []
            for symbol, names in self.companies.items():
                for name in names:
                    company_conditions.append(f"V2Organizations LIKE '%{name}%'")
                    company_conditions.append(f"Organizations LIKE '%{name}%'")
            
            company_filter = f"({' OR '.join(company_conditions)})"
            
            # Simple SQL query
            query = f"""
            SELECT 
                DATE,
                SourceCommonName,
                DocumentIdentifier,
                Organizations,
                V2Organizations,
                V2Tone,
                V2Themes
            FROM `gdelt-bq.gdeltv2.gkg_partitioned`
            WHERE 
                DATE >= {start_gdelt}
                AND DATE <= {end_gdelt}
                AND {company_filter}
                AND SourceCommonName IS NOT NULL
                AND DocumentIdentifier IS NOT NULL
            ORDER BY DATE DESC
            LIMIT {limit}
            """
            
            logger.info(f"🔍 Querying GDELT: {start_date} to {end_date}")
            results = self.client.query(query).result()
            
            articles = []
            for row in results:
                processed = self._process_row(row)
                if processed:
                    articles.append(processed)
            
            logger.info(f"✅ Found {len(articles)} GDELT articles")
            return articles
            
        except Exception as e:
            logger.error(f"❌ GDELT query failed: {e}")
            return []
    
    def _process_row(self, row):
        """Convert GDELT row to our format"""
        
        # Find mentioned companies
        mentioned_companies = []
        for org_field in [row.V2Organizations, row.Organizations]:
            if org_field:
                for entry in org_field.split(';'):
                    if ',' in entry:
                        org_name = entry.rsplit(',', 1)[0]
                        for symbol, names in self.companies.items():
                            if any(name.lower() in org_name.lower() for name in names):
                                mentioned_companies.append(symbol)
                                break
        
        if not mentioned_companies:
            return None
        
        # Simple sentiment conversion
        tone = 0
        if row.V2Tone:
            try:
                tone = float(row.V2Tone.split(',')[0])
            except:
                pass
        
        sentiment = SentimentEnum.POSITIVE if tone > 0.5 else (SentimentEnum.NEGATIVE if tone < -0.5 else SentimentEnum.NEUTRAL)
        
        # Simple category
        themes = (row.V2Themes or '').lower()
        if 'earn' in themes or 'profit' in themes:
            category = NewsCategoryEnum.EARNINGS
        elif 'legal' in themes or 'court' in themes:
            category = NewsCategoryEnum.LEGAL
        elif 'product' in themes or 'launch' in themes:
            category = NewsCategoryEnum.PRODUCT
        else:
            category = NewsCategoryEnum.GENERAL
        
        # Generate title
        domain = row.DocumentIdentifier.split('/')[2] if '//' in row.DocumentIdentifier else 'Unknown'
        title = f"News from {domain}: {', '.join(mentioned_companies)}"
        
        return {
            'title': title,
            'url': row.DocumentIdentifier,
            'source': row.SourceCommonName,
            'published_at': datetime.strptime(str(row.DATE), '%Y%m%d'),
            'content_snippet': '',
            'mentioned_companies': mentioned_companies,
            'sentiment': sentiment,
            'category': category
        }
    
    async def run_backfill(self, companies_data, start_date: str, end_date: str):
        """Run complete backfill with progress tracking"""
        
        if not self.available:
            logger.warning("GDELT not available, skipping backfill")
            return 0
        
        # Setup progress tracking
        progress.is_running = True
        progress.start_time = datetime.now()
        progress.current_date = start_date
        progress.articles_added = 0
        progress.errors = []
        
        logger.info(f"🚀 Starting GDELT backfill: {start_date} to {end_date}")
        
        try:
            # Process in 3-month chunks
            current_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Calculate total months for progress
            total_months = (end_dt.year - current_date.year) * 12 + (end_dt.month - current_date.month)
            months_processed = 0
            
            # Create company ID lookup
            company_lookup = {c['symbol']: c['id'] for c in companies_data}
            
            while current_date < end_dt:
                # 3-month batch
                batch_end = min(
                    current_date.replace(month=min(current_date.month + 3, 12)) if current_date.month + 3 <= 12 
                    else current_date.replace(year=current_date.year + 1, month=current_date.month + 3 - 12),
                    end_dt
                )
                
                batch_start_str = current_date.strftime('%Y-%m-%d')
                batch_end_str = batch_end.strftime('%Y-%m-%d')
                
                try:
                    # Get articles for this batch
                    articles = self.get_historical_articles(batch_start_str, batch_end_str, limit=1000)
                    
                    # Store in database
                    added = await self._store_articles(articles, company_lookup)
                    progress.articles_added += added
                    
                    # Update progress
                    months_processed += 3
                    progress.progress_percent = round((months_processed / total_months) * 100, 1)
                    progress.current_date = batch_start_str
                    
                    # Calculate ETA
                    if months_processed > 0:
                        elapsed = datetime.now() - progress.start_time
                        avg_time_per_month = elapsed / months_processed
                        remaining_months = total_months - months_processed
                        progress.eta = datetime.now() + (avg_time_per_month * remaining_months)
                    
                    logger.info(f"✅ Batch {batch_start_str}: {added} articles added, {progress.progress_percent}% complete")
                    
                except Exception as e:
                    error_msg = f"Batch {batch_start_str} failed: {str(e)}"
                    progress.errors.append(error_msg)
                    logger.error(error_msg)
                
                current_date = batch_end
            
            progress.is_running = False
            logger.info(f"🎉 GDELT backfill complete: {progress.articles_added} articles added")
            return progress.articles_added
            
        except Exception as e:
            progress.errors.append(f"Backfill failed: {str(e)}")
            progress.is_running = False
            logger.error(f"❌ GDELT backfill failed: {e}")
            return 0
    
    async def _store_articles(self, articles, company_lookup):
        """Store articles in database"""
        from app.database import SessionLocal
        from app.models import NewsItem, company_news_association, NewsSentiment
        
        if not articles:
            return 0
        
        db = SessionLocal()
        added = 0
        
        try:
            for article in articles:
                # Check if exists
                existing = db.query(NewsItem).filter(NewsItem.url == article['url']).first()
                if existing:
                    continue
                
                # Create news item
                news_item = NewsItem(
                    title=article['title'],
                    url=article['url'],
                    source=article['source'],
                    published_at=article['published_at'],
                    content_snippet=article['content_snippet'],
                    category=article['category']
                )
                db.add(news_item)
                db.flush()
                
                # Add company associations and sentiments
                for company_symbol in article['mentioned_companies']:
                    if company_symbol in company_lookup:
                        company_id = company_lookup[company_symbol]
                        
                        # Add association
                        db.execute(
                            company_news_association.insert().values(
                                company_id=company_id,
                                news_id=news_item.id
                            )
                        )
                        
                        # Add sentiment
                        sentiment = NewsSentiment(
                            news_id=news_item.id,
                            company_id=company_id,
                            sentiment=article['sentiment'],
                            score=0.7  # Default confidence
                        )
                        db.add(sentiment)
                
                added += 1
            
            db.commit()
            return added
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error storing articles: {e}")
            return 0
        finally:
            db.close()