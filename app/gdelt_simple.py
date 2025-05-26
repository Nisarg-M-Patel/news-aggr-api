from google.cloud import bigquery
from google.auth import default
import pandas as pd
from datetime import datetime, timedelta
import logging
import re
from typing import List, Dict, Any, Optional
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
        self.total_processed = 0
        self.duplicates_skipped = 0
    
    def to_dict(self):
        elapsed = str(datetime.now() - self.start_time).split('.')[0] if self.start_time else "0:00:00"
        return {
            "status": "running" if self.is_running else ("completed" if self.start_time else "not_started"),
            "progress_percent": self.progress_percent,
            "current_date": self.current_date,
            "articles_added": self.articles_added,
            "total_processed": self.total_processed,
            "duplicates_skipped": self.duplicates_skipped,
            "elapsed_time": elapsed,
            "estimated_completion": self.eta.strftime('%Y-%m-%d %H:%M:%S') if self.eta else None,
            "errors": self.errors[-5:] if self.errors else []  # Last 5 errors
        }

# Global progress tracker
progress = BackfillProgress()

class SimpleGDELTCollector:
    """
    FIXED GDELT collector with proper SQL queries and company matching
    
    Key fixes:
    1. Use correct GDELT BigQuery schema and field names
    2. Proper partitioned table usage for cost efficiency
    3. Better company name matching
    4. Realistic sentiment and category extraction
    5. Proper error handling and progress tracking
    """
    
    def __init__(self):
        self.client = None
        self.available = False
        self._setup_bigquery()
        
        # Better company mappings with multiple name variations
        self.companies = {
            'AAPL': ['Apple Inc', 'Apple', 'Apple Computer', 'Apple Corp'],
            'MSFT': ['Microsoft Corp', 'Microsoft', 'Microsoft Corporation'],
            'AMZN': ['Amazon.com Inc', 'Amazon', 'Amazon.com', 'Amazon Web Services', 'AWS'],
            'GOOGL': ['Alphabet Inc', 'Google', 'Alphabet', 'Google Inc', 'Alphabet Class A'],
            'NVDA': ['NVIDIA Corp', 'Nvidia', 'NVIDIA', 'NVIDIA Corporation']
        }
        
        # Create reverse lookup for faster matching
        self._build_company_lookup()
    
    def _setup_bigquery(self):
        """Setup BigQuery client with proper error handling"""
        try:
            # Try default credentials first
            credentials, project = default()
            self.client = bigquery.Client(credentials=credentials, project=project)
            
            # Test connection with a minimal query
            test_query = "SELECT 1 as test LIMIT 1"
            self.client.query(test_query).result()
            
            self.available = True
            logger.info("✅ GDELT BigQuery connected successfully")
            
        except Exception as e:
            logger.warning(f"⚠️ GDELT BigQuery not available: {e}")
            logger.info("💡 To enable GDELT:")
            logger.info("   1. Set up Google Cloud credentials")
            logger.info("   2. Enable BigQuery API")
            logger.info("   3. Set GOOGLE_APPLICATION_CREDENTIALS environment variable")
            self.available = False
    
    def _build_company_lookup(self):
        """Build efficient company name lookup"""
        self.company_lookup = {}
        for symbol, names in self.companies.items():
            for name in names:
                # Store both exact and normalized versions
                self.company_lookup[name.lower()] = symbol
                # Also store without common suffixes
                normalized = re.sub(r'\s+(inc\.?|corp\.?|corporation|company|class\s+[a-z]).*$', 
                                  '', name.lower(), flags=re.IGNORECASE).strip()
                if normalized != name.lower():
                    self.company_lookup[normalized] = symbol
    
    def is_available(self):
        return self.available
    
    def test_connection(self):
        """Test GDELT connection and show sample data"""
        if not self.available:
            return {"error": "GDELT not available"}
        
        try:
            # Test query on partitioned GKG table
            test_query = """
            SELECT DATE, SourceCommonName, V2Organizations, V2Themes, V2Tone
            FROM `gdelt-bq.gdeltv2.gkg_partitioned`
            WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            AND V2Organizations IS NOT NULL
            LIMIT 5
            """
            
            results = self.client.query(test_query).result()
            sample_data = []
            
            for row in results:
                sample_data.append({
                    'date': str(row.DATE),
                    'source': row.SourceCommonName,
                    'organizations': row.V2Organizations[:100] + '...' if row.V2Organizations else None,
                    'themes': row.V2Themes[:100] + '...' if row.V2Themes else None,
                    'tone': row.V2Tone
                })
            
            return {
                "status": "connected",
                "sample_data": sample_data,
                "message": "GDELT BigQuery connection working"
            }
            
        except Exception as e:
            logger.error(f"GDELT connection test failed: {e}")
            return {"error": f"Connection test failed: {str(e)}"}
    
    def get_historical_articles(self, start_date: str, end_date: str, limit: int = 2000):
        """
        Get historical articles from GDELT with FIXED SQL query
        
        Uses proper GDELT BigQuery schema and partitioned tables
        """
        if not self.available:
            return []
        
        try:
            # Convert dates to GDELT format (YYYYMMDDHHMMSS)
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Build proper company organization filter
            org_conditions = []
            for symbol, names in self.companies.items():
                for name in names:
                    # Use proper LIKE syntax for BigQuery
                    org_conditions.append(f"V2Organizations LIKE '%{name}%'")
            
            org_filter = "(" + " OR ".join(org_conditions) + ")"
            
            # FIXED SQL query using correct GDELT schema
            query = f"""
            SELECT 
                DATE,
                SourceCommonName,
                DocumentIdentifier,
                V2Organizations,
                V2Themes,
                V2Tone,
                V2Locations
            FROM `gdelt-bq.gdeltv2.gkg_partitioned`
            WHERE 
                _PARTITIONTIME >= TIMESTAMP("{start_date}")
                AND _PARTITIONTIME < TIMESTAMP("{end_date}")
                AND {org_filter}
                AND SourceCommonName IS NOT NULL
                AND DocumentIdentifier IS NOT NULL
                AND V2Organizations IS NOT NULL
            ORDER BY DATE DESC
            LIMIT {limit}
            """
            
            logger.info(f"🔍 Querying GDELT GKG: {start_date} to {end_date}")
            logger.debug(f"Query: {query[:200]}...")
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Process results
            articles = []
            for row in results:
                processed = self._process_gkg_row(row)
                if processed:
                    articles.append(processed)
            
            logger.info(f"✅ Found {len(articles)} GDELT articles")
            return articles
            
        except Exception as e:
            logger.error(f"❌ GDELT query failed: {e}")
            logger.error(f"Query was: {query[:500]}...")
            return []
    
    def _process_gkg_row(self, row):
        """
        Process GDELT GKG row with proper field parsing
        
        GDELT GKG fields use specific delimiters:
        - V2Organizations: semicolon-separated, each with type#name#offset format
        - V2Themes: semicolon-separated theme codes
        - V2Tone: comma-separated values (tone,positive,negative,polarity,etc)
        """
        try:
            # Extract mentioned companies using improved matching
            mentioned_companies = self._extract_mentioned_companies(row.V2Organizations)
            
            if not mentioned_companies:
                return None
            
            # Extract sentiment from V2Tone field
            sentiment = self._extract_sentiment(row.V2Tone)
            
            # Extract category from themes
            category = self._extract_category(row.V2Themes)
            
            # Generate meaningful title
            title = self._generate_title(row, mentioned_companies)
            
            # Extract date
            published_at = self._extract_date(row.DATE)
            
            return {
                'title': title,
                'url': row.DocumentIdentifier,
                'source': row.SourceCommonName or 'Unknown',
                'published_at': published_at,
                'content_snippet': '',  # GKG doesn't contain article content
                'mentioned_companies': mentioned_companies,
                'sentiment': sentiment,
                'category': category,
                'gdelt_tone': row.V2Tone,  # Store raw tone for debugging
                'gdelt_themes': row.V2Themes[:200] if row.V2Themes else None  # Truncate for storage
            }
            
        except Exception as e:
            logger.warning(f"Error processing GDELT row: {e}")
            return None
    
    def _extract_mentioned_companies(self, v2_organizations: str) -> List[str]:
        """
        Extract companies from V2Organizations field
        
        V2Organizations format: TYPE#NAME#OFFSET;TYPE#NAME#OFFSET;...
        """
        if not v2_organizations:
            return []
        
        mentioned = []
        
        try:
            # Split by semicolon to get individual organization mentions
            org_mentions = v2_organizations.split(';')
            
            for mention in org_mentions:
                if '#' in mention:
                    # Extract organization name (second field)
                    parts = mention.split('#')
                    if len(parts) >= 2:
                        org_name = parts[1].strip()
                        
                        # Match against our company lookup
                        symbol = self._match_company_name(org_name)
                        if symbol and symbol not in mentioned:
                            mentioned.append(symbol)
                else:
                    # Fallback: treat whole string as organization name
                    symbol = self._match_company_name(mention.strip())
                    if symbol and symbol not in mentioned:
                        mentioned.append(symbol)
        
        except Exception as e:
            logger.debug(f"Error parsing organizations '{v2_organizations}': {e}")
        
        return mentioned
    
    def _match_company_name(self, org_name: str) -> Optional[str]:
        """Match organization name to company symbol"""
        if not org_name:
            return None
        
        org_lower = org_name.lower().strip()
        
        # Direct lookup
        if org_lower in self.company_lookup:
            return self.company_lookup[org_lower]
        
        # Fuzzy matching for partial matches
        for name, symbol in self.company_lookup.items():
            if name in org_lower or org_lower in name:
                return symbol
        
        return None
    
    def _extract_sentiment(self, v2_tone: str) -> SentimentEnum:
        """
        Extract sentiment from V2Tone field
        
        V2Tone format: tone,positive_score,negative_score,polarity,activity_density,word_count
        """
        if not v2_tone:
            return SentimentEnum.NEUTRAL
        
        try:
            # Parse first value (overall tone)
            tone_parts = v2_tone.split(',')
            if tone_parts:
                tone = float(tone_parts[0])
                
                # GDELT tone ranges from -100 (most negative) to +100 (most positive)
                if tone > 1.0:  # Positive threshold
                    return SentimentEnum.POSITIVE
                elif tone < -1.0:  # Negative threshold
                    return SentimentEnum.NEGATIVE
                else:
                    return SentimentEnum.NEUTRAL
                    
        except (ValueError, IndexError):
            pass
        
        return SentimentEnum.NEUTRAL
    
    def _extract_category(self, v2_themes: str) -> NewsCategoryEnum:
        """
        Extract category from V2Themes field
        
        V2Themes contains semicolon-separated theme codes
        """
        if not v2_themes:
            return NewsCategoryEnum.GENERAL
        
        themes_lower = v2_themes.lower()
        
        # Map GDELT themes to our categories
        if any(keyword in themes_lower for keyword in ['earn', 'profit', 'revenue', 'financial']):
            return NewsCategoryEnum.EARNINGS
        elif any(keyword in themes_lower for keyword in ['legal', 'court', 'lawsuit', 'regulation']):
            return NewsCategoryEnum.LEGAL
        elif any(keyword in themes_lower for keyword in ['product', 'launch', 'technology']):
            return NewsCategoryEnum.PRODUCT
        elif any(keyword in themes_lower for keyword in ['market', 'stock', 'trading']):
            return NewsCategoryEnum.MARKET
        elif any(keyword in themes_lower for keyword in ['executive', 'ceo', 'leadership']):
            return NewsCategoryEnum.EXECUTIVE
        else:
            return NewsCategoryEnum.GENERAL
    
    def _generate_title(self, row, mentioned_companies: List[str]) -> str:
        """Generate meaningful title from GDELT data"""
        try:
            # Extract domain from URL for source context
            url = row.DocumentIdentifier
            domain = 'Unknown'
            if '//' in url:
                domain = url.split('//')[1].split('/')[0]
                # Clean up domain
                if domain.startswith('www.'):
                    domain = domain[4:]
            
            # Create title
            companies_str = ', '.join(mentioned_companies)
            source = row.SourceCommonName or domain
            
            return f"{companies_str} mentioned in {source} article"
            
        except:
            return f"News article mentioning {', '.join(mentioned_companies)}"
    
    def _extract_date(self, gdelt_date) -> datetime:
        """Extract datetime from GDELT DATE field"""
        try:
            # GDELT DATE format: YYYYMMDDHHMMSS
            date_str = str(gdelt_date)
            if len(date_str) >= 8:
                return datetime.strptime(date_str[:8], '%Y%m%d')
            else:
                return datetime.now()
        except:
            return datetime.now()
    
    async def run_backfill(self, companies_data, start_date: str, end_date: str):
        """
        Run GDELT backfill with proper progress tracking and error handling
        """
        if not self.available:
            logger.warning("GDELT not available, skipping backfill")
            return 0
        
        # Setup progress tracking
        progress.is_running = True
        progress.start_time = datetime.now()
        progress.current_date = start_date
        progress.articles_added = 0
        progress.total_processed = 0
        progress.duplicates_skipped = 0
        progress.errors = []
        
        logger.info(f"🚀 Starting GDELT backfill: {start_date} to {end_date}")
        
        try:
            # Process in weekly chunks (more manageable than 3-month)
            current_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Calculate total weeks for progress
            total_days = (end_dt - current_date).days
            total_weeks = max(1, total_days // 7)
            weeks_processed = 0
            
            # Create company ID lookup
            company_lookup = {c['symbol']: c['id'] for c in companies_data}
            
            while current_date < end_dt:
                # Process one week at a time
                week_end = min(current_date + timedelta(days=7), end_dt)
                
                batch_start_str = current_date.strftime('%Y-%m-%d')
                batch_end_str = week_end.strftime('%Y-%m-%d')
                
                try:
                    logger.info(f"📅 Processing week: {batch_start_str} to {batch_end_str}")
                    
                    # Get articles for this week
                    articles = self.get_historical_articles(batch_start_str, batch_end_str, limit=1000)
                    progress.total_processed += len(articles)
                    
                    # Store in database
                    added = await self._store_articles(articles, company_lookup)
                    progress.articles_added += added
                    progress.duplicates_skipped += len(articles) - added
                    
                    # Update progress
                    weeks_processed += 1
                    progress.progress_percent = round((weeks_processed / total_weeks) * 100, 1)
                    progress.current_date = batch_start_str
                    
                    # Calculate ETA
                    if weeks_processed > 0:
                        elapsed = datetime.now() - progress.start_time
                        avg_time_per_week = elapsed / weeks_processed
                        remaining_weeks = total_weeks - weeks_processed
                        progress.eta = datetime.now() + (avg_time_per_week * remaining_weeks)
                    
                    logger.info(f"✅ Week {batch_start_str}: {added} new articles, {len(articles) - added} duplicates, {progress.progress_percent}% complete")
                    
                except Exception as e:
                    error_msg = f"Week {batch_start_str} failed: {str(e)}"
                    progress.errors.append(error_msg)
                    logger.error(error_msg)
                
                current_date = week_end
            
            progress.is_running = False
            logger.info(f"🎉 GDELT backfill complete!")
            logger.info(f"📊 Total: {progress.articles_added} articles added, {progress.duplicates_skipped} duplicates skipped")
            return progress.articles_added
            
        except Exception as e:
            progress.errors.append(f"Backfill failed: {str(e)}")
            progress.is_running = False
            logger.error(f"❌ GDELT backfill failed: {e}")
            return 0
    
    async def _store_articles(self, articles, company_lookup):
        """Store articles in database with proper error handling"""
        from app.database import SessionLocal
        from app.models import NewsItem, company_news_association, NewsSentiment
        
        if not articles:
            return 0
        
        db = SessionLocal()
        added = 0
        
        try:
            for article in articles:
                try:
                    # Check if exists (by URL)
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
                                score=0.7  # Default confidence for GDELT data
                            )
                            db.add(sentiment)
                    
                    added += 1
                    
                except Exception as e:
                    logger.warning(f"Error storing individual article: {e}")
                    continue
            
            db.commit()
            return added
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error storing article batch: {e}")
            return 0
        finally:
            db.close()
    
    def get_stats(self):
        """Get collector statistics"""
        return {
            'available': self.available,
            'companies_tracked': len(self.companies),
            'company_names_mapped': len(self.company_lookup),
            'uses_partitioned_tables': True,
            'weekly_batch_processing': True
        }