import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Any
import feedparser
from urllib.parse import quote

from app.config import settings

logger = logging.getLogger(__name__)

class NewsCollector:
    """Collects news articles from Google News RSS feeds for S&P 500 companies"""
    
    def __init__(self):
        self.delay = settings.GOOGLE_NEWS_DELAY
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    async def collect_for_company(self, company_symbol: str, company_name: str, days: int = 1) -> List[Dict[str, Any]]:
        """
        Collect news for a specific company using Google News RSS feeds.
        
        Args:
            company_symbol: Stock ticker symbol
            company_name: Company name
            days: Number of days to look back
            
        Returns:
            List of news article dictionaries
        """
        # Create a search query with both symbol and name
        query = f"{company_symbol} {company_name}"
        
        # Add time constraint if days specified
        if days > 0:
            query = f"{query} when:{days}d"
        
        # Use the RSS feed URL for Google News search
        url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"
        
        logger.info(f"Fetching news for {company_symbol} from RSS feed: {url}")
        
        try:
            # Parse the RSS feed using feedparser
            feed = feedparser.parse(url)
            
            # Process the feed entries
            articles = []
            for entry in feed.entries:
                # Extract published date
                published_at = datetime.now()
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    published_at = datetime.fromtimestamp(
                        datetime(*entry.published_parsed[:6]).timestamp()
                    )
                
                # Extract source from title (Google News format: 'Title - Source')
                title_parts = entry.title.split(' - ')
                source = title_parts[-1] if len(title_parts) > 1 else "Unknown"
                title = ' - '.join(title_parts[:-1]) if len(title_parts) > 1 else entry.title
                
                # Extract content snippet
                content_snippet = ""
                if hasattr(entry, 'summary'):
                    content_snippet = entry.summary
                
                articles.append({
                    "title": title,
                    "url": entry.link,
                    "source": source,
                    "published_at": published_at,
                    "content_snippet": content_snippet,
                    "company_symbol": company_symbol,
                })
            
            logger.info(f"Collected {len(articles)} articles for {company_symbol}")
            return articles
            
        except Exception as e:
            logger.error(f"Error collecting news for {company_symbol}: {str(e)}")
            return []
    
    async def collect_for_companies(self, companies: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Collect news for multiple companies with rate limiting.
        
        Args:
            companies: List of company dicts with 'symbol' and 'name' keys
            
        Returns:
            List of news article dictionaries
        """
        all_articles = []
        
        for company in companies:
            # Respect rate limits
            await asyncio.sleep(self.delay)
            
            try:
                articles = await self.collect_for_company(
                    company["symbol"], 
                    company["name"]
                )
                all_articles.extend(articles)
                logger.info(f"Collected {len(articles)} articles for {company['symbol']}")
            except Exception as e:
                logger.error(f"Failed to collect news for {company['symbol']}: {str(e)}")
        
        return all_articles