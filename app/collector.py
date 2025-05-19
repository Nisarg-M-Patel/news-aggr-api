import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import aiohttp
from bs4 import BeautifulSoup
import re
from urllib.parse import quote

from app.config import settings

logger = logging.getLogger(__name__)

class NewsCollector:
    """Collects news articles from Google News for S&P 500 companies"""
    
    def __init__(self):
        self.delay = settings.GOOGLE_NEWS_DELAY
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    async def collect_for_company(self, company_symbol: str, company_name: str, days: int = 1) -> List[Dict[str, Any]]:
        """
        Collect news for a specific company.
        
        Args:
            company_symbol: Stock ticker symbol
            company_name: Company name
            days: Number of days to look back
            
        Returns:
            List of news article dictionaries
        """
        # Use both symbol and name for better search results
        query = f"{company_symbol} {company_name} stock"
        time_range = f"when:{days}d"
        
        url = f"https://news.google.com/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en&{time_range}"
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch news for {company_symbol}: {response.status}")
                    return []
                
                html = await response.text()
                
                # Parse the HTML
                return self._parse_google_news(html, company_symbol)
    
    def _parse_google_news(self, html: str, company_symbol: str) -> List[Dict[str, Any]]:
        """Parse Google News HTML to extract article information"""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        
        # Find all article elements
        for article_tag in soup.select('article'):
            try:
                # Extract title and URL
                title_tag = article_tag.select_one('h3 a')
                if not title_tag:
                    continue
                
                title = title_tag.text
                # Google News uses relative URLs
                url_path = title_tag.get('href', '')
                if not url_path:
                    continue
                url = f"https://news.google.com{url_path.replace('./', '/')}"
                
                # Extract source and time
                info_tag = article_tag.select_one('time')
                published_at = datetime.now()
                if info_tag and info_tag.get('datetime'):
                    try:
                        published_at = datetime.fromisoformat(info_tag.get('datetime', ''))
                    except ValueError:
                        pass
                
                source_tag = article_tag.select_one('div[data-n-tid]')
                source = source_tag.text if source_tag else "Unknown"
                
                # Extract snippet if available
                snippet_tag = article_tag.select_one('p[data-n-hl]')
                content_snippet = snippet_tag.text if snippet_tag else ""
                
                articles.append({
                    "title": title,
                    "url": url,
                    "source": source,
                    "published_at": published_at,
                    "content_snippet": content_snippet,
                    "company_symbol": company_symbol,
                })
            except Exception as e:
                logger.error(f"Error parsing article for {company_symbol}: {str(e)}")
                continue
        
        return articles
    
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