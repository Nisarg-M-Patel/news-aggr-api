import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Any
import feedparser
from urllib.parse import quote
import re
import html
from bs4 import BeautifulSoup

from app.config import settings

logger = logging.getLogger(__name__)

class NewsCollector:
    """Collects news articles from Google News RSS feeds for S&P 500 companies"""
    
    def __init__(self):
        self.delay = settings.GOOGLE_NEWS_DELAY
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def clean_content_snippet(self, raw_content: str) -> str:
        """
        Clean content snippet by removing HTML, URLs, and formatting artifacts
        
        Args:
            raw_content: Raw content from RSS feed
            
        Returns:
            Cleaned text content
        """
        if not raw_content or not raw_content.strip():
            return ""
        
        # Step 1: Remove HTML tags using BeautifulSoup
        try:
            soup = BeautifulSoup(raw_content, 'html.parser')
            # Extract text content, removing all HTML
            clean_text = soup.get_text()
        except Exception:
            # Fallback: simple HTML tag removal
            clean_text = re.sub(r'<[^>]+>', '', raw_content)
        
        # Step 2: Decode HTML entities
        clean_text = html.unescape(clean_text)
        
        # Step 3: Remove URLs
        # Remove http/https URLs
        clean_text = re.sub(r'https?://[^\s<>"]+', '', clean_text)
        # Remove www URLs
        clean_text = re.sub(r'www\.[^\s<>"]+', '', clean_text)
        
        # Step 4: Remove common RSS/news artifacts
        # Remove "target=_blank" and similar attributes
        clean_text = re.sub(r'target\s*=\s*["\']_blank["\']', '', clean_text)
        # Remove font color tags remnants
        clean_text = re.sub(r'color\s*=\s*["\'][^"\']*["\']', '', clean_text)
        # Remove common Google News artifacts
        clean_text = re.sub(r'&nbsp;', ' ', clean_text)
        clean_text = re.sub(r'&#\d+;', ' ', clean_text)
        
        # Step 5: Remove source attributions at the end
        # Remove patterns like "- CNN", "TipRanks", etc. at the end
        clean_text = re.sub(r'\s*[-–—]\s*[A-Z][a-zA-Z\s]+$', '', clean_text)
        clean_text = re.sub(r'\s*\([A-Z][A-Za-z\s]+\)$', '', clean_text)
        
        # Step 6: Clean up whitespace
        # Replace multiple spaces with single space
        clean_text = re.sub(r'\s+', ' ', clean_text)
        # Remove leading/trailing whitespace
        clean_text = clean_text.strip()
        
        # Step 7: Remove if it's just the title repeated or too short
        words = clean_text.split()
        if len(words) < 5:  # Too short to be meaningful content
            return ""
        
        return clean_text

    def extract_meaningful_content(self, title: str, raw_content: str) -> str:
        """
        Extract meaningful content, avoiding title repetition
        
        Args:
            title: Article title
            raw_content: Raw content snippet
            
        Returns:
            Meaningful content or empty string if no good content
        """
        cleaned = self.clean_content_snippet(raw_content)
        
        if not cleaned:
            return ""
        
        # Check if cleaned content is just the title repeated
        title_words = set(title.lower().split())
        content_words = set(cleaned.lower().split())
        
        # If content is mostly just the title words, it's not useful
        if len(title_words) > 0:
            overlap = len(title_words.intersection(content_words)) / len(title_words)
            if overlap > 0.8 and len(cleaned.split()) < 10:
                logger.debug(f"Content is just title repetition: {cleaned}")
                return ""  # Content is just title repetition
        
        return cleaned

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
                
                # Extract and clean content snippet
                raw_content = ""
                if hasattr(entry, 'summary'):
                    raw_content = entry.summary
                
                # Clean the content snippet
                cleaned_content = self.extract_meaningful_content(title, raw_content)
                
                # Log content cleaning for debugging
                if raw_content and not cleaned_content:
                    logger.debug(f"Filtered out poor content for '{title[:50]}...': {raw_content[:100]}...")
                elif cleaned_content:
                    logger.debug(f"Good content for '{title[:30]}...': {cleaned_content[:50]}...")
                
                articles.append({
                    "title": title,
                    "url": entry.link,
                    "source": source,
                    "published_at": published_at,
                    "content_snippet": cleaned_content,  # Now cleaned!
                    "company_symbol": company_symbol,
                })
            
            logger.info(f"Collected {len(articles)} articles for {company_symbol}")
            
            # Log content quality statistics
            articles_with_content = sum(1 for a in articles if a['content_snippet'])
            logger.info(f"Content quality: {articles_with_content}/{len(articles)} articles have meaningful content")
            
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
        
        # Overall content quality statistics
        total_articles = len(all_articles)
        articles_with_content = sum(1 for a in all_articles if a['content_snippet'])
        logger.info(f"Overall content quality: {articles_with_content}/{total_articles} ({articles_with_content/total_articles:.1%}) articles have meaningful content")
        
        return all_articles