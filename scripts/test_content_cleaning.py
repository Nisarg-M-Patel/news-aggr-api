#!/usr/bin/env python3
"""
Test script to verify content cleaning is working
Run this to see before/after content cleaning
"""

import sys
from pathlib import Path

# Add the parent directory to the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import SessionLocal
from app.models import NewsItem
import re
import html
from bs4 import BeautifulSoup

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

def test_current_content():
    """Test content cleaning on current database articles"""
    
    db = SessionLocal()
    
    try:
        # Get some articles with content snippets
        articles = db.query(NewsItem).filter(
            NewsItem.content_snippet.isnot(None),
            NewsItem.content_snippet != ''
        ).limit(10).all()
        
        print("🧪 Testing Content Cleaning on Current Articles")
        print("=" * 60)
        
        for i, article in enumerate(articles, 1):
            print(f"\n📰 Article {i}: {article.title[:60]}...")
            print(f"🏢 Company: {[c.symbol for c in article.companies]}")
            
            print(f"\n📄 BEFORE (raw):")
            print(f"'{article.content_snippet[:200]}...'")
            
            cleaned = clean_content_snippet(article.content_snippet)
            print(f"\n✨ AFTER (cleaned):")
            if cleaned:
                print(f"'{cleaned[:200]}...'")
                print(f"✅ Content length: {len(cleaned)} chars, {len(cleaned.split())} words")
            else:
                print("❌ Content filtered out (HTML/URL only)")
            
            print("-" * 60)
        
        # Statistics
        total_with_content = db.query(NewsItem).filter(
            NewsItem.content_snippet.isnot(None),
            NewsItem.content_snippet != ''
        ).count()
        
        print(f"\n📊 Content Statistics:")
        print(f"Articles with content snippets: {total_with_content}")
        
        # Test cleaning on all content
        cleaned_count = 0
        filtered_count = 0
        
        for article in db.query(NewsItem).filter(
            NewsItem.content_snippet.isnot(None),
            NewsItem.content_snippet != ''
        ).all():
            cleaned = clean_content_snippet(article.content_snippet)
            if cleaned:
                cleaned_count += 1
            else:
                filtered_count += 1
        
        print(f"Would keep after cleaning: {cleaned_count}")
        print(f"Would filter out: {filtered_count}")
        print(f"Quality improvement: {filtered_count/total_with_content:.1%} of poor content removed")
        
    finally:
        db.close()

def test_specific_examples():
    """Test cleaning on specific problematic examples"""
    
    print("\n🧪 Testing Specific Problem Cases")
    print("=" * 60)
    
    test_cases = [
        # Your example
        ('<a href="https://news.google.com/rss/articles/CBMikgFBVV85cUxPZ24zYUN6dUxodnkzMExKUVJBMURBSEh5RzIwSlVhSXo5RG91ck0xdTBDRDlURzA2Y2dILUdOS2Z3ejN4V3hOVVgtSkVXbGc2dWYyOU5PYmptcm82XzFBbEtxQTVENzdVWm5IR0RLZmVEbFhxdDFTLWFZNkhNTmcyeEpIdkx6LUVjQnFsQTByNWRPZw?oc=5" target="_blank">Alphabet Class A: A Hidden Gem in AI Growth?</a>&nbsp;&nbsp;<font color="#6f6f6f">TipRanks</font>',
         "HTML link with title"),
        
        # Other common cases
        ('Apple reports strong Q4 earnings, beating analyst expectations with iPhone sales growth of 15% year-over-year.',
         "Good content"),
        
        ('<p>Microsoft Azure revenue grows</p><br/><a href="http://example.com">Read more</a> - TechCrunch',
         "HTML with good content"),
        
        ('&nbsp;&nbsp;Google unveils AI features&nbsp;&nbsp;',
         "Content with HTML entities"),
        
        ('https://news.google.com/article/123 - CNN',
         "Just URL and source"),
    ]
    
    for i, (raw_content, description) in enumerate(test_cases, 1):
        print(f"\n📝 Test Case {i}: {description}")
        print(f"Raw: {raw_content}")
        
        cleaned = clean_content_snippet(raw_content)
        if cleaned:
            print(f"✅ Cleaned: '{cleaned}'")
        else:
            print(f"❌ Filtered out (no meaningful content)")

def main():
    """Main test function"""
    
    print("🧹 Content Cleaning Test Suite")
    
    # Test specific examples
    test_specific_examples()
    
    # Test current database content
    test_current_content()
    
    print(f"\n🎯 Next Steps:")
    print("1. Update collector.py with the cleaning functions")
    print("2. Restart API to get clean content for new articles")
    print("3. Optionally reprocess existing articles with clean content")

if __name__ == "__main__":
    main()