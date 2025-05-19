import logging
from typing import Dict, List, Any, Tuple, Optional
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from app.models import NewsCategoryEnum, SentimentEnum

logger = logging.getLogger(__name__)

class NewsProcessor:
    """Processes news articles to extract insights using FinBERT for sentiment analysis"""
    
    # Simple keyword dictionaries for categories
    CATEGORY_KEYWORDS = {
        NewsCategoryEnum.EARNINGS: ["earnings", "revenue", "profit", "quarterly", "financial results", "reported earnings"],
        NewsCategoryEnum.EXECUTIVE: ["CEO", "executive", "appoint", "resign", "leadership", "board of directors"],
        NewsCategoryEnum.LEGAL: ["lawsuit", "legal", "court", "settlement", "sue", "judge", "regulation"],
        NewsCategoryEnum.PRODUCT: ["product", "launch", "release", "unveil", "announce", "new service"],
        NewsCategoryEnum.MARKET: ["market share", "competitor", "industry", "sector performance", "trend"],
    }
    
    def __init__(self):
        """Initialize the processor with FinBERT sentiment analysis model"""
        try:
            # Load FinBERT model and tokenizer
            self.model_name = "ProsusAI/finbert"
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            
            # FinBERT labels
            self.labels = ["positive", "negative", "neutral"]
            
            self.finbert_available = True
            logger.info("FinBERT model loaded successfully")
        except Exception as e:
            self.finbert_available = False
            logger.error(f"Failed to load FinBERT model: {str(e)}")
            logger.warning("Using fallback keyword-based sentiment analysis instead")
    
    def categorize_article(self, title: str, snippet: str) -> NewsCategoryEnum:
        """
        Determine the category of a news article based on content.
        
        Args:
            title: Article title
            snippet: Article content snippet
            
        Returns:
            Category enum value
        """
        # Combine title and snippet for analysis
        text = f"{title} {snippet}".lower()
        
        for category, keywords in self.CATEGORY_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in text:
                    return category
        
        # Default category if no specific keywords match
        return NewsCategoryEnum.GENERAL
    
    def analyze_sentiment_with_finbert(self, text: str) -> Tuple[SentimentEnum, float]:
        """
        Analyze sentiment using FinBERT model.
        
        Args:
            text: Text to analyze
            
        Returns:
            Tuple of (sentiment enum, confidence score)
        """
        # Tokenize input
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
        
        # Run inference
        with torch.no_grad():
            outputs = self.model(**inputs)
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
        
        # Get prediction and confidence
        predicted_class_id = predictions.argmax().item()
        confidence = predictions[0][predicted_class_id].item()
        
        # Map to our sentiment enum
        sentiment_label = self.labels[predicted_class_id]
        if sentiment_label == "positive":
            return SentimentEnum.POSITIVE, confidence
        elif sentiment_label == "negative":
            return SentimentEnum.NEGATIVE, confidence
        else:
            return SentimentEnum.NEUTRAL, confidence
    
    def analyze_sentiment_with_keywords(self, text: str) -> Tuple[SentimentEnum, float]:
        """
        Analyze sentiment using simple keyword matching (fallback method).
        
        Args:
            text: Text to analyze
            
        Returns:
            Tuple of (sentiment enum, confidence score)
        """
        # Simple sentiment keyword dictionaries as fallback
        SENTIMENT_KEYWORDS = {
            SentimentEnum.POSITIVE: [
                "gain", "rise", "up", "grow", "profit", "success", "positive", "exceed", "beat", 
                "surge", "climb", "rally", "advance", "improvement", "record high", "bullish"
            ],
            SentimentEnum.NEGATIVE: [
                "drop", "down", "fall", "loss", "decline", "miss", "negative", "fail", "weak", 
                "plunge", "tumble", "slide", "struggle", "crash", "bearish", "sink"
            ]
        }
        
        positive_count = sum(1 for word in SENTIMENT_KEYWORDS[SentimentEnum.POSITIVE] if word in text)
        negative_count = sum(1 for word in SENTIMENT_KEYWORDS[SentimentEnum.NEGATIVE] if word in text)
        
        if positive_count > negative_count:
            confidence = min(0.5 + (positive_count / (positive_count + negative_count + 1)) * 0.5, 1.0)
            return SentimentEnum.POSITIVE, confidence
        elif negative_count > positive_count:
            confidence = min(0.5 + (negative_count / (positive_count + negative_count + 1)) * 0.5, 1.0)
            return SentimentEnum.NEGATIVE, confidence
        else:
            return SentimentEnum.NEUTRAL, 0.6
    
    def analyze_sentiment(self, title: str, snippet: str, company_name: str) -> Tuple[SentimentEnum, float]:
        """
        Analyze sentiment of article text towards a specific company.
        
        Args:
            title: Article title
            snippet: Article content snippet
            company_name: Name of company to analyze sentiment for
            
        Returns:
            Tuple of (sentiment enum, confidence score)
        """
        # Check if company is mentioned (basic)
        text = f"{title} {snippet}".lower()
        company_lower = company_name.lower()
        
        if company_lower not in text:
            # If company not directly mentioned, use neutral with low confidence
            return SentimentEnum.NEUTRAL, 0.5
        
        # Use FinBERT if available
        if self.finbert_available:
            return self.analyze_sentiment_with_finbert(text)
        else:
            return self.analyze_sentiment_with_keywords(text)
    
    def identify_mentioned_companies(self, title: str, snippet: str, companies: List[Dict[str, Any]]) -> List[int]:
        """
        Identify which companies from a list are mentioned in the article.
        
        Args:
            title: Article title
            snippet: Article content snippet
            companies: List of company dicts with 'id', 'symbol', and 'name' keys
            
        Returns:
            List of company IDs mentioned in the article
        """
        text = f"{title} {snippet}".lower()
        mentioned_ids = []
        
        for company in companies:
            # Check for company name or symbol in text
            if (company['symbol'].lower() in text or 
                company['name'].lower() in text):
                mentioned_ids.append(company['id'])
        
        return mentioned_ids
    
    def process_article(self, article: Dict[str, Any], companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a single article to extract all insights.
        
        Args:
            article: News article dictionary
            companies: List of all companies to check for mentions
            
        Returns:
            Processed article with added metadata
        """
        processed = article.copy()
        
        # Categorize the article
        category = self.categorize_article(article['title'], article.get('content_snippet', ''))
        processed['category'] = category
        
        # Identify mentioned companies
        mentioned_company_ids = self.identify_mentioned_companies(
            article['title'], 
            article.get('content_snippet', ''),
            companies
        )
        processed['mentioned_company_ids'] = mentioned_company_ids
        
        # Analyze sentiment for each mentioned company
        sentiments = []
        for company_id in mentioned_company_ids:
            company = next((c for c in companies if c['id'] == company_id), None)
            if company:
                sentiment, score = self.analyze_sentiment(
                    article['title'],
                    article.get('content_snippet', ''),
                    company['name']
                )
                sentiments.append({
                    'company_id': company_id,
                    'sentiment': sentiment,
                    'score': score
                })
        
        processed['sentiments'] = sentiments
        return processed