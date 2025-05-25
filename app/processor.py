import logging
import re
from typing import Dict, List, Any, Optional
from app.models import NewsCategoryEnum
from app.config import settings

logger = logging.getLogger(__name__)

class NewsProcessor:
    """Simplified news processor with optional ML"""
    
    # Category keywords
    CATEGORY_KEYWORDS = {
        NewsCategoryEnum.EARNINGS: [
            "earnings", "revenue", "profit", "quarterly", "financial results", 
            "q1", "q2", "q3", "q4", "fiscal", "eps"
        ],
        NewsCategoryEnum.EXECUTIVE: [
            "ceo", "executive", "appoint", "resign", "leadership", 
            "chief executive", "president", "chairman"
        ],
        NewsCategoryEnum.LEGAL: [
            "lawsuit", "legal", "court", "settlement", "regulation",
            "sec", "investigation", "fine"
        ],
        NewsCategoryEnum.PRODUCT: [
            "product", "launch", "release", "unveil", "announce",
            "update", "version", "feature"
        ],
        NewsCategoryEnum.MARKET: [
            "market share", "stock price", "shares", "trading", "analyst"
        ],
    }
    
    def __init__(self):
        """Initialize processor with optional ML classifier"""
        self.classifier = None
        self.classifier_ready = False
        
        if settings.ENABLE_ML_CLASSIFICATION:
            try:
                from app.hybrid_classifier import HybridMLCompanyClassifier
                self.classifier = HybridMLCompanyClassifier()
                logger.info("✅ ML classifier initialized")
            except Exception as e:
                logger.warning(f"⚠️ ML classifier failed to load: {e}")
                logger.info("📝 Falling back to regex-only classification")
    
    def update_company_knowledge(self, companies: List[Dict[str, Any]]):
        """Update classifier with company data"""
        if self.classifier:
            try:
                self.classifier.update_companies(companies)
                self.classifier_ready = True
                logger.info(f"🔄 Classifier updated with {len(companies)} companies")
            except Exception as e:
                logger.error(f"❌ Failed to update classifier: {e}")
    
    def categorize_article(self, title: str, snippet: str) -> NewsCategoryEnum:
        """Simple keyword-based categorization"""
        text = f"{title} {snippet}".lower()
        
        for category, keywords in self.CATEGORY_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text:
                    return category
        
        return NewsCategoryEnum.GENERAL
    
    def identify_relevant_companies_simple(self, title: str, snippet: str, companies: List[Dict]) -> List[int]:
        """Simple regex-based company identification"""
        text = f"{title} {snippet}".lower()
        mentioned_companies = []
        
        for company in companies:
            symbol = company['symbol'].lower()
            name = company['name'].lower()
            
            # Simple pattern matching
            if symbol in text or name in text:
                # Avoid false positives for common words
                if symbol in ['a', 't', 'f'] and len(symbol) == 1:
                    # More specific check for single-letter symbols
                    pattern = rf'\b{re.escape(symbol.upper())}\b'
                    if re.search(pattern, f"{title} {snippet}"):
                        mentioned_companies.append(company['id'])
                else:
                    mentioned_companies.append(company['id'])
        
        return mentioned_companies
    
    def identify_relevant_companies_ml(self, title: str, snippet: str) -> List[Dict]:
        """ML-based company identification"""
        if not self.classifier_ready:
            return []
        
        try:
            return self.classifier.classify_article(
                title=title,
                content=snippet,
                min_relevance=settings.ML_RELEVANCE_THRESHOLD
            )
        except Exception as e:
            logger.error(f"❌ ML classification failed: {e}")
            return []
    
    def process_article(self, article: Dict[str, Any], companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a single article"""
        processed = article.copy()
        
        # Categorize
        category = self.categorize_article(
            article['title'], 
            article.get('content_snippet', '')
        )
        processed['category'] = category
        
        # Company identification
        if self.classifier_ready and settings.ENABLE_ML_CLASSIFICATION:
            # Use ML classifier
            relevant_companies = self.identify_relevant_companies_ml(
                article['title'],
                article.get('content_snippet', '')
            )
            processed['mentioned_company_ids'] = [rc['company_id'] for rc in relevant_companies]
            
            # Store ML scores for debugging
            processed['company_ml_scores'] = {
                rc['company_id']: {
                    'relevance_score': rc['relevance_score'],
                    'confidence': rc['confidence']
                } for rc in relevant_companies
            }
        else:
            # Fallback to simple regex
            mentioned_company_ids = self.identify_relevant_companies_simple(
                article['title'],
                article.get('content_snippet', ''),
                companies
            )
            processed['mentioned_company_ids'] = mentioned_company_ids
            processed['company_ml_scores'] = {}
        
        # No sentiment analysis for now
        processed['sentiments'] = []
        
        return processed
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processor statistics"""
        return {
            'ml_enabled': settings.ENABLE_ML_CLASSIFICATION,
            'classifier_ready': self.classifier_ready,
            'classifier_type': 'hybrid_ml' if self.classifier_ready else 'regex_only'
        }