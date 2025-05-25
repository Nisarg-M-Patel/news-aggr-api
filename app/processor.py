import logging
from typing import Dict, List, Any
from app.models import NewsCategoryEnum

logger = logging.getLogger(__name__)

class NewsProcessor:
    """Enhanced news processor with hybrid ML company classification"""
    
    # Simple category keywords
    CATEGORY_KEYWORDS = {
        NewsCategoryEnum.EARNINGS: [
            "earnings", "revenue", "profit", "quarterly", "financial results", 
            "reported earnings", "q1", "q2", "q3", "q4", "fiscal", "eps"
        ],
        NewsCategoryEnum.EXECUTIVE: [
            "ceo", "executive", "appoint", "resign", "leadership", "board of directors",
            "chief executive", "president", "chairman", "cfo", "cto"
        ],
        NewsCategoryEnum.LEGAL: [
            "lawsuit", "legal", "court", "settlement", "sue", "judge", "regulation",
            "sec", "ftc", "antitrust", "investigation", "fine", "penalty"
        ],
        NewsCategoryEnum.PRODUCT: [
            "product", "launch", "release", "unveil", "announce", "new service",
            "update", "version", "feature", "beta", "rollout"
        ],
        NewsCategoryEnum.MARKET: [
            "market share", "competitor", "industry", "sector performance", "trend",
            "market cap", "stock price", "shares", "trading", "analyst"
        ],
    }
    
    def __init__(self):
        """Initialize with hybrid ML classifier"""
        try:
            from app.hybrid_classifier import HybridMLCompanyClassifier
            self.company_classifier = HybridMLCompanyClassifier()
            self.classifier_ready = False
            logger.info("✅ Hybrid ML classifier initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize hybrid classifier: {e}")
            self.company_classifier = None
            self.classifier_ready = False
    
    def update_company_knowledge(self, companies: List[Dict[str, Any]]):
        """Update classifier with company data"""
        if self.company_classifier:
            logger.info(f"🔄 Updating classifier with {len(companies)} companies...")
            self.company_classifier.update_companies(companies)
            self.classifier_ready = True
            
            # Log classifier stats
            stats = self.company_classifier.get_stats()
            logger.info(f"📊 Classifier stats: {stats}")
        else:
            logger.warning("⚠️ No classifier available for update")
    
    def categorize_article(self, title: str, snippet: str) -> NewsCategoryEnum:
        """Fast category classification using simple keywords"""
        text = f"{title} {snippet}".lower()
        
        # Check categories in order of specificity
        for category, keywords in self.CATEGORY_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text:
                    logger.debug(f"📂 Categorized as {category.value} (keyword: {keyword})")
                    return category
        
        logger.debug("📂 Categorized as GENERAL")
        return NewsCategoryEnum.GENERAL
    
    def identify_relevant_companies(self, title: str, snippet: str) -> List[Dict]:
        """
        Use hybrid ML to identify companies that are actually relevant to the article
        Returns list with company_id and detailed scoring
        """
        if not self.classifier_ready:
            logger.warning("⚠️ Classifier not ready, skipping company identification")
            return []
        
        try:
            # Use hybrid ML classifier with stricter threshold
            relevant_companies = self.company_classifier.classify_article(
                title=title,
                content=snippet,
                min_relevance=0.7  # Increased from 0.6 to be more strict
            )
            
            # Additional filtering for edge cases
            filtered_companies = []
            for rc in relevant_companies:
                # Only include if linguistic score is also reasonable
                if rc['linguistic_score'] >= 0.5:
                    filtered_companies.append(rc)
                else:
                    logger.debug(f"🚫 Filtered out {rc['company_symbol']} (low linguistic score: {rc['linguistic_score']:.2f})")
            
            if filtered_companies:
                company_symbols = [rc['company_symbol'] for rc in filtered_companies]
                scores = [f"{rc['company_symbol']}:{rc['relevance_score']:.2f}" for rc in filtered_companies]
                logger.info(f"🎯 Found relevant companies: {', '.join(scores)}")
            else:
                logger.debug(f"🔍 No relevant companies found for: {title[:50]}...")
            
            return filtered_companies
            
        except Exception as e:
            logger.error(f"❌ Hybrid classification failed: {e}")
            return []
                
        except Exception as e:
                logger.error(f"❌ Hybrid classification failed: {e}")
                return []
    
    def process_article(self, article: Dict[str, Any], companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process article with hybrid ML company classification
        
        Designed to handle high volume efficiently while providing accurate results
        """
        processed = article.copy()
        
        # Update company knowledge if needed (cache-friendly)
        if not self.classifier_ready and self.company_classifier:
            self.update_company_knowledge(companies)
        
        # Fast category classification
        category = self.categorize_article(
            article['title'], 
            article.get('content_snippet', '')
        )
        processed['category'] = category
        
        # Hybrid ML company identification
        relevant_companies = self.identify_relevant_companies(
            article['title'],
            article.get('content_snippet', '')
        )
        
        if relevant_companies:
            # Store company IDs and detailed ML scores
            processed['mentioned_company_ids'] = [rc['company_id'] for rc in relevant_companies]
            processed['company_ml_scores'] = {
                rc['company_id']: {
                    'relevance_score': rc['relevance_score'],
                    'linguistic_score': rc['linguistic_score'],
                    'ml_score': rc['ml_score'],
                    'confidence': rc['confidence']
                } for rc in relevant_companies
            }
            
            # Log for monitoring and debugging
            title_preview = article['title'][:60] + "..." if len(article['title']) > 60 else article['title']
            company_info = [f"{rc['company_symbol']}({rc['relevance_score']:.2f})" for rc in relevant_companies]
            logger.info(f"📰 '{title_preview}' → {', '.join(company_info)}")
        else:
            processed['mentioned_company_ids'] = []
            processed['company_ml_scores'] = {}
            logger.debug(f"📰 No companies: {article['title'][:50]}...")
        
        # No sentiment analysis (as requested)
        processed['sentiments'] = []
        
        return processed
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get statistics about processing performance"""
        stats = {
            'processor_type': 'hybrid_ml',
            'classifier_ready': self.classifier_ready,
            'supports_sp500': True,
            'ml_enhanced': True,
            'category_classification': 'keyword_based',
            'company_classification': 'hybrid_ml'
        }
        
        if self.company_classifier:
            stats.update(self.company_classifier.get_stats())
        
        return stats
    
    def adjust_relevance_threshold(self, new_threshold: float):
        """
        Adjust the relevance threshold for company classification
        
        Args:
            new_threshold: Float between 0.0 and 1.0
                          Lower = more companies included (less strict)
                          Higher = fewer companies included (more strict)
        """
        if 0.0 <= new_threshold <= 1.0:
            self.relevance_threshold = new_threshold
            logger.info(f"🎚️ Relevance threshold updated to {new_threshold}")
        else:
            logger.warning(f"⚠️ Invalid threshold {new_threshold}, must be between 0.0 and 1.0")
    
    def get_classification_quality_report(self, articles: List[Dict]) -> Dict:
        """
        Generate a quality report for recent classifications
        Useful for monitoring and tuning
        """
        if not articles:
            return {"error": "No articles provided"}
        
        total_articles = len(articles)
        articles_with_companies = sum(1 for a in articles if a.get('mentioned_company_ids'))
        articles_without_companies = total_articles - articles_with_companies
        
        # Count companies per article
        company_counts = [len(a.get('mentioned_company_ids', [])) for a in articles]
        avg_companies_per_article = sum(company_counts) / len(company_counts) if company_counts else 0
        
        # Get confidence scores
        confidence_scores = []
        for article in articles:
            ml_scores = article.get('company_ml_scores', {})
            for scores in ml_scores.values():
                confidence_scores.append(scores.get('confidence', 0))
        
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        
        report = {
            'total_articles': total_articles,
            'articles_with_companies': articles_with_companies,
            'articles_without_companies': articles_without_companies,
            'classification_rate': articles_with_companies / total_articles if total_articles > 0 else 0,
            'avg_companies_per_article': round(avg_companies_per_article, 2),
            'avg_confidence': round(avg_confidence, 3),
            'min_confidence': min(confidence_scores) if confidence_scores else 0,
            'max_confidence': max(confidence_scores) if confidence_scores else 0,
            'processor_stats': self.get_processing_stats()
        }
        
        return report