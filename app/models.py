from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, Enum, Table
from sqlalchemy.orm import relationship
import enum

from app.database import Base

class NewsCategoryEnum(enum.Enum):
    """Categories for news articles"""
    EARNINGS = "earnings"
    EXECUTIVE = "executive"
    LEGAL = "legal"
    PRODUCT = "product"
    MARKET = "market"
    GENERAL = "general"
    OTHER = "other"

class SentimentEnum(enum.Enum):
    """Sentiment classification for news articles"""
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"

# Association table for many-to-many relationship between companies and news
# Using a plain table instead of a class to simplify the relationship
company_news_association = Table(
    "company_news_association",
    Base.metadata,
    Column("company_id", Integer, ForeignKey("companies.id"), primary_key=True),
    Column("news_id", Integer, ForeignKey("news_items.id"), primary_key=True)
)

class Company(Base):
    """Model representing a company in the S&P 500 index"""
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, index=True, nullable=False)
    name = Column(String(255), nullable=False)
    sector = Column(String(100), index=True)
    industry = Column(String(255))
    
    # Relationships - using the association table directly
    news_items = relationship("NewsItem", secondary=company_news_association, back_populates="companies")

class NewsItem(Base):
    """Model representing a news article"""
    __tablename__ = "news_items"
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(512), nullable=False)
    url = Column(String(1024), unique=True, nullable=False)
    source = Column(String(255), nullable=False)
    published_at = Column(DateTime, nullable=False, index=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    content_snippet = Column(Text)
    category = Column(Enum(NewsCategoryEnum), default=NewsCategoryEnum.GENERAL)
    
    # Relationships
    companies = relationship("Company", secondary=company_news_association, back_populates="news_items")
    sentiments = relationship("NewsSentiment", back_populates="news_item")

class NewsSentiment(Base):
    """Model representing sentiment analysis for a company in a news article"""
    __tablename__ = "news_sentiments"
    
    id = Column(Integer, primary_key=True, index=True)
    news_id = Column(Integer, ForeignKey("news_items.id"))
    company_id = Column(Integer, ForeignKey("companies.id"))
    sentiment = Column(Enum(SentimentEnum), nullable=False)
    score = Column(Float)  # Confidence score for sentiment
    
    # Relationships
    news_item = relationship("NewsItem", back_populates="sentiments")
    company = relationship("Company")