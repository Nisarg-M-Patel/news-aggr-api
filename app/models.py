from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, Enum
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

class Company(Base):
    """Model representing a company in the S&P 500 index"""
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, index=True, nullable=False)
    name = Column(String(255), nullable=False)
    sector = Column(String(100), index=True)
    industry = Column(String(255))
    
    # Relationships
    news_items = relationship("NewsItem", back_populates="company", 
                             secondary="company_news_association")

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
    companies = relationship("Company", back_populates="news_items", 
                           secondary="company_news_association")
    sentiments = relationship("NewsSentiment", back_populates="news_item")

class CompanyNewsAssociation(Base):
    """Association table for many-to-many relationship between companies and news"""
    __tablename__ = "company_news_association"
    
    company_id = Column(Integer, ForeignKey("companies.id"), primary_key=True)
    news_id = Column(Integer, ForeignKey("news_items.id"), primary_key=True)

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