from datetime import datetime
from typing import List, Optional, Generic, TypeVar, Any
from pydantic import BaseModel, HttpUrl, Field

from app.models import NewsCategoryEnum, SentimentEnum

# Define a TypeVar for generic pagination
T = TypeVar('T')

# Company schemas
class CompanyBase(BaseModel):
    symbol: str
    name: str
    sector: Optional[str] = None
    industry: Optional[str] = None

class CompanyCreate(CompanyBase):
    pass

class Company(CompanyBase):
    id: int
    
    model_config = {"from_attributes": True}

# News schemas
class NewsSentimentBase(BaseModel):
    sentiment: SentimentEnum
    score: float = Field(ge=0.0, le=1.0)

class NewsSentiment(NewsSentimentBase):
    company_id: int
    company: Optional[Company] = None
    
    model_config = {"from_attributes": True}

class NewsItemBase(BaseModel):
    title: str
    url: HttpUrl
    source: str
    published_at: datetime
    content_snippet: Optional[str] = None
    category: NewsCategoryEnum = NewsCategoryEnum.GENERAL

class NewsItemCreate(NewsItemBase):
    pass

class NewsItem(NewsItemBase):
    id: int
    fetched_at: datetime
    companies: List[Company] = []
    sentiments: List[NewsSentiment] = []
    
    model_config = {"from_attributes": True}

# Search and filter schemas
class NewsFilter(BaseModel):
    company_symbol: Optional[str] = None
    sector: Optional[str] = None
    category: Optional[NewsCategoryEnum] = None
    sentiment: Optional[SentimentEnum] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

# Pagination schema that can handle different types
class PaginatedResponse(BaseModel):
    items: List[Any]  # Use Any to handle different item types
    total: int
    page: int
    size: int
    pages: int
    
    model_config = {"arbitrary_types_allowed": True}