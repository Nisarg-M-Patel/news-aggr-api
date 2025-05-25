from datetime import datetime
from typing import List, Optional, Any
from pydantic import BaseModel, HttpUrl, Field

from app.models import NewsCategoryEnum

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
    
    model_config = {"from_attributes": True}

# Pagination schema
class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    size: int
    pages: int
    
    model_config = {"arbitrary_types_allowed": True}