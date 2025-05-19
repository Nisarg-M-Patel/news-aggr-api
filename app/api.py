from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from app import models, schemas
from app.database import get_db
from app.config import settings

router = APIRouter(prefix=settings.API_PREFIX)

# --- Companies Endpoints ---

@router.get("/companies", response_model=List[schemas.Company])
async def get_companies(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    sector: Optional[str] = None
):
    """
    Get list of S&P 500 companies, optionally filtered by sector.
    """
    query = db.query(models.Company)
    
    if sector:
        query = query.filter(models.Company.sector == sector)
    
    companies = query.offset(skip).limit(limit).all()
    return companies

@router.get("/companies/{symbol}", response_model=schemas.Company)
async def get_company(symbol: str = Path(..., title="Company stock symbol"), db: Session = Depends(get_db)):
    """
    Get details about a specific company by symbol.
    """
    company = db.query(models.Company).filter(models.Company.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company with symbol {symbol} not found")
    return company

@router.get("/companies/sectors", response_model=List[dict])
async def get_sectors(db: Session = Depends(get_db)):
    """
    Get list of all sectors with company counts.
    """
    # Using SQLAlchemy to fetch sectors and count companies in each
    from sqlalchemy import func
    
    sectors = db.query(
        models.Company.sector,
        func.count(models.Company.id).label("company_count")
    ).group_by(models.Company.sector).all()
    
    return [{"sector": sector, "company_count": count} for sector, count in sectors]

# --- News Endpoints ---

@router.get("/news", response_model=schemas.PaginatedResponse)
async def get_news(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 20,
    company_symbol: Optional[str] = None,
    sector: Optional[str] = None,
    category: Optional[models.NewsCategoryEnum] = None,
    sentiment: Optional[models.SentimentEnum] = None,
    days: Optional[int] = Query(7, description="Number of days to look back")
):
    """
    Get latest news across all companies with various filters.
    """
    # Start with a base query joining news and companies
    query = db.query(models.NewsItem).join(
        models.CompanyNewsAssociation,
        models.NewsItem.id == models.CompanyNewsAssociation.news_id
    ).join(
        models.Company,
        models.Company.id == models.CompanyNewsAssociation.company_id
    )
    
    # Apply filters
    if company_symbol:
        query = query.filter(models.Company.symbol == company_symbol.upper())
    
    if sector:
        query = query.filter(models.Company.sector == sector)
    
    if category:
        query = query.filter(models.NewsItem.category == category)
    
    if days:
        start_date = datetime.utcnow() - timedelta(days=days)
        query = query.filter(models.NewsItem.published_at >= start_date)
    
    if sentiment:
        # Filtering by sentiment requires joining the sentiments table
        query = query.join(
            models.NewsSentiment,
            models.NewsItem.id == models.NewsSentiment.news_id
        ).filter(models.NewsSentiment.sentiment == sentiment)
    
    # Count total matching items for pagination
    total = query.count()
    
    # Apply pagination
    news_items = query.order_by(models.NewsItem.published_at.desc()).offset(skip).limit(limit).all()
    
    # Calculate pagination metadata
    pages = (total + limit - 1) // limit if limit > 0 else 1
    page = (skip // limit) + 1 if limit > 0 else 1
    
    return {
        "items": news_items,
        "total": total,
        "page": page,
        "size": limit,
        "pages": pages
    }

@router.get("/companies/{symbol}/news", response_model=schemas.PaginatedResponse)
async def get_company_news(
    symbol: str = Path(..., title="Company stock symbol"),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 20,
    days: Optional[int] = Query(7, description="Number of days to look back"),
    category: Optional[models.NewsCategoryEnum] = None,
    sentiment: Optional[models.SentimentEnum] = None
):
    """
    Get news for a specific company.
    """
    # First check if company exists
    company = db.query(models.Company).filter(models.Company.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company with symbol {symbol} not found")
    
    # Query news for this company
    query = db.query(models.NewsItem).join(
        models.CompanyNewsAssociation,
        models.NewsItem.id == models.CompanyNewsAssociation.news_id
    ).filter(models.CompanyNewsAssociation.company_id == company.id)
    
    # Apply filters
    if category:
        query = query.filter(models.NewsItem.category == category)
    
    if days:
        start_date = datetime.utcnow() - timedelta(days=days)
        query = query.filter(models.NewsItem.published_at >= start_date)
    
    if sentiment:
        # Get news with specific sentiment for this company
        query = query.join(
            models.NewsSentiment,
            models.NewsItem.id == models.NewsSentiment.news_id
        ).filter(
            models.NewsSentiment.company_id == company.id,
            models.NewsSentiment.sentiment == sentiment
        )
    
    # Count total for pagination
    total = query.count()
    
    # Apply pagination
    news_items = query.order_by(models.NewsItem.published_at.desc()).offset(skip).limit(limit).all()
    
    # Calculate pagination metadata
    pages = (total + limit - 1) // limit if limit > 0 else 1
    page = (skip // limit) + 1 if limit > 0 else 1
    
    return {
        "items": news_items,
        "total": total,
        "page": page,
        "size": limit,
        "pages": pages
    }

@router.get("/sectors/{sector}/news", response_model=schemas.PaginatedResponse)
async def get_sector_news(
    sector: str = Path(..., title="Industry sector"),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 20,
    days: Optional[int] = Query(7, description="Number of days to look back"),
    category: Optional[models.NewsCategoryEnum] = None,
    sentiment: Optional[models.SentimentEnum] = None
):
    """
    Get news for companies in a sector.
    """
    # First check if sector exists
    sector_exists = db.query(models.Company.sector).filter(models.Company.sector == sector).first()
    if not sector_exists:
        raise HTTPException(status_code=404, detail=f"Sector {sector} not found")
    
    # Query news for companies in this sector
    query = db.query(models.NewsItem).join(
        models.CompanyNewsAssociation,
        models.NewsItem.id == models.CompanyNewsAssociation.news_id
    ).join(
        models.Company,
        models.Company.id == models.CompanyNewsAssociation.company_id
    ).filter(models.Company.sector == sector)
    
    # Apply filters
    if category:
        query = query.filter(models.NewsItem.category == category)
    
    if days:
        start_date = datetime.utcnow() - timedelta(days=days)
        query = query.filter(models.NewsItem.published_at >= start_date)
    
    if sentiment:
        # Get news with specific sentiment for companies in this sector
        query = query.join(
            models.NewsSentiment,
            models.NewsItem.id == models.NewsSentiment.news_id
        ).filter(models.NewsSentiment.sentiment == sentiment)
    
    # Count total for pagination
    total = query.count()
    
    # Apply pagination
    news_items = query.order_by(models.NewsItem.published_at.desc()).offset(skip).limit(limit).all()
    
    # Calculate pagination metadata
    pages = (total + limit - 1) // limit if limit > 0 else 1
    page = (skip // limit) + 1 if limit > 0 else 1
    
    return {
        "items": news_items,
        "total": total,
        "page": page,
        "size": limit,
        "pages": pages
    }

# --- Search & Analytics ---

@router.get("/search", response_model=schemas.PaginatedResponse)
async def search_news(
    q: str = Query(..., min_length=3, description="Search query"),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 20
):
    """
    Search across all news content.
    """
    # Basic search implementation using LIKE
    query = db.query(models.NewsItem).filter(
        (models.NewsItem.title.ilike(f"%{q}%")) | 
        (models.NewsItem.content_snippet.ilike(f"%{q}%"))
    )
    
    # Count total for pagination
    total = query.count()
    
    # Apply pagination
    news_items = query.order_by(models.NewsItem.published_at.desc()).offset(skip).limit(limit).all()
    
    # Calculate pagination metadata
    pages = (total + limit - 1) // limit if limit > 0 else 1
    page = (skip // limit) + 1 if limit > 0 else 1
    
    return {
        "items": news_items,
        "total": total,
        "page": page,
        "size": limit,
        "pages": pages
    }

@router.get("/trends/sentiment", response_model=List[dict])
async def get_sentiment_trends(
    db: Session = Depends(get_db),
    days: int = Query(30, description="Number of days to analyze"),
    company_symbol: Optional[str] = None,
    sector: Optional[str] = None
):
    """
    Get sentiment trends over time.
    """
    from sqlalchemy import func
    from sqlalchemy.sql import expression
    
    # Base query
    query = db.query(
        func.date_trunc('day', models.NewsItem.published_at).label('date'),
        models.NewsSentiment.sentiment,
        func.count().label('count')
    ).join(
        models.NewsSentiment,
        models.NewsItem.id == models.NewsSentiment.news_id
    )
    
    # Apply filters
    if company_symbol:
        company = db.query(models.Company).filter(models.Company.symbol == company_symbol.upper()).first()
        if not company:
            raise HTTPException(status_code=404, detail=f"Company with symbol {company_symbol} not found")
        query = query.filter(models.NewsSentiment.company_id == company.id)
    
    if sector:
        query = query.join(
            models.Company,
            models.NewsSentiment.company_id == models.Company.id
        ).filter(models.Company.sector == sector)
    
    # Time range
    start_date = datetime.utcnow() - timedelta(days=days)
    query = query.filter(models.NewsItem.published_at >= start_date)
    
    # Group and order
    result = query.group_by(
        func.date_trunc('day', models.NewsItem.published_at),
        models.NewsSentiment.sentiment
    ).order_by(
        func.date_trunc('day', models.NewsItem.published_at)
    ).all()
    
    # Convert to list of dictionaries
    trends = [
        {
            "date": date.strftime("%Y-%m-%d"),
            "sentiment": sentiment.value,
            "count": count
        }
        for date, sentiment, count in result
    ]
    
    return trends

# --- Management ---

@router.post("/refresh/{symbol}")
async def refresh_company(
    symbol: str = Path(..., title="Company stock symbol"),
    db: Session = Depends(get_db)
):
    """
    Trigger manual refresh for a company. (Placeholder - actual implementation would call collector)
    """
    # Check if company exists
    company = db.query(models.Company).filter(models.Company.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company with symbol {symbol} not found")
    
    # This would call the news collector in a real implementation
    return {"status": "success", "message": f"Refresh triggered for {symbol}"}

@router.post("/refresh/all")
async def refresh_all(db: Session = Depends(get_db)):
    """
    Trigger full index refresh. (Placeholder - actual implementation would call collector)
    """
    # This would call the news collector in a real implementation
    return {"status": "success", "message": "Full refresh triggered"}