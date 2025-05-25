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

@router.get("/companies/sectors", response_model=List[dict])
async def get_sectors(db: Session = Depends(get_db)):
    """
    Get list of all sectors with company counts.
    """
    from sqlalchemy import func
    
    sectors = db.query(
        models.Company.sector,
        func.count(models.Company.id).label("company_count")
    ).group_by(models.Company.sector).all()
    
    return [{"sector": sector, "company_count": count} for sector, count in sectors]

@router.get("/companies/{symbol}", response_model=schemas.Company)
async def get_company(symbol: str = Path(..., title="Company stock symbol"), db: Session = Depends(get_db)):
    """
    Get details about a specific company by symbol.
    """
    company = db.query(models.Company).filter(models.Company.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company with symbol {symbol} not found")
    return company

# --- News Endpoints ---

@router.get("/news/company", response_model=schemas.PaginatedResponse)
async def get_company_news(
    c: str = Query(..., description="Company symbol (e.g., AAPL)"),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 20,
    days: int = Query(1, description="Number of days to look back (default: 1)")
):
    """
    Get news for a specific company.
    
    Usage: /news/company?c=AAPL&days=7
    """
    # Validate company exists
    company = db.query(models.Company).filter(models.Company.symbol == c.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company with symbol {c} not found")
    
    # Query news for this company
    query = db.query(models.NewsItem).join(
        models.company_news_association,
        models.NewsItem.id == models.company_news_association.c.news_id
    ).filter(models.company_news_association.c.company_id == company.id)
    
    # Apply time filter
    start_date = datetime.utcnow() - timedelta(days=days)
    query = query.filter(models.NewsItem.published_at >= start_date)
    
    # Remove duplicates and count total
    query = query.distinct()
    total = query.count()
    
    # Apply pagination and ordering
    news_items = query.order_by(models.NewsItem.published_at.desc()).offset(skip).limit(limit).all()
    
    # Calculate pagination metadata
    pages = (total + limit - 1) // limit if limit > 0 else 1
    page = (skip // limit) + 1 if limit > 0 else 1
    
    # Convert to Pydantic models
    pydantic_items = [schemas.NewsItem.model_validate(item) for item in news_items]
    
    return {
        "items": pydantic_items,
        "total": total,
        "page": page,
        "size": limit,
        "pages": pages
    }

@router.get("/news/sector", response_model=schemas.PaginatedResponse)
async def get_sector_news(
    s: str = Query(..., description="Sector name (e.g., 'Information Technology')"),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 20,
    days: int = Query(1, description="Number of days to look back (default: 1)")
):
    """
    Get news for companies in a specific sector.
    
    Usage: /news/sector?s=Information Technology&days=7
    """
    # Validate sector exists
    sector_exists = db.query(models.Company.sector).filter(models.Company.sector == s).first()
    if not sector_exists:
        raise HTTPException(status_code=404, detail=f"Sector '{s}' not found")
    
    # Query news for companies in this sector
    query = db.query(models.NewsItem).join(
        models.company_news_association,
        models.NewsItem.id == models.company_news_association.c.news_id
    ).join(
        models.Company,
        models.Company.id == models.company_news_association.c.company_id
    ).filter(models.Company.sector == s)
    
    # Apply time filter
    start_date = datetime.utcnow() - timedelta(days=days)
    query = query.filter(models.NewsItem.published_at >= start_date)
    
    # Remove duplicates and count total
    query = query.distinct()
    total = query.count()
    
    # Apply pagination and ordering
    news_items = query.order_by(models.NewsItem.published_at.desc()).offset(skip).limit(limit).all()
    
    # Calculate pagination metadata
    pages = (total + limit - 1) // limit if limit > 0 else 1
    page = (skip // limit) + 1 if limit > 0 else 1
    
    # Convert to Pydantic models
    pydantic_items = [schemas.NewsItem.model_validate(item) for item in news_items]
    
    return {
        "items": pydantic_items,
        "total": total,
        "page": page,
        "size": limit,
        "pages": pages
    }

# --- Management ---

@router.post("/refresh/all")
async def refresh_all(db: Session = Depends(get_db)):
    """
    Trigger full index refresh.
    """
    return {"status": "success", "message": "Full refresh triggered"}

@router.post("/refresh/{symbol}")
async def refresh_company(
    symbol: str = Path(..., title="Company stock symbol"),
    db: Session = Depends(get_db)
):
    """
    Trigger manual refresh for a company.
    """
    # Check if company exists
    company = db.query(models.Company).filter(models.Company.symbol == symbol.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company with symbol {symbol} not found")
    
    return {"status": "success", "message": f"Refresh triggered for {symbol}"}