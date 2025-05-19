import logging
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import asyncio
from typing import List

from app.config import settings
from app.database import init_db
from app.api import router
from app.collector import NewsCollector
from app.processor import NewsProcessor
from app import models, schemas

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="API for aggregating and analyzing news for S&P 500 companies",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router)

# Simple API key authentication middleware
@app.middleware("http")
async def authenticate_requests(request: Request, call_next):
    """Check for API key in header for authenticated endpoints"""
    # Skip auth for docs and OpenAPI spec
    if request.url.path in ["/api/docs", "/api/redoc", "/api/openapi.json"]:
        return await call_next(request)
    
    # Skip auth if API_KEY is "dev_api_key" (development mode)
    if settings.API_KEY == "dev_api_key":
        return await call_next(request)
    
    # Check API key for all other endpoints
    api_key = request.headers.get("X-API-Key")
    if not api_key or api_key != settings.API_KEY:
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid or missing API key"}
        )
    
    return await call_next(request)

# Health check endpoint
@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

# Background task for collecting news
async def collect_news_periodically():
    """Background task to collect news at regular intervals"""
    collector = NewsCollector()
    processor = NewsProcessor()
    
    while True:
        try:
            logger.info("Starting scheduled news collection")
            
            # In a real implementation, this would:
            # 1. Fetch companies from the database
            # 2. Collect news for all companies
            # 3. Process the news (sentiment, categorization)
            # 4. Store in the database
            
            # Example of how it would work (commented out as DB integration would be needed)
            '''
            from app.database import SessionLocal
            
            db = SessionLocal()
            try:
                # Get all companies
                companies = db.query(models.Company).all()
                company_data = [{"symbol": c.symbol, "name": c.name} for c in companies]
                
                # Collect news
                articles = await collector.collect_for_companies(company_data)
                
                # Process each article
                for article in articles:
                    processed = processor.process_article(article, company_data)
                    
                    # Check if article already exists
                    existing = db.query(models.NewsItem).filter(models.NewsItem.url == processed["url"]).first()
                    if existing:
                        continue
                    
                    # Create news item
                    news_item = models.NewsItem(
                        title=processed["title"],
                        url=processed["url"],
                        source=processed["source"],
                        published_at=processed["published_at"],
                        content_snippet=processed.get("content_snippet", ""),
                        category=processed["category"]
                    )
                    db.add(news_item)
                    db.flush()
                    
                    # Link to mentioned companies
                    for company_id in processed["mentioned_company_ids"]:
                        association = models.CompanyNewsAssociation(
                            company_id=company_id,
                            news_id=news_item.id
                        )
                        db.add(association)
                    
                    # Add sentiment analysis
                    for sentiment_data in processed["sentiments"]:
                        sentiment = models.NewsSentiment(
                            news_id=news_item.id,
                            company_id=sentiment_data["company_id"],
                            sentiment=sentiment_data["sentiment"],
                            score=sentiment_data["score"]
                        )
                        db.add(sentiment)
                
                db.commit()
                logger.info(f"Added {len(articles)} new articles")
            
            except Exception as e:
                db.rollback()
                logger.error(f"Error in news collection: {str(e)}")
            finally:
                db.close()
            '''
            
            # For demonstration, just log
            logger.info("Completed scheduled news collection")
            
        except Exception as e:
            logger.error(f"Error in news collection task: {str(e)}")
        
        # Wait for next collection interval
        await asyncio.sleep(settings.REFRESH_INTERVAL * 60)

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database and start background tasks"""
    # Initialize database
    init_db()
    logger.info("Database initialized")
    
    # Start background task for news collection
    asyncio.create_task(collect_news_periodically())
    logger.info("Background news collection task started")

if __name__ == "__main__":
    import uvicorn
    
    # Run the app with uvicorn when script is executed directly
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=settings.DEBUG
    )