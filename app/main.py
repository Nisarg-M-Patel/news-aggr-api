import logging
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import asyncio
from typing import List
from datetime import datetime, timedelta  # Add this import

from app.config import settings
from app.database import init_db, SessionLocal
from app.api import router
from app.collector import NewsCollector
from app.processor import NewsProcessor
from app import models, schemas
from app.gdelt_simple import SimpleGDELTCollector, progress

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
    
    # Print debug info
    print(f"API_KEY setting: {settings.API_KEY}")
    print(f"Header API key: {request.headers.get('X-API-Key')}")

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

# Debug endpoint for manual news collection
@app.get("/api/debug/collect")
async def debug_collect_news():
    """Debug endpoint to manually trigger news collection"""
    # Create collector and processor
    collector = NewsCollector()
    processor = NewsProcessor()
    
    # Get companies from the database
    db = SessionLocal()
    try:
        # Get all companies
        companies = db.query(models.Company).all()
        if not companies:
            return {"status": "error", "message": "No companies found. Run the seed_companies.py script first."}
        
        company_data = [{"id": c.id, "symbol": c.symbol, "name": c.name} for c in companies]
        
        # Collect news for the first company only (for quicker debugging)
        test_company = company_data[0]
        logger.info(f"Debug collecting news for {test_company['symbol']}")
        
        articles = await collector.collect_for_company(
            test_company["symbol"], 
            test_company["name"],
            days=7  # Look back 7 days
        )
        
        if not articles:
            return {"status": "warning", "message": f"No articles found for {test_company['symbol']}"}
        
        # Process and store a sample article
        if articles:
            sample_article = articles[0]
            processed = processor.process_article(sample_article, company_data)
            
            # Check if article already exists
            existing = db.query(models.NewsItem).filter(models.NewsItem.url == processed["url"]).first()
            if existing:
                return {
                    "status": "info", 
                    "message": "Sample article already exists in database",
                    "article": {
                        "title": sample_article["title"],
                        "url": sample_article["url"],
                        "source": sample_article["source"]
                    }
                }
            
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
                db.execute(
                    models.company_news_association.insert().values(
                        company_id=company_id,
                        news_id=news_item.id
                    )
                )
            
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
            
            return {
                "status": "success", 
                "message": f"Added sample article for {test_company['symbol']}",
                "article": {
                    "title": sample_article["title"],
                    "url": sample_article["url"],
                    "source": sample_article["source"],
                    "category": processed["category"].value,
                    "mentioned_companies": processed["mentioned_company_ids"],
                    "sentiments": [
                        {"company_id": s["company_id"], "sentiment": s["sentiment"].value, "score": s["score"]} 
                        for s in processed["sentiments"]
                    ]
                }
            }
        
        return {"status": "error", "message": "Failed to process any articles"}
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error in debug collection: {str(e)}")
        return {"status": "error", "message": str(e)}
    finally:
        db.close()

@app.get("/api/backfill/progress")
async def get_backfill_progress():
    """Monitor GDELT backfill progress"""
    return progress.to_dict()

async def run_gdelt_backfill_if_needed():
    """Run GDELT backfill if no historical data exists"""
    
    collector = SimpleGDELTCollector()
    
    if not collector.is_available():
        logger.info("⚠️ GDELT not configured, skipping historical backfill")
        return
    
    db = SessionLocal()
    try:
        # Get companies
        companies = db.query(models.Company).all()
        company_data = [{"id": c.id, "symbol": c.symbol, "name": c.name} for c in companies]
        
        # Check if we have historical data (older than 30 days)
        cutoff_date = datetime.now() - timedelta(days=30)
        historical_count = db.query(models.NewsItem).filter(
            models.NewsItem.published_at < cutoff_date
        ).count()
        
        if historical_count == 0:
            logger.info("🚀 No historical data found, starting GDELT backfill...")
            
            # Backfill last 2 years
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
            
            total_added = await collector.run_backfill(company_data, start_date, end_date)
            logger.info(f"✅ GDELT backfill complete: {total_added} articles added")
        else:
            logger.info(f"📊 Found {historical_count} historical articles, skipping backfill")
            
    finally:
        db.close()


# Background task for collecting news
async def collect_news_periodically():
    """Background task to collect news at regular intervals"""
    collector = NewsCollector()
    processor = NewsProcessor()
    
    # Sleep initially to give the app time to start up
    await asyncio.sleep(10)
    
    while True:
        try:
            logger.info("Starting scheduled news collection")
            
            # Get companies from the database
            from app.database import SessionLocal
            
            db = SessionLocal()
            try:
                # Get all companies
                companies = db.query(models.Company).all()
                company_data = [{"id": c.id, "symbol": c.symbol, "name": c.name} for c in companies]
                
                logger.info(f"Found {len(company_data)} companies in database")
                if not company_data:
                    logger.error("No companies found in database. Have you run seed_companies.py?")
                    await asyncio.sleep(60)  # Wait a minute before trying again
                    continue
                
                # Collect news
                logger.info(f"Collecting news for companies: {[c['symbol'] for c in company_data]}")
                articles = await collector.collect_for_companies(company_data)
                
                logger.info(f"Collected {len(articles)} total articles")
                
                # Process each article
                articles_added = 0
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
                    db.flush()  # Flush to get the id
                    
                    # Link to mentioned companies using the association table
                    for company_id in processed["mentioned_company_ids"]:
                        # Use execute to insert directly into the association table
                        db.execute(
                            models.company_news_association.insert().values(
                                company_id=company_id,
                                news_id=news_item.id
                            )
                        )
                    
                    # Add sentiment analysis
                    for sentiment_data in processed["sentiments"]:
                        sentiment = models.NewsSentiment(
                            news_id=news_item.id,
                            company_id=sentiment_data["company_id"],
                            sentiment=sentiment_data["sentiment"],
                            score=sentiment_data["score"]
                        )
                        db.add(sentiment)
                    
                    articles_added += 1
                
                db.commit()
                logger.info(f"Added {articles_added} new articles")
            
            except Exception as e:
                db.rollback()
                logger.error(f"Error in news collection: {str(e)}")
            finally:
                db.close()
            
        except Exception as e:
            logger.error(f"Error in news collection task: {str(e)}")
        
        # Wait for next collection interval
        logger.info(f"Waiting {settings.REFRESH_INTERVAL} minutes before next collection")
        await asyncio.sleep(settings.REFRESH_INTERVAL * 60)

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database and start background tasks"""
    # Initialize database
    init_db()
    logger.info("Database initialized")
    
    # Check if companies exist, if not seed them
    db = SessionLocal()
    try:
        company_count = db.query(models.Company).count()
        if company_count == 0:
            logger.warning("No companies found in database. Running seed script...")
            from scripts.seed_companies import seed_companies
            seed_companies()
            logger.info("Companies seeded successfully")
    except Exception as e:
        logger.error(f"Error checking or seeding companies: {str(e)}")
    finally:
        db.close()
    
    await run_gdelt_backfill_if_needed()
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