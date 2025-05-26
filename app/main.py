import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.database import init_db, SessionLocal
from app.api import router
from app import models

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global background task reference
background_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown events"""
    global background_task
    
    # Startup
    logger.info("🚀 Starting News API...")
    
    try:
        # Initialize database
        init_db()
        logger.info("✅ Database initialized")
        
        # Seed companies if needed
        await ensure_companies_exist()
        
        # Run GDELT backfill if enabled and needed
        if settings.ENABLE_GDELT:
            await run_gdelt_backfill_if_needed()
        
        # Start background collection (optional)
        if settings.ENABLE_BACKGROUND_COLLECTION:
            background_task = asyncio.create_task(news_collection_loop())
            logger.info("✅ Background news collection started")
        else:
            logger.info("ℹ️ Background collection disabled")
            
        logger.info("🎉 News API startup complete")
        
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down News API...")
    
    if background_task and not background_task.done():
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            logger.info("✅ Background task cancelled")
    
    logger.info("✅ Shutdown complete")

# Create FastAPI app with lifespan
app = FastAPI(
    title=settings.APP_NAME,
    description="API for aggregating and analyzing news for S&P 500 companies",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router)

# Simplified authentication middleware
@app.middleware("http")
async def authenticate_requests(request: Request, call_next):
    """Simple API key check"""
    # Skip auth for docs
    if request.url.path in ["/api/docs", "/api/redoc", "/api/openapi.json"]:
        return await call_next(request)
    
    # Development mode - skip auth
    if settings.API_KEY == "dev_api_key":
        return await call_next(request)
    
    # Check API key
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
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0",
        "features": {
            "ml_classification": settings.ENABLE_ML_CLASSIFICATION,
            "gdelt_enabled": settings.ENABLE_GDELT,
            "background_collection": settings.ENABLE_BACKGROUND_COLLECTION
        }
    }

@app.get("/api/debug/collect")
async def debug_collect_news():
    """Manually trigger news collection for testing"""
    try:
        from app.collector import NewsCollector
        from app.processor import NewsProcessor
        
        collector = NewsCollector()
        processor = NewsProcessor()
        
        db = SessionLocal()
        try:
            # Get companies
            companies = db.query(models.Company).all()
            if not companies:
                return {
                    "status": "error", 
                    "message": "No companies found. Run seed script first."
                }
            
            company_data = [{"id": c.id, "symbol": c.symbol, "name": c.name} for c in companies]
            
            # Test with first company only
            test_company = company_data[0]
            logger.info(f"Debug collecting for {test_company['symbol']}")
            
            articles = await collector.collect_for_company(
                test_company["symbol"], 
                test_company["name"],
                days=1
            )
            
            if not articles:
                return {
                    "status": "warning", 
                    "message": f"No articles found for {test_company['symbol']}"
                }
            
            # Process first article
            sample_article = articles[0]
            processed = processor.process_article(sample_article, company_data)
            
            # Check if exists
            existing = db.query(models.NewsItem).filter(
                models.NewsItem.url == processed["url"]
            ).first()
            
            if existing:
                return {
                    "status": "info",
                    "message": "Article already exists",
                    "article": {
                        "title": sample_article["title"],
                        "url": sample_article["url"],
                        "companies": processed.get("mentioned_company_ids", [])
                    }
                }
            
            # Store new article
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
            
            # Add company associations
            for company_id in processed.get("mentioned_company_ids", []):
                db.execute(
                    models.company_news_association.insert().values(
                        company_id=company_id,
                        news_id=news_item.id
                    )
                )
            
            db.commit()
            
            return {
                "status": "success",
                "message": f"Added article for {test_company['symbol']}",
                "article": {
                    "title": processed["title"],
                    "category": processed["category"].value,
                    "companies": processed.get("mentioned_company_ids", []),
                    "ml_scores": processed.get("company_ml_scores", {})
                }
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Debug collection failed: {e}")
        return {"status": "error", "message": str(e)}

# GDELT endpoints
@app.get("/api/gdelt/test")
async def test_gdelt():
    """Test GDELT connection and show sample data"""
    try:
        from app.gdelt_simple import SimpleGDELTCollector
        collector = SimpleGDELTCollector()
        return collector.test_connection()
    except Exception as e:
        logger.error(f"GDELT test failed: {e}")
        return {"error": f"GDELT test failed: {str(e)}"}

@app.get("/api/gdelt/progress")
async def get_gdelt_progress():
    """Get GDELT backfill progress"""
    try:
        from app.gdelt_simple import progress
        return progress.to_dict()
    except Exception as e:
        return {"error": f"Failed to get progress: {str(e)}"}

@app.post("/api/gdelt/backfill")
async def start_gdelt_backfill(
    start_date: str = "2024-02-01",
    end_date: str = "2024-05-01"
):
    """Start GDELT backfill for specified date range"""
    if not settings.ENABLE_GDELT:
        return {"error": "GDELT is not enabled"}
    
    try:
        from app.gdelt_simple import SimpleGDELTCollector, progress
        
        if progress.is_running:
            return {"error": "Backfill already running"}
        
        # Validate dates
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return {"error": "Invalid date format. Use YYYY-MM-DD"}
        
        # Start backfill in background
        collector = SimpleGDELTCollector()
        db = SessionLocal()
        try:
            companies = db.query(models.Company).all()
            company_data = [{"id": c.id, "symbol": c.symbol, "name": c.name} for c in companies]
        finally:
            db.close()
        
        # Start backfill task in background (don't await)
        task = asyncio.create_task(collector.run_backfill(company_data, start_date, end_date))
        # Don't await the task - let it run in background
        
        return {
            "status": "started",
            "message": f"GDELT backfill started for {start_date} to {end_date}",
            "monitor_url": "/api/gdelt/progress"
        }
        
    except Exception as e:
        logger.error(f"GDELT backfill start failed: {e}")
        return {"error": f"Failed to start backfill: {str(e)}"}

async def run_gdelt_backfill_if_needed():
    """Run GDELT backfill if no historical data exists"""
    try:
        from app.gdelt_simple import SimpleGDELTCollector
        
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
                
                # Backfill last 3 months
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                
                total_added = await collector.run_backfill(company_data, start_date, end_date)
                logger.info(f"✅ GDELT backfill complete: {total_added} articles added")
            else:
                logger.info(f"📊 Found {historical_count} historical articles, skipping backfill")
                
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"GDELT backfill check failed: {e}")

async def ensure_companies_exist():
    """Ensure companies are seeded in database"""
    db = SessionLocal()
    try:
        company_count = db.query(models.Company).count()
        if company_count == 0:
            logger.info("🌱 Seeding companies...")
            await seed_sample_companies(db)
            logger.info("✅ Companies seeded")
        else:
            logger.info(f"✅ Found {company_count} companies in database")
    except Exception as e:
        logger.error(f"❌ Error checking companies: {e}")
        raise
    finally:
        db.close()

async def seed_sample_companies(db):
    """Seed sample S&P 500 companies"""
    sample_companies = [
        {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Information Technology", "industry": "Technology Hardware"},
        {"symbol": "MSFT", "name": "Microsoft Corp", "sector": "Information Technology", "industry": "Software"},
        {"symbol": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary", "industry": "Internet Retail"},
        {"symbol": "NVDA", "name": "NVIDIA Corp", "sector": "Information Technology", "industry": "Semiconductors"},
        {"symbol": "GOOGL", "name": "Alphabet Inc. Class A", "sector": "Communication Services", "industry": "Internet"},
    ]
    
    for data in sample_companies:
        company = models.Company(
            symbol=data["symbol"],
            name=data["name"],
            sector=data["sector"],
            industry=data["industry"]
        )
        db.add(company)
    
    db.commit()

async def news_collection_loop():
    """Background news collection loop"""
    from app.collector import NewsCollector
    from app.processor import NewsProcessor
    
    collector = NewsCollector()
    processor = NewsProcessor()
    
    # Initial delay
    await asyncio.sleep(30)
    
    while True:
        try:
            logger.info("🔄 Starting scheduled news collection")
            
            db = SessionLocal()
            try:
                # Get companies
                companies = db.query(models.Company).all()
                company_data = [{"id": c.id, "symbol": c.symbol, "name": c.name} for c in companies]
                
                if not company_data:
                    logger.warning("No companies found, skipping collection")
                    await asyncio.sleep(300)  # Wait 5 minutes
                    continue
                
                # Update processor with companies
                if hasattr(processor, 'update_company_knowledge'):
                    processor.update_company_knowledge(company_data)
                
                # Collect and process
                articles = await collector.collect_for_companies(company_data)
                articles_added = 0
                
                for article in articles:
                    try:
                        processed = processor.process_article(article, company_data)
                        
                        # Skip if exists
                        existing = db.query(models.NewsItem).filter(
                            models.NewsItem.url == processed["url"]
                        ).first()
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
                        
                        # Add associations
                        for company_id in processed.get("mentioned_company_ids", []):
                            db.execute(
                                models.company_news_association.insert().values(
                                    company_id=company_id,
                                    news_id=news_item.id
                                )
                            )
                        
                        articles_added += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing article: {e}")
                        continue
                
                db.commit()
                logger.info(f"✅ Collection complete: {articles_added} new articles")
                
            except Exception as e:
                db.rollback()
                logger.error(f"❌ Collection failed: {e}")
            finally:
                db.close()
                
        except asyncio.CancelledError:
            logger.info("News collection cancelled")
            break
        except Exception as e:
            logger.error(f"Collection loop error: {e}")
        
        # Wait for next cycle
        await asyncio.sleep(settings.REFRESH_INTERVAL * 60)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )