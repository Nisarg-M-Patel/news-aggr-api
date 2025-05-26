import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings configuration"""
    
    # API settings
    APP_NAME: str = "S&P 500 News API"
    API_PREFIX: str = "/api"
    DEBUG: bool = False
    
    # Database settings
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql://postgres:newsapi_secure_password@db:5432/newsapi"
    )
    
    # Background processing
    ENABLE_BACKGROUND_COLLECTION: bool = True
    REFRESH_INTERVAL: int = 120  # Minutes between collections
    
    # News collection settings
    GOOGLE_NEWS_DELAY: float = 1.0  # Delay between requests
    
    # Security
    API_KEY: str = os.getenv("API_KEY", "dev_api_key")
    
    # GDELT settings
    ENABLE_GDELT: bool = False
    GOOGLE_CLOUD_PROJECT: str = os.getenv("GOOGLE_CLOUD_PROJECT", "news-aggr-api")
    
    # ML settings
    ENABLE_ML_CLASSIFICATION: bool = True
    ML_RELEVANCE_THRESHOLD: float = 0.5
    
    class Config:
        env_file = ".env"
        case_sensitive = True

# Global settings instance
settings = Settings()