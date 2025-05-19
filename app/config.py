import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings configuration."""
    # API settings
    APP_NAME: str = "S&P 500 News API"
    API_PREFIX: str = "/api"
    DEBUG: bool = False
    
    # Database settings
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/newsapi")
    
    # Google News settings
    GOOGLE_NEWS_DELAY: float = 1.0  # Delay between requests in seconds
    
    # News collection settings
    REFRESH_INTERVAL: int = 120  # Minutes between full refresh
    PRIORITY_REFRESH_INTERVAL: int = 30  # Minutes between priority company refresh
    
    # Security settings (basic auth for simplicity)
    API_KEY: str = os.getenv("API_KEY", "dev_api_key")
    
    class Config:
        env_file = ".env"
        case_sensitive = True

# Global settings instance
settings = Settings()