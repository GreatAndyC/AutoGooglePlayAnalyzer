import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """
    Configuration class to handle environment variables.
    """
    # Database configuration
    DB_NAME = os.getenv("DB_NAME", "google_play_analysis")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")

    # App configuration
    APP_ID = os.getenv("APP_ID", "com.zhiliaoapp.musically")

    # Scraper configuration
    SCRAPE_COUNT = int(os.getenv("SCRAPE_COUNT", "1000"))
    COUNTRY = os.getenv("COUNTRY", "us")
    LANGUAGE = os.getenv("LANGUAGE", "en")

    # Analysis scope
    TOTAL_TO_ANALYZE = int(os.getenv("TOTAL_TO_ANALYZE") or "1000")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE") or "50")
    START_DATE = os.getenv("START_DATE") or "2000-01-01"
    END_DATE = os.getenv("END_DATE") or "2099-12-31"

    # OpenAI configuration
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")

    @classmethod
    def validate(cls):
        """
        Validates that essential configuration is present.
        """
        error_found = False
        if not cls.OPENAI_API_KEY:
            print("❌ ERROR: Missing OPENAI_API_KEY in .env file.")
            error_found = True
        
        if not cls.APP_ID:
            print("❌ ERROR: Missing APP_ID in .env file.")
            error_found = True
            
        if error_found:
            print("Please ensure your .env file is correctly configured.")
            return False
        return True
