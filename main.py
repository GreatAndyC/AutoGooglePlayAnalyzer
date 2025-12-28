import logging
from config import Config
from database import DatabaseManager
from scraper import GooglePlayScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline(target_count: int = 10000):
    """
    Main pipeline to scrape Google Play reviews and save them to PostgreSQL.
    """
    logger.info("--- Starting AutoGooglePlayAnalyzer Pipeline ---")
    
    # 1. Initialize Database
    try:
        logger.info("Initializing database and ensuring tables exist...")
        DatabaseManager.create_tables()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return

    # 2. Scrape Data
    # We scrape in the script and then pass to DB manager
    # For a robust production system, we might insert in batches during scraping,
    # but for now we'll follow the Phase 3 requirement of integration.
    try:
        scraper = GooglePlayScraper(app_id=Config.APP_ID)
        reviews_data = scraper.fetch_reviews(target_count=target_count, batch_size=150)
        
        if not reviews_data:
            logger.warning("No reviews were fetched. Exiting pipeline.")
            return

        # 3. Store in Database
        logger.info(f"Starting database insertion for {len(reviews_data)} reviews...")
        DatabaseManager.insert_reviews(reviews_data, Config.APP_ID)
        
    except Exception as e:
        logger.error(f"An error occurred during the pipeline execution: {e}")
    finally:
        # 4. Cleanup
        DatabaseManager.close_all_connections()
        logger.info("--- Pipeline Execution Finished ---")

if __name__ == "__main__":
    # You can adjust the target_count here. 
    # Starting with 1000 for a solid initial run, change to 10000 for full scale.
    run_pipeline(target_count=1000)
