import time
import logging
from typing import List, Dict, Optional, Tuple
from google_play_scraper import reviews, Sort
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GooglePlayScraper:
    """
    Scraper class to fetch reviews from Google Play Store efficiently.
    Supports pagination via continuation_token to fetch large datasets.
    
    Country Reference (Common abbreviations):
    - us: United States
    - gb: United Kingdom
    - ca: Canada
    - au: Australia
    - de: Germany
    - fr: France
    - jp: Japan
    - kr: South Korea
    - in: India
    - br: Brazil
    """

    def __init__(self, app_id: str, lang: str = None, country: str = None):
        self.app_id = app_id
        self.lang = lang or Config.LANGUAGE
        self.country = country or Config.COUNTRY

    def fetch_reviews(self, target_count: int = None, batch_size: int = 200) -> List[Dict]:
        """
        Fetches a target number of reviews using pagination.
        
        Args:
            target_count (int): Total number of reviews to fetch.
            batch_size (int): Number of reviews per request (max 199 for google-play-scraper).
            
        Returns:
            List[Dict]: List of review dictionaries.
        """
        target_count = target_count or Config.SCRAPE_COUNT
        all_reviews = []
        continuation_token = None
        
        logger.info(f"Starting to fetch {target_count} reviews for App ID: {self.app_id} [Country: {self.country}, Lang: {self.lang}]")
        
        try:
            while len(all_reviews) < target_count:
                # Calculate how many more reviews we need
                remaining = target_count - len(all_reviews)
                current_batch_count = min(batch_size, remaining)

                # Fetch reviews
                result, token = reviews(
                    self.app_id,
                    lang=self.lang,
                    country=self.country,
                    sort=Sort.NEWEST,
                    count=current_batch_count,
                    continuation_token=continuation_token
                )

                if not result:
                    logger.warning("No more reviews found before reaching the target count.")
                    break

                all_reviews.extend(result)
                continuation_token = token
                
                # Progress update
                logger.info(f"Progress: {len(all_reviews)}/{target_count} reviews fetched.")

                # Ethical scraping: add a small delay to avoid rate limiting
                if continuation_token:
                    time.sleep(1)
                else:
                    logger.info("Reached the end of available reviews.")
                    break

        except Exception as e:
            logger.error(f"An error occurred during scraping: {e}")
            # Return what we've collected so far instead of crashing
        
        logger.info(f"Successfully fetched a total of {len(all_reviews)} reviews.")
        return all_reviews

if __name__ == "__main__":
    # Test execution
    scraper = GooglePlayScraper(app_id=Config.APP_ID)
    # Testing with a smaller number first to verify logic
    test_reviews = scraper.fetch_reviews(target_count=200, batch_size=100)
    print(f"Sample review content: {test_reviews[0]['content'] if test_reviews else 'No reviews'}")
