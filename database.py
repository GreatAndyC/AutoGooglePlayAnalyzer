import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from config import Config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages PostgreSQL database connections and operations.
    Supports connection pooling for efficiency.
    """
    _connection_pool = None

    @classmethod
    def initialize_pool(cls):
        """
        Initializes the connection pool using configuration from Config class.
        """
        if cls._connection_pool is None:
            try:
                cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                    1, 20,
                    user=Config.DB_USER,
                    password=Config.DB_PASSWORD,
                    host=Config.DB_HOST,
                    port=Config.DB_PORT,
                    database=Config.DB_NAME
                )
                logger.info("Database connection pool initialized successfully.")
            except (Exception, psycopg2.DatabaseError) as error:
                logger.error(f"Error while connecting to PostgreSQL: {error}")
                raise

    @classmethod
    def get_connection(cls):
        """
        Retrieves a connection from the pool.
        """
        if cls._connection_pool is None:
            cls.initialize_pool()
        return cls._connection_pool.getconn()

    @classmethod
    def release_connection(cls, conn):
        """
        Returns a connection back to the pool.
        """
        if cls._connection_pool:
            cls._connection_pool.putconn(conn)

    @classmethod
    def create_tables(cls):
        """
        Creates the google_play_reviews table if it doesn't exist.
        """
        query = """
        CREATE TABLE IF NOT EXISTS google_play_reviews (
            id SERIAL PRIMARY KEY,
            review_id VARCHAR(255) UNIQUE NOT NULL,
            user_name TEXT,
            user_image TEXT,
            content TEXT,
            score INTEGER,
            thumbs_up_count INTEGER,
            review_created_version TEXT,
            at TIMESTAMP,
            reply_content TEXT,
            replied_at TIMESTAMP,
            app_id VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_app_id ON google_play_reviews(app_id);
        CREATE INDEX IF NOT EXISTS idx_review_at ON google_play_reviews(at);
        """
        conn = cls.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
                logger.info("Table 'google_play_reviews' checked/created successfully.")
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            logger.error(f"Error creating table: {error}")
        finally:
            cls.release_connection(conn)

    @classmethod
    def insert_reviews(cls, reviews, app_id):
        """
        Batch inserts reviews into the database using execute_values.
        
        Args:
            reviews (list): A list of dictionary objects from google-play-scraper.
            app_id (str): The ID of the app being scraped.
        """
        query = """
        INSERT INTO google_play_reviews (
            review_id, user_name, user_image, content, score, 
            thumbs_up_count, review_created_version, at, 
            reply_content, replied_at, app_id
        ) VALUES %s
        ON CONFLICT (review_id) DO NOTHING;
        """
        
        # Transform the list of dicts into a list of tuples for execute_values
        data = [
            (
                r.get('reviewId'),
                r.get('userName'),
                r.get('userImage'),
                r.get('content'),
                r.get('score'),
                r.get('thumbsUpCount'),
                r.get('reviewCreatedVersion'),
                r.get('at'),
                r.get('replyContent'),
                r.get('repliedAt'),
                app_id
            )
            for r in reviews
        ]

        conn = cls.get_connection()
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, query, data)
                conn.commit()
                logger.info(f"Successfully inserted {len(data)} reviews (excluding duplicates).")
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            logger.error(f"Error inserting reviews: {error}")
        finally:
            cls.release_connection(conn)

    @classmethod
    def close_all_connections(cls):
        """
        Closes all connections in the pool.
        """
        if cls._connection_pool:
            cls._connection_pool.closeall()
            logger.info("Database connection pool closed.")

if __name__ == "__main__":
    # Test table creation
    try:
        DatabaseManager.create_tables()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
    finally:
        DatabaseManager.close_all_connections()
