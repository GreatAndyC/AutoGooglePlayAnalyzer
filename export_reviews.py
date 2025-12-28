import json
import os
import logging
from datetime import datetime
from config import Config
from database import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def export_reviews_to_json():
    """
    Exports reviews from the database view 'view_vesync_latest' to a JSON file.
    """
    # 1. Database Connection
    conn = None
    try:
        conn = DatabaseManager.get_connection()
        with conn.cursor() as cursor:
            # 2. Target Query
            # The user requested fields: content, score, at, userName
            # We use Config.TOTAL_TO_ANALYZE for the limit
            limit = Config.TOTAL_TO_ANALYZE
            query = f"""
                SELECT 
                    content, 
                    score, 
                    at, 
                    user_name AS "userName" 
                FROM google_play_reviews 
                WHERE app_id = 'com.etekcity.vesyncplatform'
                ORDER BY at DESC 
                LIMIT {limit}
            """
            
            logger.info("Executing export query on 'view_vesync_latest'...")
            cursor.execute(query)
            
            # Fetch all results
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # 3. Data Preprocessing
            export_data = []
            for row in rows:
                item = dict(zip(columns, row))
                # Convert datetime to ISO format
                if isinstance(item.get('at'), datetime):
                    item['at'] = item['at'].isoformat()
                export_data.append(item)
            
            # Prepare result structure
            result = {
                "metadata": {
                    "app_id": Config.APP_ID,
                    "export_at": datetime.now().isoformat(),
                    "total_count": len(export_data)
                },
                "data": export_data
            }
            
            # 4. File Output
            export_dir = "exports"
            if not os.path.exists(export_dir):
                os.makedirs(export_dir)
                logger.info(f"Created directory: {export_dir}")

            output_filename = os.path.join(export_dir, "vesync_raw_audit_data.json")
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Successfully exported {len(export_data)} reviews to {output_filename}")
            
    except Exception as e:
        logger.error(f"Failed to export reviews: {e}")
    finally:
        # 5. Exception Handling: Release connection
        if conn:
            DatabaseManager.release_connection(conn)
            DatabaseManager.close_all_connections()

if __name__ == "__main__":
    export_reviews_to_json()
