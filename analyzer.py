import logging
import json
from typing import List, Dict
from openai import OpenAI
from config import Config
from database import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReviewAnalyzer:
    """
    Analyzes Google Play reviews using LLM (GPT) with a Map-Reduce approach.
    """

    def __init__(self):
        self.client = OpenAI(
            api_key=Config.OPENAI_API_KEY,
            base_url=Config.OPENAI_API_BASE
        )
        self.model = Config.OPENAI_MODEL

    def get_reviews_from_db(self, app_id: str, limit: int = 1000) -> List[Dict]:
        """
        Fetches reviews for analysis from the database.
        """
        query = "SELECT content, score FROM google_play_reviews WHERE app_id = %s LIMIT %s"
        conn = DatabaseManager.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (app_id, limit))
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            DatabaseManager.release_connection(conn)

    def process_batch(self, reviews_batch: List[Dict]) -> str:
        """
        Phase 1 (Map): Summarize keywords and pain points for a small batch of reviews.
        """
        reviews_text = "\n".join([f"- [{r['score']} stars] {r['content']}" for r in reviews_batch])
        
        prompt = f"""
        You are a product expert. Analyze the following app reviews:
        
        {reviews_text}
        
        Provide a concise summary of:
        1. Top 3 Keywords/Themes
        2. Primary User Pain Points
        3. Feature Requests (if any)
        
        Format the output clearly as a short list.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            return ""

    def generate_final_report(self, intermediate_summaries: List[str]) -> str:
        """
        Phase 2 (Reduce): Synthesize all intermediate summaries into a final report.
        """
        combined_summaries = "\n\n---\n\n".join(intermediate_summaries)
        
        prompt = f"""
        You are a Senior Product Strategist. Below are summarized insights from thousands of user reviews across multiple batches:
        
        {combined_summaries}
        
        Based on this data, generate a Hardcore Product Analysis Report including:
        1. Executive Summary: What is the overall sentiment and market position?
        2. Critical Issues: What are the deal-breaking bugs or UX failures?
        3. Strategic Recommendations: What 3 things should the product team do IMMEDIATELY to improve NPS?
        4. Competitive Edge: What do users love that should be doubled down on?
        
        Format the report in professional Markdown.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error in final report generation: {e}")
            return "Failed to generate final report."

    def run_analysis(self, app_id: str, total_to_analyze: int = 1000, batch_size: int = 50):
        """
        Full analysis pipeline: Batch Fetch -> Map -> Reduce.
        """
        logger.info(f"Starting analysis for App: {app_id}")
        
        # 1. Fetch data
        all_reviews = self.get_reviews_from_db(app_id, limit=total_to_analyze)
        if not all_reviews:
            logger.warning("No reviews found in database for this app.")
            return
        
        logger.info(f"Analyzing {len(all_reviews)} reviews in batches of {batch_size}...")

        # 2. Map Phase
        intermediate_summaries = []
        for i in range(0, len(all_reviews), batch_size):
            batch = all_reviews[i:i+batch_size]
            logger.info(f"Processing batch {(i//batch_size)+1}/{(len(all_reviews)//batch_size)+1}...")
            summary = self.process_batch(batch)
            if summary:
                intermediate_summaries.append(summary)

        # 3. Reduce Phase
        logger.info("Generating final product analysis report...")
        final_report = self.generate_final_report(intermediate_summaries)
        
        # 4. Save/Output Report
        import os
        from datetime import datetime
        
        if not os.path.exists("reports"):
            os.makedirs("reports")
            logger.info("Created 'reports' directory.")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"reports/analysis_{app_id.replace('.', '_')}_{timestamp}.md"
        
        with open(report_filename, "w") as f:
            f.write(final_report)
        
        logger.info(f"Analysis complete! Full report saved to {report_filename}")
        print("\n" + "="*50 + "\nFINAL REPORT PREVIEW (First 1000 chars)\n" + "="*50)
        print(final_report[:1000] + "...") 

if __name__ == "__main__":
    analyzer = ReviewAnalyzer()
    analyzer.run_analysis(app_id=Config.APP_ID, total_to_analyze=500) # Test with 500 reviews
