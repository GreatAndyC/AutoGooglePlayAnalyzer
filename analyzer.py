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
        [Role]: Data Scientist / HCI Researcher
        [Task]: Perform a high-granularity technical audit on {len(reviews_batch)} user reviews for Instagram.
        
        [Input Data]:
        {reviews_text}
        
        [Analysis Framework]: For EACH review, identify the following 5 dimensions:
        1. **Usage (用途)**: Specific use case (e.g., Content Creation, Ad Management, Social Interaction).
        2. **Persona (画像)**: Who is this user? (e.g., Professional Creator, General User, Tech Developer, Business Owner).
        3. **Pros (优点)**: Concrete technical or UX strengths.
        4. **Cons (缺点)**: Specific bugs, latencies, UI failures, or algorithmic biases.
        5. **Needs (需求)**: Explicit or latent user requirements (e.g., better API, improved upload quality).
        
        [Constraint]: 
        - If a dimension is missing, mark as "Unknown". 
        - Be objective and technical. 
        - Provide a aggregated summary of these 5 dimensions for this batch.
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
        [Objective]: Synthesize Batch Summaries into a formal Research Paper Section (Chapter 3.2: User Needs and Market Feedback).
        
        [Source Summaries]:
        {combined_summaries}
        
        [Required Output Structure]:
        # 3.2 User Requirement Analysis
        
        ## 3.2.1 Core Usage Patterns (用途分析)
        - Categorize findings into technical domains (e.g., Content Creation, Connectivity, Privacy).
        - Provide **Primary Conclusions** and **Secondary Conclusions**.
        
        ## 3.2.2 User Personas (用户画像)
        - Detail the distribution of professional vs. general users.
        - Analyze the research significance of the dominant groups.
        
        ## 3.2.3 Pros & Cons Synthesis (优缺点总结)
        - **Pros**: Focus on system strengths and user experience highlights.
        - **Cons**: Categorize into "Systemic Failures", "HCI Issues", and "Performance Bottlenecks".
        
        ## 3.2.4 Future Technical Requirements (用户需求)
        - Strategic optimization directions for the engineering team.
        - Link these needs to potential PhD-level research questions (e.g., scalability, data integrity).
        
        [Tone]: Academic, rigorous, data-driven. Avoid fluff.
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

    def run_analysis(self, app_id: str, total_to_analyze: int = 1000, batch_size: int = 200):
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
