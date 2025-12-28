import asyncio
import logging
import json
import os
from datetime import datetime
from collections import Counter
from typing import List, Dict, Any
from openai import AsyncOpenAI
from config import Config
from database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ReviewAnalyzer:
    """
    Advanced Review Analyzer: Data Annotation + Quantitative Statistics + Deep Business Audit.
    Optimized for High-Concurrency with AsyncIO.
    """

    def __init__(self):
        Config.validate()
        # Initialize AsyncOpenAI with 300s timeout as requested
        self.client = AsyncOpenAI(
            api_key=Config.OPENAI_API_KEY,
            base_url=Config.OPENAI_API_BASE,
            timeout=300.0
        )
        self.model = Config.OPENAI_MODEL

    def get_reviews_from_db(self, app_id: str, limit: int, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetches filtered reviews from the database (Synchronous DB call).
        """
        # Defensive check for dates
        start_date = start_date if start_date and start_date.strip() else "2000-01-01"
        end_date = end_date if end_date and end_date.strip() else "2099-12-31"

        query = """
            SELECT content, score, at 
            FROM google_play_reviews 
            WHERE app_id = %s 
              AND at >= %s 
              AND at <= %s 
            ORDER BY at DESC 
            LIMIT %s
        """
        conn = DatabaseManager.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (app_id, start_date, end_date, limit))
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                logger.info(f"Retrieved {len(results)} reviews from DB for period {start_date} to {end_date}.")
                return results
        finally:
            DatabaseManager.release_connection(conn)

    async def process_batch(self, reviews_batch: List[Dict], batch_id: int) -> List[Dict]:
        """
        Map Phase: Atomic annotation of each review using JSON Mode (Asynchronous).
        """
        reviews_text = ""
        for idx, r in enumerate(reviews_batch):
            reviews_text += f"ID: {idx} | Content: {r['content'][:300]}\n"

        prompt = """
        [Role]: Senior Product Auditor & User Researcher.
        [Task]: Atomic annotation of the following user reviews into structured JSON.
        
        [Annotation Schema]:
        For EACH review, return an object with:
        - "u" (Usage): Main use case (e.g., "Calorie Tracking", "AI Identification", "Social Sharing").
        - "p" (Persona): User identity category (e.g., "Paid Subscriber", "Newbie", "Tech Enthusiast").
        - "c" (Con_Type): Defect category (e.g., "Algorithm Failure", "HCI Issue", "Payment Bug", "Performance Lag", "None").
        - "s" (Sample_Quote): Most representative short quote (max 20 words).

        [Output Format]: 
        Return a JSON object containing a list called "annotations".
        Example: {"annotations": [{"u": "...", "p": "...", "c": "...", "s": "..."}, ...]}
        
        [Constraint]:
        - Be objective. 
        - If unclear, use "Unknown".
        - Use Chinese for usage and persona descriptions.
        """

        try:
            logger.info(f"Starting async labeling for batch {batch_id}...")
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that outputs JSON."},
                    {"role": "user", "content": f"{prompt}\n\n[Data]:\n{reviews_text}"}
                ],
                response_format={"type": "json_object"},
                temperature=0.1
            )
            raw_json = response.choices[0].message.content
            parsed = json.loads(raw_json)
            annotations = parsed.get("annotations", [])
            logger.info(f"Completed batch {batch_id} with {len(annotations)} annotations.")
            return annotations
        except Exception as e:
            logger.error(f"Error in JSON batch processing (Batch {batch_id}): {e}")
            return []

    def aggregate_stats(self, all_annotations: List[Dict]) -> Dict[str, Any]:
        """
        Python Layer: Quantitative statistics using collections.Counter.
        """
        total = len(all_annotations)
        if total == 0:
            return {}

        usage_counts = Counter(a.get("u", "Unknown") for a in all_annotations)
        persona_counts = Counter(a.get("p", "Unknown") for a in all_annotations)
        con_counts = Counter(a.get("c", "Unknown") for a in all_annotations if a.get("c") != "None")
        
        # Collect sample quotes for each defect type
        evidence = {}
        for a in all_annotations:
            c_type = a.get("c")
            if c_type and c_type != "None" and c_type != "Unknown":
                if c_type not in evidence:
                    evidence[c_type] = []
                if len(evidence[c_type]) < 3: # Keep top 3 samples
                    evidence[c_type].append(a.get("s", ""))

        def to_pct(counts):
            return {k: {"count": v, "percent": f"{(v/total)*100:.1f}%"} for k, v in counts.most_common(10)}

        return {
            "total_samples": total,
            "usage_stats": to_pct(usage_counts),
            "persona_stats": to_pct(persona_counts),
            "con_stats": to_pct(con_counts),
            "evidence": evidence
        }

    async def generate_final_report(self, stats: Dict[str, Any]) -> str:
        """
        Reduce Phase: Final synthesized business audit report (Asynchronous).
        """
        stats_json = json.dumps(stats, ensure_ascii=False, indent=2)
        evidence_json = json.dumps(stats.get("evidence", {}), ensure_ascii=False, indent=2)
        
        prompt = f"""
你是一位资深的高级产品研究员和数据科学家。现在，请基于 Python 后端统计出的量化数据 {{stats_summary}} 和 原始证据样本 {{samples}}，生成一份结构极其严谨的产品分析报告名。

**数据源：**
- 量化统计 (stats_summary): {stats_json}
- 原始证据 (samples): {evidence_json}

**报告生成准则：**
1. **分类定义化**：每个核心维度（用途、画像、优缺点、需求）必须先给出“定义”和“包含内容”，再展示数据。
2. **数据表格化**：必须使用 Markdown 表格展示每个维度的“计次”和“比例”。
3. **结论层级化**：每个维度的数据后，必须通过“主要结论”和“次要结论”进行深度解读。
4. **证据关联化**：在“用户需求”章节，必须严格对应原始文档中的原声引用（Original Quotes）。

**报告大纲结构：**

## 1. 用户使用用途分析 (Usage Analysis)
- **分类标准定义**：将识别出的 Top 5-8 个用途进行定义。例如：[编程辅助、学习教育、日常管理...]
- **数据统计表**：展示 | 用途分类 | 数量 | 比例 |
- **主要结论**：分析占比最高的 2-3 个核心用途及其代表的价值。
- **次要结论**：分析占比低但具有增长潜力的细分领域。

## 2. 用户画像分析 (User Persona)
- **人群分类定义**：描述识别出的用户身份类别（如技术从业者、学生、职场人士等）。
- **数据统计表**：展示 | 用户画像 | 数量 | 比例 |
- **核心洞察**：分析主导人群特征及其对产品的期望差异。

## 3. 优缺点深度总结 (Pros & Cons Synthesis)
- **优点分析**：列出 Top 3 核心优势，给出定义并配以数据比例。
- **缺点审计**：将技术缺陷分类（如算法幻觉、HCI问题、性能瓶颈），列出 | 缺点分类 | 数量 | 比例 |。
- **关联分析**：分析缺点如何影响了特定用户画像的留存。

## 4. 潜在用户需求提取 (Latent Requirements)
- **分项需求描述**：对每个需求类别（如工具优化、内容生成、可靠性提升）进行详细描述。
- **原始文档体现**：**[核心要求]** 必须在此处插入具体的 {{samples}} 中的原始用户评论作为证据，格式为："> 用户提到：‘...’"。
- **需求数据表**：展示 | 可能的需求分类 | 数量 | 比例 |

## 5. 总结与优化建议
- 结合优缺点和需求，给出基于数据优先级的优化路线图。

**语气要求：** 极其学术、严谨、客观，严禁使用“我认为”、“可能”等虚词，必须基于提供的统计数据进行陈述。用中文回复。
"""
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error in final report generation: {e}")
            return "Failed to generate final report."

    async def run_analysis(self):
        """
        Full Execution Pipeline (Asynchronous).
        """
        logger.info(f"--- Starting Advanced Business Audit for {Config.APP_ID} ---")
        
        # 1. Fetch filtered reviews (Synchronous)
        reviews = self.get_reviews_from_db(
            Config.APP_ID, 
            Config.TOTAL_TO_ANALYZE, 
            Config.START_DATE, 
            Config.END_DATE
        )
        
        if not reviews:
            logger.warning("No data found to analyze.")
            return

        # 2. Map Phase (Atomic Annotation with Concurrency)
        logger.info(f"Preparing to process {len(reviews)} reviews in {len(reviews)//Config.BATCH_SIZE + 1} batches concurrently...")
        tasks = []
        batch_size = Config.BATCH_SIZE
        for i in range(0, len(reviews), batch_size):
            batch = reviews[i:i+batch_size]
            batch_id = (i // batch_size) + 1
            tasks.append(self.process_batch(batch, batch_id))

        # Start all tasks concurrently
        results = await asyncio.gather(*tasks)
        
        all_annotations = []
        for res in results:
            all_annotations.extend(res)

        # 3. Quant Phase (Statistics)
        logger.info("Calculating quantitative metrics...")
        stats = self.aggregate_stats(all_annotations)
        
        # 4. Reduce Phase (Synthesis)
        logger.info("Synthesizing final report...")
        final_report = await self.generate_final_report(stats)
        
        # 5. Save Report
        if not os.path.exists("reports"):
            os.makedirs("reports")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/audit_{Config.APP_ID.replace('.', '_')}_{timestamp}.md"
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(final_report)
            
        logger.info(f"Audit Complete! Report saved: {filename}")
        print("\n" + "★"*30 + "\nAUDIT REPORT SUMMARY\n" + "★"*30)
        print(final_report[:800] + "...")

async def main():
    analyzer = ReviewAnalyzer()
    await analyzer.run_analysis()

if __name__ == "__main__":
    asyncio.run(main())
