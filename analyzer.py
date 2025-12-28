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
你是一位顶级战略咨询顾问（如麦肯锡级别）兼资深数据科学家。请基于以下原始数据，产出具有深度商业洞察的产品审计报告。

**数据源存根：**
- 量化统计数据: {stats_json}
- 原始用户证言: {evidence_json}

**核心重构准则（严格执行）：**
1. **分类标准专业化**：严禁使用“其他”、“未知”作为分析重点。必须基于业务逻辑对类别进行定义，阐述其背后的【用户动机】。
2. **数据对比分析**：在展示表格时，不仅要展示比例，还要对比各维度间的相关性（例如：特定画像用户是否更倾向于反馈特定缺陷）。
3. **结论穿透力**：结论必须包含【现状 -> 风险点 -> 商业价值影响力】的逻辑链条，避免平铺直叙。
4. **原声证据锚定**：每一条核心结论下方，必须引用 2-3 条最尖锐、最具代表性的原始证言作为“不可辩驳的证据”。

---

**报告结构指令：**

## 1. 核心应用场景分析 (Critical Usage Scenarios)
- **维度定义**：采用 MECE 原则对识别出的前 8 个用途进行定义，解释该用途满足了用户的哪些底层需求。
- **量化分布表**：展示 | 场景类别 | 计次 | 比例 | 需求强度评分(1-5) |
- **主要洞察**：分析核心用途的“黏性”来源。
- **边际发现**：识别占比虽小但反映“极客用户”或“痛点爆发”的新兴场景。

## 2. 用户权力画像与分层 (User Segmentation & Persona Power)
- **人群画像建模**：不仅描述身份，还要描述其“对产品的依赖程度”和“对缺陷的容忍度”。
- **量化分布表**：展示 | 核心画像 | 数量 | 比例 | 核心关注点 |
- **交叉洞察**：分析主导人群（如普通用户）与核心功能之间的匹配失调风险。

## 3. 产品体验赤字审计 (Experience Deficit Audit)
- **技术缺陷矩阵**：严格区分 [HCI 交互、算法可靠性、系统性能、连接稳定性]。展示 | 缺陷分类 | 频率 | 影响程度(P0-P2) |
- **穿透式分析**：深挖缺陷背后的技术债。例如：某项算法故障是否正在摧毁高价值长期用户的信任？
- **证据存证**：在此处密集插入 {{samples}} 中的原声。

## 4. 潜在需求挖掘与商业机会 (Strategic Opportunities)
- **潜在需求映射**：将用户抱怨转化为未被满足的功能需求。
- **需求优先级矩阵**：展示 | 需求描述 | 原始用户证据引用 | 商业紧迫性 |
- **Gap Analysis**：分析现有功能与用户期望之间的断层。

## 5. 战略 Roadmap 与行动建议
- **执行优先级**：基于【修复成本 vs 留存价值】给出 P0/P1/P2 建议。
- **技术建议**：针对发现的问题提出具体的工程改进方向。

**语气要求：** 极其冷峻、犀利、结构化。禁止任何含糊词汇。用中文回复。
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
