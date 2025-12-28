# AutoGooglePlayAnalyzer 🚀

An engineering-centric data pipeline designed to extract and synthesize large-scale user feedback from the Google Play Store using **PostgreSQL** and **LLM-based Map-Reduce architecture**.

## 🧬 System Architecture
The system is built with modularity and scalability in mind:
1. **Ingestion Layer**: Recursive crawler with pagination support for stable extraction of 10,000+ reviews.
2. **Persistence Layer**: High-performance storage using PostgreSQL with connection pooling.
3. **Intelligence Layer**: A Map-Reduce analysis engine that batches unstructured text for recursive summarization via GPT-4o.

## 🛠️ Tech Stack
- **Language**: Python 3.9+
- **Database**: PostgreSQL
- **LLM**: OpenAI GPT-4o / GPT-4o-mini
- **Infrastructure**: Git (SSH/HTTPS), Dotenv for secure config management.

## 📊 Sample Insights (From our AI Engine)
*Currently analyzing user pain points in Robotics (HCI) and Digital Health applications to identify algorithmic edge cases.*