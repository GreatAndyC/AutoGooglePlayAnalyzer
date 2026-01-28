# AutoGooglePlayAnalyzer 使用指南 (User Guide)

本项目是一个基于 Python 和 OpenAI 的 Google Play 应用评论自动化采集与分析工具。它可以自动抓取评论、存入数据库、导出数据并生成深度的商业洞察报告。

## 📁 项目结构

*   `main.py`: **主要入口**。用于执行数据抓取并将评论存入 PostgreSQL 数据库。
*   `analyzer.py`: **分析引擎**。读取数据库中的评论，调用 OpenAI 进行深度分析，并生成 Markdown 报告。
*   `export_reviews.py`: **数据导出**。将数据库中的最新评论导出为 JSON 格式，便于人工查看或存档。
*   `config.py`: 配置管理。从 `.env` 文件加载环境变量。
*   `database.py`: 数据库连接与操作管理。
*   `reports/`: 存放生成的分析报告。
*   `exports/`: 存放导出的原始数据 JSON 文件。

## ⚙️ 环境配置 (Setup)

在运行项目之前，请确保根目录下的 `.env` 文件已正确配置：

```env
# 数据库配置
DB_NAME=google_play_analysis
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432

# 目标应用配置
APP_ID=com.example.app  # 你要分析的 Google Play包名 (例如 com.etekcity.vesyncplatform)
COUNTRY=us              # 商店区域
LANGUAGE=en             # 评论语言

# 抓取配置
SCRAPE_COUNT=1000       # 每次运行抓取的目标数量

# 分析配置
OPENAI_API_KEY=sk-...   # OpenAI API Key
OPENAI_MODEL=gpt-4o     # 使用的模型
TOTAL_TO_ANALYZE=1000   # 参与分析的评论数量 (最新)
BATCH_SIZE=50           # 并发处理的批次大小
Config.START_DATE=2024-01-01 # (可选) 分析起始日期
```

## 🚀 使用步骤 (Workflow)

### 第一步：抓取评论 (Scraping)

运行 `main.py` 从 Google Play Store 抓取最新评论并存入数据库。

```bash
python main.py
```

*   **功能**：初始化数据库表（如果不存在），抓取指定数量的评论，去重后存入数据库。
*   **日志**：查看 `pipeline.log` 了解详细进度。

### 第二步：导出数据 (Exporting) - 可选

如果你需要查看原始数据，可以运行导出脚本。

```bash
python export_reviews.py
```

*   **输出**：会在 `exports/` 目录下生成类似 `raw_data_{app_id}_{date}.json` 的文件。
*   **内容**：包含评论内容、评分、时间和用户名。

### 第三步：生成分析报告 (Analysis)

运行 `analyzer.py` 使用 LLM 进行深度商业分析。

```bash
python analyzer.py
```

*   **流程**：
    1.  从数据库读取最新的 `TOTAL_TO_ANALYZE` 条评论。
    2.  **Map 阶段**：并发调用 OpenAI 对评论进行“原子化标注”（识别用户画像、用途、缺陷类型）。
    3.  **Reduce 阶段**：汇总统计数据，生成一份麦肯锡风格的深度商业审计报告。
*   **输出**：报告保存在 `reports/` 目录，文件名为 `audit_{app_id}_{date}.md`。
*   **日志**：查看 `analysis.log` 了解分析进度。

## 📊 常见问题 (Troubleshooting)

1.  **数据库连接失败**：
    *   检查 Docker 中 PostgreSQL 是否启动。
    *   检查 `.env` 中的 `DB_PASSWORD` 是否正确。
2.  **OpenAI API 错误**：
    *   确保 `.env` 中 `OPENAI_API_KEY` 有效且有额度。
    *   如果是国内网络，可能需要配置 `OPENAI_API_BASE`。
3.  **抓取数量为 0**：
    *   检查 `APP_ID` 是否正确。
    *   可能是网络原因导致连接 Google Play 失败，请检查网络设置。
