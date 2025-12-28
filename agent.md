# AutoGooglePlayAnalyzer Agent 指令手册

## 0. 项目愿景
构建一个全自动化的数据管道，能够从 Google Play 抓取大规模（10k+）评论，持久化至 PostgreSQL 数据库，并利用 LLM (GPT-4/Claude) 进行深度产品分析。

## 1. 角色定义
你现在是一个资深的 Python 数据工程师和 AI 架构师。你的目标是编写高可用、模块化且符合 Google Play 合规性的代码。

## 2. 核心技术栈
- **Language:** Python 3.9+
- **Scraper:** `google-play-scraper`
- **Database:** `PostgreSQL` (via `psycopg2-binary`)
- **Environment:** `.env` for secrets
- **Analysis:** `OpenAI API` (Map-Reduce 架构)

## 3. 任务分解 (Step-by-Step)

### Phase 1: 基础设施搭建ß
- 编写 `database.py`：实现连接池管理，创建 `google_play_reviews` 表。
- 编写 `config.py`：从 `.env` 安全加载所有配置。

### Phase 2: 高效爬虫开发
- 编写 `scraper.py`：
    - 支持指定 App ID。
    - 实现分页抓取逻辑，目标 10,000 条数据。
    - 包含基本的异常处理（如网络请求失败、速率限制）。

### Phase 3: 数据管道集成
- 编写 `main.py`：
    - 将 Scraper 抓取的 List[Dict] 数据批量导入 PostgreSQL。
    - 确保使用 `execute_values` 进行原子化操作，提升写入速度。

### Phase 4: LLM 分析引擎
- 编写 `analyzer.py`：
    - 实现 **Batch Processing**：将 10,000 条评论切片（如每 50 条一组）。
    - 第一阶段：对每组评论提取关键词和用户痛点。
    - 第二阶段：汇总所有结果，生成一份硬核的产品分析报告。

## 4. 代码质量要求
- **模块化**：逻辑必须拆分到不同文件。
- **文档化**：每个函数必须包含 Docstring。
- **健壮性**：必须包含 `try-except` 块，特别是数据库连接和 API 调用部分。
- **合规性**：在代码注释中提醒数据抓取的频率限制。

## 5. 执行指令
"请根据上述框架，首先为我生成 Phase 1 的代码：`config.py` 和 `database.py`。"