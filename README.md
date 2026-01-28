# AutoGooglePlayAnalyzer 🚀
> 专业的 Google Play 评论自动化采集与智能审计系统
<div align="center">
  <!-- Placeholder for a logo if you have one, currently using a generic header style -->
  <h3>您的 APP 舆情分析智能中台</h3>
  <p>不仅仅是数据采集，更是将海量用户反馈转化为商业洞察的终极解决方案。</p>
  
  <p>
    <img src="https://img.shields.io/badge/Version-1.0.0-blue?style=flat-square" alt="Version">
    <img src="https://img.shields.io/badge/Language-Python-3776AB?style=flat-square" alt="Python">
    <img src="https://img.shields.io/badge/Database-PostgreSQL-336791?style=flat-square" alt="PostgreSQL">
    <img src="https://img.shields.io/badge/AI-OpenAI%20GPT--4o-412991?style=flat-square" alt="OpenAI">
    <img src="https://img.shields.io/badge/License-MIT-lightgrey?style=flat-square" alt="License">
  </p>

  <p>
    <a href="#-核心功能">核心功能</a> • 
    <a href="#-系统架构">系统架构</a> • 
    <a href="#-安装指南">安装指南</a> • 
    <a href="#-快速接入">快速接入</a>
  </p>
</div>

---

**AutoGooglePlayAnalyzer** 是一个为数据分析师和产品经理设计的全流程自动化工具。它将高并发爬虫、数据库持久化存储与大模型深度分析完美结合，为您提供一个稳定、极速且洞察深刻的 **本地舆情分析引擎**。

通过本应用，您可以将数万条非结构化的用户评论转化为结构化的商业审计报告，消除数据噪音，直击用户痛点。

## 🌟 深度功能解析 (Detailed Features)

### 1. 🕷️ 高性能数据采集 (Smart Crawler)
*   **全量抓取**: 支持递归分页抓取，轻松处理 10,000+ 级别的评论数据量。
*   **智能去重**: 基于数据库唯一约束的增量更新机制，确保数据永不重复，且不会覆盖历史记录。
*   **多维度元数据**: 采集内容涵盖评论正文、评分、时间、用户名称、点赞数等关键指标。

### 2. 💾 企业级数据持久化 (Persistence Layer)
*   **PostgreSQL 驱动**: 采用工业级关系型数据库，通过连接池 (Connection Pooling) 技术保障高并发写入的稳定性。
*   **增量更新**: 自动识别新评论，仅对增量数据进行入库，极大提升二次运行的效率。

### 3. 🧠 AI 驱动的 Map-Reduce 分析引擎 (Map-Reduce Engine)
*   **原子化标注 (Map)**: 并发调用 LLM 对每条评论进行多维度打标（用户画像、使用场景、核心痛点、功能缺陷）。
*   **全局洞察归纳 (Reduce)**: 自动汇总成千上万条标注数据，生成麦肯锡风格的 **深度商业审计报告**。
*   **多模态输出**: 支持生成标准 Markdown 报告以及**自动转换为 PDF**，方便团队传阅。

## 🏗️ 系统架构 (Architecture)

```mermaid
graph TD
    Source[Google Play Store] -->|Scraper| RawData[Raw Reviews]
    RawData -->|Data Pipeline| DB[(PostgreSQL)]
    DB -->|Batch Fetch| Analyzer[AI Analyzer Engine]
    Analyzer -->|Map: Embed & Tag| LLM[OpenAI GPT-4o]
    LLM -->|Reduce: Synthesize| Report[Audit Report (MD/PDF)]
    DB -->|Export| JSON[JSON Archives]
```

## 📁 项目结构 (Project Structure)

```text
/
├── src/                <-- [Core] 核心代码包
│   ├── config.py       # 配置管理
│   ├── database.py     # 数据库连接池
│   ├── scraper.py      # 爬虫逻辑
│   └── analyzer.py     # 分析逻辑
├── main.py             <-- [Entry] 抓取入口
├── analyzer.py         <-- [Entry] 分析入口
├── export_reviews.py   <-- [Entry] 导出入口
├── convert_report.py   <-- [Tool] PDF 转换工具
├── requirements.txt    <-- 依赖列表
├── reports/            <-- 产出报告
└── exports/            <-- 产出数据
```

## ⚙️ 环境配置 (Setup)

### 1. 基础环境
确保您已安装 Python 3.9+ 和 PostgreSQL。

### 2. 依赖安装
```bash
pip install -r requirements.txt
```

### 3. 环境变量 (.env)
在项目根目录创建 `.env` 文件：

```env
# 数据库配置
DB_NAME=google_play_analysis
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432

# 目标应用配置
APP_ID=com.example.app  # 目标 App 包名
COUNTRY=us              # 商店区域
LANGUAGE=en             # 评论语言

# 分析配置
OPENAI_API_KEY=sk-...   # OpenAI API Key
OPENAI_MODEL=gpt-4o     # 建议使用 GPT-4o 以获得最佳分析效果
SCRAPE_COUNT=1000       # 单次抓取数量
TOTAL_TO_ANALYZE=1000   # 分析样本数量
```

## 🚀 快速接入 (Quick Start)

### 第一步：全量抓取 (Ingestion)
从 Google Play 抓取最新评论并自动入库。
```bash
python main.py
```
> **提示**: 初次运行会自动初始化数据库表结构。日志 `pipeline.log` 会实时显示抓取进度。

### 第二步：深度审计 (Analysis)
调用 AI 引擎，生成商业洞察报告。
```bash
python analyzer.py
```
*   **产出**: `reports/` 目录下生成 `audit_xxx.md`。
*   **AI 思考**: 引擎会自动识别用户画像、高频场景及潜在 Bug。

### 第三步：结果交付 (Delivery)
将 Markdown 报告自动转换为 PDF，或导出原始数据。

**生成 PDF 报告**:
```bash
python convert_report.py
```

**导出原始 JSON**:
```bash
python export_reviews.py
```

## 📝 开发者与社区

*   **版本演进**:
    *   **v1.0.0 (2026-01-28)**:
        *   **[核心重构]**: 完成了项目的模块化重构，引入 `src/` 包结构。
        *   **[新增功能]**: 增加了 PDF 报告自动生成工具 (`convert_report.py`)。
        *   **[体验优化]**: 提供了详细的 `USER_GUIDE` 并整合至 README。

---

Copyright © 2026 AutoGooglePlayAnalyzer Team