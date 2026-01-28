from flask import Flask, render_template_string, jsonify, request, Response
import subprocess
import os
import json
import webbrowser
import threading
import time
from datetime import datetime
from src.config import Config
from src.database import DatabaseManager
from src.scraper import GooglePlayScraper

app = Flask(__name__)

# Config file path for persistence
CONFIG_FILE = os.path.join(os.path.dirname(__file__), '.dashboard_config.json')
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# Runtime config (can be modified via UI)
def load_config():
    """Load config from file or use defaults"""
    default_config = {
        'app_id': Config.APP_ID,
        'country': Config.COUNTRY,
        'language': Config.LANGUAGE,
        'scrape_count': Config.SCRAPE_COUNT or 1000,
        'analyze_count': Config.TOTAL_TO_ANALYZE or 500,
        'batch_count': 5,  # 批次数量：分成几次发给AI，推荐5-10次
        'openai_api_key': Config.OPENAI_API_KEY or '',
        'openai_model': Config.OPENAI_MODEL or 'deepseek-chat',
        'openai_api_base': Config.OPENAI_API_BASE or 'https://api.deepseek.com'
    }
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
                default_config.update(saved)
        except:
            pass
    return default_config

def save_config(config):
    """Save config to file"""
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    except:
        pass

runtime_config = load_config()

# Google Play supported countries and languages
COUNTRIES = [
    ('us', '🇺🇸 United States'),
    ('cn', '🇨🇳 China'),
    ('jp', '🇯🇵 Japan'),
    ('kr', '🇰🇷 South Korea'),
    ('gb', '🇬🇧 United Kingdom'),
    ('de', '🇩🇪 Germany'),
    ('fr', '🇫🇷 France'),
    ('in', '🇮🇳 India'),
    ('br', '🇧🇷 Brazil'),
    ('ru', '🇷🇺 Russia'),
    ('au', '🇦🇺 Australia'),
    ('ca', '🇨🇦 Canada'),
    ('es', '🇪🇸 Spain'),
    ('it', '🇮🇹 Italy'),
    ('mx', '🇲🇽 Mexico'),
    ('tw', '🇹🇼 Taiwan'),
    ('hk', '🇭🇰 Hong Kong'),
    ('sg', '🇸🇬 Singapore'),
    ('id', '🇮🇩 Indonesia'),
    ('th', '🇹🇭 Thailand'),
]

LANGUAGES = [
    ('en', 'English'),
    ('zh-CN', '简体中文'),
    ('zh-TW', '繁體中文'),
    ('ja', '日本語'),
    ('ko', '한국어'),
    ('de', 'Deutsch'),
    ('fr', 'Français'),
    ('es', 'Español'),
    ('pt', 'Português'),
    ('ru', 'Русский'),
    ('it', 'Italiano'),
    ('ar', 'العربية'),
    ('hi', 'हिन्दी'),
    ('th', 'ไทย'),
    ('vi', 'Tiếng Việt'),
]

# HTML Template
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AutoGooglePlayAnalyzer Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/font/bootstrap-icons.css" rel="stylesheet">
    <style>
        body { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; }
        .dashboard-card { background: rgba(255,255,255,0.95); border-radius: 16px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
        .btn-action { padding: 12px 24px; font-size: 1.1em; border-radius: 12px; }
        .log-box { background: #1a1a2e; color: #0f0; font-family: 'Courier New', monospace; font-size: 13px; border-radius: 8px; padding: 16px; min-height: 250px; max-height: 400px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; }
        .status-badge { font-size: 0.9em; padding: 6px 12px; border-radius: 20px; }
        .nav-pills .nav-link.active { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }
        .report-content { max-height: 600px; overflow-y: auto; background: #f8f9fa; padding: 20px; border-radius: 8px; }
        .config-input { border-radius: 8px; }
        .path-display { background: #e9ecef; padding: 8px 12px; border-radius: 6px; font-family: monospace; font-size: 12px; word-break: break-all; cursor: pointer; }
        .path-display:hover { background: #dee2e6; }
        .progress-container { margin-top: 15px; }
        .progress { height: 28px; border-radius: 14px; background: #e9ecef; overflow: hidden; }
        .progress-bar { background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); font-weight: bold; font-size: 14px; line-height: 28px; }
        .slider-combo { display: flex; align-items: center; gap: 10px; }
        .slider-combo input[type="range"] { flex: 1; }
        .slider-combo input[type="number"] { width: 90px; text-align: center; font-weight: bold; border-radius: 8px; }
        .slider-label { font-size: 0.85em; color: #6c757d; }
    </style>
</head>
<body>
    <div class="container py-4">
        <!-- Header -->
        <div class="text-center text-white mb-4">
            <h1><i class="bi bi-rocket-takeoff"></i> AutoGooglePlayAnalyzer</h1>
            <p class="lead">AI 驱动的 Google Play 评论采集与商业智能分析平台</p>
        </div>
        
        <!-- Config Card -->
        <div class="dashboard-card p-4 mb-4">
            <h5 class="mb-3"><i class="bi bi-gear"></i> 运行时配置 <small class="text-muted">(修改后自动保存)</small></h5>
            
            <!-- Row 1: App Config -->
            <div class="row g-3 mb-3">
                <div class="col-md-4">
                    <label class="form-label"><strong>App ID (包名)</strong></label>
                    <input type="text" class="form-control config-input" id="appId" value="{{ config.app_id }}" placeholder="com.example.app">
                </div>
                <div class="col-md-3">
                    <label class="form-label"><strong>国家或地区</strong></label>
                    <select class="form-select config-input" id="country">
                        {% for code, name in countries %}
                        <option value="{{ code }}" {{ 'selected' if code == config.country else '' }}>{{ name }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div class="col-md-3">
                    <label class="form-label"><strong>语言</strong></label>
                    <select class="form-select config-input" id="language">
                        {% for code, name in languages %}
                        <option value="{{ code }}" {{ 'selected' if code == config.language else '' }}>{{ name }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div class="col-md-2 d-flex align-items-end">
                    <button class="btn btn-outline-primary w-100" onclick="saveConfig()">
                        <i class="bi bi-check-lg"></i> 应用
                    </button>
                </div>
            </div>
            
            <!-- Row 2: LLM Config (Collapsible) -->
            <div class="accordion" id="llmConfigAccordion">
                <div class="accordion-item border-0">
                    <h2 class="accordion-header">
                        <button class="accordion-button collapsed bg-light" type="button" data-bs-toggle="collapse" data-bs-target="#llmConfig">
                            <i class="bi bi-cpu me-2"></i> LLM API 配置
                        </button>
                    </h2>
                    <div id="llmConfig" class="accordion-collapse collapse" data-bs-parent="#llmConfigAccordion">
                        <div class="accordion-body">
                            <div class="row g-3">
                                <div class="col-md-4">
                                    <label class="form-label"><strong>API Key</strong></label>
                                    <input type="password" class="form-control config-input" id="openaiApiKey" value="{{ config.openai_api_key }}" placeholder="sk-...">
                                </div>
                                <div class="col-md-4">
                                    <label class="form-label"><strong>Model</strong></label>
                                    <input type="text" class="form-control config-input" id="openaiModel" value="{{ config.openai_model }}" placeholder="gpt-4o">
                                </div>
                                <div class="col-md-4">
                                    <label class="form-label"><strong>API Base URL</strong></label>
                                    <input type="text" class="form-control config-input" id="openaiApiBase" value="{{ config.openai_api_base }}" placeholder="https://api.openai.com/v1">
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="row mt-3">
                <div class="col-12 text-center">
                    <span class="badge bg-primary status-badge" id="db-count">加载中...</span>
                    <span class="badge bg-secondary status-badge ms-2" id="config-status"></span>
                </div>
            </div>
        </div>
        
        <!-- Main Tabs (Only 2 tabs now) -->
        <div class="dashboard-card p-4">
            <ul class="nav nav-pills nav-fill mb-4" id="mainTab" role="tablist">
                <li class="nav-item"><a class="nav-link active" data-bs-toggle="pill" href="#scrape"><i class="bi bi-cloud-download"></i> 数据采集</a></li>
                <li class="nav-item"><a class="nav-link" data-bs-toggle="pill" href="#analyze"><i class="bi bi-cpu"></i> AI 分析</a></li>
            </ul>
            
            <div class="tab-content">
                <!-- Scrape Tab -->
                <div class="tab-pane fade show active" id="scrape">
                    <div class="row">
                        <div class="col-md-4">
                            <label class="form-label"><strong>抓取数量</strong></label>
                            <div class="slider-combo">
                                <input type="range" class="form-range" min="100" max="10000" step="100" value="{{ config.scrape_count }}" id="scrapeCountSlider" oninput="syncSlider('scrapeCountSlider', 'scrapeCountInput')">
                                <input type="number" class="form-control" min="100" max="50000" step="100" value="{{ config.scrape_count }}" id="scrapeCountInput" oninput="syncInput('scrapeCountInput', 'scrapeCountSlider')">
                            </div>
                            <div class="d-flex justify-content-between slider-label"><span>100</span><span>拖动或输入</span><span>10000+</span></div>
                            
                            <button class="btn btn-primary btn-action w-100 mt-3" id="btnScrape" onclick="runScrape()">
                                <i class="bi bi-play-fill"></i> 开始抓取
                            </button>
                            
                            <button class="btn btn-outline-info w-100 mt-2" onclick="runExport()">
                                <i class="bi bi-file-earmark-arrow-down"></i> 导出当前 App 数据为 JSON
                            </button>
                            <div class="mt-2" id="exportResult"></div>
                            
                            <!-- Progress Bar -->
                            <div class="progress-container" id="progressContainer" style="display:none;">
                                <div class="progress">
                                    <div class="progress-bar progress-bar-striped progress-bar-animated" id="progressBar" role="progressbar" style="width: 0%">0%</div>
                                </div>
                                <div class="text-center mt-2" id="progressText"><span class="badge bg-secondary">准备中...</span></div>
                            </div>
                        </div>
                        <div class="col-md-8">
                            <label class="form-label"><strong>运行日志</strong> <small class="text-muted">(实时更新)</small></label>
                            <div class="log-box" id="scrapeLog">等待操作...</div>
                        </div>
                    </div>
                </div>
                
                <!-- Analyze Tab -->
                <div class="tab-pane fade" id="analyze">
                    <!-- Analysis Config -->
                    <div class="row mb-3">
                        <div class="col-md-5">
                            <label class="form-label"><strong>分析条数</strong> <small class="text-muted">(从数据库抽取多少条评论)</small></label>
                            <div class="slider-combo">
                                <input type="range" class="form-range" min="50" max="2000" step="50" value="{{ config.analyze_count }}" id="analyzeCountSlider" oninput="syncSlider('analyzeCountSlider', 'analyzeCountInput'); updateBatchInfo();">
                                <input type="number" class="form-control" min="50" max="10000" step="50" value="{{ config.analyze_count }}" id="analyzeCountInput" oninput="syncInput('analyzeCountInput', 'analyzeCountSlider'); updateBatchInfo();">
                            </div>
                            <div class="d-flex justify-content-between slider-label"><span>50</span><span>拖动或输入</span><span>2000+</span></div>
                        </div>
                        <div class="col-md-4">
                            <label class="form-label"><strong>批次数量</strong> <small class="text-muted">(分几次发给 AI)</small></label>
                            <div class="slider-combo">
                                <input type="range" class="form-range" min="1" max="20" step="1" value="{{ config.batch_count }}" id="batchCountSlider" oninput="syncSlider('batchCountSlider', 'batchCountInput'); updateBatchInfo();">
                                <input type="number" class="form-control" min="1" max="50" step="1" value="{{ config.batch_count }}" id="batchCountInput" oninput="syncInput('batchCountInput', 'batchCountSlider'); updateBatchInfo();">
                            </div>
                            <div class="d-flex justify-content-between slider-label"><span>1</span><span>批次越少越快</span><span>20+</span></div>
                        </div>
                        <div class="col-md-3 d-flex align-items-end">
                            <button class="btn btn-success btn-action w-100" id="btnAnalyze" onclick="runAnalysis()">
                                <i class="bi bi-lightning-fill"></i> 运行 AI 分析
                            </button>
                        </div>
                    </div>
                    
                    <!-- Batch Info Card -->
                    <div class="alert alert-info py-2 mb-3" id="batchInfoCard">
                        <i class="bi bi-info-circle"></i> 
                        <strong>每批处理:</strong> <span id="perBatchCount">-</span> 条 | 
                        <strong>推荐:</strong> 基于 DeepSeek-V3 (128K 上下文), 建议每批 ≤200 条。当前配置 <span id="batchStatus" class="badge bg-success">合适</span>
                    </div>
                    
                    
                    <label class="form-label"><strong>分析日志</strong> <small class="text-muted">(实时更新)</small></label>
                    <div class="log-box mb-3" id="analyzeLog" style="min-height:150px;">等待操作...</div>
                    
                    <hr>
                    
                    <!-- Report Viewer & PDF Conversion -->
                    <div class="row">
                        <div class="col-md-5">
                            <label class="form-label"><strong>选择报告</strong></label>
                            <select class="form-select" id="reportSelect" onchange="loadReport()">
                                <option value="">-- 选择报告查看 --</option>
                            </select>
                        </div>
                        <div class="col-md-2">
                            <label class="form-label">&nbsp;</label>
                            <button class="btn btn-warning w-100" onclick="runPdfConvert()">
                                <i class="bi bi-file-pdf"></i> 生成 PDF
                            </button>
                        </div>
                        <div class="col-md-2">
                            <label class="form-label">&nbsp;</label>
                            <button class="btn btn-info w-100" id="btnViewPdf" onclick="viewPdf()" disabled>
                                <i class="bi bi-eye"></i> 查看 PDF
                            </button>
                        </div>
                        <div class="col-md-3">
                            <label class="form-label">&nbsp;</label>
                            <button class="btn btn-outline-secondary w-100" onclick="loadReportsList()">
                                <i class="bi bi-arrow-clockwise"></i> 刷新列表
                            </button>
                        </div>
                    </div>
                    <div class="mt-2" id="pdfResult"></div>
                    
                    <!-- Toggle between MD and PDF view -->
                    <ul class="nav nav-tabs mt-3" id="reportViewTab">
                        <li class="nav-item">
                            <a class="nav-link active" data-bs-toggle="tab" href="#mdView">Markdown 预览</a>
                        </li>
                        <li class="nav-item">
                            <a class="nav-link" data-bs-toggle="tab" href="#pdfView" id="pdfViewTab">PDF 预览</a>
                        </li>
                    </ul>
                    <div class="tab-content">
                        <div class="tab-pane fade show active" id="mdView">
                            <div class="report-content mt-3" id="reportContent">选择一个报告查看内容...</div>
                        </div>
                        <div class="tab-pane fade" id="pdfView">
                            <div class="mt-3" style="height: 600px; background: #f8f9fa; border-radius: 8px; display: flex; align-items: center; justify-content: center;" id="pdfContainer">
                                <p class="text-muted">请先生成 PDF 后查看</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="text-center text-white mt-4">
            <small>Created with ❤️ by AutoGooglePlayAnalyzer</small>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <script>
        // Sync slider and input
        function syncSlider(sliderId, inputId) {
            document.getElementById(inputId).value = document.getElementById(sliderId).value;
        }
        function syncInput(inputId, sliderId) {
            let val = parseInt(document.getElementById(inputId).value) || 0;
            let slider = document.getElementById(sliderId);
            let max = parseInt(slider.max);
            if (val <= max) {
                slider.value = val;
            } else {
                slider.value = max;
            }
        }
        
        // Load initial data
        window.onload = function() {
            updateDbCount();
            loadReportsList();
            updateBatchInfo();
        };
        
        function updateDbCount() {
            let appId = document.getElementById('appId').value;
            fetch('/api/db_count?app_id=' + encodeURIComponent(appId)).then(r => r.json()).then(data => {
                document.getElementById('db-count').textContent = '📊 ' + data.count + ' 条评论';
            });
        }
        
        function updateBatchInfo() {
            let analyzeCount = parseInt(document.getElementById('analyzeCountInput').value) || 500;
            let batchCount = parseInt(document.getElementById('batchCountInput').value) || 5;
            let perBatch = Math.ceil(analyzeCount / batchCount);
            
            document.getElementById('perBatchCount').textContent = perBatch;
            
            let statusEl = document.getElementById('batchStatus');
            if (perBatch <= 200) {
                statusEl.className = 'badge bg-success';
                statusEl.textContent = '✓ 合适';
            } else if (perBatch <= 300) {
                statusEl.className = 'badge bg-warning text-dark';
                statusEl.textContent = '⚠ 偏多';
            } else {
                statusEl.className = 'badge bg-danger';
                statusEl.textContent = '✗ 过多，建议增加批次';
            }
        }
        
        function loadReportsList() {
            fetch('/api/reports').then(r => r.json()).then(data => {
                let select = document.getElementById('reportSelect');
                select.innerHTML = '<option value="">-- 选择报告查看 --</option>';
                data.reports.forEach(r => {
                    select.innerHTML += '<option value="' + r + '">' + r + '</option>';
                });
            });
        }
        
        function saveConfig() {
            let config = {
                app_id: document.getElementById('appId').value,
                country: document.getElementById('country').value,
                language: document.getElementById('language').value,
                scrape_count: parseInt(document.getElementById('scrapeCountInput').value),
                analyze_count: parseInt(document.getElementById('analyzeCountInput').value),
                batch_count: parseInt(document.getElementById('batchCountInput').value),
                openai_api_key: document.getElementById('openaiApiKey').value,
                openai_model: document.getElementById('openaiModel').value,
                openai_api_base: document.getElementById('openaiApiBase').value
            };
            
            fetch('/api/config', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            }).then(r => r.json()).then(data => {
                document.getElementById('config-status').textContent = '✅ 配置已保存';
                setTimeout(() => { document.getElementById('config-status').textContent = ''; }, 2000);
                updateDbCount();
            });
        }
        
        function runScrape() {
            // Save config first
            saveConfig();
            
            let count = document.getElementById('scrapeCountInput').value;
            let appId = document.getElementById('appId').value;
            let country = document.getElementById('country').value;
            let language = document.getElementById('language').value;
            let log = document.getElementById('scrapeLog');
            let btn = document.getElementById('btnScrape');
            let progressContainer = document.getElementById('progressContainer');
            let progressBar = document.getElementById('progressBar');
            let progressText = document.getElementById('progressText');
            
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> 抓取中...';
            log.textContent = '';
            progressContainer.style.display = 'block';
            progressBar.style.width = '0%';
            progressBar.textContent = '0%';
            progressBar.classList.add('progress-bar-animated');
            progressText.innerHTML = '<span class="badge bg-info">进行中...</span>';
            
            const evtSource = new EventSource('/api/scrape_stream?count=' + count + '&app_id=' + encodeURIComponent(appId) + '&country=' + country + '&language=' + language);
            
            evtSource.onmessage = function(event) {
                let msg = event.data;
                
                // Check for PROGRESS message (hidden from log)
                if (msg.startsWith('PROGRESS:')) {
                    let parts = msg.split(':');
                    let current = parseInt(parts[1]);
                    let total = parseInt(parts[2]);
                    let pct = Math.round((current / total) * 100);
                    progressBar.style.width = pct + '%';
                    progressBar.textContent = pct + '%';
                    progressText.innerHTML = '<span class="badge bg-primary">' + current + ' / ' + total + ' 条</span>';
                    return;  // Don't add PROGRESS message to log
                }
                
                log.textContent += msg + '\\n';
                log.scrollTop = log.scrollHeight;
                
                // Parse final progress
                let progressMatch = msg.match(/累计: (\\d+)\\/(\\d+)/);
                if (progressMatch) {
                    let current = parseInt(progressMatch[1]);
                    let total = parseInt(progressMatch[2]);
                    let pct = Math.round((current / total) * 100);
                    progressBar.style.width = pct + '%';
                    progressBar.textContent = pct + '%';
                    progressText.innerHTML = '<span class="badge bg-primary">' + current + ' / ' + total + ' 条</span>';
                }
            };

            
            evtSource.onerror = function() {
                evtSource.close();
                btn.disabled = false;
                btn.innerHTML = '<i class="bi bi-play-fill"></i> 开始抓取';
                progressBar.style.width = '100%';
                progressBar.textContent = '✓ 完成';
                progressBar.classList.remove('progress-bar-animated');
                progressText.innerHTML = '<span class="badge bg-success">抓取完成</span>';
                updateDbCount();
            };
        }
        
        function runAnalysis() {
            // Save config first
            saveConfig();
            
            let log = document.getElementById('analyzeLog');
            let btn = document.getElementById('btnAnalyze');
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> 分析中...';
            log.textContent = '';
            
            setTimeout(() => {
                let analyzeCount = document.getElementById('analyzeCountInput').value;
                let batchCount = document.getElementById('batchCountInput').value;
                
                const evtSource = new EventSource('/api/analyze_stream?analyze_count=' + analyzeCount + '&batch_count=' + batchCount);
                
                evtSource.onmessage = function(event) {
                    log.textContent += event.data + '\\n';
                    log.scrollTop = log.scrollHeight;
                };
                
                evtSource.onerror = function() {
                    evtSource.close();
                    btn.disabled = false;
                    btn.innerHTML = '<i class="bi bi-lightning-fill"></i> 运行 AI 分析';
                    loadReportsList();
                };
            }, 300);
        }
        
        function loadReport() {
            let name = document.getElementById('reportSelect').value;
            if (!name) return;
            fetch('/api/report?name=' + encodeURIComponent(name)).then(r => r.json()).then(data => {
                document.getElementById('reportContent').innerHTML = marked.parse(data.content);
            });
        }
        
        function copyPath(path) {
            navigator.clipboard.writeText(path).then(() => {
                alert('路径已复制到剪贴板！');
            });
        }
        
        function runExport() {
            let appId = document.getElementById('appId').value;
            fetch('/api/export?app_id=' + encodeURIComponent(appId)).then(r => r.json()).then(data => {
                let result = document.getElementById('exportResult');
                if (data.success) {
                    result.innerHTML = '<div class="alert alert-success py-2 mb-1"><small>✅ 导出成功</small></div>' +
                        '<div class="path-display" onclick="copyPath(\\'' + data.path.replace(/\\\\/g, '\\\\\\\\') + '\\')" title="点击复制"><i class="bi bi-clipboard"></i> ' + data.path + '</div>';
                } else {
                    result.innerHTML = '<div class="alert alert-danger py-2">' + data.message + '</div>';
                }
            });
        }
        
        var currentPdfPath = null;
        
        function runPdfConvert() {
            let reportName = document.getElementById('reportSelect').value;
            if (!reportName) {
                alert('请先选择一个报告');
                return;
            }
            fetch('/api/pdf_convert?report=' + encodeURIComponent(reportName)).then(r => r.json()).then(data => {
                let result = document.getElementById('pdfResult');
                if (data.success) {
                    result.innerHTML = '<div class="alert alert-success py-2 mb-1"><small>✅ PDF 转换成功</small></div>' +
                        '<div class="path-display" onclick="copyPath(\\'' + data.path.replace(/\\\\/g, '\\\\\\\\') + '\\')" title="点击复制"><i class="bi bi-clipboard"></i> ' + data.path + '</div>';
                    // Enable PDF view button
                    document.getElementById('btnViewPdf').disabled = false;
                    currentPdfPath = data.pdf_name;
                } else {
                    result.innerHTML = '<div class="alert alert-danger py-2">' + data.message + '</div>';
                }
            });
        }
        
        function viewPdf() {
            if (!currentPdfPath) {
                alert('请先生成 PDF');
                return;
            }
            // Switch to PDF tab
            document.querySelector('#pdfViewTab').click();
            // Load PDF in iframe
            document.getElementById('pdfContainer').innerHTML = 
                '<iframe src="/api/pdf_file?name=' + encodeURIComponent(currentPdfPath) + '" width="100%" height="100%" style="border: none; border-radius: 8px;"></iframe>';
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, 
        config=runtime_config,
        countries=COUNTRIES,
        languages=LANGUAGES
    )

@app.route('/api/config', methods=['POST'])
def api_config():
    global runtime_config
    data = request.get_json()
    runtime_config['app_id'] = data.get('app_id', runtime_config['app_id'])
    runtime_config['country'] = data.get('country', runtime_config['country'])
    runtime_config['language'] = data.get('language', runtime_config['language'])
    runtime_config['scrape_count'] = data.get('scrape_count', runtime_config['scrape_count'])
    runtime_config['analyze_count'] = data.get('analyze_count', runtime_config['analyze_count'])
    runtime_config['batch_count'] = data.get('batch_count', runtime_config.get('batch_count', 5))
    runtime_config['openai_api_key'] = data.get('openai_api_key', runtime_config['openai_api_key'])
    runtime_config['openai_model'] = data.get('openai_model', runtime_config['openai_model'])
    runtime_config['openai_api_base'] = data.get('openai_api_base', runtime_config['openai_api_base'])
    
    # Save to file for persistence
    save_config(runtime_config)
    
    # Update environment variables for LLM
    os.environ['OPENAI_API_KEY'] = runtime_config['openai_api_key']
    os.environ['OPENAI_MODEL'] = runtime_config['openai_model']
    os.environ['OPENAI_API_BASE'] = runtime_config['openai_api_base']
    
    return jsonify({'success': True})

@app.route('/api/db_count')
def api_db_count():
    app_id = request.args.get('app_id', runtime_config['app_id'])
    try:
        conn = DatabaseManager.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM google_play_reviews WHERE app_id = %s", (app_id,))
            count = cur.fetchone()[0]
        DatabaseManager.release_connection(conn)
        return jsonify({'count': count})
    except:
        return jsonify({'count': 0})

@app.route('/api/reports')
def api_reports():
    report_dir = os.path.join(PROJECT_DIR, "reports")
    if os.path.exists(report_dir):
        reports = sorted([f for f in os.listdir(report_dir) if f.endswith(".md")], reverse=True)
        return jsonify({'reports': reports})
    return jsonify({'reports': []})

@app.route('/api/report')
def api_report():
    name = request.args.get('name', '')
    file_path = os.path.join(PROJECT_DIR, "reports", name)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return jsonify({'content': f.read()})
    except:
        return jsonify({'content': '读取失败'})

@app.route('/api/scrape_stream')
def api_scrape_stream():
    count = request.args.get('count', 1000, type=int)
    app_id = request.args.get('app_id', runtime_config['app_id'])
    country = request.args.get('country', runtime_config['country'])
    language = request.args.get('language', runtime_config['language'])
    
    def generate():
        from google_play_scraper import reviews, Sort
        
        yield f"data: 🚀 开始抓取 {count} 条评论...\n\n"
        yield f"data: 📱 App ID: {app_id}\n\n"
        yield f"data: 🌍 国家或地区: {country} | 语言: {language}\n\n"
        yield "data: " + "─" * 30 + "\n\n"
        
        try:
            yield "data: 📦 正在初始化数据库...\n\n"
            DatabaseManager.create_tables()
            yield "data: ✅ 数据库初始化完成\n\n"
            
            yield f"data: 🔍 正在连接 Google Play Store...\n\n"
            
            # Custom batch fetching with real-time progress
            all_reviews = []
            continuation_token = None
            batch_size = 199
            
            yield f"data: 📥 开始分批抓取 (每批 {batch_size} 条)...\n\n"
            
            while len(all_reviews) < count:
                remaining = count - len(all_reviews)
                current_batch_count = min(batch_size, remaining)
                
                try:
                    result, token = reviews(
                        app_id,
                        lang=language,
                        country=country,
                        sort=Sort.NEWEST,
                        count=current_batch_count,
                        continuation_token=continuation_token
                    )
                except Exception as e:
                    yield f"data: ⚠️ 抓取错误: {str(e)}\n\n"
                    break
                
                if not result:
                    yield "data: ⚠️ 没有更多评论可抓取\n\n"
                    break
                
                all_reviews.extend(result)
                continuation_token = token
                
                # Real-time progress update
                pct = min(100, round(len(all_reviews) / count * 100))
                yield f"data: 📊 进度: {len(all_reviews)}/{count} ({pct}%) ✓ 本批 {len(result)} 条\n\n"
                yield f"data: PROGRESS:{len(all_reviews)}:{count}\n\n"
                
                if not continuation_token:
                    yield "data: ℹ️ 已到达评论列表末尾\n\n"
                    break
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
            
            yield "data: " + "─" * 30 + "\n\n"
            yield f"data: ✓ 抓取完成，累计: {len(all_reviews)}/{count}\n\n"
            
            if all_reviews:
                yield f"data: 💾 正在存入数据库 ({len(all_reviews)} 条)...\n\n"
                DatabaseManager.insert_reviews(all_reviews, app_id)
                yield f"data: ✅ 成功抓取并存储 {len(all_reviews)} 条评论！\n\n"
            else:
                yield "data: ⚠️ 未抓取到任何评论，请检查 App ID 或网络连接\n\n"
                
        except Exception as e:
            yield f"data: ❌ 错误: {str(e)}\n\n"
        finally:
            DatabaseManager.close_all_connections()
            yield "data: 🏁 抓取任务结束\n\n"
    
    return Response(generate(), mimetype='text/event-stream')

@app.route('/api/analyze_stream')
def api_analyze_stream():
    analyze_count = request.args.get('analyze_count', runtime_config['analyze_count'], type=int)
    batch_count = request.args.get('batch_count', runtime_config.get('batch_count', 5), type=int)
    
    # Calculate batch_size from batch_count
    batch_size = max(10, analyze_count // batch_count) if batch_count > 0 else analyze_count
    
    def generate():
        yield "data: 🧠 正在启动 AI 分析引擎...\n\n"
        yield f"data: 📊 分析条数: {analyze_count} | 批次数量: {batch_count} | 每批: {batch_size} 条\n\n"
        yield "data: ⏳ 这可能需要 1-5 分钟，请耐心等待...\n\n"
        yield "data: " + "─" * 30 + "\n\n"
        
        # Prepare environment with current LLM config
        env = os.environ.copy()
        env['OPENAI_API_KEY'] = runtime_config.get('openai_api_key', '')
        env['OPENAI_MODEL'] = runtime_config.get('openai_model', 'deepseek-chat')
        env['OPENAI_API_BASE'] = runtime_config.get('openai_api_base', 'https://api.deepseek.com')
        env['TOTAL_TO_ANALYZE'] = str(analyze_count)
        env['BATCH_SIZE'] = str(batch_size)
        
        try:
            process = subprocess.Popen(
                [os.path.join(PROJECT_DIR, "venv/bin/python"), "-u", "analyzer.py"],
                cwd=PROJECT_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env
            )
            
            for line in iter(process.stdout.readline, ''):
                if line:
                    yield f"data: {line.strip()}\n\n"
            
            process.wait()
            
            yield "data: " + "─" * 30 + "\n\n"
            
            if process.returncode == 0:
                yield "data: ✅ 分析完成！报告已生成。\n\n"
            else:
                yield f"data: ❌ 分析退出码: {process.returncode}\n\n"
                
        except Exception as e:
            yield f"data: ❌ 错误: {str(e)}\n\n"
        
        yield "data: 🏁 分析任务结束\n\n"
    
    return Response(generate(), mimetype='text/event-stream')

@app.route('/api/export')
def api_export():
    app_id = request.args.get('app_id', runtime_config['app_id'])
    
    try:
        # Create exports directory if not exists
        export_dir = os.path.join(PROJECT_DIR, "exports")
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        # Generate filename with current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_app_id = app_id.replace('.', '_')
        filename = f"export_{safe_app_id}_{timestamp}.json"
        full_path = os.path.join(export_dir, filename)
        
        # Fetch data from database
        conn = DatabaseManager.get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT review_id, user_name, content, score, thumbs_up_count, at, app_id 
                FROM google_play_reviews 
                WHERE app_id = %s 
                ORDER BY at DESC
            """, (app_id,))
            rows = cur.fetchall()
        DatabaseManager.release_connection(conn)
        
        # Format data
        export_data = []
        for row in rows:
            export_data.append({
                'review_id': row[0],
                'user_name': row[1],
                'content': row[2],
                'score': row[3],
                'thumbs_up_count': row[4],
                'date': row[5].isoformat() if row[5] else None,
                'app_id': row[6]
            })
        
        # Write to file
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump({'count': len(export_data), 'app_id': app_id, 'exported_at': timestamp, 'reviews': export_data}, f, ensure_ascii=False, indent=2)
        
        return jsonify({'success': True, 'path': full_path, 'message': f'导出 {len(export_data)} 条评论'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'❌ {str(e)}'})

@app.route('/api/pdf_convert')
def api_pdf_convert():
    report_name = request.args.get('report', '')
    
    if not report_name:
        return jsonify({'success': False, 'message': '❌ 请选择要转换的报告'})
    
    try:
        report_dir = os.path.join(PROJECT_DIR, "reports")
        md_path = os.path.join(report_dir, report_name)
        pdf_name = report_name.replace('.md', '.pdf')
        pdf_path = os.path.join(report_dir, pdf_name)
        
        if not os.path.exists(md_path):
            return jsonify({'success': False, 'message': '❌ 报告文件不存在'})
        
        # Read markdown content
        with open(md_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Convert to PDF
        from markdown_pdf import MarkdownPdf, Section
        pdf = MarkdownPdf(toc_level=2)
        pdf.add_section(Section(content, toc=False))
        pdf.save(pdf_path)
        
        return jsonify({'success': True, 'path': pdf_path, 'pdf_name': pdf_name, 'message': 'PDF转换成功'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'❌ {str(e)}'})

@app.route('/api/pdf_file')
def api_pdf_file():
    """Serve PDF file for viewing in browser"""
    from flask import send_file
    pdf_name = request.args.get('name', '')
    if not pdf_name:
        return "No PDF specified", 400
    
    pdf_path = os.path.join(PROJECT_DIR, "reports", pdf_name)
    if not os.path.exists(pdf_path):
        return "PDF not found", 404
    
    return send_file(pdf_path, mimetype='application/pdf')


def open_browser():
    """Open browser after a short delay"""
    time.sleep(1.5)
    webbrowser.open('http://127.0.0.1:5001')

def kill_port(port):
    """Kill any process using the specified port"""
    try:
        # Find process using the port
        result = subprocess.run(
            ['lsof', '-ti', f':{port}'],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split('\n')
        for pid in pids:
            if pid:
                subprocess.run(['kill', '-9', pid], capture_output=True)
                print(f"🔄 已终止占用端口 {port} 的进程 (PID: {pid})")
    except:
        pass

if __name__ == '__main__':
    PORT = 5001
    
    # Kill any existing process on the port
    kill_port(PORT)
    
    print("\n" + "=" * 50)
    print("🚀 AutoGooglePlayAnalyzer Dashboard")
    print("=" * 50)
    print(f"📍 地址: http://127.0.0.1:{PORT}")
    print("📌 浏览器将自动打开...")
    print("⌨️  按 Ctrl+C 停止服务器")
    print("=" * 50 + "\n")
    
    # Auto open browser in a separate thread
    threading.Thread(target=open_browser, daemon=True).start()
    
    app.run(host='127.0.0.1', port=PORT, debug=False, threaded=True)
