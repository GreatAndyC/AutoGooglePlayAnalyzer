from flask import Flask, render_template_string, jsonify, request
import subprocess
import os
import json
from src.config import Config
from src.database import DatabaseManager
from src.scraper import GooglePlayScraper

app = Flask(__name__)

# HTML Template with Bootstrap
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
        .log-box { background: #1a1a2e; color: #0f0; font-family: monospace; border-radius: 8px; padding: 16px; min-height: 200px; max-height: 400px; overflow-y: auto; }
        .status-badge { font-size: 0.9em; padding: 6px 12px; border-radius: 20px; }
        .nav-pills .nav-link.active { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }
        .report-content { max-height: 600px; overflow-y: auto; background: #f8f9fa; padding: 20px; border-radius: 8px; }
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
        <div class="dashboard-card p-3 mb-4">
            <div class="row text-center">
                <div class="col-md-3"><strong>App ID:</strong> <code>{{ config.app_id }}</code></div>
                <div class="col-md-3"><strong>区域:</strong> {{ config.country }}</div>
                <div class="col-md-3"><strong>语言:</strong> {{ config.language }}</div>
                <div class="col-md-3"><strong>数据库:</strong> <span id="db-count" class="badge bg-primary status-badge">加载中...</span></div>
            </div>
        </div>
        
        <!-- Main Tabs -->
        <div class="dashboard-card p-4">
            <ul class="nav nav-pills nav-fill mb-4" id="mainTab" role="tablist">
                <li class="nav-item"><a class="nav-link active" data-bs-toggle="pill" href="#scrape"><i class="bi bi-cloud-download"></i> 数据采集</a></li>
                <li class="nav-item"><a class="nav-link" data-bs-toggle="pill" href="#analyze"><i class="bi bi-cpu"></i> AI 分析</a></li>
                <li class="nav-item"><a class="nav-link" data-bs-toggle="pill" href="#tools"><i class="bi bi-tools"></i> 工具箱</a></li>
            </ul>
            
            <div class="tab-content">
                <!-- Scrape Tab -->
                <div class="tab-pane fade show active" id="scrape">
                    <div class="row">
                        <div class="col-md-4">
                            <label class="form-label"><strong>抓取数量</strong></label>
                            <input type="range" class="form-range" min="100" max="10000" step="100" value="1000" id="scrapeCount">
                            <div class="text-center"><span id="scrapeCountLabel">1000</span> 条</div>
                            <button class="btn btn-primary btn-action w-100 mt-3" id="btnScrape" onclick="runScrape()">
                                <i class="bi bi-play-fill"></i> 开始抓取
                            </button>
                        </div>
                        <div class="col-md-8">
                            <label class="form-label"><strong>运行日志</strong></label>
                            <div class="log-box" id="scrapeLog">等待操作...</div>
                        </div>
                    </div>
                </div>
                
                <!-- Analyze Tab -->
                <div class="tab-pane fade" id="analyze">
                    <div class="row mb-4">
                        <div class="col-md-6">
                            <button class="btn btn-success btn-action w-100" id="btnAnalyze" onclick="runAnalysis()">
                                <i class="bi bi-lightning-fill"></i> 运行 AI 分析
                            </button>
                        </div>
                        <div class="col-md-6">
                            <select class="form-select" id="reportSelect" onchange="loadReport()">
                                <option value="">-- 选择报告查看 --</option>
                            </select>
                        </div>
                    </div>
                    <div class="log-box mb-3" id="analyzeLog" style="min-height:100px;">等待操作...</div>
                    <div class="report-content" id="reportContent">选择一个报告查看内容...</div>
                </div>
                
                <!-- Tools Tab -->
                <div class="tab-pane fade" id="tools">
                    <div class="row">
                        <div class="col-md-6 mb-3">
                            <button class="btn btn-info btn-action w-100" onclick="runExport()">
                                <i class="bi bi-file-earmark-arrow-down"></i> 导出数据为 JSON
                            </button>
                            <div class="mt-2 text-center" id="exportResult"></div>
                        </div>
                        <div class="col-md-6 mb-3">
                            <button class="btn btn-warning btn-action w-100" onclick="runPdfConvert()">
                                <i class="bi bi-file-pdf"></i> 转换最新报告为 PDF
                            </button>
                            <div class="mt-2 text-center" id="pdfResult"></div>
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
        // Update slider label
        document.getElementById('scrapeCount').oninput = function() {
            document.getElementById('scrapeCountLabel').textContent = this.value;
        };
        
        // Load initial data
        window.onload = function() {
            fetch('/api/db_count').then(r => r.json()).then(data => {
                document.getElementById('db-count').textContent = data.count + ' 条';
            });
            loadReportsList();
        };
        
        function loadReportsList() {
            fetch('/api/reports').then(r => r.json()).then(data => {
                let select = document.getElementById('reportSelect');
                select.innerHTML = '<option value="">-- 选择报告查看 --</option>';
                data.reports.forEach(r => {
                    select.innerHTML += '<option value="' + r + '">' + r + '</option>';
                });
            });
        }
        
        function runScrape() {
            let count = document.getElementById('scrapeCount').value;
            let log = document.getElementById('scrapeLog');
            let btn = document.getElementById('btnScrape');
            btn.disabled = true;
            log.textContent = '🚀 开始抓取 ' + count + ' 条评论...\\n';
            
            fetch('/api/scrape?count=' + count).then(r => r.json()).then(data => {
                log.textContent += data.log;
                btn.disabled = false;
                fetch('/api/db_count').then(r => r.json()).then(d => {
                    document.getElementById('db-count').textContent = d.count + ' 条';
                });
            }).catch(e => {
                log.textContent += '❌ 错误: ' + e;
                btn.disabled = false;
            });
        }
        
        function runAnalysis() {
            let log = document.getElementById('analyzeLog');
            let btn = document.getElementById('btnAnalyze');
            btn.disabled = true;
            log.textContent = '🧠 正在启动 AI 分析引擎...\\n⏳ 这可能需要 1-2 分钟，请稍候...\\n';
            
            fetch('/api/analyze').then(r => r.json()).then(data => {
                log.textContent += data.log;
                btn.disabled = false;
                loadReportsList();
            }).catch(e => {
                log.textContent += '❌ 错误: ' + e;
                btn.disabled = false;
            });
        }
        
        function loadReport() {
            let name = document.getElementById('reportSelect').value;
            if (!name) return;
            fetch('/api/report?name=' + encodeURIComponent(name)).then(r => r.json()).then(data => {
                document.getElementById('reportContent').innerHTML = marked.parse(data.content);
            });
        }
        
        function runExport() {
            fetch('/api/export').then(r => r.json()).then(data => {
                document.getElementById('exportResult').innerHTML = '<span class="badge bg-' + (data.success ? 'success' : 'danger') + '">' + data.message + '</span>';
            });
        }
        
        function runPdfConvert() {
            fetch('/api/pdf_convert').then(r => r.json()).then(data => {
                document.getElementById('pdfResult').innerHTML = '<span class="badge bg-' + (data.success ? 'success' : 'danger') + '">' + data.message + '</span>';
            });
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, config={
        'app_id': Config.APP_ID,
        'country': Config.COUNTRY,
        'language': Config.LANGUAGE
    })

@app.route('/api/db_count')
def api_db_count():
    try:
        conn = DatabaseManager.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM google_play_reviews WHERE app_id = %s", (Config.APP_ID,))
            count = cur.fetchone()[0]
        conn.close()
        return jsonify({'count': count})
    except:
        return jsonify({'count': 0})

@app.route('/api/reports')
def api_reports():
    report_dir = "reports"
    if os.path.exists(report_dir):
        reports = sorted([f for f in os.listdir(report_dir) if f.endswith(".md")], reverse=True)
        return jsonify({'reports': reports})
    return jsonify({'reports': []})

@app.route('/api/report')
def api_report():
    name = request.args.get('name', '')
    file_path = os.path.join("reports", name)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return jsonify({'content': f.read()})
    except:
        return jsonify({'content': '读取失败'})

@app.route('/api/scrape')
def api_scrape():
    count = request.args.get('count', 1000, type=int)
    logs = []
    try:
        logs.append("📦 初始化数据库...")
        DatabaseManager.create_tables()
        
        logs.append(f"🔍 正在从 Google Play 抓取 {Config.APP_ID} 的评论...")
        scraper = GooglePlayScraper(app_id=Config.APP_ID)
        reviews_data = scraper.fetch_reviews(target_count=count, batch_size=150)
        
        if reviews_data:
            logs.append(f"� 正在存入数据库 ({len(reviews_data)} 条)...")
            DatabaseManager.insert_reviews(reviews_data, Config.APP_ID)
            logs.append(f"✅ 成功抓取并存储 {len(reviews_data)} 条评论！")
        else:
            logs.append("⚠️ 未抓取到任何评论")
    except Exception as e:
        logs.append(f"❌ 错误: {str(e)}")
    finally:
        DatabaseManager.close_all_connections()
    
    return jsonify({'log': '\n'.join(logs)})

@app.route('/api/analyze')
def api_analyze():
    try:
        result = subprocess.run(
            ["/Users/andycao/Documents/Project/AutoGooglePlayAnalyzer/venv/bin/python", "analyzer.py"],
            cwd="/Users/andycao/Documents/Project/AutoGooglePlayAnalyzer",
            capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            return jsonify({'log': '✅ 分析完成！报告已生成。'})
        return jsonify({'log': f'❌ 分析失败: {result.stderr[-500:]}'})
    except subprocess.TimeoutExpired:
        return jsonify({'log': '⏰ 分析超时'})
    except Exception as e:
        return jsonify({'log': f'❌ 错误: {str(e)}'})

@app.route('/api/export')
def api_export():
    try:
        result = subprocess.run(
            ["/Users/andycao/Documents/Project/AutoGooglePlayAnalyzer/venv/bin/python", "export_reviews.py"],
            cwd="/Users/andycao/Documents/Project/AutoGooglePlayAnalyzer",
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            return jsonify({'success': True, 'message': '✅ 导出成功！查看 exports/ 目录'})
        return jsonify({'success': False, 'message': '❌ 导出失败'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'❌ {str(e)}'})

@app.route('/api/pdf_convert')
def api_pdf_convert():
    try:
        result = subprocess.run(
            ["/Users/andycao/Documents/Project/AutoGooglePlayAnalyzer/venv/bin/python", "convert_report.py"],
            cwd="/Users/andycao/Documents/Project/AutoGooglePlayAnalyzer",
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            return jsonify({'success': True, 'message': '✅ PDF 转换成功！'})
        return jsonify({'success': False, 'message': '❌ 转换失败'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'❌ {str(e)}'})

if __name__ == '__main__':
    print("\n🚀 Dashboard 启动成功！")
    print("📍 请在浏览器中打开: http://127.0.0.1:5000\n")
    app.run(host='127.0.0.1', port=5000, debug=False)
