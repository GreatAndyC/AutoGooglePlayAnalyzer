from flask import Blueprint, render_template, jsonify, request, Response, send_file
import subprocess
import os
import json
import time
from datetime import datetime
from src.database import DatabaseManager
from web.config import runtime_config, save_config, COUNTRIES, LANGUAGES, PROJECT_DIR

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    return render_template('index.html', 
        config=runtime_config,
        countries=COUNTRIES,
        languages=LANGUAGES
    )

@main_bp.route('/api/config', methods=['POST'])
def api_config():
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

@main_bp.route('/api/db_count')
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

@main_bp.route('/api/reports')
def api_reports():
    report_dir = os.path.join(PROJECT_DIR, "reports")
    if os.path.exists(report_dir):
        reports = sorted([f for f in os.listdir(report_dir) if f.endswith(".md")], reverse=True)
        return jsonify({'reports': reports})
    return jsonify({'reports': []})

@main_bp.route('/api/report')
def api_report():
    name = request.args.get('name', '')
    file_path = os.path.join(PROJECT_DIR, "reports", name)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return jsonify({'content': f.read()})
    except:
        return jsonify({'content': '读取失败'})

@main_bp.route('/api/scrape_stream')
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

@main_bp.route('/api/analyze_stream')
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
            # Use venv python from PROJECT_DIR
            python_path = os.path.join(PROJECT_DIR, "venv/bin/python")
            if not os.path.exists(python_path):
                 # Fallback to system python if venv not found (e.g. Docker)
                 python_path = "python"

            process = subprocess.Popen(
                [python_path, "-u", "analyzer.py"],
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

@main_bp.route('/api/export')
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

@main_bp.route('/api/pdf_convert')
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

@main_bp.route('/api/pdf_file')
def api_pdf_file():
    """Serve PDF file for viewing in browser"""
    pdf_name = request.args.get('name', '')
    if not pdf_name:
        return "No PDF specified", 400
    
    pdf_path = os.path.join(PROJECT_DIR, "reports", pdf_name)
    if not os.path.exists(pdf_path):
        return "PDF not found", 404
    
    return send_file(pdf_path, mimetype='application/pdf')
