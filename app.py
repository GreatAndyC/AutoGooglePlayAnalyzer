import threading
import time
import webbrowser
import subprocess
from web import create_app

def open_browser(port):
    """Open browser after a short delay"""
    time.sleep(1.5)
    webbrowser.open(f'http://127.0.0.1:{port}')

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
    
    app = create_app()
    
    print("\n" + "=" * 50)
    print("🚀 AutoGooglePlayAnalyzer Dashboard")
    print("=" * 50)
    print(f"📍 地址: http://127.0.0.1:{PORT}")
    print("📌 浏览器将自动打开...")
    print("⌨️  按 Ctrl+C 停止服务器")
    print("=" * 50 + "\n")
    
    # Auto open browser in a separate thread
    threading.Thread(target=open_browser, args=(PORT,), daemon=True).start()
    
    app.run(host='127.0.0.1', port=PORT, debug=False, threaded=True)
