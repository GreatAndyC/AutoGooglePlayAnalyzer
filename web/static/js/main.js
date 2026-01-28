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
window.onload = function () {
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
    if (perBatch <= 50) {
        statusEl.className = 'badge bg-success';
        statusEl.textContent = '✓ 合适';
    } else if (perBatch <= 80) {
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
        headers: { 'Content-Type': 'application/json' },
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

    evtSource.onmessage = function (event) {
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

        log.textContent += msg + '\n';
        log.scrollTop = log.scrollHeight;

        // Parse final progress
        let progressMatch = msg.match(/累计: (\d+)\/(\d+)/);
        if (progressMatch) {
            let current = parseInt(progressMatch[1]);
            let total = parseInt(progressMatch[2]);
            let pct = Math.round((current / total) * 100);
            progressBar.style.width = pct + '%';
            progressBar.textContent = pct + '%';
            progressText.innerHTML = '<span class="badge bg-primary">' + current + ' / ' + total + ' 条</span>';
        }
    };


    evtSource.onerror = function () {
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

        evtSource.onmessage = function (event) {
            log.textContent += event.data + '\n';
            log.scrollTop = log.scrollHeight;
        };

        evtSource.onerror = function () {
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
                '<div class="path-display" onclick="copyPath(\'' + data.path.replace(/\\/g, '\\\\') + '\')" title="点击复制"><i class="bi bi-clipboard"></i> ' + data.path + '</div>';
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
                '<div class="path-display" onclick="copyPath(\'' + data.path.replace(/\\/g, '\\\\') + '\')" title="点击复制"><i class="bi bi-clipboard"></i> ' + data.path + '</div>';
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
