import os
import json
from src.config import Config

# Project directories
WEB_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(WEB_DIR)
CONFIG_FILE = os.path.join(PROJECT_DIR, '.dashboard_config.json')

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

# Global runtime config instance
runtime_config = load_config()
