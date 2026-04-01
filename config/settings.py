# config/settings.py
import os
from pathlib import Path

# ========== 项目路径 ==========
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = DATA_DIR / "logs"

# ========== IBKR 配置 ==========
IB_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IBKR_PORT", "4002"))  # 纸交易: 4002, 实盘: 4001
IB_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", "23"))  # 与原项目(22)区分

# ========== 数据库路径 ==========
HISTORICAL_DB_PATH = os.getenv(
    "HISTORICAL_DB_PATH", 
    str(DATA_DIR / "ibkr_us_stocks.db")
)
INTRADAY_DB_PATH = os.getenv(
    "INTRADAY_DB_PATH", 
    str(DATA_DIR / "ibkr_intraday.db")
)

# ========== DeepSeek API 配置 ==========
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com/v1")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# ========== 决策参数 ==========
DECISION_INTERVAL_MINUTES = 5
MAX_POOL_SIZE = 15              # 持仓 + 最多 7 只新股票
TOKEN_LIMIT_PER_STOCK = 500     # 每只股票注入的 Token 上限
MAX_BARS_5MIN = 78              # 5分钟线最多注入条数 (6.5小时交易时段)
MAX_BARS_HOURLY = 13            # 小时线最多注入条数
MAX_BARS_2HOUR = 4
MAX_BARS_DAILY = 20             # 日线最多注入条数

# ========== 风控参数 ==========
MAX_POSITION_PER_STOCK = 0.25   # 单股票最大仓位 25%
MAX_DAILY_TRADES = 20           # 单日最大交易次数
MIN_CASH_RESERVE = 0.1          # 最小现金保留比例 10%
ENABLE_REAL_TRADING = False     # 🛡️ 实盘保护开关（默认关闭）

# ========== 日志配置 ==========
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = LOGS_DIR / "trader_{date}.log"

SAVE_PROMPT_LOGS = True       # 是否保存 Prompt 日志
PROMPT_LOG_KEEP_LAST = 5      # 保留最近 N 个 Prompt 文件
PROMPT_LOG_DIR = "data/prompts"