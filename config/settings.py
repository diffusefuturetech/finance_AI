"""Global configuration for finance_AI."""

import os
from pathlib import Path

# Base directories
PROJECT_ROOT = Path(__file__).parent.parent
HOME_DIR = Path.home()

# Qlib data
QLIB_DATA_DIR = Path(
    os.getenv("QLIB_DATA_DIR", str(HOME_DIR / ".qlib/qlib_data/cn_data_finance_ai"))
).expanduser()

# Cache database
CACHE_DB_PATH = Path(
    os.getenv("CACHE_DB_PATH", str(HOME_DIR / ".finance_ai/cache.db"))
).expanduser()

# Staging directory for CSV files before Qlib conversion
CSV_STAGING_DIR = Path("/tmp/finance_ai_staging")

# Chart output directory
CHART_OUTPUT_DIR = Path("/tmp/finance_ai_charts")

# Claude API
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

# Feishu/Lark
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL", "")

# Data defaults
DEFAULT_START_DATE = "20150101"
DEFAULT_MARKET = "csi300"

# Stock code prefix mapping
# 6xx, 9xx -> Shanghai (SH); 0xx, 2xx, 3xx -> Shenzhen (SZ)
SH_PREFIXES = ("6", "9")
SZ_PREFIXES = ("0", "2", "3")


def symbol_to_qlib(code: str) -> str:
    """Convert bare stock code to Qlib format. e.g. '600519' -> 'SH600519'."""
    code = code.strip()
    if code.startswith(("SH", "SZ")):
        return code
    if code.startswith(SH_PREFIXES):
        return f"SH{code}"
    elif code.startswith(SZ_PREFIXES):
        return f"SZ{code}"
    return code


def qlib_to_symbol(qlib_code: str) -> str:
    """Convert Qlib code to bare code. e.g. 'SH600519' -> '600519'."""
    if qlib_code.startswith(("SH", "SZ")):
        return qlib_code[2:]
    return qlib_code


def ensure_dirs() -> None:
    """Create necessary directories if they don't exist."""
    for d in [QLIB_DATA_DIR, CACHE_DB_PATH.parent, CSV_STAGING_DIR, CHART_OUTPUT_DIR]:
        d.mkdir(parents=True, exist_ok=True)
