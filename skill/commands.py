"""Command parsing and stock code/name resolution."""

import re


# Common Chinese command patterns
QUERY_PATTERNS = [
    r"查询\s*(.+)",
    r"查\s*(.+)",
    r"看看\s*(.+)",
    r"(.+)\s*行情",
    r"(.+)\s*怎么样",
    r"(.+)\s*多少钱",
]

TECHNICAL_PATTERNS = [
    r"技术分析\s*(.+)",
    r"技术面\s*(.+)",
    r"(.+)\s*技术分析",
    r"(.+)\s*技术面",
]

ANALYZE_PATTERNS = [
    r"分析\s*(.+)",
    r"帮我分析\s*(.+)",
    r"(.+)\s*分析报告",
    r"研报\s*(.+)",
]

SCREEN_PATTERNS = [
    r"选股\s*(.*)",
    r"(低估值|价值|动量|成长|高质量)\s*选股",
    r"筛选\s*(.*)",
]

BACKTEST_PATTERNS = [
    r"回测\s*(.+)",
    r"策略回测\s*(.+)",
]

MARKET_PATTERNS = [
    r"大盘",
    r"市场总结",
    r"今日市场",
    r"市场概况",
]

# Strategy name mapping
STRATEGY_MAP = {
    "低估值": "value",
    "价值": "value",
    "动量": "momentum",
    "趋势": "momentum",
    "高质量": "quality",
    "质量": "quality",
    "成长": "growth",
    "增长": "growth",
    "value": "value",
    "momentum": "momentum",
    "quality": "quality",
    "growth": "growth",
}

# Backtest period mapping
PERIOD_MAP = {
    "三个月": "3m",
    "3个月": "3m",
    "半年": "6m",
    "六个月": "6m",
    "6个月": "6m",
    "一年": "1y",
    "1年": "1y",
    "两年": "2y",
    "2年": "2y",
    "最近半年": "6m",
    "最近一年": "1y",
    "3m": "3m",
    "6m": "6m",
    "1y": "1y",
    "2y": "2y",
}

# Market name mapping
MARKET_MAP = {
    "沪深300": "csi300",
    "中证500": "csi500",
    "中证1000": "csi1000",
    "csi300": "csi300",
    "csi500": "csi500",
    "300": "csi300",
    "500": "csi500",
}


def parse_command(text: str) -> tuple[str, dict]:
    """Parse a Chinese natural language command.

    Returns:
        (command_type, params) tuple where command_type is one of:
        'query', 'technical', 'analyze', 'screen', 'backtest', 'market', 'unknown'
    """
    text = text.strip()

    # Market summary
    for pattern in MARKET_PATTERNS:
        if re.search(pattern, text):
            return "market", {}

    # Stock screening
    for pattern in SCREEN_PATTERNS:
        m = re.search(pattern, text)
        if m:
            strategy_text = m.group(1).strip() if m.group(1) else "value"
            strategy = STRATEGY_MAP.get(strategy_text, "value")
            return "screen", {"strategy": strategy}

    # Backtest
    for pattern in BACKTEST_PATTERNS:
        m = re.search(pattern, text)
        if m:
            params_text = m.group(1).strip()
            market = "csi300"
            period = "6m"
            for key, val in MARKET_MAP.items():
                if key in params_text:
                    market = val
                    break
            for key, val in PERIOD_MAP.items():
                if key in params_text:
                    period = val
                    break
            return "backtest", {"market": market, "period": period}

    # Full analysis
    for pattern in ANALYZE_PATTERNS:
        m = re.search(pattern, text)
        if m:
            target = m.group(1).strip()
            return "analyze", {"target": target}

    # Technical analysis
    for pattern in TECHNICAL_PATTERNS:
        m = re.search(pattern, text)
        if m:
            target = m.group(1).strip()
            return "technical", {"target": target}

    # Stock query
    for pattern in QUERY_PATTERNS:
        m = re.search(pattern, text)
        if m:
            target = m.group(1).strip()
            return "query", {"target": target}

    # If it looks like a stock code, treat as query
    if re.match(r"^\d{6}$", text):
        return "query", {"target": text}

    return "unknown", {"text": text}
