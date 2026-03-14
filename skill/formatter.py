"""Format analysis results for Lark/Feishu display."""

import pandas as pd


class LarkFormatter:
    """Format analysis results as markdown for Lark/Feishu rendering."""

    @staticmethod
    def format_quote(quote: dict) -> str:
        """Format real-time quote as markdown."""
        change_emoji = "📈" if quote.get("change_pct", 0) >= 0 else "📉"
        change_sign = "+" if quote.get("change_pct", 0) >= 0 else ""

        # Format volume/amount
        volume = quote.get("volume", 0)
        volume_str = f"{volume/10000:.1f}万手" if volume > 10000 else f"{volume:.0f}手"
        amount = quote.get("amount", 0)
        amount_str = f"{amount/1e8:.2f}亿" if amount > 1e8 else f"{amount/1e4:.1f}万"

        # Format market cap
        mcap = quote.get("total_market_cap", 0)
        mcap_str = f"{mcap/1e12:.2f}万亿" if mcap > 1e12 else f"{mcap/1e8:.1f}亿"

        lines = [
            f"## {change_emoji} {quote.get('name', '')}（{quote.get('code', '')}）",
            "",
            f"| 指标 | 值 |",
            f"|------|------|",
            f"| 最新价 | ¥{quote.get('price', 'N/A')} |",
            f"| 涨跌幅 | {change_sign}{quote.get('change_pct', 'N/A')}% |",
            f"| 涨跌额 | {change_sign}{quote.get('change_amount', 'N/A')} |",
            f"| 今开 | ¥{quote.get('open', 'N/A')} |",
            f"| 最高 | ¥{quote.get('high', 'N/A')} |",
            f"| 最低 | ¥{quote.get('low', 'N/A')} |",
            f"| 昨收 | ¥{quote.get('prev_close', 'N/A')} |",
            f"| 成交量 | {volume_str} |",
            f"| 成交额 | {amount_str} |",
            f"| 换手率 | {quote.get('turnover_rate', 'N/A')}% |",
            f"| 市盈率 | {quote.get('pe', 'N/A')} |",
            f"| 市净率 | {quote.get('pb', 'N/A')} |",
            f"| 总市值 | {mcap_str} |",
        ]
        return "\n".join(lines)

    @staticmethod
    def format_technical_signals(signals: dict, chart_path: str | None = None) -> str:
        """Format technical analysis signals."""
        score = signals.get("score", 0)
        overall = signals.get("overall", "未知")

        # Score bar visualization
        bar_len = 20
        filled = int((score + 100) / 200 * bar_len)
        bar = "█" * filled + "░" * (bar_len - filled)

        lines = [
            f"## 📊 技术分析信号",
            "",
            f"**综合评估: {overall}** (评分: {score})",
            f"`[{bar}]`",
            "",
            "| 指标 | 信号 |",
            "|------|------|",
        ]

        label_map = {
            "macd_signal": "MACD",
            "rsi_signal": "RSI",
            "kdj_signal": "KDJ",
            "boll_signal": "布林带",
            "ma_alignment": "均线排列",
        }

        for key, label in label_map.items():
            if key in signals:
                lines.append(f"| {label} | {signals[key]} |")

        if chart_path:
            lines.extend(["", f"📈 技术分析图表已生成: `{chart_path}`"])

        return "\n".join(lines)

    @staticmethod
    def format_screener_table(df: pd.DataFrame, strategy_name: str = "") -> str:
        """Format screening results as ranked table."""
        if df.empty:
            return "未找到符合条件的股票。"

        lines = [
            f"## 🔍 {strategy_name}选股结果",
            "",
            "| 排名 | 代码 | 名称 | 现价 | 涨跌% | PE | PB | 市值(亿) | 评分 |",
            "|------|------|------|------|-------|-----|-----|---------|------|",
        ]

        for _, row in df.iterrows():
            rank = int(row.get("rank", 0))
            code = row.get("code", "")
            name = row.get("name", "")
            price = f"{row.get('price', 0):.2f}"
            change = f"{row.get('change_pct', 0):+.2f}"
            pe = f"{row.get('pe_ttm', 0):.1f}" if row.get("pe_ttm") else "N/A"
            pb = f"{row.get('pb', 0):.2f}" if row.get("pb") else "N/A"
            mcap = f"{row.get('market_cap_yi', 0):.0f}" if row.get("market_cap_yi") else "N/A"
            score = f"{row.get('score', 0):.1f}" if row.get("score") else "N/A"
            lines.append(f"| {rank} | {code} | {name} | {price} | {change}% | {pe} | {pb} | {mcap} | {score} |")

        return "\n".join(lines)

    @staticmethod
    def format_backtest_summary(
        total_return: float,
        annual_return: float,
        max_drawdown: float,
        sharpe_ratio: float,
        win_rate: float,
        trade_count: int,
        chart_path: str | None = None,
    ) -> str:
        """Format backtest results."""
        lines = [
            "## 📈 策略回测结果",
            "",
            "| 指标 | 值 |",
            "|------|------|",
            f"| 总收益 | {total_return*100:.2f}% |",
            f"| 年化收益 | {annual_return*100:.2f}% |",
            f"| 最大回撤 | {max_drawdown*100:.2f}% |",
            f"| 夏普比率 | {sharpe_ratio:.2f} |",
            f"| 胜率 | {win_rate*100:.1f}% |",
            f"| 交易次数 | {trade_count} |",
        ]

        if chart_path:
            lines.extend(["", f"📊 净值曲线已生成: `{chart_path}`"])

        return "\n".join(lines)

    @staticmethod
    def format_error(error_msg: str) -> str:
        """Format error message."""
        return f"❌ **错误**: {error_msg}"

    @staticmethod
    def format_disclaimer() -> str:
        """Return standard disclaimer."""
        return "\n\n---\n⚠️ *本报告由AI生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。*"
