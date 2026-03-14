"""AI-powered analysis report generation.

Supports both OpenAI-compatible APIs and Anthropic Claude API.
"""

import logging
import os

import pandas as pd
import requests

from ai.prompts import (
    STOCK_ANALYSIS_PROMPT,
    MARKET_SUMMARY_PROMPT,
    SCREENER_REPORT_PROMPT,
    BACKTEST_REPORT_PROMPT,
)

logger = logging.getLogger(__name__)

# API configuration - supports OpenAI-compatible endpoints
API_BASE_URL = os.getenv("LLM_API_BASE_URL", "https://api.cloubic.com/v1")
API_KEY = os.getenv("LLM_API_KEY", os.getenv("ANTHROPIC_API_KEY", ""))
API_MODEL = os.getenv("LLM_MODEL", "gpt-5.4")


class AIReporter:
    """Generate natural language analysis reports using LLM API."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        base_url: str | None = None,
    ):
        self.api_key = api_key or API_KEY
        self.model = model or API_MODEL
        self.base_url = (base_url or API_BASE_URL).rstrip("/")

    def _call_llm(self, prompt: str, max_tokens: int = 2000) -> str:
        """Call LLM API (OpenAI-compatible) and return response text."""
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.7,
        }

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]
        except requests.exceptions.HTTPError as e:
            logger.error(f"LLM API HTTP error: {e}, response: {resp.text[:500]}")
            return f"AI报告生成失败: HTTP {resp.status_code}"
        except Exception as e:
            logger.error(f"LLM API call failed: {e}")
            return f"AI报告生成失败: {e}"

    def generate_stock_report(
        self,
        symbol: str,
        name: str,
        quote: dict,
        technical_signals: dict,
        fundamental_data: dict | None = None,
        factor_score: float | None = None,
    ) -> str:
        """Generate comprehensive stock analysis report."""
        # Format technical signals
        signal_lines = []
        for key, value in technical_signals.items():
            if key not in ("score", "overall"):
                label = {
                    "macd_signal": "MACD",
                    "rsi_signal": "RSI",
                    "kdj_signal": "KDJ",
                    "boll_signal": "布林带",
                    "ma_alignment": "均线排列",
                }.get(key, key)
                signal_lines.append(f"- {label}: {value}")
        signal_lines.append(f"- 综合评估: {technical_signals.get('overall', '未知')}")
        signal_lines.append(f"- 技术评分: {technical_signals.get('score', 'N/A')}")

        # Format market cap
        market_cap = quote.get("total_market_cap", 0)
        if market_cap > 1e12:
            market_cap_str = f"{market_cap/1e12:.1f}万亿"
        elif market_cap > 1e8:
            market_cap_str = f"{market_cap/1e8:.1f}亿"
        else:
            market_cap_str = f"{market_cap:.0f}"

        # Format volume
        volume = quote.get("volume", 0)
        volume_str = f"{volume/10000:.1f}万手" if volume > 10000 else f"{volume:.0f}手"

        # Format amount
        amount = quote.get("amount", 0)
        amount_str = f"{amount/1e8:.2f}亿" if amount > 1e8 else f"{amount/1e4:.1f}万"

        # PE/PB from fundamental data
        pe_val = quote.get("pe") or (fundamental_data or {}).get("pe_ttm", "N/A")
        pb_val = quote.get("pb") or (fundamental_data or {}).get("pb", "N/A")

        # Add extra fundamental info if available
        extra_fundamental = ""
        if fundamental_data:
            fd = fundamental_data
            parts = []
            if fd.get("roe") is not None:
                parts.append(f"- ROE: {fd['roe']}%")
            if fd.get("gross_margin") is not None:
                parts.append(f"- 毛利率: {fd['gross_margin']}%")
            if fd.get("net_margin") is not None:
                parts.append(f"- 净利率: {fd['net_margin']}%")
            if fd.get("revenue_growth") is not None:
                parts.append(f"- 营收增长率: {fd['revenue_growth']}%")
            if fd.get("profit_growth") is not None:
                parts.append(f"- 净利润增长率: {fd['profit_growth']}%")
            if fd.get("debt_ratio") is not None:
                parts.append(f"- 资产负债率: {fd['debt_ratio']}%")
            if parts:
                extra_fundamental = "\n\n## 财务指标\n" + "\n".join(parts)

        prompt = STOCK_ANALYSIS_PROMPT.format(
            stock_name=name,
            symbol=symbol,
            price=quote.get("price", "N/A"),
            change_pct=quote.get("change_pct", "N/A"),
            volume=volume_str,
            amount=amount_str,
            turnover_rate=quote.get("turnover_rate", "N/A"),
            technical_signals="\n".join(signal_lines),
            pe=pe_val,
            pb=pb_val,
            market_cap=market_cap_str,
            factor_score=f"{factor_score:.1f}" if factor_score is not None else "未计算",
        )

        if extra_fundamental:
            prompt += extra_fundamental

        return self._call_llm(prompt)

    def generate_market_summary(
        self,
        index_data: str,
        up_count: int,
        down_count: int,
        limit_up: int,
        limit_down: int,
        total_amount: str,
        top_sectors: str,
        bottom_sectors: str,
    ) -> str:
        """Generate daily market summary report."""
        prompt = MARKET_SUMMARY_PROMPT.format(
            index_data=index_data,
            up_count=up_count,
            down_count=down_count,
            limit_up=limit_up,
            limit_down=limit_down,
            total_amount=total_amount,
            top_sectors=top_sectors,
            bottom_sectors=bottom_sectors,
        )
        return self._call_llm(prompt, max_tokens=1000)

    def generate_screener_report(
        self,
        screener_results: pd.DataFrame,
        strategy_name: str,
        criteria_description: str,
    ) -> str:
        """Generate narrative report explaining screened stocks."""
        results_str = screener_results.to_string(index=False) if not screener_results.empty else "无结果"

        prompt = SCREENER_REPORT_PROMPT.format(
            strategy_name=strategy_name,
            criteria_description=criteria_description,
            screener_results=results_str,
        )
        return self._call_llm(prompt, max_tokens=800)

    def generate_backtest_report(
        self,
        total_return: float,
        annual_return: float,
        max_drawdown: float,
        sharpe_ratio: float,
        win_rate: float,
        trade_count: int,
        strategy_description: str,
        test_period: str,
    ) -> str:
        """Generate backtest performance analysis report."""
        prompt = BACKTEST_REPORT_PROMPT.format(
            strategy_description=strategy_description,
            total_return=f"{total_return*100:.2f}",
            annual_return=f"{annual_return*100:.2f}",
            max_drawdown=f"{max_drawdown*100:.2f}",
            sharpe_ratio=f"{sharpe_ratio:.2f}",
            win_rate=f"{win_rate*100:.1f}",
            trade_count=trade_count,
            test_period=test_period,
        )
        return self._call_llm(prompt, max_tokens=1000)
