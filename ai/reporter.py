"""AI-powered analysis report generation.

Supports both OpenAI-compatible APIs and Anthropic Claude API.
"""

import logging
import os

import pandas as pd
import requests

from ai.prompts import (
    STOCK_ANALYSIS_PROMPT,
    COMPREHENSIVE_ANALYSIS_PROMPT,
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
            resp = requests.post(url, json=payload, headers=headers, timeout=120)
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
        factor_scores: dict[str, float] | None = None,
    ) -> str:
        """Generate comprehensive stock analysis report."""
        from datetime import datetime

        fd = fundamental_data or {}

        # Format technical signals
        signal_lines = []
        label_map = {
            "macd_signal": "MACD", "rsi_signal": "RSI", "kdj_signal": "KDJ",
            "boll_signal": "布林带", "ma_alignment": "均线排列",
        }
        for key, value in technical_signals.items():
            if key in label_map:
                signal_lines.append(f"- {label_map[key]}: {value}")
        signal_lines.append(f"- 综合评估: {technical_signals.get('overall', '未知')}")
        signal_lines.append(f"- 技术评分: {technical_signals.get('score', 'N/A')}")

        # Format market cap
        market_cap = quote.get("total_market_cap", 0) or fd.get("total_mv", 0)
        if market_cap > 1e12:
            market_cap_str = f"{market_cap/1e12:.1f}万亿"
        elif market_cap > 1e8:
            market_cap_str = f"{market_cap/1e8:.1f}亿"
        else:
            market_cap_str = f"{market_cap:.0f}"

        # Format volume / amount
        volume = quote.get("volume", 0)
        volume_str = f"{volume/10000:.1f}万手" if volume > 10000 else f"{volume:.0f}手"
        amount = quote.get("amount", 0)
        amount_str = f"{amount/1e8:.2f}亿" if amount > 1e8 else f"{amount/1e4:.1f}万"

        # Factor breakdown
        if factor_scores:
            factor_lines = [f"- {k}: {v:.1f}" for k, v in factor_scores.items()]
            avg = sum(factor_scores.values()) / len(factor_scores)
            factor_lines.append(f"- 综合得分: {avg:.1f}")
            factor_breakdown = "\n".join(factor_lines)
        else:
            factor_breakdown = f"综合得分: {factor_score:.1f}" if factor_score is not None else "未计算"

        # Data freshness
        report_date = fd.get("report_date", "")
        data_age_days = "未知"
        freshness_warning = ""
        if report_date:
            try:
                rd = datetime.strptime(str(report_date)[:10], "%Y-%m-%d")
                age = (datetime.now() - rd).days
                data_age_days = str(age)
                if age > 180:
                    freshness_warning = "\n  ⚠️ 财报数据超过180天，请关注最新财报披露"
            except (ValueError, TypeError):
                pass

        # Helper to format optional values
        def fv(val, suffix=""):
            if val is None:
                return "N/A"
            return f"{val}{suffix}"

        prompt = STOCK_ANALYSIS_PROMPT.format(
            stock_name=name,
            symbol=symbol,
            price=quote.get("price", "N/A"),
            change_pct=quote.get("change_pct", "N/A"),
            volume=volume_str,
            amount=amount_str,
            turnover_rate=quote.get("turnover_rate", "N/A"),
            pe=fv(quote.get("pe") or fd.get("pe_ttm")),
            pb=fv(quote.get("pb") or fd.get("pb")),
            ps=fv(quote.get("ps") or fd.get("ps_ttm")),
            market_cap=market_cap_str,
            eps=fv(fd.get("eps"), "元"),
            bps=fv(fd.get("bps"), "元"),
            ocfps=fv(fd.get("ocfps"), "元"),
            roe=fv(fd.get("roe"), "%"),
            gross_margin=fv(fd.get("gross_margin"), "%"),
            net_margin=fv(fd.get("net_margin"), "%"),
            operating_margin=fv(fd.get("operating_margin"), "%"),
            revenue_growth=fv(fd.get("revenue_growth"), "%"),
            profit_growth=fv(fd.get("profit_growth"), "%"),
            debt_ratio=fv(fd.get("debt_ratio"), "%"),
            current_ratio=fv(fd.get("current_ratio")),
            quick_ratio=fv(fd.get("quick_ratio")),
            technical_signals="\n".join(signal_lines),
            factor_breakdown=factor_breakdown,
            industry=fd.get("industry", "未知"),
            report_date=report_date or "未知",
            data_age_days=data_age_days,
            data_freshness_warning=freshness_warning,
        )

        return self._call_llm(prompt, max_tokens=3000)

    def generate_comprehensive_report(
        self,
        symbol: str,
        name: str,
        quote: dict,
        technical_signals: dict,
        fundamental_data: dict | None = None,
        factor_score: float | None = None,
        factor_scores: dict[str, float] | None = None,
        web_collected_data: str = "",
    ) -> str:
        """Generate comprehensive analysis report with buy/sell recommendation.

        This is the upgraded version of generate_stock_report() that includes
        web-collected data (news, ratings, fund flow, etc.) and produces
        explicit investment recommendations.
        """
        from datetime import datetime

        fd = fundamental_data or {}

        # Format technical signals
        signal_lines = []
        label_map = {
            "macd_signal": "MACD", "rsi_signal": "RSI", "kdj_signal": "KDJ",
            "boll_signal": "布林带", "ma_alignment": "均线排列",
        }
        for key, value in technical_signals.items():
            if key in label_map:
                signal_lines.append(f"- {label_map[key]}: {value}")
        signal_lines.append(f"- 综合评估: {technical_signals.get('overall', '未知')}")
        signal_lines.append(f"- 技术评分: {technical_signals.get('score', 'N/A')}")

        # Format market cap
        market_cap = quote.get("total_market_cap", 0) or fd.get("total_mv", 0)
        if market_cap > 1e12:
            market_cap_str = f"{market_cap/1e12:.1f}万亿"
        elif market_cap > 1e8:
            market_cap_str = f"{market_cap/1e8:.1f}亿"
        else:
            market_cap_str = f"{market_cap:.0f}"

        # Format volume / amount
        volume = quote.get("volume", 0)
        volume_str = f"{volume/10000:.1f}万手" if volume > 10000 else f"{volume:.0f}手"
        amount = quote.get("amount", 0)
        amount_str = f"{amount/1e8:.2f}亿" if amount > 1e8 else f"{amount/1e4:.1f}万"

        # Factor breakdown
        if factor_scores:
            factor_lines = [f"- {k}: {v:.1f}" for k, v in factor_scores.items()]
            avg = sum(factor_scores.values()) / len(factor_scores)
            factor_lines.append(f"- 综合得分: {avg:.1f}")
            factor_breakdown = "\n".join(factor_lines)
        else:
            factor_breakdown = f"综合得分: {factor_score:.1f}" if factor_score is not None else "未计算"

        # Data freshness
        report_date = fd.get("report_date", "")
        data_age_days = "未知"
        freshness_warning = ""
        if report_date:
            try:
                rd = datetime.strptime(str(report_date)[:10], "%Y-%m-%d")
                age = (datetime.now() - rd).days
                data_age_days = str(age)
                if age > 180:
                    freshness_warning = "\n  财报数据超过180天，请关注最新财报披露"
            except (ValueError, TypeError):
                pass

        def fv(val, suffix=""):
            if val is None:
                return "N/A"
            return f"{val}{suffix}"

        prompt = COMPREHENSIVE_ANALYSIS_PROMPT.format(
            stock_name=name,
            symbol=symbol,
            price=quote.get("price", "N/A"),
            change_pct=quote.get("change_pct", "N/A"),
            volume=volume_str,
            amount=amount_str,
            turnover_rate=quote.get("turnover_rate", "N/A"),
            pe=fv(quote.get("pe") or fd.get("pe_ttm")),
            pb=fv(quote.get("pb") or fd.get("pb")),
            ps=fv(quote.get("ps") or fd.get("ps_ttm")),
            market_cap=market_cap_str,
            eps=fv(fd.get("eps"), "元"),
            bps=fv(fd.get("bps"), "元"),
            ocfps=fv(fd.get("ocfps"), "元"),
            roe=fv(fd.get("roe"), "%"),
            gross_margin=fv(fd.get("gross_margin"), "%"),
            net_margin=fv(fd.get("net_margin"), "%"),
            operating_margin=fv(fd.get("operating_margin"), "%"),
            revenue_growth=fv(fd.get("revenue_growth"), "%"),
            profit_growth=fv(fd.get("profit_growth"), "%"),
            debt_ratio=fv(fd.get("debt_ratio"), "%"),
            current_ratio=fv(fd.get("current_ratio")),
            quick_ratio=fv(fd.get("quick_ratio")),
            technical_signals="\n".join(signal_lines),
            factor_breakdown=factor_breakdown,
            industry=fd.get("industry", "未知"),
            report_date=report_date or "未知",
            data_age_days=data_age_days,
            data_freshness_warning=freshness_warning,
            web_collected_data=web_collected_data or "暂无额外市场数据",
        )

        return self._call_llm(prompt, max_tokens=6000)

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
