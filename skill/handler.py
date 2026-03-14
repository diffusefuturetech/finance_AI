#!/usr/bin/env python3
"""OpenClaw skill entry point. Called via bash by the OpenClaw agent."""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import ensure_dirs
from data.cache import DataCache
from data.fetcher import StockDataFetcher
from analysis.technical import TechnicalAnalyzer
from analysis.factor import FactorEngine
from analysis.screener import StockScreener, ScreenCriteria
from analysis.backtest import QlibBacktester
from ai.reporter import AIReporter
from charts.plotter import ChartPlotter
from skill.formatter import LarkFormatter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,  # Log to stderr, output to stdout
)
logger = logging.getLogger(__name__)


class SkillHandler:
    """Routes commands to appropriate analysis functions."""

    def __init__(self):
        ensure_dirs()
        self.cache = DataCache()
        self.fetcher = StockDataFetcher(self.cache)
        self.factor_engine = FactorEngine()
        self.plotter = ChartPlotter()
        self.formatter = LarkFormatter()
        self._reporter = None

    @property
    def reporter(self) -> AIReporter:
        if self._reporter is None:
            self._reporter = AIReporter()
        return self._reporter

    def handle_query(self, target: str) -> str:
        """Handle stock quote query."""
        try:
            quote = self.fetcher.get_realtime_quote(target)
            return self.formatter.format_quote(quote)
        except Exception as e:
            return self.formatter.format_error(f"查询失败: {e}")

    def handle_technical(self, target: str, period: int = 120) -> str:
        """Handle technical analysis request."""
        try:
            # Resolve symbol
            code = self.fetcher._resolve_symbol(target)
            quote = self.fetcher.get_realtime_quote(code)
            name = quote.get("name", code)

            # Get historical data
            df = self.fetcher.get_daily_history(code)
            if df is None or df.empty:
                return self.formatter.format_error(f"无法获取{code}的历史数据")

            # Calculate indicators
            analyzer = TechnicalAnalyzer(df)
            signals = analyzer.generate_signals()
            indicators = analyzer.compute_all()

            # Generate chart
            chart_path = self.plotter.plot_technical_dashboard(
                df, indicators, code, name, last_n=period
            )

            # Format output
            output = self.formatter.format_technical_signals(signals, chart_path)
            return output

        except Exception as e:
            return self.formatter.format_error(f"技术分析失败: {e}")

    def handle_screen(self, strategy: str, top_n: int = 20) -> str:
        """Handle stock screening request."""
        try:
            screener = StockScreener(self.fetcher, self.factor_engine)

            strategy_methods = {
                "value": ("低估值", screener.value_picks),
                "momentum": ("动量", screener.momentum_picks),
                "quality": ("高质量", screener.quality_picks),
                "growth": ("成长", screener.growth_picks),
            }

            strategy_name, method = strategy_methods.get(
                strategy, ("低估值", screener.value_picks)
            )

            results = method(top_n=top_n)
            output = self.formatter.format_screener_table(results, strategy_name)

            # Generate AI commentary if results exist
            if not results.empty:
                try:
                    ai_report = self.reporter.generate_screener_report(
                        results, strategy_name,
                        f"{strategy_name}策略，选取前{top_n}名"
                    )
                    output += f"\n\n### AI 点评\n{ai_report}"
                except Exception as e:
                    logger.warning(f"AI report generation failed: {e}")

            output += self.formatter.format_disclaimer()
            return output

        except Exception as e:
            return self.formatter.format_error(f"选股失败: {e}")

    def handle_analyze(self, target: str, export_docx: bool = False) -> str:
        """Handle full AI analysis report generation."""
        try:
            # Get real-time quote
            code = self.fetcher._resolve_symbol(target)
            quote = self.fetcher.get_realtime_quote(code)
            name = quote.get("name", code)

            # Get historical data and technical signals
            df = self.fetcher.get_daily_history(code)
            signals = {}
            chart_path = None
            radar_path = None
            if df is not None and not df.empty:
                analyzer = TechnicalAnalyzer(df)
                signals = analyzer.generate_signals()
                indicators = analyzer.compute_all()
                chart_path = self.plotter.plot_technical_dashboard(
                    df, indicators, code, name
                )

            # Get fundamental data
            fundamental = self.fetcher.get_financial_data(code)

            # Factor scores (multi-dimensional, connected to technical signals)
            factor_scores = self._compute_factor_scores(quote, fundamental, signals)
            factor_score = (
                sum(factor_scores.values()) / len(factor_scores)
                if factor_scores else None
            )

            # Generate radar chart if factor scores available
            if factor_scores:
                radar_path = self.plotter.plot_factor_radar(
                    factor_scores, code, name
                )

            # Get historical valuation for percentile chart
            valuation_path = None
            try:
                val_df = self.fetcher.get_historical_valuation(code)
                if not val_df.empty:
                    valuation_path = self.plotter.plot_valuation_history(
                        val_df, code, name
                    )
            except Exception as e:
                logger.warning(f"Historical valuation chart failed: {e}")

            # Generate AI report
            report = self.reporter.generate_stock_report(
                symbol=code,
                name=name,
                quote=quote,
                technical_signals=signals,
                fundamental_data=fundamental,
                factor_score=factor_score,
                factor_scores=factor_scores,
            )

            # Export to Word if requested
            if export_docx:
                from ai.docx_export import DocxExporter
                exporter = DocxExporter()
                docx_path = exporter.generate_stock_report(
                    symbol=code,
                    name=name,
                    quote=quote,
                    fundamental=fundamental,
                    signals=signals,
                    factor_scores=factor_scores,
                    ai_commentary=report,
                    technical_chart_path=chart_path,
                    radar_chart_path=radar_path,
                    valuation_chart_path=valuation_path,
                )
                return f"Word报告已生成: {docx_path}"

            # Compose full output
            output_parts = [
                self.formatter.format_quote(quote),
                "",
                self.formatter.format_technical_signals(signals, chart_path),
                "",
                "## AI 分析报告",
                "",
                report,
            ]

            return "\n".join(output_parts)

        except Exception as e:
            return self.formatter.format_error(f"分析失败: {e}")

    def _compute_factor_scores(
        self, quote: dict, fundamental: dict, signals: dict | None = None,
    ) -> dict[str, float]:
        """Compute multi-dimensional factor scores using sigmoid curves.

        Dimensions: 价值, 质量, 成长, 动量, 盈利质量, 安全
        """
        import math

        def sigmoid(x: float, center: float, steepness: float) -> float:
            """Sigmoid mapping to 0-100 scale, centered at `center`."""
            try:
                return 100 / (1 + math.exp(steepness * (x - center)))
            except OverflowError:
                return 0.0 if steepness * (x - center) > 0 else 100.0

        scores = {}
        pe = quote.get("pe") or (fundamental or {}).get("pe_ttm")
        pb = quote.get("pb") or (fundamental or {}).get("pb")
        fd = fundamental or {}

        # 价值 (Value): lower PE/PB = higher score, sigmoid curve
        value_parts = []
        if pe is not None and pe != 0:
            if pe > 0:
                # Center at PE=25, steepness 0.15: PE=10→85, PE=25→50, PE=50→15
                value_parts.append(sigmoid(pe, 25, 0.15))
            else:
                # Negative PE (loss-making) → low value score
                value_parts.append(max(0, 10 - abs(pe) * 0.02))
        if pb is not None and pb > 0:
            # Center at PB=3, steepness 1.5: PB=1→95, PB=3→50, PB=6→5
            value_parts.append(sigmoid(pb, 3, 1.5))
        if value_parts:
            scores["价值"] = round(sum(value_parts) / len(value_parts), 1)

        # 质量 (Quality): ROE sigmoid centered at 12% (A-share median)
        roe = fd.get("roe")
        if roe is not None:
            # Center at ROE=12%, steepness -0.3: ROE=25→98, ROE=12→50, ROE=0→3
            scores["质量"] = round(sigmoid(roe, 12, -0.3), 1)

        # 成长 (Growth): revenue + profit growth
        rev_g = fd.get("revenue_growth")
        prof_g = fd.get("profit_growth")
        if rev_g is not None or prof_g is not None:
            g_vals = [v for v in [rev_g, prof_g] if v is not None]
            avg_g = sum(g_vals) / len(g_vals)
            # Center at 15% growth, steepness -0.1
            scores["成长"] = round(sigmoid(avg_g, 15, -0.1), 1)

        # 动量 (Momentum): from technical signals score (-100~100 → 0~100)
        if signals and "score" in signals:
            scores["动量"] = round((signals["score"] + 100) / 2, 1)
        else:
            scores["动量"] = 50.0

        # 盈利质量 (Earnings Quality): OCF/EPS ratio
        ocfps = fd.get("ocfps")
        eps = fd.get("eps")
        if ocfps is not None and eps is not None and abs(eps) > 0.001:
            ocf_eps_ratio = ocfps / eps
            # Center at 0.8 ratio, steepness -3: ratio=1.5→92, ratio=0.8→50, ratio=0→8
            scores["盈利质量"] = round(sigmoid(ocf_eps_ratio, 0.8, -3), 1)
        elif eps is not None and eps < 0:
            scores["盈利质量"] = 10.0  # Loss-making → poor quality

        # 安全 (Safety): debt ratio + liquidity
        debt = fd.get("debt_ratio")
        current = fd.get("current_ratio")
        if debt is not None:
            # Center at 50% debt, steepness 0.08
            safety = sigmoid(debt, 50, 0.08)
            # Boost/penalize based on current ratio if available
            if current is not None:
                if current >= 2.0:
                    safety = min(100, safety + 10)
                elif current < 1.0:
                    safety = max(0, safety - 15)
            scores["安全"] = round(safety, 1)

        return scores

    def handle_backtest(self, market: str, period: str) -> str:
        """Handle backtesting request."""
        try:
            backtester = QlibBacktester()
            result = backtester.quick_backtest(market=market, test_period=period)

            # Format metrics
            output = self.formatter.format_backtest_summary(
                total_return=result.total_return,
                annual_return=result.annual_return,
                max_drawdown=result.max_drawdown,
                sharpe_ratio=result.sharpe_ratio,
                win_rate=result.win_rate,
                trade_count=result.trade_count,
            )

            # Generate equity curve chart
            if result.cumulative_returns is not None and len(result.cumulative_returns) > 0:
                chart_path = self.plotter.plot_equity_curve(
                    result.cumulative_returns,
                    result.benchmark_returns,
                    f"LGBModel-{market}",
                )
                output += f"\n\n📊 净值曲线: `{chart_path}`"

            # AI commentary
            try:
                ai_report = self.reporter.generate_backtest_report(
                    total_return=result.total_return,
                    annual_return=result.annual_return,
                    max_drawdown=result.max_drawdown,
                    sharpe_ratio=result.sharpe_ratio,
                    win_rate=result.win_rate,
                    trade_count=result.trade_count,
                    strategy_description=f"LGBModel + Alpha158因子 + TopkDropout策略, 市场: {market}",
                    test_period=f"最近{period}",
                )
                output += f"\n\n### AI 分析\n{ai_report}"
            except Exception as e:
                logger.warning(f"AI backtest report failed: {e}")

            output += self.formatter.format_disclaimer()
            return output

        except Exception as e:
            return self.formatter.format_error(f"回测失败: {e}")

    def handle_market(self) -> str:
        """Handle daily market summary."""
        try:
            import akshare as ak

            # Get major indices
            indices = {
                "上证指数": "000001",
                "深证成指": "399001",
                "创业板指": "399006",
                "沪深300": "000300",
            }

            index_lines = []
            for name, code in indices.items():
                try:
                    df = ak.stock_zh_index_spot_em()
                    row = df[df["代码"] == code]
                    if not row.empty:
                        r = row.iloc[0]
                        change = float(r.get("涨跌幅", 0))
                        sign = "+" if change >= 0 else ""
                        index_lines.append(
                            f"- {name}: {r.get('最新价', 'N/A')} ({sign}{change:.2f}%)"
                        )
                except Exception:
                    pass

            # Get market breadth
            spot_df = ak.stock_zh_a_spot_em()
            changes = spot_df["涨跌幅"].astype(float)
            up_count = int((changes > 0).sum())
            down_count = int((changes < 0).sum())
            limit_up = int((changes >= 9.9).sum())
            limit_down = int((changes <= -9.9).sum())

            total_amount = float(spot_df["成交额"].astype(float).sum())
            total_amount_str = f"{total_amount/1e12:.2f}万亿"

            # Sector performance (simplified)
            top_sectors = "暂无板块数据"
            bottom_sectors = "暂无板块数据"
            try:
                sector_df = ak.stock_board_industry_name_em()
                if not sector_df.empty and "涨跌幅" in sector_df.columns:
                    sector_df["涨跌幅"] = pd.to_numeric(sector_df["涨跌幅"], errors="coerce")
                    sorted_sectors = sector_df.sort_values("涨跌幅", ascending=False)
                    top3 = sorted_sectors.head(3)
                    bottom3 = sorted_sectors.tail(3)
                    top_sectors = ", ".join(
                        f"{r['板块名称']}({r['涨跌幅']:+.2f}%)"
                        for _, r in top3.iterrows()
                    )
                    bottom_sectors = ", ".join(
                        f"{r['板块名称']}({r['涨跌幅']:+.2f}%)"
                        for _, r in bottom3.iterrows()
                    )
            except Exception:
                pass

            # Generate AI summary
            index_data = "\n".join(index_lines) if index_lines else "暂无指数数据"

            ai_summary = self.reporter.generate_market_summary(
                index_data=index_data,
                up_count=up_count,
                down_count=down_count,
                limit_up=limit_up,
                limit_down=limit_down,
                total_amount=total_amount_str,
                top_sectors=top_sectors,
                bottom_sectors=bottom_sectors,
            )

            # Compose output
            output_parts = [
                "## 📊 今日A股市场总结",
                "",
                "### 主要指数",
                index_data,
                "",
                f"### 涨跌统计",
                f"- 上涨: {up_count} | 下跌: {down_count} | 涨停: {limit_up} | 跌停: {limit_down}",
                f"- 沪深成交额: {total_amount_str}",
                "",
                f"### 板块表现",
                f"- 领涨: {top_sectors}",
                f"- 领跌: {bottom_sectors}",
                "",
                "### AI 市场解读",
                ai_summary,
                self.formatter.format_disclaimer(),
            ]

            return "\n".join(output_parts)

        except Exception as e:
            return self.formatter.format_error(f"市场总结生成失败: {e}")


def main():
    parser = argparse.ArgumentParser(description="Finance AI Skill Handler")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # query
    query_p = subparsers.add_parser("query", help="Query stock quote")
    query_p.add_argument("target", help="Stock code or name")

    # technical
    tech_p = subparsers.add_parser("technical", help="Technical analysis")
    tech_p.add_argument("target", help="Stock code or name")
    tech_p.add_argument("--period", type=int, default=120, help="Days to analyze")

    # screen
    screen_p = subparsers.add_parser("screen", help="Stock screening")
    screen_p.add_argument("strategy", nargs="?", default="value",
                          choices=["value", "momentum", "quality", "growth"],
                          help="Screening strategy")
    screen_p.add_argument("--top-n", type=int, default=20)

    # analyze
    analyze_p = subparsers.add_parser("analyze", help="Full AI analysis")
    analyze_p.add_argument("target", help="Stock code or name")
    analyze_p.add_argument("--docx", action="store_true", help="Export as Word document")

    # backtest
    bt_p = subparsers.add_parser("backtest", help="Strategy backtest")
    bt_p.add_argument("market", nargs="?", default="csi300",
                      help="Market: csi300, csi500")
    bt_p.add_argument("period", nargs="?", default="6m",
                      help="Period: 3m, 6m, 1y, 2y")

    # market
    subparsers.add_parser("market", help="Daily market summary")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    handler = SkillHandler()

    dispatch = {
        "query": lambda: handler.handle_query(args.target),
        "technical": lambda: handler.handle_technical(args.target, args.period),
        "screen": lambda: handler.handle_screen(args.strategy, getattr(args, "top_n", 20)),
        "analyze": lambda: handler.handle_analyze(args.target, getattr(args, "docx", False)),
        "backtest": lambda: handler.handle_backtest(args.market, args.period),
        "market": lambda: handler.handle_market(),
    }

    result = dispatch[args.command]()
    print(result)  # OpenClaw captures stdout


if __name__ == "__main__":
    main()
