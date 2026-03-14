"""全网信息采集器 - 聚合AKShare多维度数据用于综合分析。

采集8大维度：新闻舆情、机构研报、资金流向、龙虎榜、
盈利预测、股东变动、融资融券、市场情绪。
每个模块独立 try/except，单模块失败不影响整体。
"""

import logging
from datetime import date, timedelta

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)


class WebCollector:
    """全网信息采集器 - 为指定股票聚合多维度公开数据。"""

    def collect_all(self, code: str) -> dict:
        """一次性采集8大维度数据。

        Args:
            code: 6位股票代码，如 '605318'

        Returns:
            dict with keys: news, ratings, fund_flow, lhb,
                           forecast, holders, margin, sentiment
        """
        result = {}
        collectors = {
            "news": self._collect_news,
            "ratings": self._collect_ratings,
            "fund_flow": self._collect_fund_flow,
            "lhb": self._collect_lhb,
            "forecast": self._collect_forecast,
            "holders": self._collect_holders,
            "margin": self._collect_margin,
            "sentiment": self._collect_sentiment,
        }
        for key, func in collectors.items():
            try:
                result[key] = func(code)
            except Exception as e:
                logger.warning(f"[WebCollector] {key} failed for {code}: {e}")
                result[key] = None
        return result

    # ------------------------------------------------------------------
    # 1. 新闻舆情
    # ------------------------------------------------------------------
    def _collect_news(self, code: str) -> list[dict] | None:
        """获取最近新闻标题和摘要（东方财富）。"""
        df = ak.stock_news_em(symbol=code)
        if df is None or df.empty:
            return None
        df = df.head(20)
        news_list = []
        for _, row in df.iterrows():
            news_list.append({
                "title": str(row.get("新闻标题", "")),
                "content": str(row.get("新闻内容", ""))[:200],
                "time": str(row.get("发布时间", "")),
                "source": str(row.get("文章来源", "")),
            })
        return news_list

    # ------------------------------------------------------------------
    # 2. 机构研报与评级
    # ------------------------------------------------------------------
    def _collect_ratings(self, code: str) -> dict | None:
        """获取机构研报（东方财富）。"""
        df = ak.stock_research_report_em(symbol=code)
        if df is None or df.empty:
            return None
        df = df.head(20)
        ratings = []
        for _, row in df.iterrows():
            ratings.append({
                "broker": str(row.get("机构", "")),
                "rating": str(row.get("东财评级", "")),
                "date": str(row.get("日期", ""))[:10],
                "title": str(row.get("报告名称", "")),
            })

        # 统计评级分布
        rating_counts = {}
        for r in ratings:
            rt = r["rating"]
            if rt and rt != "nan":
                rating_counts[rt] = rating_counts.get(rt, 0) + 1

        return {
            "details": ratings,
            "rating_distribution": rating_counts,
            "total_count": len(ratings),
        }

    # ------------------------------------------------------------------
    # 3. 资金流向
    # ------------------------------------------------------------------
    def _collect_fund_flow(self, code: str) -> dict | None:
        """获取个股资金流向（stock_individual_fund_flow）。"""
        market = "sh" if code.startswith(("6", "9")) else "sz"
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is None or df.empty:
            return None

        # Take last 5 trading days
        df = df.tail(5)
        flow_list = []
        for _, row in df.iterrows():
            flow_list.append({
                "date": str(row.get("日期", "")),
                "main_net_inflow": self._safe_float(row.get("主力净流入-净额")),
                "main_net_pct": self._safe_float(row.get("主力净流入-净占比")),
                "super_large_net": self._safe_float(row.get("超大单净流入-净额")),
                "large_net": self._safe_float(row.get("大单净流入-净额")),
                "medium_net": self._safe_float(row.get("中单净流入-净额")),
                "small_net": self._safe_float(row.get("小单净流入-净额")),
            })

        # Sum last 5 days
        total_main = sum(
            f["main_net_inflow"] for f in flow_list
            if f["main_net_inflow"] is not None
        )

        return {
            "daily": flow_list,
            "main_net_5d": total_main,
        }

    # ------------------------------------------------------------------
    # 4. 龙虎榜
    # ------------------------------------------------------------------
    def _collect_lhb(self, code: str) -> list[dict] | None:
        """获取龙虎榜数据（东方财富 - 近一月统计）。"""
        try:
            df = ak.stock_lhb_stock_statistic_em(symbol="近一月")
            if df is None or df.empty:
                return None
            # Filter for our stock
            code_col = None
            for col in df.columns:
                if "代码" in col:
                    code_col = col
                    break
            if code_col is None:
                return None
            matched = df[df[code_col].astype(str) == code]
            if matched.empty:
                return None
            records = []
            for _, row in matched.iterrows():
                records.append({
                    "date": str(row.get("上榜日期", row.get("日期", ""))),
                    "reason": str(row.get("上榜原因", "")),
                    "buy_total": self._safe_float(row.get("买入总额")),
                    "sell_total": self._safe_float(row.get("卖出总额")),
                    "net_amount": self._safe_float(row.get("净买入额")),
                    "appearances": self._safe_int(row.get("上榜次数")),
                })
            return records if records else None
        except Exception as e:
            logger.debug(f"LHB failed: {e}")
            return None

    # ------------------------------------------------------------------
    # 5. 盈利预测
    # ------------------------------------------------------------------
    def _collect_forecast(self, code: str) -> dict | None:
        """获取机构盈利预测（东方财富研报中的预测数据）。"""
        try:
            df = ak.stock_profit_forecast_em(symbol=code)
            if df is None or df.empty:
                return None
            forecasts = []
            for _, row in df.iterrows():
                forecasts.append({
                    "year": str(row.get("年度", "")),
                    "eps_forecast": self._safe_float(row.get("预测每股收益")),
                    "profit_forecast": self._safe_float(row.get("预测净利润")),
                    "pe_forecast": self._safe_float(row.get("预测市盈率")),
                    "broker_count": self._safe_int(row.get("预测机构数")),
                })
            return {"details": forecasts}
        except Exception:
            pass

        # Fallback: extract from research reports
        try:
            df = ak.stock_research_report_em(symbol=code)
            if df is not None and not df.empty:
                # Look for earnings forecast columns
                forecast_cols = [c for c in df.columns if "盈利预测" in c or "收益" in c]
                if forecast_cols:
                    forecasts = []
                    latest = df.iloc[0]
                    for col in forecast_cols:
                        val = self._safe_float(latest.get(col))
                        if val is not None:
                            forecasts.append({"year": col, "eps_forecast": val})
                    if forecasts:
                        return {"details": forecasts}
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # 6. 股东变动
    # ------------------------------------------------------------------
    def _collect_holders(self, code: str) -> dict | None:
        """获取十大流通股东（东方财富）。"""
        prefix = "sh" if code.startswith(("6", "9")) else "sz"
        # Try recent quarter dates
        today = date.today()
        quarter_dates = []
        year = today.year
        for q_month in [3, 6, 9, 12]:
            q_date = f"{year}{q_month:02d}{'31' if q_month in [3,12] else '30'}"
            quarter_dates.append(q_date)
        # Also previous year
        for q_month in [3, 6, 9, 12]:
            q_date = f"{year-1}{q_month:02d}{'31' if q_month in [3,12] else '30'}"
            quarter_dates.append(q_date)
        # Sort descending
        quarter_dates.sort(reverse=True)
        # Filter out future dates
        today_str = today.strftime("%Y%m%d")
        quarter_dates = [d for d in quarter_dates if d <= today_str]

        for qd in quarter_dates[:4]:
            try:
                df = ak.stock_gdfx_free_top_10_em(
                    symbol=f"{prefix}{code}", date=qd
                )
                if df is not None and not df.empty:
                    holders = []
                    for _, row in df.head(10).iterrows():
                        holders.append({
                            "name": str(row.get("股东名称", ""))[:30],
                            "pct": self._safe_float(row.get("占总流通股本持股比例")),
                            "change": str(row.get("增减", "")),
                            "holder_type": str(row.get("股东性质", "")),
                        })
                    return {
                        "report_date": f"{qd[:4]}-{qd[4:6]}-{qd[6:8]}",
                        "holders": holders,
                    }
            except Exception:
                continue
        return None

    # ------------------------------------------------------------------
    # 7. 融资融券
    # ------------------------------------------------------------------
    def _collect_margin(self, code: str) -> dict | None:
        """获取融资融券数据。"""
        # Try recent dates
        today = date.today()
        for days_back in range(0, 10):
            dt = (today - timedelta(days=days_back)).strftime("%Y%m%d")
            try:
                if code.startswith(("6", "9")):
                    df = ak.stock_margin_detail_sse(date=dt)
                else:
                    df = ak.stock_margin_detail_szse(date=dt)

                if df is None or df.empty:
                    continue

                # Find our stock
                code_col = None
                for col in df.columns:
                    if "代码" in col or "证券代码" in col:
                        code_col = col
                        break
                if code_col is None:
                    continue

                matched = df[df[code_col].astype(str).str.contains(code)]
                if matched.empty:
                    continue

                row = matched.iloc[0]
                return {
                    "date": dt,
                    "rz_balance": self._safe_float(row.get("融资余额", row.get("融资余额(元)"))),
                    "rz_buy": self._safe_float(row.get("融资买入额", row.get("融资买入额(元)"))),
                    "rq_balance": self._safe_float(row.get("融券余量", row.get("融券余量(股)"))),
                    "rq_sell": self._safe_float(row.get("融券卖出量", row.get("融券卖出量(股)"))),
                }
            except Exception:
                continue
        return None

    # ------------------------------------------------------------------
    # 8. 市场情绪
    # ------------------------------------------------------------------
    def _collect_sentiment(self, code: str) -> dict | None:
        """获取市场情绪数据（机构参与度+人气排名）。"""
        result = {}

        # 机构参与度
        try:
            df = ak.stock_comment_detail_zlkp_jgcyd_em(symbol=code)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                result["institution_participation"] = {
                    "date": str(latest.iloc[0]) if len(latest) > 0 else "",
                    "value": self._safe_float(latest.iloc[-1]) if len(latest) > 1 else None,
                }
        except Exception as e:
            logger.debug(f"Institution participation failed: {e}")

        # 综合评价历史评分
        try:
            df = ak.stock_comment_detail_zhpj_lspf_em(symbol=code)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                result["comprehensive_score"] = {
                    "date": str(latest.iloc[0]) if len(latest) > 0 else "",
                    "score": self._safe_float(latest.iloc[-1]) if len(latest) > 1 else None,
                }
        except Exception as e:
            logger.debug(f"Comprehensive score failed: {e}")

        # 人气排名
        try:
            df = ak.stock_hot_rank_detail_em(symbol=code)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                result["hot_rank"] = {
                    "date": str(latest.iloc[0]) if len(latest) > 0 else "",
                    "rank": self._safe_int(latest.iloc[-1]) if len(latest) > 1 else None,
                }
        except Exception as e:
            logger.debug(f"Hot rank failed: {e}")

        return result if result else None

    # ------------------------------------------------------------------
    # 辅助方法
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_float(val) -> float | None:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            v = float(val)
            return v if abs(v) < 1e15 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(val) -> int | None:
        if val is None:
            return None
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None

    def format_for_llm(self, data: dict) -> str:
        """将采集数据格式化为LLM可读的文本。"""
        sections = []

        # 新闻舆情
        news = data.get("news")
        if news:
            lines = ["## 近期新闻舆情"]
            for n in news[:10]:
                lines.append(f"- [{n['time']}] {n['title']}")
            sections.append("\n".join(lines))

        # 机构研报
        ratings = data.get("ratings")
        if ratings:
            lines = ["## 机构研报"]
            rd = ratings.get("rating_distribution", {})
            if rd:
                lines.append(f"评级分布: {', '.join(f'{k}:{v}' for k,v in rd.items())}")
            lines.append(f"近期研报数: {ratings.get('total_count', 0)}份")
            for r in (ratings.get("details") or [])[:5]:
                line = f"- {r.get('date','')} {r.get('broker','')} [{r.get('rating','')}]"
                if r.get("title"):
                    line += f" 《{r['title'][:40]}》"
                lines.append(line)
            sections.append("\n".join(lines))

        # 资金流向
        fund_flow = data.get("fund_flow")
        if fund_flow:
            lines = ["## 资金流向（近5日）"]
            total = fund_flow.get("main_net_5d")
            if total is not None:
                direction = "净流入" if total > 0 else "净流出"
                lines.append(f"主力5日合计{direction}: {abs(total)/1e4:.1f}万元")
            for f in (fund_flow.get("daily") or []):
                net = f.get("main_net_inflow")
                if net is not None:
                    lines.append(
                        f"- {f.get('date','')}: 主力净{'流入' if net>0 else '流出'}"
                        f"{abs(net)/1e4:.1f}万 (占比{f.get('main_net_pct','?')}%)"
                    )
            sections.append("\n".join(lines))

        # 龙虎榜
        lhb = data.get("lhb")
        if lhb:
            lines = ["## 龙虎榜（近一月）"]
            for r in lhb[:5]:
                net = r.get("net_amount")
                net_str = f"净买入{net/1e4:.1f}万" if net and net > 0 else (
                    f"净卖出{abs(net)/1e4:.1f}万" if net else "")
                times = f"(上榜{r['appearances']}次)" if r.get("appearances") else ""
                lines.append(f"- {r.get('reason','')} {net_str} {times}")
            sections.append("\n".join(lines))
        else:
            sections.append("## 龙虎榜\n近一月未上榜")

        # 盈利预测
        forecast = data.get("forecast")
        if forecast:
            lines = ["## 机构盈利预测"]
            for f in (forecast.get("details") or [])[:5]:
                line = f"- {f.get('year','')}: "
                if f.get("eps_forecast"):
                    line += f"预测EPS {f['eps_forecast']:.4f}元"
                if f.get("pe_forecast"):
                    line += f", 预测PE {f['pe_forecast']:.1f}"
                if f.get("broker_count"):
                    line += f" ({f['broker_count']}家机构)"
                lines.append(line)
            sections.append("\n".join(lines))

        # 股东变动
        holders = data.get("holders")
        if holders:
            lines = ["## 十大流通股东"]
            rd = holders.get("report_date", "")
            if rd:
                lines.append(f"报告期: {rd}")
            for h in (holders.get("holders") or [])[:10]:
                pct = f"{h['pct']:.2f}%" if h.get("pct") else ""
                change = h.get("change", "")
                lines.append(f"- {h.get('name','')}: {pct} [{change}] {h.get('holder_type','')}")
            sections.append("\n".join(lines))

        # 融资融券
        margin = data.get("margin")
        if margin:
            lines = ["## 融资融券"]
            rz = margin.get("rz_balance")
            if rz:
                lines.append(f"融资余额: {rz/1e8:.2f}亿元")
            rz_buy = margin.get("rz_buy")
            if rz_buy:
                lines.append(f"融资买入额: {rz_buy/1e4:.1f}万元")
            rq = margin.get("rq_balance")
            if rq:
                lines.append(f"融券余量: {rq:.0f}股")
            lines.append(f"数据日期: {margin.get('date', '')}")
            sections.append("\n".join(lines))

        # 市场情绪
        sentiment = data.get("sentiment")
        if sentiment:
            lines = ["## 市场情绪"]
            ip = sentiment.get("institution_participation", {})
            if ip.get("value"):
                lines.append(f"机构参与度: {ip['value']}")
            cs = sentiment.get("comprehensive_score", {})
            if cs.get("score"):
                lines.append(f"综合评分: {cs['score']}")
            hr = sentiment.get("hot_rank", {})
            if hr.get("rank"):
                lines.append(f"人气排名: 第{hr['rank']}名")
            if len(lines) > 1:
                sections.append("\n".join(lines))

        return "\n\n".join(sections) if sections else "暂无额外市场数据"
